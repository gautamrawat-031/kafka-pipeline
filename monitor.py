import os
import json
import time
import threading
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from collections import deque
from datetime import datetime, timezone
from confluent_kafka import Consumer as KafkaConsumer, TopicPartition
from confluent_kafka.admin import AdminClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [MONITOR] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC             = os.getenv("TOPIC", "orders")
DLQ_TOPIC         = os.getenv("DLQ_TOPIC", f"{TOPIC}.dlq")
GROUP_ID          = os.getenv("GROUP_ID", "orders-consumer-group")
PORT              = int(os.getenv("PORT", "8080"))
POLL_INTERVAL     = 5

_metrics = {
    "lag_total":          0,
    "lag_per_partition":  {},
    "dlq_messages":       0,
    "throughput_history": deque(maxlen=60),
    "last_updated":       None,
    "status":             "STARTING",
}
_lock = threading.Lock()


def _collect_metrics():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    wm_consumer = KafkaConsumer({
        "bootstrap.servers":  BOOTSTRAP_SERVERS,
        "group.id":           GROUP_ID,
        "enable.auto.commit": False,
    })

    while True:
        try:
            meta         = admin.list_topics(topic=TOPIC, timeout=5)
            partition_ids = list(meta.topics[TOPIC].partitions.keys())
            total_lag    = 0
            lag_map      = {}

            for pid in partition_ids:
                tp             = TopicPartition(TOPIC, pid)
                committed_list = wm_consumer.committed([tp], timeout=5)
                committed      = committed_list[0].offset if committed_list and committed_list[0].offset >= 0 else 0
                lo, hi         = wm_consumer.get_watermark_offsets(tp, timeout=5)
                lag            = max(0, hi - committed)
                total_lag     += lag
                lag_map[pid]   = lag

            dlq_depth = 0
            try:
                dlq_meta = admin.list_topics(topic=DLQ_TOPIC, timeout=5)
                for pid in dlq_meta.topics[DLQ_TOPIC].partitions:
                    lo, hi     = wm_consumer.get_watermark_offsets(
                        TopicPartition(DLQ_TOPIC, pid), timeout=5)
                    dlq_depth += hi
            except Exception:
                pass

            if total_lag < 500 and dlq_depth == 0:
                status = "HEALTHY"
            elif total_lag < 2000 or dlq_depth < 50:
                status = "DEGRADED"
            else:
                status = "CRITICAL"

            with _lock:
                _metrics["lag_total"]         = total_lag
                _metrics["lag_per_partition"] = lag_map
                _metrics["dlq_messages"]      = dlq_depth
                _metrics["status"]            = status
                _metrics["last_updated"]      = datetime.now(timezone.utc).isoformat()
                _metrics["throughput_history"].append({
                    "ts": _metrics["last_updated"], "lag": total_lag})

            log.info("lag=%d dlq=%d status=%s", total_lag, dlq_depth, status)

        except Exception as exc:
            log.error("Metrics collection error: %s", exc)

        time.sleep(POLL_INTERVAL)


_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="5">
<title>Kafka Monitor</title>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
        background:#0f1117;color:#e2e8f0;padding:28px}}
  h1{{font-size:20px;font-weight:500;margin-bottom:22px}}
  .grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:26px}}
  .card{{background:#1e2130;border:1px solid #2d3148;border-radius:10px;padding:18px}}
  .label{{font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-bottom:5px}}
  .value{{font-size:30px;font-weight:500}}
  .green{{color:#34d399}}.amber{{color:#fbbf24}}.red{{color:#f87171}}.blue{{color:#60a5fa}}
  table{{width:100%;border-collapse:collapse;background:#1e2130;border:1px solid #2d3148;border-radius:10px;overflow:hidden}}
  th,td{{padding:11px 15px;text-align:left;font-size:13px;border-bottom:1px solid #2d3148}}
  th{{background:#161824;color:#64748b;font-weight:500;font-size:11px;text-transform:uppercase}}
  tr:last-child td{{border-bottom:none}}
  .badge{{display:inline-block;padding:2px 9px;border-radius:4px;font-size:11px;font-weight:500}}
  .ok{{background:#064e3b;color:#34d399}}
  .warn{{background:#78350f;color:#fbbf24}}
  .crit{{background:#7f1d1d;color:#f87171}}
  .footer{{font-size:11px;color:#475569;margin-top:18px}}
</style>
</head>
<body>
<h1>Kafka Pipeline Monitor</h1>
<div class="grid">
  <div class="card"><div class="label">Consumer Lag (total)</div><div class="value {lag_cls}">{lag}</div></div>
  <div class="card"><div class="label">DLQ Depth</div><div class="value {dlq_cls}">{dlq}</div></div>
  <div class="card"><div class="label">Active Partitions</div><div class="value blue">{parts}</div></div>
  <div class="card"><div class="label">Pipeline Status</div><div class="value {st_cls}">{status}</div></div>
</div>
<table>
  <thead><tr><th>Partition</th><th>Lag</th><th>Health</th></tr></thead>
  <tbody>{rows}</tbody>
</table>
<p class="footer">Auto-refreshes every 5s &nbsp;|&nbsp; Last updated: {updated} &nbsp;|&nbsp;
  <a href="/metrics" style="color:#60a5fa">JSON metrics</a></p>
</body></html>
"""


def _build_html() -> bytes:
    with _lock:
        m       = dict(_metrics)
        lag_map = dict(m["lag_per_partition"])

    lag    = m["lag_total"]
    dlq    = m["dlq_messages"]
    status = m["status"]

    lag_cls = "green" if lag < 100  else ("amber" if lag < 1000  else "red")
    dlq_cls = "green" if dlq == 0   else ("amber" if dlq  < 50   else "red")
    st_cls  = "green" if status == "HEALTHY" else ("amber" if status == "DEGRADED" else "red")

    rows = ""
    for pid, p_lag in sorted(lag_map.items()):
        cls = "ok" if p_lag < 100 else ("warn" if p_lag < 500 else "crit")
        lbl = "OK"  if p_lag < 100 else ("WARNING" if p_lag < 500 else "CRITICAL")
        rows += f"<tr><td>Partition {pid}</td><td>{p_lag}</td><td><span class='badge {cls}'>{lbl}</span></td></tr>"

    if not rows:
        rows = "<tr><td colspan='3' style='color:#475569;text-align:center'>Waiting for data...</td></tr>"

    return _HTML.format(
        lag=lag, lag_cls=lag_cls,
        dlq=dlq, dlq_cls=dlq_cls,
        parts=len(lag_map),
        status=status, st_cls=st_cls,
        rows=rows,
        updated=m["last_updated"] or "—",
    ).encode("utf-8")


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        if self.path == "/metrics":
            with _lock:
                body = json.dumps(
                    {k: (list(v) if hasattr(v, "__iter__") and not isinstance(v, (str, dict)) else v)
                     for k, v in _metrics.items()},
                    default=str
                ).encode("utf-8")
            self._respond(body, "application/json")
        elif self.path == "/health":
            with _lock:
                st = _metrics["status"]
            code = 200 if st in ("HEALTHY", "DEGRADED") else 503
            self.send_response(code)
            self.end_headers()
            self.wfile.write(st.encode())
        else:
            self._respond(_build_html(), "text/html")

    def _respond(self, body: bytes, content_type: str, code: int = 200):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def run():
    t = threading.Thread(target=_collect_metrics, daemon=True, name="metrics-collector")
    t.start()
    server = HTTPServer(("0.0.0.0", PORT), _Handler)
    log.info("Dashboard → http://localhost:%d", PORT)
    log.info("Metrics   → http://localhost:%d/metrics", PORT)
    log.info("Health    → http://localhost:%d/health", PORT)
    server.serve_forever()


if __name__ == "__main__":
    run()