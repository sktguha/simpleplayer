const { spawn } = require("child_process");
const http = require("http");
const https = require("https");
const fs = require("fs");
const path = require("path");

const args = Object.fromEntries(
  process.argv.slice(2).map(a => {
    const i = a.indexOf("=");
    return [a.slice(2, i), a.slice(i + 1)];
  })
);

const BASE = args.base || "http://100.121.250.119:8080/stream";
const SEGMENT_MS = Number(args.segmentMs || 2000);
const BACKFILL_THRESHOLD_MS = Number(args.backfillThreshold || 200);
const BACKFILL_GRACE = Number(args.backfillGrace || 2); // new option

// No minimum size check – trust aria2c success
const STREAM_DIR = args.streamDir || path.join(process.cwd(), "stream");
fs.rmSync(STREAM_DIR, { recursive: true, force: true });
fs.mkdirSync(STREAM_DIR, { recursive: true });

const ariaArgs =
  (args.ariaExtraArgs || "").match(/"[^"]*"|'[^']*'|\S+/g) || [];

let latest = -1;
let lastSeen = 0;
let segmentMs = SEGMENT_MS;
let lastTargetAttempted = -1; // highest id we've ever tried to download

const downloaded = new Set(); // IDs of segments successfully downloaded
const dead = new Set(); // IDs permanently dead (failed or grace expired)
const pending = new Set(); // IDs skipped but still recoverable (within grace)

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function fetchLatest() {
  return new Promise(resolve => {
    const transport = BASE.startsWith("https") ? https : http;
    transport
      .get(`${BASE}/latest.txt`, res => {
        let s = "";
        res.on("data", d => (s += d));
        res.on("end", () => resolve(parseInt(s.trim(), 10)));
      })
      .on("error", () => resolve(NaN));
  });
}

function download(id) {
  return new Promise(resolve => {
    if (downloaded.has(id)) {
      resolve({ elapsed: 0, ok: true });
      return;
    }

    const file = `segment_${String(id).padStart(6, "0")}.mp4`;
    const filePath = path.join(STREAM_DIR, file);

    const start = Date.now();

    const p = spawn(
      "aria2c",
      [
        "-d",
        STREAM_DIR,
        "-o",
        file,
        ...ariaArgs,
        `${BASE}/${file}`
      ],
      { stdio: "ignore" }
    );

    p.on("close", code => {
      const elapsed = Date.now() - start;

      if (code === 0) {
        downloaded.add(id);
        console.log(`${file} OK ${elapsed}ms`);
        resolve({ elapsed, ok: true });
      } else {
        // Remove incomplete file
        try { fs.unlinkSync(filePath); } catch { }
        console.log(`${file} FAIL (exit ${code}) ${elapsed}ms`);
        resolve({ elapsed, ok: false });
      }
    });
  });
}

// ============================================================
// HTTP SERVER – serves /state and /segment/... for the player
// ============================================================
const PORT = 7500;

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const pathname = url.pathname;

  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") {
    res.writeHead(204);
    res.end();
    return;
  }

  if (req.method === "GET" && pathname === "/state") {
    const ids = new Set([...downloaded, ...dead]);
    const segments = Array.from(ids)
      .sort((a, b) => a - b)
      .map(id => ({
        id: id,
        status: downloaded.has(id) ? "ready" : "dead"
      }));
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ segments }));
    return;
  }

  if (req.method === "GET" && pathname.startsWith("/segment/")) {
    const fileName = path.basename(pathname);
    const filePath = path.join(STREAM_DIR, fileName);
    fs.stat(filePath, (err, stat) => {
      if (err || !stat.isFile()) {
        res.writeHead(404, { "Content-Type": "text/plain" });
        res.end("Not Found");
        return;
      }
      res.writeHead(200, {
        "Content-Type": "video/mp4",
        "Content-Length": stat.size,
        "Cache-Control": "no-store"
      });
      fs.createReadStream(filePath).pipe(res);
    });
    return;
  }

  res.writeHead(404, { "Content-Type": "text/plain" });
  res.end("Not Found");
});

server.listen(PORT, () => {
  console.log(`HTTP server listening on port ${PORT}, serving from ${STREAM_DIR}`);
});

// ============================================================
// MAIN DOWNLOAD LOOP (with grace period for skipped segments)
// ============================================================
(async () => {
  while (true) {
    const n = await fetchLatest();

    if (Number.isNaN(n)) {
      await sleep(100);
      continue;
    }

    const now = Date.now();

    if (latest >= 0 && n > latest) {
      const observed = (now - lastSeen) / (n - latest);
      segmentMs = segmentMs * 0.8 + observed * 0.2;
    }

    latest = n;
    lastSeen = now;

    // Move pending segments to dead once they are older than latest - grace
    const expired = [];
    for (const id of pending) {
      if (latest >= id + BACKFILL_GRACE) {
        expired.push(id);
      }
    }
    for (const id of expired) {
      pending.delete(id);
      dead.add(id);
      console.log(`segment_${String(id).padStart(6, "0")}.mp4 DEAD (grace expired)`);
    }

    const target = Math.max(0, latest - 1);

    // Mark skipped gaps as pending (recoverable) instead of dead immediately
    if (target > lastTargetAttempted + 1) {
      for (let id = lastTargetAttempted + 1; id < target; id++) {
        if (!downloaded.has(id)) {
          pending.add(id);
          console.log(`segment_${String(id).padStart(6, "0")}.mp4 SKIPPED (recoverable)`);
        }
      }
    }

    const result = await download(target);
    lastTargetAttempted = Math.max(lastTargetAttempted, target);

    if (result.ok) {
      // Successfully downloaded target – remove from any recovery sets
      pending.delete(target);
      dead.delete(target);
    } else {
      // Target failed – move to dead
      pending.delete(target);
      dead.add(target);
    }

    let wait = Math.max(0, Math.round(segmentMs - result.elapsed));

    // Use spare time to recover one recent segment (from pending or dead)
    if (wait > BACKFILL_THRESHOLD_MS) {
      let retryId = -1;
      // Check the two most recent slots before target
      for (const id of [target - 1, target - 2]) {
        if (id >= 0 && !downloaded.has(id)) {
          if (pending.has(id) || dead.has(id)) {
            retryId = id;
            break;
          }
        }
      }

      if (retryId >= 0) {
        console.log(
          `Backfill segment_${String(retryId).padStart(6, "0")}.mp4`
        );

        // Remove from pending/dead before attempt (will be re-added if fails)
        pending.delete(retryId);
        dead.delete(retryId);

        const r = await download(retryId);

        if (r.ok) {
          // success – already added to downloaded inside download()
        } else {
          // failure – move to dead
          dead.add(retryId);
        }

        wait = Math.max(0, wait - r.elapsed);
      }
    }

    console.log(`sleep ${wait}ms`); await sleep(wait);
  }
})();