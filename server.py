"""Simple HTTP server with Range request support for PMTiles."""

import http.server
from pathlib import Path


class RangeHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def send_head(self):
        path = Path(self.translate_path(self.path))
        if path.is_dir():
            return super().send_head()

        if not path.exists():
            self.send_error(404, "File not found")
            return None

        file_size = path.stat().st_size

        # Handle Range requests
        range_header = self.headers.get("Range")
        if range_header:
            try:
                range_spec = range_header.replace("bytes=", "")
                start, end = range_spec.split("-")
                start = int(start) if start else 0
                end = int(end) if end else file_size - 1
                end = min(end, file_size - 1)
                length = end - start + 1

                self.send_response(206)
                self.send_header("Content-type", self.guess_type(path))
                self.send_header("Content-Length", length)
                self.send_header(
                    "Content-Range", f"bytes {start}-{end}/{file_size}"
                )
                self.send_header("Accept-Ranges", "bytes")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()

                f = path.open("rb")
                f.seek(start)
                return _RangeFile(f, length)
            except (ValueError, IndexError):
                pass

        # Normal request
        self.send_response(200)
        self.send_header("Content-type", self.guess_type(path))
        self.send_header("Content-Length", file_size)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        return path.open("rb")


class _RangeFile:
    def __init__(self, f, length):
        self.f = f
        self.remaining = length

    def read(self, size=-1):
        if size < 0:
            size = self.remaining
        size = min(size, self.remaining)
        self.remaining -= size
        return self.f.read(size)

    def close(self):
        self.f.close()


if __name__ == "__main__":
    http.server.HTTPServer(("", 8080), RangeHTTPRequestHandler).serve_forever()
