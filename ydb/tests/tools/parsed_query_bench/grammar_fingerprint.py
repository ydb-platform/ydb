import hashlib
import sys
from pathlib import Path

digest = hashlib.sha256(b"parsed-query-parser-v1\0")
for path in sys.argv[2:]:
    data = Path(path).read_bytes()
    digest.update(len(data).to_bytes(8, "big"))
    digest.update(data)

escaped = "".join(f"\\x{byte:02x}" for byte in digest.digest())
Path(sys.argv[1]).write_text(
    "#pragma once\nnamespace NKikimr::NKqp::NParsedQueryBench {\n"
    f'inline constexpr char GrammarFingerprint[] = "{escaped}";\n'
    "}\n"
)
