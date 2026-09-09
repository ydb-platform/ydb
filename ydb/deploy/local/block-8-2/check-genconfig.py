#!/usr/bin/env python3
"""Exercise the actual CLI generator, including its minimum-domain rejection."""
from pathlib import Path
import re
import subprocess
import tempfile


with tempfile.TemporaryDirectory(prefix="block82-genconfig-") as directory:
    for nodes in (12, 11):
        source = Path(directory) / f"drives-{nodes}.txt"
        source.write_text("\n".join(
            'Drive { Type: "SSD" Path: "/data/pdisk.dat" '
            f'Guid: {n} PDiskId: 1 NodeId: {n} DataCenterId: 1 RoomId: 1 RackId: {n} BodyId: {n} }}'
            for n in range(1, nodes + 1)
        ))
        result = subprocess.run(
            ["/opt/ydb/bin/ydbd", "admin", "blobstorage", "genconfig", "static",
             "--erasure", "block-8-2", "--pdisktype", "SSD",
             "--bs-format-file", str(source)],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30,
        )
        if nodes == 12:
            assert result.returncode == 0, result.stderr
            assert "ErasureSpecies: 19" in result.stdout, result.stdout
            assert len(re.findall(r"\bVDisks \{", result.stdout)) == 12, result.stdout
            assert len(re.findall(r"\bFailDomains \{", result.stdout)) == 12, result.stdout
        else:
            assert result.returncode != 0, result.stdout
            assert "Can't allocate group:" in result.stderr, result.stderr
print("genconfig static: 12 domains/species 19 passed; 11 domains rejected.")
