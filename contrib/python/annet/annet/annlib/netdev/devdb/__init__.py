import functools
import json
import re
from pathlib import Path
from typing import Any

from annet.annlib.netdev.db import find_true_sequences, get_db
from annet.lib import get_context


@functools.lru_cache(None)
def parse_hw_model(hw_model):
    prepared = _prepare_db()
    (tree, all_sequences) = get_db(prepared)
    true_sequences = find_true_sequences(hw_model, tree)
    return (
        sorted(true_sequences),
        all_sequences.difference(true_sequences),
    )


def _prepare_db() -> dict[tuple[str, ...], Any]:
    try:
        from library.python import resource

        raw = json.loads(
            resource.resfs_read("contrib/python/annet/annet/annlib/netdev/devdb/data/devdb.json").decode("utf-8")
        )
    except ImportError:
        devdb_file = Path(get_context().get("devdb", {}).get("path", Path(__file__).parent / "data" / "devdb.json"))
        with devdb_file.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    return {tuple(seq.split(".")): re.compile(regexp) for (seq, regexp) in raw.items()}
