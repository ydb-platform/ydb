import codecs
import importlib.resources
import re
from typing import Optional, Union

SYMBOLS_FILE: str = importlib.resources.files(__package__) / "unimathsymbols.txt"
SYMBOLS: Optional[dict[str, str]] = None


def convert_symbol(symbol: str) -> Union[str, None]:
    global SYMBOLS
    if not SYMBOLS:
        SYMBOLS = parse_symbols()
    return SYMBOLS.get(symbol, None)


def parse_symbols() -> dict[str, str]:
    _symbols: dict[str, str] = {}
    with SYMBOLS_FILE.open(encoding="utf-8") as f:
        for line in f:
            if line.startswith("#"):
                continue
            columns = line.strip().split("^")
            _unicode = columns[0]
            latex = columns[2]
            unicode_math = columns[3]
            if latex and latex not in _symbols:
                _symbols[latex] = _unicode
            if unicode_math and unicode_math not in _symbols:
                _symbols[unicode_math] = _unicode
            for equivalent in re.findall(r"[=#]\s*(\\[^,^ ]+),?", columns[-1]):
                if equivalent not in _symbols:
                    _symbols[equivalent] = _unicode
    _symbols.update(
        {
            r"\And": _symbols[r"\ampersand"],
            r"\bigcirc": _symbols[r"\lgwhtcircle"],
            r"\Box": _symbols[r"\square"],
            r"\circledS": "024C8",
            r"\diagdown": "02572",
            r"\diagup": "02571",
            r"\dots": "02026",
            r"\dotsb": _symbols[r"\cdots"],
            r"\dotsc": "02026",
            r"\dotsi": _symbols[r"\cdots"],
            r"\dotsm": _symbols[r"\cdots"],
            r"\dotso": "02026",
            r"\emptyset": "02205",
            r"\gggtr": "022D9",
            r"\gvertneqq": "02269",
            r"\gt": _symbols[r"\greater"],
            r"\ldotp": _symbols[r"\period"],
            r"\llless": _symbols[r"\lll"],
            r"\lt": _symbols[r"\less"],
            r"\lvert": _symbols[r"\vert"],
            r"\lVert": _symbols[r"\Vert"],
            r"\lvertneqq": _symbols[r"\lneqq"],
            r"\ngeqq": _symbols[r"\ngeq"],
            r"\nshortmid": _symbols[r"\nmid"],
            r"\nshortparallel": _symbols[r"\nparallel"],
            r"\nsubseteqq": _symbols[r"\nsubseteq"],
            r"\omicron": _symbols[r"\upomicron"],
            r"\rvert": _symbols[r"\vert"],
            r"\rVert": _symbols[r"\Vert"],
            r"\shortmid": _symbols[r"\mid"],
            r"\smallfrown": _symbols[r"\frown"],
            r"\smallint": "0222B",
            r"\smallsmile": _symbols[r"\smile"],
            r"\surd": _symbols[r"\sqrt"],
            r"\thicksim": "0223C",
            r"\thickapprox": _symbols[r"\approx"],
            r"\varsubsetneqq": _symbols[r"\subsetneqq"],
            r"\varsupsetneq": "0228B",
            r"\varsupsetneqq": _symbols[r"\supsetneqq"],
        }
    )
    del _symbols[r"\mathring"]  # FIXME: improve tokenizer without removing this
    return _symbols
