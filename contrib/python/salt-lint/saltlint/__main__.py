# -*- coding: utf-8 -*-
# Copyright (c) 2013-2014 Will Thames <will@thames.id.au>
# Modified work Copyright (c) 2020 Warpnet B.V.

import sys
import errno

from saltlint.cli import run


if __name__ == "__main__":
    try:
        sys.exit(run())
    except IOError as exc:
        if exc.errno != errno.EPIPE:
            raise
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc
