#!/usr/bin/env python3
"""Deprecated autopilot entrypoint.

Use ``dutyctl.py`` + AGENTS.md instead:

  python3 dutyctl.py init-token
  python3 dutyctl.py prepare -c CONTEXT.json -o RUN_DIR
  python3 dutyctl.py bisect -c CONTEXT.json -o RUN_DIR
  # write analysis.md + problems.json
  python3 dutyctl.py validate -o RUN_DIR
  python3 dutyctl.py write-result -c CONTEXT.json -o RUN_DIR

See AGENTS.md.
"""

from __future__ import annotations

import sys


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] in ("init-token", "init-tokens"):
        # Convenience: forward token bootstrap to dutyctl
        from dutyctl import main as duty_main

        return duty_main(argv)
    print(
        "run.py autopilot is removed.\n"
        "Use: python3 dutyctl.py <subcommand> …\n"
        "See AGENTS.md.",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
