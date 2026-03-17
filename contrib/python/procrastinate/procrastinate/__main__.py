from __future__ import annotations

from procrastinate import cli


def main(name: str):
    # The curse of 100% coverage
    if name == "__main__":
        cli.main()


main(__name__)
