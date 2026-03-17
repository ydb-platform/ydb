import argparse
import sys
from pathlib import Path

import argclass
import __res


class GreetCommand(argclass.Parser):
    user: str = argclass.Argument("user", help="User to greet")

    def __call__(self) -> int:
        print(f"Hello, {self.user}!")
        return 0


class Parser(argclass.Parser):
    verbose: bool = False
    secret_key: str = argclass.Secret(help="Secret API key")
    greet = GreetCommand()

    def __call__(self) -> int:
        self.print_help()
        return 0


def main() -> None:
    parser = Parser(
        prog=f"{Path(sys.executable).name} -m argclass",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            "This code produces this help:\n\n```"
            f"python\n{__res.find(b'resfs/file/py/argclass/__main__.py').decode('utf-8').strip()}\n```"
        ),
    )
    parser.parse_args()
    parser.sanitize_env()
    exit(parser())


if __name__ == "__main__":
    main()
