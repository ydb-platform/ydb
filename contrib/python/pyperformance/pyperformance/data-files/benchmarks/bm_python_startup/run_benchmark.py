"""
Benchmark Python startup.
"""
import sys

import pyperf


def add_cmdline_args(cmd, args):
    if args.no_site:
        cmd.append("--no-site")
    if args.exit:
        cmd.append("--exit")


if __name__ == "__main__":
    runner = pyperf.Runner(values=10, add_cmdline_args=add_cmdline_args)
    runner.argparser.add_argument("--no-site", action="store_true")
    runner.argparser.add_argument("--exit", action="store_true")

    runner.metadata['description'] = "Performance of the Python startup"
    args = runner.parse_args()
    name = 'python_startup'
    if args.no_site:
        name += "_no_site"
    if args.exit:
        name += "_exit"

    command = [sys.executable]
    if args.no_site:
        command.append("-S")
    if args.exit:
        command.extend(("-c", "import os; os._exit(0)"))
    else:
        command.extend(("-c", "pass"))

    runner.bench_command(name, command)
