import argparse

from opensfm import commands
from opensfm import log


def main():
    log.setup()

    #  Create the top-level parser
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(
        help='Command to run', dest='command', metavar='command')

    #  Create one subparser for each subcommand
    subcommands = [module.Command() for module in commands.opensfm_commands]

    for command in subcommands:
        subparser = subparsers.add_parser(
            command.name, help=command.help)
        command.add_arguments(subparser)

    #  Parse arguments
    args = parser.parse_args()

    #  Run the selected subcommand
    for command in subcommands:
        if args.command == command.name:
            command.run(args)
        log.setup()


if __name__ == '__main__':
    main()
