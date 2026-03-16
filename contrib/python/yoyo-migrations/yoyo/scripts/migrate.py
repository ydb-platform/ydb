# Copyright 2015 Oliver Cope
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import sys
import re
import warnings

import tabulate

from yoyo import (
    read_migrations,
    default_migration_table,
    ancestors,
    descendants,
)
from yoyo.migrations import topological_sort
from yoyo.scripts.main import InvalidArgument, get_backend
from yoyo import utils


def install_argparsers(global_parser, subparsers):
    # Standard options
    standard_options_parser = argparse.ArgumentParser(add_help=False)
    standard_options_parser.add_argument(
        "sources", nargs="*", help="Source directory of migration scripts"
    )

    standard_options_parser.add_argument(
        "-d",
        "--database",
        default=None,
        help="Database, eg 'sqlite:///path/to/sqlite.db' "
        "or 'postgresql://user@host/db'",
    )
    standard_options_parser.add_argument(
        "-p",
        "--prompt-password",
        dest="prompt_password",
        action="store_true",
        help="Prompt for the database password",
    )
    standard_options_parser.add_argument(
        "--migration-table",
        dest="migration_table",
        action="store",
        default=default_migration_table,
        help="Name of table to use for storing " "migration metadata",
    )

    # Options related to filtering the list of migrations
    filter_parser = argparse.ArgumentParser(add_help=False)
    filter_parser.add_argument(
        "-m",
        "--match",
        help="Select migrations matching PATTERN (regular expression)",
        metavar="PATTERN",
    )
    filter_parser.add_argument(
        "-r",
        "--revision",
        help=("Apply/rollback migration with id REVISION and all its dependencies"),
        metavar="REVISION",
    )

    # Options related to applying/rolling back migrations
    apply_parser = argparse.ArgumentParser(add_help=False, parents=[filter_parser])
    apply_parser.add_argument(
        "-a",
        "--all",
        dest="all",
        action="store_true",
        help="Select all migrations, regardless of whether "
        "they have been previously applied",
    )
    apply_parser.add_argument(
        "-f",
        "--force",
        dest="force",
        action="store_true",
        help="Force apply/rollback of steps even if previous steps have failed",
    )

    parser_apply = subparsers.add_parser(
        "apply",
        help="Apply migrations",
        parents=[global_parser, standard_options_parser, apply_parser],
    )
    parser_apply.set_defaults(func=apply, command_name="apply")
    parser_apply.add_argument(
        "-1",
        "--one",
        help="Apply a single migration. "
        "If there are no unapplied migrations, reapply the last migration",
    )

    parser_develop = subparsers.add_parser(
        "develop",
        help="Apply migrations without prompting. "
        "If there are no unapplied migrations, reapply the last migration",
        parents=[global_parser, standard_options_parser, apply_parser],
    )
    parser_develop.set_defaults(func=develop, command_name="develop")
    parser_develop.add_argument(
        "-n",
        type=int,
        help="Act on the last N migrations",
        default=1,
    )

    parser_list = subparsers.add_parser(
        "list",
        help="List all available and applied migrations. "
        "Each listed migration is prefixed with either A (applied) or U "
        "(unapplied)",
        parents=[global_parser, standard_options_parser, filter_parser],
    )
    parser_list.set_defaults(func=list_migrations, command_name="list")

    parser_rollback = subparsers.add_parser(
        "rollback",
        parents=[global_parser, standard_options_parser, apply_parser],
        help="Rollback migrations",
    )
    parser_rollback.set_defaults(func=rollback, command_name="rollback")

    parser_reapply = subparsers.add_parser(
        "reapply",
        parents=[global_parser, standard_options_parser, apply_parser],
        help="Rollback then reapply migrations",
    )
    parser_reapply.set_defaults(func=reapply, command_name="reapply")

    parser_mark = subparsers.add_parser(
        "mark",
        parents=[global_parser, standard_options_parser, apply_parser],
        help="Mark migrations as applied, without running them",
    )
    parser_mark.set_defaults(func=mark, command_name="mark")

    parser_unmark = subparsers.add_parser(
        "unmark",
        parents=[global_parser, standard_options_parser, apply_parser],
        help="Unmark applied migrations, without rolling them back",
    )
    parser_unmark.set_defaults(func=unmark, command_name="unmark")

    parser_break_lock = subparsers.add_parser(
        "break-lock",
        parents=[global_parser, standard_options_parser],
        help="Break migration locks",
    )
    parser_break_lock.set_defaults(func=break_lock, command_name="break-lock")


def filter_migrations(migrations, pattern):
    if not pattern:
        return migrations

    search = re.compile(pattern).search
    return migrations.filter(lambda m: search(m.id) is not None)


def migrations_to_revision(migrations, revision, direction):
    if not revision:
        return migrations
    assert direction in {"apply", "rollback"}

    targets = [m for m in migrations if revision in m.id]
    if len(targets) == 0:
        raise InvalidArgument("'{}' doesn't match any revisions.".format(revision))
    if len(targets) > 1:
        raise InvalidArgument(
            "'{}' matches multiple revisions. "
            "Please specify one of {}.".format(
                revision, ", ".join(m.id for m in targets)
            )
        )

    target = targets[0]

    # apply: apply target and all its dependencies
    if direction == "apply":
        deps = ancestors(target, migrations)
        target_plus_deps = deps | {target}
        migrations = migrations.filter(lambda m: m in target_plus_deps)

    # rollback/reapply: rollback target and everything that depends on it
    else:
        deps = descendants(target, migrations)
        target_plus_deps = deps | {target}
        migrations = migrations.filter(lambda m: m in target_plus_deps)

    return migrations


def get_migrations(args, backend, direction=None):
    sources = args.sources
    dburi = args.database

    if not sources:
        raise InvalidArgument("Please specify the migration source directory")

    if not direction:
        direction = "apply" if args.func in {mark, apply} else "rollback"
    migrations = read_migrations(*sources)
    migrations = filter_migrations(migrations, args.match)
    migrations = migrations_to_revision(migrations, args.revision, direction)

    if direction == "apply":
        migrations = backend.to_apply(migrations)
    else:
        migrations = backend.to_rollback(migrations)

    if not args.batch_mode and not args.revision:
        migrations = prompt_migrations(backend, migrations, args.command_name)

    if args.batch_mode and not args.revision and not args.all and args.func is rollback:
        if len(migrations) > 1:
            warnings.warn(
                "Only rolling back a single migration."
                "To roll back multiple migrations, "
                "either use interactive mode or use "
                "--revision or --all"
            )
            migrations = migrations[:1]

    if not args.batch_mode and migrations:
        print("")
        print(
            "Selected",
            utils.plural(len(migrations), "%d migration:", "%d migrations:"),
        )
        for m in migrations:
            print("  [{m.id}]".format(m=m))
        prompt = "{} {} to {}".format(
            args.command_name.title(),
            utils.plural(len(migrations), "this migration", "these %d migrations"),
            dburi,
        )
        if not utils.confirm(prompt, default="y"):
            return migrations.replace([])
    return migrations


def apply(args, config) -> int:
    backend = get_backend(args, config)
    with backend.lock():
        migrations = get_migrations(args, backend)
        backend.apply_migrations(migrations, args.force)
    return 0


def reapply(args, config) -> int:
    backend = get_backend(args, config)
    with backend.lock():
        migrations = get_migrations(args, backend)
        backend.rollback_migrations(migrations, args.force)
        migrations = backend.to_apply(migrations)
        backend.apply_migrations(migrations, args.force)
    return 0


def rollback(args, config) -> int:
    backend = get_backend(args, config)
    with backend.lock():
        migrations = get_migrations(args, backend)
        backend.rollback_migrations(migrations, args.force)
    return 0


def develop(args, config) -> int:
    args.batch_mode = True
    backend = get_backend(args, config)
    with backend.lock():
        migrations = get_migrations(args, backend, "apply")
        if migrations:
            print(
                "Applying",
                utils.plural(len(migrations), "%d migration:", "%d migrations:"),
            )
            for m in migrations:
                print(f"  [{m.id}]")
            backend.apply_migrations(migrations, args.force)
        else:
            migrations = get_migrations(args, backend, "rollback")[: args.n]
            print(
                "Reapplying",
                utils.plural(len(migrations), "%d migration:", "%d migrations:"),
            )
            for m in migrations:
                print(f"  [{m.id}]")
            backend.rollback_migrations(migrations, args.force)
            migrations = get_migrations(args, backend, "apply")
            backend.apply_migrations(migrations, args.force)
    return 0


def mark(args, config) -> int:
    backend = get_backend(args, config)
    with backend.lock():
        migrations = get_migrations(args, backend)
        backend.mark_migrations(migrations)
    return 0


def unmark(args, config) -> int:
    backend = get_backend(args, config)
    with backend.lock():
        migrations = get_migrations(args, backend)
        backend.unmark_migrations(migrations)
    return 0


def break_lock(args, config) -> int:
    backend = get_backend(args, config)
    backend.break_lock()
    return 0


def list_migrations(args, config) -> int:
    backend = get_backend(args, config)
    migrations = read_migrations(*args.sources)
    migrations = filter_migrations(migrations, args.match)

    APPLIED, UNAPPLIED = "A", "U"
    if sys.stdout.isatty():
        APPLIED = "\033[92m{APPLIED}\033[0m".format(APPLIED=APPLIED)
        UNAPPLIED = "\033[91m{UNAPPLIED}\033[0m".format(UNAPPLIED=UNAPPLIED)

    with backend.lock():
        migrations = migrations.__class__(topological_sort(migrations))
        applied = backend.to_rollback(migrations)

        data = (
            (APPLIED if m in applied else UNAPPLIED, m.id, m.source_dir)
            for m in migrations
        )
        print(tabulate.tabulate(data, headers=("STATUS", "ID", "SOURCE")))
    return 0


def prompt_migrations(backend, migrations, direction):
    """
    Iterate through the list of migrations and prompt the user to
    apply/rollback each. Return a list of user selected migrations.

    direction
        one of 'apply' or 'rollback'
    """

    class prompted_migration(object):
        def __init__(self, migration, default=None):
            super(prompted_migration, self).__init__()
            self.migration = migration
            self.choice = default

    to_prompt = [prompted_migration(m) for m in migrations]

    position = 0
    while position < len(to_prompt):
        mig = to_prompt[position]

        choice = mig.choice
        if choice is None:
            is_applied = backend.is_applied(mig.migration)
            if direction == "apply":
                choice = "n" if is_applied else "y"
            else:
                choice = "y" if is_applied else "n"
        options = "".join(o.upper() if o == choice else o.lower() for o in "ynvdaqjk?")

        print("")
        print("[%s]" % (mig.migration.id,))
        response = utils.prompt("Shall I %s this migration?" % (direction,), options)

        if response == "?":
            print("")
            print("y: %s this migration" % (direction,))
            print("n: don't %s it" % (direction,))
            print("")
            print("v: view this migration in full")
            print("")
            print(
                "d: %s the selected migrations, skipping any remaining" % (direction,)
            )
            print("a: %s all the remaining migrations" % (direction,))
            print("q: cancel without making any changes")
            print("")
            print("j: skip to next migration")
            print("k: back up to previous migration")
            print("")
            print("?: show this help")
            continue

        if response in "yn":
            mig.choice = response
            position += 1
            continue

        if response == "v":
            print(mig.migration.source)
            continue

        if response == "j":
            position = min(len(to_prompt), position + 1)
            continue

        if response == "k":
            position = max(0, position - 1)

        if response == "d":
            break

        if response == "a":
            for mig in to_prompt[position:]:
                mig.choice = "y"
            break

        if response == "q":
            for mig in to_prompt:
                mig.choice = "n"
            break

    return migrations.replace(m.migration for m in to_prompt if m.choice == "y")
