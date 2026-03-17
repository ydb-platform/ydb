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

from collections import Counter
from collections import OrderedDict
from collections import abc
from contextlib import ExitStack
from copy import copy
from glob import glob
from importlib import resources
from itertools import chain
from itertools import count
from itertools import zip_longest
from logging import getLogger
import atexit
import typing as t
import hashlib
import importlib.util
import os
import pathlib
import re
import sys
import inspect
import types
import textwrap

import sqlparse

from yoyo import exceptions
from yoyo.utils import plural

logger = getLogger("yoyo.migrations")
default_migration_table = "_yoyo_migration"

hash_function = hashlib.sha256


def _is_migration_file(path: pathlib.Path):
    """
    Return True if the given path matches a migration file pattern
    """
    from yoyo.scripts import newmigration

    return (
        path.is_file()
        and path.suffix in {".py", ".sql"}
        and not path.name.startswith(newmigration.tempfile_prefix)
    )


def get_migration_hash(migration_id):
    """
    Return a unique hash given a migration_id, that can be used as a database
    key.

    :param migration_id: a migration id (ie filename without extension), or
                         ``None`` if this is a new migration
    """
    if migration_id is None:
        return None
    return hash_function(migration_id.encode("utf-8")).hexdigest()


# eg: "-- depends: 1 2"
DirectivesType = t.Dict[str, str]

LeadingCommentType = str

SqlType = str


def parse_metadata_from_sql_comments(
    s: str,
) -> t.Tuple[DirectivesType, LeadingCommentType, SqlType]:
    directive_names = ["transactional", "depends"]
    comment_or_empty = re.compile(r"^(\s*|\s*--.*)$").match
    directive_pattern = re.compile(
        r"^\s*--\s*({})\s*:\s*(.*)$".format("|".join(map(re.escape, directive_names)))
    )

    lineending = re.search(r"\n|\r\n|\r", s + "\n").group(0)  # type: ignore
    lines = iter(s.split(lineending))
    directives: DirectivesType = {}
    leading_comments = []
    sql = []
    for line in lines:
        match = directive_pattern.match(line)
        if match:
            k, v = match.groups()
            if k in directives:
                directives[k] += " {}".format(v)
            else:
                directives[k] = v
        elif comment_or_empty(line):
            decommented = line.strip().lstrip("--").strip()
            leading_comments.append(decommented)
        else:
            sql.append(line)
            break
    sql.extend(lines)
    return (
        directives,
        textwrap.dedent(lineending.join(leading_comments)),
        lineending.join(sql),
    )


def read_sql_migration(
    path: str,
) -> t.Tuple[DirectivesType, LeadingCommentType, t.List[str]]:
    directives: DirectivesType = {}
    leading_comment = ""
    statements = []
    if os.path.exists(path):
        with open(path, "r", encoding="UTF-8") as f:
            statements = sqlparse.split(f.read())
            if statements:
                (
                    directives,
                    leading_comment,
                    sql,
                ) = parse_metadata_from_sql_comments(statements[0])
                statements[0] = sql
    statements = [s for s in statements if s.strip()]
    return directives, leading_comment, statements


class Migration(object):
    __all_migrations: t.Dict[str, "Migration"] = {}

    def __init__(self, id, path, source_dir):
        self.id = id
        self.hash = get_migration_hash(id)
        self.path = path
        self.steps = None
        self.source = None
        self.source_dir = source_dir
        self.use_transactions = True
        self._depends = None
        self.__all_migrations[id] = self
        self.module = None

    def __repr__(self):
        return "<{} {!r} from {}>".format(self.__class__.__name__, self.id, self.path)

    def is_raw_sql(self):
        return self.path.endswith(".sql")

    @property
    def loaded(self):
        return self.steps is not None

    @property
    def depends(self):
        self.load()
        return self._depends

    def load(self):
        if self.loaded:
            return

        collector = StepCollector(migration=self)
        with open(self.path, "r") as f:
            self.source = f.read()

        if self.is_raw_sql():
            self.module = types.ModuleType(self.path)
        else:
            spec = importlib.util.spec_from_file_location(self.path, self.path)
            assert spec is not None
            self.module = importlib.util.module_from_spec(spec)

        self.module.step = collector.add_step  # type: ignore
        self.module.group = collector.add_step_group  # type: ignore
        self.module.transaction = collector.add_step_group  # type: ignore
        self.module.__yoyo_collector__ = collector  # type: ignore
        if self.is_raw_sql():
            directives, leading_comment, statements = read_sql_migration(self.path)
            _, _, rollback_statements = read_sql_migration(
                os.path.splitext(self.path)[0] + ".rollback.sql"
            )
            rollback_statements.reverse()
            statements_with_rollback = zip_longest(
                statements, rollback_statements, fillvalue=None
            )

            for s, r in statements_with_rollback:
                collector.add_step(s, r)
            self.module.__doc__ = leading_comment
            setattr(
                self.module,
                "__transactional__",
                {"true": True, "false": False}[
                    directives.get("transactional", "true").lower()
                ],
            )
            setattr(
                self.module,
                "__depends__",
                {d for d in directives.get("depends", "").split() if d},
            )

        else:
            try:
                if spec and spec.loader:
                    spec.loader.exec_module(self.module)
                else:
                    logger.exception(
                        "Could not import migration from %r: "
                        "ModuleSpec has no loader attached",
                        self.path,
                    )
                    raise exceptions.BadMigration(self.path)

            except Exception as e:
                logger.exception("Could not import migration from %r: %r", self.path, e)
                raise exceptions.BadMigration(self.path, e)
        depends = getattr(self.module, "__depends__", [])
        if isinstance(depends, (str, bytes)):
            depends = [depends]
        self._depends = {self.__all_migrations.get(id, None) for id in depends}
        self.use_transactions = getattr(self.module, "__transactional__", True)
        if None in self._depends:
            raise exceptions.BadMigration(
                "Could not resolve dependencies in {}".format(self.path)
            )
        self.steps = collector.create_steps(self.use_transactions)

    def process_steps(self, backend, direction, force=False):
        self.load()
        reverse = {"rollback": "apply", "apply": "rollback"}[direction]

        steps = self.steps
        assert steps is not None
        if direction == "rollback":
            steps = reversed(steps)  # type: ignore

        executed_steps = []
        if self.use_transactions:
            transaction = backend.transaction
        else:
            transaction = backend.disable_transactions

        with transaction():
            for step in steps:
                try:
                    getattr(step, direction)(backend, force)
                    executed_steps.append(step)
                except backend.DatabaseError:
                    exc_info = sys.exc_info()

                    if not backend.has_transactional_ddl or not self.use_transactions:
                        # Any DDL statements that have been executed have been
                        # committed. Go through the rollback steps to undo
                        # these inasmuch is possible.
                        try:
                            for step in reversed(executed_steps):
                                getattr(step, reverse)(backend)
                        except backend.DatabaseError:
                            logger.exception("Could not %s step %s", direction, step.id)
                    if exc_info[1] is not None:
                        raise exc_info[1].with_traceback(exc_info[2])


class PostApplyHookMigration(Migration):
    """
    A special migration that is run after successfully applying a set of
    migrations. Unlike a normal migration this will be run every time
    migrations are applied script is called.
    """


class StepBase(object):
    id = None

    def __repr__(self):
        return "<{} #{}>".format(self.__class__.__name__, self.id)

    def apply(self, backend, force=False):
        raise NotImplementedError()

    def rollback(self, backend, force=False):
        raise NotImplementedError()


class TransactionWrapper(StepBase):
    """
    A :class:~`yoyo.migrations.TransactionWrapper` object causes a step to be
    run within a single database transaction. Nested transactions are
    implemented via savepoints.
    """

    def __init__(self, step, ignore_errors=None):
        assert ignore_errors in (None, "all", "apply", "rollback")
        self.step = step
        self.ignore_errors = ignore_errors

    def __repr__(self):
        return "<TransactionWrapper {!r}>".format(self.step)

    def apply(self, backend, force=False, direction="apply"):
        try:
            with backend.transaction():
                getattr(self.step, direction)(backend, force)
        except backend.DatabaseError:
            if force or self.ignore_errors in (direction, "all"):
                logger.exception("Ignored error in %r", self.step)
            else:
                raise

    def rollback(self, backend, force=False):
        self.apply(backend, force, "rollback")


class Transactionless(StepBase):
    """
    A :class:~`yoyo.migrations.TransactionWrapper` object causes a step to be
    run outside of a database transaction.
    """

    def __init__(self, step, ignore_errors=None):
        assert ignore_errors in (None, "all", "apply", "rollback")
        self.step = step
        self.ignore_errors = ignore_errors

    def __repr__(self):
        return "<TransactionWrapper {!r}>".format(self.step)

    def apply(self, backend, force=False, direction="apply"):
        try:
            getattr(self.step, direction)(backend, force)
        except backend.DatabaseError:
            if force or self.ignore_errors in (direction, "all"):
                logger.exception("Ignored error in %r", self.step)
                return
            else:
                raise

    def rollback(self, backend, force=False):
        self.apply(backend, force, "rollback")


class MigrationStep(StepBase):
    """
    Model a single migration.

    Each migration step comprises apply and rollback steps of up and down SQL
    statements.
    """

    def __init__(self, id, apply, rollback):
        self.id = id
        self._rollback = rollback
        self._apply = apply

    def _execute(self, cursor, stmt, out=None):
        """
        Execute the given statement. If rows are returned, output these in a
        tabulated format.
        """
        if out is None:
            out = sys.stdout
        if isinstance(stmt, str):
            logger.debug(" - executing %r", stmt.encode("ascii", "replace"))
        else:
            logger.debug(" - executing %r", stmt)
        cursor.execute(stmt)
        if cursor.description:
            result = [[str(value) for value in row] for row in cursor.fetchall()]
            column_names = [desc[0] for desc in cursor.description]
            column_sizes = [len(c) for c in column_names]

            for row in result:
                for ix, value in enumerate(row):
                    if len(value) > column_sizes[ix]:
                        column_sizes[ix] = len(value)
            format = "|".join(" %%- %ds " % size for size in column_sizes)
            format += "\n"
            out.write(format % tuple(column_names))
            out.write("+".join("-" * (size + 2) for size in column_sizes) + "\n")
            for row in result:
                out.write(format % tuple(row))
            out.write(plural(len(result), "(%d row)", "(%d rows)") + "\n")

    def apply(self, backend, force=False):
        """
        Apply the step.

        :param force: If true, errors will be logged but not be re-raised
        """
        logger.info(" - applying step %d", self.id)
        if not self._apply:
            return
        if isinstance(self._apply, str):
            cursor = backend.cursor()
            try:
                self._execute(cursor, self._apply)
            finally:
                cursor.close()
        else:
            self._apply(backend.connection)

    def rollback(self, backend, force=False):
        """
        Rollback the step.
        """
        logger.info(" - rolling back step %d", self.id)
        if self._rollback is None:
            return
        if isinstance(self._rollback, str):
            cursor = backend.cursor()
            try:
                self._execute(cursor, self._rollback)
            finally:
                cursor.close()
        else:
            self._rollback(backend.connection)


class StepGroup(MigrationStep):
    """
    Multiple steps aggregated together
    """

    def __init__(self, steps):
        self.steps = steps

    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, self.steps)

    def apply(self, backend, force=False):
        for item in self.steps:
            item.apply(backend, force)

    def rollback(self, backend, force=False):
        for item in reversed(self.steps):
            item.rollback(backend, force)


def _expand_sources(sources) -> t.Iterable[t.Tuple[str, t.List[str]]]:
    package_match = re.compile(r"^package:([^\s\/:]+):(.*)$").match

    filecontext = ExitStack()
    atexit.register(filecontext.close)

    for source in sources:
        mo = package_match(source)
        if mo:
            package_name = mo.group(1)
            resource_dir = mo.group(2)
            try:
                pkg_files = resources.files(package_name).joinpath(resource_dir)
                if pkg_files.is_dir():
                    all_files = (
                        filecontext.enter_context(resources.as_file(traversable))
                        for traversable in pkg_files.iterdir()
                        if traversable.is_file()
                    )
                    paths = [str(f) for f in sorted(all_files) if _is_migration_file(f)]
                    yield (source, paths)
            except FileNotFoundError:
                continue
        else:
            for directory in map(pathlib.Path, glob(source)):
                paths = [str(f) for f in directory.iterdir() if _is_migration_file(f)]
                yield (str(directory), sorted(paths))


def read_migrations(*sources):
    """
    Return a ``MigrationList`` containing all migrations from ``sources``.
    """
    migrations: t.Dict[str, MigrationList] = OrderedDict()

    for source, paths in _expand_sources(sources):
        for path in paths:
            if path.endswith(".rollback.sql"):
                continue
            filename = os.path.splitext(os.path.basename(path))[0]

            migration_class = Migration
            if filename.startswith("post-apply"):
                migration_class = PostApplyHookMigration

            migration = migration_class(
                os.path.splitext(os.path.basename(path))[0],
                path,
                source_dir=source,
            )
            ml = migrations.setdefault(source, MigrationList())
            if migration_class is PostApplyHookMigration:
                ml.post_apply.append(migration)
            else:
                ml.append(migration)
    merged_migrations = MigrationList(
        chain(*migrations.values()),
        chain(*(m.post_apply for m in migrations.values())),
    )
    return merged_migrations


class MigrationList(abc.MutableSequence):
    """
    A list of database migrations.
    """

    def __init__(self, items=None, post_apply=None):
        self.items = list(items) if items else []
        self.post_apply = list(post_apply) if post_apply else []
        self.keys = set(item.id for item in self.items)
        self.check_conflicts()

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, repr(self.items))

    def check_conflicts(self):
        c: t.Dict[str, int] = Counter()
        for item in self:
            c[item.id] += 1
            if c[item.id] > 1:
                raise exceptions.MigrationConflict(item.id)

    def __getitem__(self, n):
        if isinstance(n, slice):
            return self.__class__(self.items.__getitem__(n))
        return self.items.__getitem__(n)

    def __setitem__(self, n, ob):
        removing = self.items[n]
        if not isinstance(removing, list):
            remove_ids = set([item.id for item in removing])
            new_ids = {ob.id}
        else:
            remove_ids = set(item.id for item in removing)
            new_ids = {item.id for item in ob}

        for id in new_ids:
            if id in self.keys and id not in remove_ids:
                raise exceptions.MigrationConflict(id)

        self.keys.difference_update(removing)
        self.keys.update(new_ids)
        return self.items.__setitem__(n, ob)

    def __len__(self):
        return len(self.items)

    def __delitem__(self, i):
        self.keys.remove(self.items[i].id)
        self.items.__delitem__(i)

    def insert(self, i, x):
        if x.id in self.keys:
            raise exceptions.MigrationConflict(x.id)
        self.keys.add(x.id)
        return self.items.insert(i, x)

    def __add__(self, other):
        ob = copy(self)
        ob.extend(other)
        return ob

    def filter(self, predicate):
        return self.__class__([m for m in self if predicate(m)], self.post_apply)

    def replace(self, newmigrations):
        return self.__class__(newmigrations, self.post_apply)


class StepCollector(object):
    """
    Provide the ``step`` and ``transaction`` functions used in migration
    scripts.

    Each call to step/transaction updates the StepCollector's ``steps`` list.
    """

    def __init__(self, migration):
        self.migration = migration
        self.steps = OrderedDict()
        self.step_id = count(0)

    def add_step(self, apply, rollback=None, ignore_errors=None):
        """
        Wrap the given apply and rollback code in a transaction, and add it
        to the list of steps.
        Return the transaction-wrapped step.
        """

        def do_add(use_transactions):
            wrapper = TransactionWrapper if use_transactions else Transactionless
            step: StepBase = MigrationStep(next(self.step_id), apply, rollback)
            step = wrapper(step, ignore_errors)
            return step

        self.steps[do_add] = 1
        return do_add

    def add_step_group(self, *args, **kwargs):
        """
        Create a ``StepGroup`` group of steps.
        """
        if "steps" in kwargs:
            if args:
                raise ValueError(
                    "steps cannot be called with both keyword "
                    "and positional 'steps' argument"
                )

            steps = kwargs["steps"]
        else:
            steps = list(
                chain(*(s if isinstance(s, abc.Iterable) else [s] for s in args))
            )
        for s in steps:
            del self.steps[s]

        def do_add(use_transactions):
            ignore_errors = kwargs.pop("ignore_errors", None)
            wrapper = TransactionWrapper if use_transactions else Transactionless

            group = StepGroup([create_step(use_transactions) for create_step in steps])
            return wrapper(group, ignore_errors)

        self.steps[do_add] = 1
        return do_add

    def create_steps(self, use_transactions):
        return [create_step(use_transactions) for create_step in self.steps]


def _get_collector():
    frame = inspect.currentframe()
    try:
        while frame is not None:
            if "__yoyo_collector__" in frame.f_globals:
                return frame.f_globals["__yoyo_collector__"]
            frame = frame.f_back
    except KeyError:
        raise AssertionError(
            "Expected to be called in the context of a migration module import"
        )


def step(*args, **kwargs):
    return _get_collector().add_step(*args, **kwargs)


def group(*args, **kwargs):
    return _get_collector().add_step_group(*args, **kwargs)


#: Alias for compatibility purposes.
#: This no longer affects transaction handling.
transaction = group


def ancestors(migration, population):
    """
    Return the dependencies for ``migration`` from ``population``.

    :param migration: a :class:`~yoyo.migrations.Migration` object
    :param population: a collection of migrations
    """
    to_process = set()
    for m in migration.depends:
        to_process.add(m)

    deps = set()
    while to_process:
        m = to_process.pop()
        deps.add(m)
        for d in m.depends:
            if d in deps:
                continue
            deps.add(d)
            to_process.add(d)
    return deps


def descendants(migration, population):
    """
    Return all descendants of ``migration`` from ``population``.

    :param migration: a :class:`~yoyo.migrations.Migration` object
    :param population: a collection of migrations
    """
    population = set(population)
    descendants = {migration}
    while True:
        found = False
        for m in population - descendants:
            if set(m.depends) & descendants:
                descendants.add(m)
                found = True
        if not found:
            break
    descendants.remove(migration)
    return descendants


def heads(migration_list):
    """
    Return the set of migrations that have no child dependencies
    """
    heads = set(migration_list)
    for m in migration_list:
        heads -= m.depends
    return heads


def topological_sort(migrations: t.Iterable[Migration]) -> t.Iterable[Migration]:
    from yoyo.topologicalsort import topological_sort as topological_sort_impl
    from yoyo.topologicalsort import CycleError

    migration_list = list(migrations)
    all_migrations = set(migration_list)
    dependency_graph = {m: (m.depends & all_migrations) for m in migration_list}
    try:
        return topological_sort_impl(migration_list, dependency_graph)
    except CycleError as e:
        raise exceptions.BadMigration(
            "Circular dependencies among these migrations {}".format(
                ", ".join(m.id for m in e.args[1])
            )
        )
