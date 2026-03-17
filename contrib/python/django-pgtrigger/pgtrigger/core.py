from __future__ import annotations

import contextlib
import copy
import functools
import hashlib
import operator
import re
from typing import TYPE_CHECKING, Any, Generator, List, Tuple, Union

from django.db import DEFAULT_DB_ALIAS, models, router, transaction
from django.db.models.expressions import Col
from django.db.models.fields.related import RelatedField
from django.db.models.sql import Query
from django.db.models.sql.datastructures import BaseTable
from django.db.utils import ProgrammingError

from pgtrigger import compiler, features, registry, utils

if TYPE_CHECKING:
    from typing_extensions import Self

if utils.psycopg_maj_version == 2:
    import psycopg2.extensions
elif utils.psycopg_maj_version == 3:
    import psycopg.adapt
else:
    raise AssertionError


# Postgres only allows identifiers to be 63 chars max. Since "pgtrigger_"
# is the prefix for trigger names, and since an additional "_" and
# 5 character hash is added, the user-defined name of the trigger can only
# be 47 chars.
# NOTE: We can do something more sophisticated later by allowing users
# to name their triggers and then hashing the names when actually creating
# the triggers.
MAX_NAME_LENGTH = 47

# Installation states for a triggers
INSTALLED = "INSTALLED"
UNINSTALLED = "UNINSTALLED"
OUTDATED = "OUTDATED"
PRUNE = "PRUNE"
UNALLOWED = "UNALLOWED"


class _Primitive:
    """Boilerplate for some of the primitive operations"""

    def __init__(self, name):
        assert name in self.values
        self.name = name

    def __str__(self):
        return self.name

    def __hash__(self):
        return hash(self.name)


class Level(_Primitive):
    values = ("ROW", "STATEMENT")


Row = Level("ROW")
"""
For specifying row-level triggers (the default)
"""

Statement = Level("STATEMENT")
"""
For specifying statement-level triggers
"""


class Referencing:
    """For specifying the `REFERENCING` clause of a statement-level trigger"""

    def __init__(self, *, old=None, new=None):
        if not old and not new:
            raise ValueError(
                'Must provide either "old" and/or "new" to the referencing construct of a trigger'
            )

        self.old = old
        self.new = new

    def __eq__(self, other):
        if not isinstance(other, Referencing):  # pragma: no cover
            return False

        return self.old == other.old and self.new == other.new

    def __str__(self):
        ref = "REFERENCING"
        if self.old:
            ref += f" OLD TABLE AS {self.old} "

        if self.new:
            ref += f" NEW TABLE AS {self.new} "

        return ref


class When(_Primitive):
    values = ("BEFORE", "AFTER", "INSTEAD OF")


Before = When("BEFORE")
"""
For specifying `BEFORE` in the when clause of a trigger.
"""

After = When("AFTER")
"""
For specifying `AFTER` in the when clause of a trigger.
"""

InsteadOf = When("INSTEAD OF")
"""
For specifying `INSTEAD OF` in the when clause of a trigger.
"""


class Operation(_Primitive):
    values = ("UPDATE", "DELETE", "TRUNCATE", "INSERT")

    def __or__(self, other):
        assert isinstance(other, Operation)
        return Operations(self, other)

    def __contains__(self, other):  # pragma: no cover
        return self == other

    def __iter__(self):  # pragma: no cover
        return iter([self])

    def __str__(self):
        return self.name


class Operations(Operation):
    """For providing multiple operations `OR`ed together.

    Note that using the `|` operator is preferred syntax.
    """

    def __init__(self, *operations):
        for operation in operations:
            assert isinstance(operation, Operation)

        self.operations = operations

    def __or__(self, other):
        assert isinstance(other, Operation)
        return Operations(*self.operations, other)

    def __str__(self):
        return " OR ".join(str(operation) for operation in self.operations)

    def __contains__(self, other):  # pragma: no cover
        return other in self.operations

    def __iter__(self):  # pragma: no cover
        return iter(self.operations)


Update = Operation("UPDATE")
"""
For specifying `UPDATE` as the trigger operation.
"""

Delete = Operation("DELETE")
"""
For specifying `DELETE` as the trigger operation.
"""

Truncate = Operation("TRUNCATE")
"""
For specifying `TRUNCATE` as the trigger operation.
"""

Insert = Operation("INSERT")
"""
For specifying `INSERT` as the trigger operation.
"""


class UpdateOf(Operation):
    """For specifying `UPDATE OF` as the trigger operation."""

    def __init__(self, *columns):
        if not columns:
            raise ValueError("Must provide at least one column")

        self.columns = columns

    def __str__(self):
        columns = ", ".join(f"{utils.quote(col)}" for col in self.columns)
        return f"UPDATE OF {columns}"


class Timing(_Primitive):
    values = ("IMMEDIATE", "DEFERRED")


Immediate = Timing("IMMEDIATE")
"""
For deferrable triggers that run immediately by default
"""

Deferred = Timing("DEFERRED")
"""
For deferrable triggers that run at the end of the transaction by default
"""


class Condition:
    """For specifying free-form SQL in the condition of a trigger."""

    sql: str | None = None

    def __init__(self, sql: str | None = None):
        self.sql = sql or self.sql

        if not self.sql:
            raise ValueError("Must provide SQL to condition")

    def resolve(self, model):
        return self.sql


class _OldNewQuery(Query):
    """
    A special Query object for referencing the `OLD` and `NEW` variables in a
    trigger. Only used by the [pgtrigger.Q][] object.
    """

    def build_lookup(self, lookups, lhs, rhs):
        # Django does not allow custom lookups on foreign keys, even though
        # DISTINCT FROM is a comnpletely valid lookup. Trick django into
        # being able to apply this lookup to related fields.
        if lookups == ["df"] and isinstance(lhs.output_field, RelatedField):
            lhs = copy.deepcopy(lhs)
            lhs.output_field = models.IntegerField(null=lhs.output_field.null)

        return super().build_lookup(lookups, lhs, rhs)

    def build_filter(self, filter_expr, *args, **kwargs):
        if isinstance(filter_expr, Q):
            return super().build_filter(filter_expr, *args, **kwargs)

        if filter_expr[0].startswith("old__"):
            alias = "OLD"
        elif filter_expr[0].startswith("new__"):
            alias = "NEW"
        else:  # pragma: no cover
            raise ValueError("Filter expression on trigger.Q object must reference old__ or new__")

        filter_expr = (filter_expr[0][5:], filter_expr[1])
        node, _ = super().build_filter(filter_expr, *args, **kwargs)

        self.alias_map[alias] = BaseTable(alias, alias)
        for child in node.children:
            child.lhs = Col(
                alias=alias,
                target=child.lhs.target,
                output_field=child.lhs.output_field,
            )

        return node, {alias}


class F(models.F):
    """
    Similar to Django's `F` object, allows referencing the old and new
    rows in a trigger condition.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        if self.name.startswith("old__"):
            self.row_alias = "OLD"
        elif self.name.startswith("new__"):
            self.row_alias = "NEW"
        else:
            raise ValueError("F() values must reference old__ or new__")

        self.col_name = self.name[5:]

    @property
    def resolved_name(self) -> str:
        return f"{self.row_alias}.{utils.quote(self.col_name)}"

    def resolve_expression(self, query=None, *args, **kwargs) -> Col:
        return Col(
            alias=self.row_alias,
            target=query.model._meta.get_field(self.col_name),
        )


@models.ForeignKey.register_lookup
@models.fields.Field.register_lookup
class IsDistinctFrom(models.Lookup):
    """
    A custom `IS DISTINCT FROM` field lookup for common trigger conditions.
    For example, `pgtrigger.Q(old__field__df=pgtrigger.F("new__field"))`.
    """

    lookup_name = "df"

    def as_sql(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        params = lhs_params + rhs_params
        return "%s IS DISTINCT FROM %s" % (lhs, rhs), params


@models.ForeignKey.register_lookup
@models.fields.Field.register_lookup
class IsNotDistinctFrom(models.Lookup):
    """
    A custom `IS NOT DISTINCT FROM` field lookup for common trigger conditions.
    For example, `pgtrigger.Q(old__field__ndf=pgtrigger.F("new__field"))`.
    """

    lookup_name = "ndf"

    def as_sql(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        params = lhs_params + rhs_params
        return "%s IS NOT DISTINCT FROM %s" % (lhs, rhs), params


class Q(models.Q, Condition):
    """
    Similar to Django's `Q` object, allows building filter clauses based
    on the old and new rows in a trigger condition.
    """

    def resolve(self, model: type[models.Model]) -> str:
        query = _OldNewQuery(model)
        connection = utils.connection()
        sql, args = self.resolve_expression(query).as_sql(
            compiler=query.get_compiler("default"),
            connection=connection,
        )
        sql = sql.replace('"OLD"', "OLD").replace('"NEW"', "NEW")

        def _quote(val):
            """Given a value, quote it and handle psycopg2/3 differences"""
            if utils.psycopg_maj_version == 2:
                return psycopg2.extensions.adapt(val).getquoted()
            elif utils.psycopg_maj_version == 3:
                transformer = psycopg.adapt.Transformer()
                return transformer.as_literal(val) if val is not None else b"NULL"
            else:
                raise AssertionError

        args = tuple(_quote(arg).decode() for arg in args)

        return sql % args

    if TYPE_CHECKING:

        def __or__(self, other: Self) -> Self: ...
        def __and__(self, other: Self) -> Self: ...
        def __invert__(self) -> Self: ...


def _normalize_fields(model: type[models.Model], fields: list[str]) -> Generator[str, None, None]:
    """Normalize and validate fields.

    Fields such as "my_foreign_key_id" will be normalized to the name on the model, such
    as "my_foreign_key".

    Fields such as M2Ms are not allowed in triggers, so they raised
    errors here.
    """
    fields = [model._meta.get_field(field).name for field in fields]

    for field in fields:
        dj_field = model._meta.get_field(field)
        if hasattr(dj_field, "m2m_db_table"):
            raise ValueError(f'Cannot filter on many-to-many field "{field}".')

        yield dj_field.name


class _Change(Condition):
    """For specifying a condition based on changes to fields.

    See child classes for more documentation on arguments.
    """

    fields: Union[List[str], None] = None
    exclude: Union[List[str], None] = None
    exclude_auto: bool = False

    def __init__(
        self,
        *fields: str,
        exclude: Union[List[str], None] = None,
        exclude_auto: Union[bool, None] = None,
        all: bool = False,
        comparison: str = "df",
    ):
        self.fields = list(fields) or self.fields or []
        self.exclude = exclude or self.exclude or []
        self.exclude_auto = self.exclude_auto if exclude_auto is None else exclude_auto
        self._negated = False
        self.all = all
        self.comparison = comparison

    def __invert__(self):
        inverted = copy.copy(self)
        inverted._negated = not inverted._negated
        return inverted

    def resolve(self, model: type[models.Model]) -> str:
        fields = list(_normalize_fields(model, self.fields))
        exclude = set(_normalize_fields(model, self.exclude))
        model_fields = {f.name for f in model._meta.fields}

        if self.exclude_auto:
            for f in model._meta.fields:
                if getattr(f, "auto_now", False) or getattr(f, "auto_now_add", False):
                    exclude.add(f.name)

        fields = sorted(f for f in (fields or model_fields) if f not in exclude)

        if set(fields) == model_fields and not self.all:
            if self.comparison == "df":
                expr = "OLD.* IS DISTINCT FROM NEW.*"
            elif self.comparison == "ndf":
                expr = "OLD.* IS NOT DISTINCT FROM NEW.*"
            else:  # pragma: no cover
                raise ValueError(f'Invalid comparison "{self.comparison}"')
        else:
            reduce_op = operator.and_ if self.all else operator.or_
            q = functools.reduce(
                reduce_op,
                [
                    Q(**{f"old__{field}__{self.comparison}": F(f"new__{field}")})
                    for field in fields
                ],
            )
            expr = q.resolve(model)

        return f"NOT ({expr})" if self._negated else expr


class AnyChange(_Change):
    """If any supplied fields change, trigger the condition."""

    def __init__(
        self,
        *fields: str,
        exclude: Union[List[str], None] = None,
        exclude_auto: Union[bool, None] = None,
    ):
        """
        If any supplied fields change, trigger the condition.

        Args:
            *fields: If any supplied fields change, trigger the condition.
                If no fields are supplied, defaults to all fields on the model.
            exclude: Fields to exclude.
            exclude_auto: Exclude all `auto_now` and `auto_now_add` fields automatically.
        """
        super().__init__(
            *fields, exclude=exclude, exclude_auto=exclude_auto, all=False, comparison="df"
        )


class AnyDontChange(_Change):
    """If any supplied fields don't change, trigger the condition."""

    def __init__(
        self,
        *fields: str,
        exclude: Union[List[str], None] = None,
        exclude_auto: Union[bool, None] = None,
    ):
        """
        If any supplied fields don't change, trigger the condition.

        Args:
            *fields: If any supplied fields don't change, trigger the condition.
                If no fields are supplied, defaults to all fields on the model.
            exclude: Fields to exclude.
            exclude_auto: Exclude all `auto_now` and `auto_now_add` fields automatically.
        """
        super().__init__(
            *fields, exclude=exclude, exclude_auto=exclude_auto, all=False, comparison="ndf"
        )


class AllChange(_Change):
    """If all supplied fields change, trigger the condition."""

    def __init__(
        self,
        *fields: str,
        exclude: Union[List[str], None] = None,
        exclude_auto: Union[bool, None] = None,
    ):
        """
        If all supplied fields change, trigger the condition.

        Args:
            *fields: If all supplied fields change, trigger the condition.
                If no fields are supplied, defaults to all fields on the model.
            exclude: Fields to exclude.
            exclude_auto: Exclude all `auto_now` and `auto_now_add` fields automatically.
        """
        super().__init__(
            *fields, exclude=exclude, exclude_auto=exclude_auto, all=True, comparison="df"
        )


class AllDontChange(_Change):
    """If all supplied don't fields change, trigger the condition."""

    def __init__(
        self,
        *fields: str,
        exclude: Union[List[str], None] = None,
        exclude_auto: Union[bool, None] = None,
    ):
        """
        If all supplied fields don't change, trigger the condition.

        Args:
            *fields: If all supplied fields don't change, trigger the condition.
                If no fields are supplied, defaults to all fields on the model.
            exclude: Fields to exclude.
            exclude_auto: Exclude all `auto_now` and `auto_now_add` fields automatically.
        """
        super().__init__(
            *fields, exclude=exclude, exclude_auto=exclude_auto, all=True, comparison="ndf"
        )


class Func:
    """
    Allows for rendering a function with access to the "meta", "fields",
    and "columns" variables of the current model.

    For example, `func=Func("SELECT {columns.id} FROM {meta.db_table};")` makes it
    possible to do inline SQL in the `Meta` of a model and reference its properties.
    """

    def __init__(self, func):
        self.func = func

    def render(self, **kwargs: Any) -> str:
        """
        Render the SQL of the function.

        Args:
            **kwargs: Keyword arguments to pass to the function template.

        Returns:
            The rendered SQL.
        """
        return self.func.format(**kwargs)


# Allows Trigger methods to be used as context managers, mostly for
# testing purposes
@contextlib.contextmanager
def _cleanup_on_exit(cleanup):
    yield
    cleanup()


def _ignore_func_name() -> str:
    ignore_func = "_pgtrigger_should_ignore"
    if features.schema():  # pragma: no branch
        ignore_func = f"{utils.quote(features.schema())}.{ignore_func}"

    return ignore_func


class Trigger:
    """
    For specifying a free-form PL/pgSQL trigger function or for
    creating derived trigger classes.
    """

    name: str | None = None
    level: Level = Row
    when: When | None = None
    operation: Operation | None = None
    condition: Condition | None = None
    referencing: Referencing | None = None
    func: Func | str | None = None
    declare: list[tuple[str, str]] | None = None
    timing: Timing | None = None

    def __init__(
        self,
        *,
        name: str | None = None,
        level: Level | None = None,
        when: When | None = None,
        operation: Operation | None = None,
        condition: Condition | None = None,
        referencing: Referencing | None = None,
        func: Func | str | None = None,
        declare: List[Tuple[str, str]] | None = None,
        timing: Timing | None = None,
    ) -> None:
        self.name = name or self.name
        self.level = level or self.level
        self.when = when or self.when
        self.operation = operation or self.operation
        self.condition = condition or self.condition
        self.referencing = referencing or self.referencing
        self.func = func or self.func
        self.declare = declare or self.declare
        self.timing = timing or self.timing

        if not self.level or not isinstance(self.level, Level):
            raise ValueError(f'Invalid "level" attribute: {self.level}')

        if not self.when or not isinstance(self.when, When):
            raise ValueError(f'Invalid "when" attribute: {self.when}')

        if not self.operation or not isinstance(self.operation, Operation):
            raise ValueError(f'Invalid "operation" attribute: {self.operation}')

        if self.timing and not isinstance(self.timing, Timing):
            raise ValueError(f'Invalid "timing" attribute: {self.timing}')

        if self.level == Row and self.referencing:
            raise ValueError('Row-level triggers cannot have a "referencing" attribute')

        if self.timing and self.level != Row:
            raise ValueError('Deferrable triggers must have "level" attribute as "pgtrigger.Row"')

        if self.timing and self.when != After:
            raise ValueError('Deferrable triggers must have "when" attribute as "pgtrigger.After"')

        if not self.name:
            raise ValueError('Trigger must have "name" attribute')

        self.validate_name()

    def __str__(self) -> str:  # pragma: no cover
        return self.name

    def validate_name(self) -> None:
        """Verifies the name is under the maximum length and has valid characters.

        Raises:
            ValueError: If the name is invalid
        """
        if len(self.name) > MAX_NAME_LENGTH:
            raise ValueError(f'Trigger name "{self.name}" > {MAX_NAME_LENGTH} characters.')

        if not re.match(r"^[a-zA-Z0-9-_]+$", self.name):
            raise ValueError(
                f'Trigger name "{self.name}" has invalid characters.'
                " Only alphanumeric characters, hyphens, and underscores are allowed."
            )

    def get_pgid(self, model: models.Model) -> str:
        """The ID of the trigger and function object in postgres

        All objects are prefixed with "pgtrigger_" in order to be
        discovered/managed by django-pgtrigger.

        Args:
            model: The model.

        Returns:
            The Postgres ID.
        """
        model_hash = hashlib.sha1(self.get_uri(model).encode()).hexdigest()[:5]
        pgid = f"pgtrigger_{self.name}_{model_hash}"

        if len(pgid) > 63:
            raise ValueError(f'Trigger identifier "{pgid}" is greater than 63 chars')

        # NOTE - Postgres always stores names in lowercase. Ensure that all
        # generated IDs are lowercase so that we can properly do installation
        # and pruning tasks.
        return pgid.lower()

    def get_condition(self, model: models.Model) -> Condition:
        """Get the condition of the trigger.

        Args:
            model: The model.

        Returns:
            The condition.
        """
        return self.condition

    def get_declare(self, model: models.Model) -> List[Tuple[str, str]]:
        """
        Gets the DECLARE part of the trigger function if any variables
        are used.

        Args:
            model: The model

        Returns:
            A list of variable name / type tuples that will
            be shown in the DECLARE. For example [('row_data', 'JSONB')]
        """
        return self.declare or []

    def get_func(self, model: models.Model) -> Union[str, Func]:
        """
        Returns the trigger function that comes between the BEGIN and END
        clause.

        Args:
            model: The model

        Returns:
            The trigger function as a SQL string or [pgtrigger.Func][] object.
        """
        if not self.func:
            raise ValueError("Must define func attribute or implement get_func")
        return self.func

    def get_uri(self, model: models.Model) -> str:
        """The URI for the trigger.

        Args:
            model: The model

        Returns:
            The URI in the format of the "<app>.<model>:<trigger>"
        """

        return f"{model._meta.app_label}.{model._meta.object_name}:{self.name}"

    def render_condition(self, model: models.Model) -> str:
        """Renders the condition SQL in the trigger declaration.

        Args:
            model: The model.

        Returns:
            The rendered condition SQL
        """
        condition = self.get_condition(model)
        resolved = condition.resolve(model).strip() if condition else ""

        if resolved:
            if not resolved.startswith("("):
                resolved = f"({resolved})"
            resolved = f"WHEN {resolved}"

        return resolved

    def render_declare(self, model: models.Model) -> str:
        """Renders the DECLARE of the trigger function, if any.

        Args:
            model: The model.

        Returns:
            The rendered declare SQL.
        """
        declare = self.get_declare(model)
        if declare:
            rendered_declare = "DECLARE " + " ".join(
                f"{var_name} {var_type};" for var_name, var_type in declare
            )
        else:
            rendered_declare = ""

        return rendered_declare

    def render_execute(self, model: models.Model) -> str:
        """
        Renders what should be executed by the trigger. This defaults
        to the trigger function.

        Args:
            model: The model

        Returns:
            The SQL for the execution of the trigger function.
        """
        return f"{self.get_pgid(model)}()"

    def get_func_template_kwargs(self, model: models.Model) -> dict[str, Any]:
        """
        Returns a dictionary of keyword arguments to pass to the function template
        when using Func() classes.
        """
        fields = utils.AttrDict({field.name: field for field in model._meta.fields})
        columns = utils.AttrDict({field.name: field.column for field in model._meta.fields})
        return {"meta": model._meta, "fields": fields, "columns": columns}

    def render_func(self, model: models.Model) -> str:
        """
        Renders the func.

        Args:
            model: The model

        Returns:
            The rendered SQL of the trigger function
        """
        func = self.get_func(model)

        if isinstance(func, Func):
            return func.render(**self.get_func_template_kwargs(model))
        else:
            return func

    def compile(self, model: models.Model) -> compiler.Trigger:
        """
        Create a compiled representation of the trigger. useful for migrations.

        Args:
            model: The model

        Returns:
            The compiled trigger object.
        """
        return compiler.Trigger(
            name=self.name,
            sql=compiler.UpsertTriggerSql(
                ignore_func_name=_ignore_func_name(),
                pgid=self.get_pgid(model),
                declare=self.render_declare(model),
                func=self.render_func(model),
                table=model._meta.db_table,
                constraint="CONSTRAINT" if self.timing else "",
                when=self.when,
                operation=self.operation,
                timing=f"DEFERRABLE INITIALLY {self.timing}" if self.timing else "",
                referencing=self.referencing or "",
                level=self.level,
                condition=self.render_condition(model),
                execute=self.render_execute(model),
            ),
        )

    def allow_migrate(self, model: models.Model, database: Union[str, None] = None) -> bool:
        """True if the trigger for this model can be migrated.

        Defaults to using the router's allow_migrate.

        Args:
            model: The model.
            database: The name of the database configuration.

        Returns:
            `True` if the trigger for the model can be migrated.
        """
        model = model._meta.concrete_model
        return utils.is_postgres(database) and router.allow_migrate(
            database or DEFAULT_DB_ALIAS, model._meta.app_label, model_name=model._meta.model_name
        )

    def format_sql(self, sql: str) -> str:
        """Returns SQL as one line that has trailing whitespace removed from each line.

        Args:
            sql: The unformatted SQL

        Returns:
            The formatted SQL
        """
        return " ".join(line.strip() for line in sql.split("\n") if line.strip()).strip()

    def exec_sql(
        self,
        sql: str,
        model: models.Model,
        database: Union[str, None] = None,
        fetchall: bool = False,
    ) -> Any:
        """Conditionally execute SQL if migrations are allowed.

        Args:
            sql: The SQL string.
            model: The model.
            database: The name of the database configuration.
            fetchall: True if all results should be fetched

        Returns:
            A psycopg cursor result
        """
        if self.allow_migrate(model, database=database):
            return utils.exec_sql(str(sql), database=database, fetchall=fetchall)

    def get_installation_status(
        self, model: models.Model, database: Union[str, None] = None
    ) -> Tuple[str, Union[bool, None]]:
        """Returns the installation status of a trigger.

        The return type is (status, enabled), where status is one of:

        1. `INSTALLED`: If the trigger is installed
        2. `UNINSTALLED`: If the trigger is not installed
        3. `OUTDATED`: If the trigger is installed but has been modified
        4. `IGNORED`: If migrations are not allowed

        "enabled" is True if the trigger is installed and enabled or false
        if installed and disabled (or uninstalled).

        Args:
            model: The model.
            database: The name of the database configuration.

        Returns:
            A tuple with the installation and enablement status.
        """
        if not self.allow_migrate(model, database=database):
            return (UNALLOWED, None)

        trigger_exists_sql = f"""
            SELECT oid, obj_description(oid) AS hash, tgenabled AS enabled
            FROM pg_trigger
            WHERE tgname='{self.get_pgid(model)}'
                AND tgrelid='{utils.quote(model._meta.db_table)}'::regclass;
        """
        try:
            with transaction.atomic(using=database):
                results = self.exec_sql(
                    trigger_exists_sql, model, database=database, fetchall=True
                )
        except ProgrammingError:  # pragma: no cover
            # When the table doesn't exist yet, possibly because migrations
            # haven't been executed, a ProgrammingError will happen because
            # of an invalid regclass cast. Return 'UNINSTALLED' for this
            # case
            return (UNINSTALLED, None)

        if not results:
            return (UNINSTALLED, None)
        else:
            hash = self.compile(model).hash
            if hash != results[0][1]:
                return (OUTDATED, results[0][2] == "O")
            else:
                return (INSTALLED, results[0][2] == "O")

    def register(self, *models: models.Model):
        """Register model classes with the trigger

        Args:
            *models: Models to register to this trigger.
        """
        for model in models:
            registry.set(self.get_uri(model), model=model, trigger=self)

        return _cleanup_on_exit(lambda: self.unregister(*models))

    def unregister(self, *models: models.Model):
        """Unregister model classes with the trigger.

        Args:
            *models: Models to unregister to this trigger.
        """
        for model in models:
            registry.delete(self.get_uri(model))

        return _cleanup_on_exit(lambda: self.register(*models))

    def install(self, model: models.Model, database: Union[str, None] = None):
        """Installs the trigger for a model.

        Args:
            model: The model.
            database: The name of the database configuration.
        """
        install_sql = self.compile(model).install_sql
        with transaction.atomic(using=database):
            self.exec_sql(install_sql, model, database=database)
        return _cleanup_on_exit(lambda: self.uninstall(model, database=database))

    def uninstall(self, model: models.Model, database: Union[str, None] = None):
        """Uninstalls the trigger for a model.

        Args:
            model: The model.
            database: The name of the database configuration.
        """
        uninstall_sql = self.compile(model).uninstall_sql
        self.exec_sql(uninstall_sql, model, database=database)
        return _cleanup_on_exit(  # pragma: no branch
            lambda: self.install(model, database=database)
        )

    def enable(self, model: models.Model, database: Union[str, None] = None):
        """Enables the trigger for a model.

        Args:
            model: The model.
            database: The name of the database configuration.
        """
        enable_sql = self.compile(model).enable_sql
        self.exec_sql(enable_sql, model, database=database)
        return _cleanup_on_exit(  # pragma: no branch
            lambda: self.disable(model, database=database)
        )

    def disable(self, model: models.Model, database: Union[str, None] = None):
        """Disables the trigger for a model.

        Args:
            model: The model.
            database: The name of the database configuration.
        """
        disable_sql = self.compile(model).disable_sql
        self.exec_sql(disable_sql, model, database=database)
        return _cleanup_on_exit(lambda: self.enable(model, database=database))  # pragma: no branch
