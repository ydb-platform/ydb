"""Additional goodies"""

from __future__ import annotations

import functools
import itertools
import operator
from typing import Any, List, Tuple, Union

from django.db import models

from pgtrigger import core, utils

# A sentinel value to determine if a kwarg is unset
_unset = object()


def _get_column(model, field):
    field = field if isinstance(field, models.Field) else model._meta.get_field(field)
    col = field.column
    if not col:  # pragma: no cover
        if getattr(field, "columns", None):
            raise ValueError(
                f"Field {field.name} references a composite key and is not supported."
            )
        else:
            raise ValueError(f"Field {field.name} does not reference a database column.")
    return col


def _get_columns(model, field):
    field = field if isinstance(field, models.Field) else model._meta.get_field(field)
    col = field.column
    cols = [col] if col else getattr(field, "columns", None)
    if not cols:  # pragma: no cover
        raise ValueError(f"Field {field.name} does not reference a database column.")
    return cols


class Composer(core.Trigger):
    """A trigger that can work as a statement or row level trigger.

    Automatically includes proper transition tables for statement-level triggers
    based on the operation. If using a condition, provides utility variables
    for accessing filtered transition tables.

    See [the statement-level trigger section](./statement.md) for more information.
    """

    func: dict[core.Level, core.Func | str] | core.Func | str | None = None  # type: ignore

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        assert self.operation is not None

        if self.referencing is not None:
            raise ValueError("Composer triggers do not support referencing declarations.")

        # Make old/new rows available based on the operation or combination of operations.
        if self.level == core.Statement:
            # NOTE(@wesleykendall): Postgres doesn't support transition tables on more than one
            # operation.
            if self.operation == core.Update:
                self.referencing = core.Referencing(old="old_values", new="new_values")
            elif self.operation == core.Delete:
                self.referencing = core.Referencing(old="old_values")
            elif self.operation == core.Insert:
                self.referencing = core.Referencing(new="new_values")

    def get_func(self, model: models.Model) -> Union[str, core.Func]:
        """Allow a dict of funcs for statement/row level triggers."""
        if isinstance(self.func, dict):
            if self.level == core.Statement:
                return self.func[core.Statement]
            else:
                return self.func[core.Row]
        else:
            return super().get_func(model)

    def render_condition(self, model: models.Model) -> str:
        """Ignore condition rendering in a statement level trigger."""
        return "" if self.level == core.Statement else super().render_condition(model)

    def get_func_template_kwargs(self, model: models.Model) -> dict[str, Any]:
        """
        Provides `cond_joined_values`, `cond_old_values`, and `cond_new_values` variables to the
        function template.
        """
        func_template_kwargs = super().get_func_template_kwargs(model)

        if self.level == core.Statement:  # pragma: no branch
            condition = super().render_condition(model)

            condition = condition.replace("OLD.", "old_values.").replace("NEW.", "new_values.")
            if condition.startswith("WHEN "):
                condition = f"WHERE {condition[5:]}"

            pk_columns = _get_columns(model, model._meta.pk)
            old_pk_cols = ", ".join(f'old_values."{col}"' for col in pk_columns)
            new_pk_cols = ", ".join(f'new_values."{col}"' for col in pk_columns)
            cond_joined_values = (
                f"old_values JOIN new_values ON ({old_pk_cols}) = ({new_pk_cols}) {condition}"
            )

            if "new_values." in condition and "old_values." in condition:
                cond_old_values = cond_joined_values
                cond_new_values = cond_joined_values
            elif "new_values." in condition:
                cond_old_values = cond_joined_values
                cond_new_values = f"new_values {condition}"
            elif "old_values." in condition:
                cond_old_values = f"old_values {condition}"
                cond_new_values = cond_joined_values
            else:
                cond_old_values = f"old_values {condition}"
                cond_new_values = f"new_values {condition}"

            func_template_kwargs |= {
                "cond_joined_values": cond_joined_values,
                "cond_old_values": cond_old_values,
                "cond_new_values": cond_new_values,
            }

        return func_template_kwargs

    def render_func(self, model: models.Model) -> str:
        rendered = super().render_func(model)

        # Check if the function references OLD or NEW values when it shouldn't
        if self.level == core.Statement:  # pragma: no branch
            references_old = self.referencing and self.referencing.old
            references_new = self.referencing and self.referencing.new

            if "new_values." in rendered and not references_new:
                raise ValueError(
                    f'Composer trigger references NEW values, but operation "{self.operation}"'
                    " doesn't allow for that."
                )
            elif "old_values." in rendered and not references_old:
                raise ValueError(
                    f'Composer trigger references OLD values, but operation "{self.operation}"'
                    " doesn't allow for that."
                )

        return rendered


class Protect(Composer):
    """A trigger that raises an exception."""

    when: core.When = core.Before

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self.when = core.After if self.level == core.Statement else self.when

    def get_func(self, model):
        if self.level == core.Row:
            sql = f"""
                RAISE EXCEPTION
                    'pgtrigger: Cannot {str(self.operation).lower()} rows from % table',
                TG_TABLE_NAME;
            """
            return self.format_sql(sql)
        elif self.level == core.Statement:
            if not self.referencing:  # pragma: no cover
                raise ValueError(
                    f'Unsupported operation "{self.operation}" for statement-level protect trigger'
                )

            if self.referencing.new and self.referencing.old:
                transition_table = "cond_joined_values"
            elif self.referencing.new:
                transition_table = "cond_new_values"
            elif self.referencing.old:
                transition_table = "cond_old_values"
            else:
                raise AssertionError(f'Unexpected referencing declaration "{self.referencing}"')

            return core.Func(
                f"""
                IF EXISTS (SELECT 1 FROM {{{transition_table}}}) THEN
                    RAISE EXCEPTION
                        'pgtrigger: Cannot {str(self.operation).lower()} rows from % table',
                    TG_TABLE_NAME;
                END IF;
                RETURN NULL;
                """
            )
        else:
            raise AssertionError(f'Unexpected level "{self.level}"')


class ReadOnly(Protect):
    """A trigger that prevents edits to fields.

    If `fields` are provided, will protect edits to only those fields.
    If `exclude` is provided, will protect all fields except the ones
    excluded.
    If none of these arguments are provided, all fields cannot be edited.
    """

    fields: Union[List[str], None] = None
    exclude: Union[List[str], None] = None
    operation: core.Operation = core.Update

    def __init__(
        self,
        *,
        fields: Union[List[str], None] = None,
        exclude: Union[List[str], None] = None,
        **kwargs: Any,
    ):
        self.fields = fields or self.fields
        self.exclude = exclude or self.exclude

        if self.fields and self.exclude:
            raise ValueError('Must provide only one of "fields" or "exclude" to ReadOnly trigger')

        super().__init__(**kwargs)

    def get_condition(self, model):
        if not self.fields and not self.exclude:
            return core.Condition("OLD.* IS DISTINCT FROM NEW.*")
        else:
            if self.exclude:
                # Sanity check that the exclude list contains valid fields
                for field in self.exclude:
                    model._meta.get_field(field)

                fields = [f.name for f in model._meta.fields if f.name not in self.exclude]
            else:
                fields = [model._meta.get_field(field).name for field in self.fields]

            return functools.reduce(
                operator.or_,
                [core.Q(**{f"old__{field}__df": core.F(f"new__{field}")}) for field in fields],
            )


class FSM(core.Trigger):
    """Enforces a finite state machine on a field.

    Supply the trigger with the `field` that transitions and then
    a list of tuples of valid transitions to the `transitions` argument.

    !!! note

        Only non-null `CharField` fields without quotes are currently supported.
        If your strings have a colon symbol in them, you must override the
        "separator" argument to be a value other than a colon.
    """

    when: core.When = core.Before
    operation: core.Operation = core.Update
    field: str = None
    transitions: List[Tuple[str, str]] = None
    separator: str = ":"

    def __init__(
        self,
        *,
        name: str = None,
        condition: Union[core.Condition, None] = None,
        field: str = None,
        transitions: List[Tuple[str, str]] = None,
        separator: str = None,
    ):
        self.field = field or self.field
        self.transitions = transitions or self.transitions
        self.separator = separator or self.separator

        if not self.field:  # pragma: no cover
            raise ValueError('Must provide "field" for FSM')

        if not self.transitions:  # pragma: no cover
            raise ValueError('Must provide "transitions" for FSM')

        # This trigger doesn't accept quoted values or values that
        # contain the configured separator
        for value in itertools.chain(*self.transitions):
            if "'" in value or '"' in value:
                raise ValueError(f'FSM transition value "{value}" contains quotes')
            elif self.separator in value:
                raise ValueError(
                    f'FSM value "{value}" contains separator "{self.separator}".'
                    ' Configure your trigger with a different "separator" attribute'
                )

        # The separator must be a single character that isn't a quote
        if len(self.separator) != 1:
            raise ValueError(f'Separator "{self.separator}" must be a single character')
        elif self.separator in ('"', "'"):
            raise ValueError("Separator must not have quotes")

        super().__init__(name=name, condition=condition)

    def get_declare(self, model):
        return [("_is_valid_transition", "BOOLEAN")]

    def get_func(self, model):
        col = _get_column(model, self.field)
        transition_uris = (
            "{" + ",".join([f"{old}{self.separator}{new}" for old, new in self.transitions]) + "}"
        )

        sql = f"""
            SELECT CONCAT(OLD.{utils.quote(col)}, '{self.separator}', NEW.{utils.quote(col)}) = ANY('{transition_uris}'::text[])
                INTO _is_valid_transition;

            IF (_is_valid_transition IS FALSE AND OLD.{utils.quote(col)} IS DISTINCT FROM NEW.{utils.quote(col)}) THEN
                RAISE EXCEPTION
                    'pgtrigger: Invalid transition of field "{self.field}" from "%" to "%" on table %',
                    OLD.{utils.quote(col)},
                    NEW.{utils.quote(col)},
                    TG_TABLE_NAME;
            ELSE
                RETURN NEW;
            END IF;
        """  # noqa
        return self.format_sql(sql)


class SoftDelete(core.Trigger):
    """Sets a field to a value when a delete happens.

    Supply the trigger with the "field" that will be set
    upon deletion and the "value" to which it should be set.
    The "value" defaults to `False`.

    !!! note

        This trigger currently only supports nullable `BooleanField`,
        `CharField`, and `IntField` fields.
    """

    when: core.When = core.Before
    operation: core.Operation = core.Delete
    field: str = None
    value: Union[bool, str, int, None] = False

    def __init__(
        self,
        *,
        name: str = None,
        condition: Union[core.Condition, None] = None,
        field: str = None,
        value: Union[bool, str, int, None] = _unset,
    ):
        self.field = field or self.field
        self.value = value if value is not _unset else self.value

        if not self.field:  # pragma: no cover
            raise ValueError('Must provide "field" for soft delete')

        super().__init__(name=name, condition=condition)

    def get_func(self, model):
        soft_field = _get_column(model, self.field)

        # Support composite primary keys in Django 5.2+
        pk_cols = _get_columns(model, model._meta.pk)
        table_pk_cols = ",".join(utils.quote(col) for col in pk_cols)
        trigger_pk_cols = ",".join(f"OLD.{utils.quote(col)}" for col in pk_cols)

        if len(pk_cols) > 1:
            table_pk_cols = f"({table_pk_cols})"
            trigger_pk_cols = f"({trigger_pk_cols})"

        def _render_value():
            if self.value is None:
                return "NULL"
            elif isinstance(self.value, str):
                return f"'{self.value}'"
            else:
                return str(self.value)

        sql = f"""
            UPDATE {utils.quote(model._meta.db_table)}
            SET {soft_field} = {_render_value()}
            WHERE {table_pk_cols} = {trigger_pk_cols};
            RETURN NULL;
        """
        return self.format_sql(sql)


class UpdateSearchVector(core.Trigger):
    """Updates a `django.contrib.postgres.search.SearchVectorField` from document fields.

    Supply the trigger with the `vector_field` that will be updated with
    changes to the `document_fields`. Optionally provide a `config_name`, which
    defaults to `pg_catalog.english`.

    This trigger uses `tsvector_update_trigger` to update the vector field.
    See [the Postgres docs](https://www.postgresql.org/docs/current/textsearch-features.html#TEXTSEARCH-UPDATE-TRIGGERS)
    for more information.

    !!! note

        `UpdateSearchVector` triggers are not compatible with [pgtrigger.ignore][] since
        it references a built-in trigger. Trying to ignore this trigger results in a
        `RuntimeError`.
    """  # noqa

    when: core.When = core.Before
    vector_field: str = None
    document_fields: List[str] = None
    config_name: str = "pg_catalog.english"

    def __init__(
        self,
        *,
        name: str = None,
        vector_field: str = None,
        document_fields: List[str] = None,
        config_name: str = None,
    ):
        self.vector_field = vector_field or self.vector_field
        self.document_fields = document_fields or self.document_fields
        self.config_name = config_name or self.config_name

        if not self.vector_field:
            raise ValueError('Must provide "vector_field" to update search vector')

        if not self.document_fields:
            raise ValueError('Must provide "document_fields" to update search vector')

        if not self.config_name:  # pragma: no cover
            raise ValueError('Must provide "config_name" to update search vector')

        super().__init__(name=name, operation=core.Insert | core.UpdateOf(*document_fields))

    def ignore(self, model):
        raise RuntimeError(f"Cannot ignore {self.__class__.__name__} triggers")

    def get_func(self, model):
        return ""

    def render_execute(self, model):
        document_cols = [_get_column(model, field) for field in self.document_fields]
        rendered_document_cols = ", ".join(utils.quote(col) for col in document_cols)
        vector_col = _get_column(model, self.vector_field)
        return (
            f"tsvector_update_trigger({utils.quote(vector_col)},"
            f" {utils.quote(self.config_name)}, {rendered_document_cols})"
        )
