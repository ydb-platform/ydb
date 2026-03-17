import collections
import hashlib
from enum import Enum
from typing import Any, Optional, Sequence, Union

import pgtrigger
from pgtrigger import utils


class _Unset(Enum):
    token = 0


_unset = _Unset.token


class UpsertTriggerSql(collections.UserString):
    """SQL for inserting or updating a trigger

    This class is intended to be versionable since migrations
    reference it. Older migrations need to be able to point
    to earlier versions of the installation template used
    for triggers.
    """

    def get_template(self) -> str:
        """
        This is v1 of the installation template. Do NOT edit
        this template unless you are absolutely sure it is
        backwards compatible, otherwise it may affect migrations
        that reference it.

        If it does need to be changed, we will need to introduce
        a version variable to be backwards compatible.

        Note: Postgres 14 has CREATE OR REPLACE syntax that
        we might consider using. This SQL is executed in
        a transaction, so dropping and recreating shouldn't
        be a problem.
        """
        return """
            CREATE OR REPLACE FUNCTION {ignore_func_name}(
                trigger_name NAME
            )
            RETURNS BOOLEAN AS $$
                DECLARE
                    _pgtrigger_ignore TEXT[];
                    _result BOOLEAN;
                BEGIN
                    BEGIN
                        SELECT INTO _pgtrigger_ignore
                            CURRENT_SETTING('pgtrigger.ignore');
                        EXCEPTION WHEN OTHERS THEN
                    END;
                    IF _pgtrigger_ignore IS NOT NULL THEN
                        SELECT trigger_name = ANY(_pgtrigger_ignore)
                        INTO _result;
                        RETURN _result;
                    ELSE
                        RETURN FALSE;
                    END IF;
                END;
            $$ LANGUAGE plpgsql;

            CREATE OR REPLACE FUNCTION {pgid}()
            RETURNS TRIGGER AS $$
                {declare}
                BEGIN
                    IF ({ignore_func_name}(TG_NAME) IS TRUE) THEN
                        IF (TG_OP = 'DELETE') THEN
                            RETURN OLD;
                        ELSE
                            RETURN NEW;
                        END IF;
                    END IF;
                    {func}
                END;
            $$ LANGUAGE plpgsql;

            DROP TRIGGER IF EXISTS {pgid} ON {table};
            CREATE {constraint} TRIGGER {pgid}
                {when} {operation} ON {table}
                {timing}
                {referencing}
                FOR EACH {level} {condition}
                EXECUTE PROCEDURE {execute};

            COMMENT ON TRIGGER {pgid} ON {table} IS '{hash}';
        """

    def get_defaults(self, pgid: str) -> dict[str, str]:
        """
        These are the default values for the installation
        template. Do NOT edit these default values. Keys
        may be added, but existing keys should never be updated,
        otherwise existing migrations may no longer be correct.

        If it does need to be changed, we will need to introduce
        a version variable to be backwards compatible.
        """
        return {
            "ignore_func_name": '"public"._pgtrigger_should_ignore',
            "declare": "",
            "constraint": "",
            "timing": "",
            "referencing": "",
            "level": "ROW",
            "condition": "",
            "execute": f"{pgid}()",
        }

    def __init__(
        self,
        *,
        ignore_func_name: Union[str, _Unset] = _unset,
        pgid: str,
        declare: Union[str, _Unset] = _unset,
        func: str,
        table: str,
        constraint: Union[str, _Unset] = _unset,
        when: Union["pgtrigger.core.When", str, None],
        operation: Union["pgtrigger.core.Operation", str, None],
        timing: Union[str, _Unset] = _unset,
        referencing: Union["pgtrigger.core.Referencing", str, _Unset] = _unset,
        level: Union["pgtrigger.core.Level", str, _Unset] = _unset,
        condition: Union[str, _Unset] = _unset,
        execute: Union[str, _Unset] = _unset,
        hash: Optional[str] = None,
    ):
        """Initialize the SQL and store it in the `.data` attribute."""
        self.kwargs = {
            key: str(val)
            for key, val in locals().items()
            if key not in ("self", "hash") and val is not _unset
        }
        self.defaults = self.get_defaults(pgid)
        sql_args = {**self.defaults, **self.kwargs, **{"table": utils.quote(table)}}

        self.hash = (
            hash
            or hashlib.sha1(
                self.get_template().format(**{**sql_args, **{"hash": ""}}).encode()
            ).hexdigest()
        )
        self.data = self.get_template().format(**{**sql_args, **{"hash": self.hash}})
        self.pgid = pgid
        self.table = table

    def deconstruct(self) -> tuple[str, Sequence[Any], dict[str, Any]]:
        """
        Serialize the construction of this class so that it can be used in migrations.
        """
        kwargs = {
            key: val for key, val in self.kwargs.items() if self.defaults.get(key, _unset) != val
        }

        path = f"{self.__class__.__module__}.{self.__class__.__name__}"
        return path, [], {**kwargs, **{"hash": self.hash}}


class _TriggerDdlSql(collections.UserString):
    def get_template(self) -> str:
        raise NotImplementedError

    def __init__(self, *, pgid: str, table: str) -> None:
        """Initialize the SQL and store it in the `.data` attribute."""
        sql_args = {**locals(), **{"table": utils.quote(table)}}

        self.data = self.get_template().format(**sql_args)


class DropTriggerSql(_TriggerDdlSql):
    """SQL for dropping a trigger

    Triggers are dropped in migrations, so any edits to
    the drop trigger template should be backwards compatible
    or versioned. I.e. older migrations need to always point to
    the SQL here
    """

    def get_template(self) -> str:
        return "DROP TRIGGER IF EXISTS {pgid} ON {table};"


class EnableTriggerSql(_TriggerDdlSql):
    """SQL for enabling a trigger

    We don't currently perform enabling/disabling in migrations,
    so this SQL can be changed without consequences to past
    migrations.
    """

    def get_template(self) -> str:
        return "ALTER TABLE {table} ENABLE TRIGGER {pgid};"


class DisableTriggerSql(_TriggerDdlSql):
    """SQL for disabling a trigger

    We don't currently perform enabling/disabling in migrations,
    so this SQL can be changed without consequences to past
    migrations.
    """

    def get_template(self) -> str:
        return "ALTER TABLE {table} DISABLE TRIGGER {pgid};"


class Trigger:
    """
    A compiled trigger that's added to internal model state of migrations. It consists
    of a name and the trigger SQL for migrations.
    """

    def __init__(self, *, name: Optional[str], sql: UpsertTriggerSql) -> None:
        self.name = name
        self.sql = sql
        assert isinstance(sql, UpsertTriggerSql)

    def __eq__(self, other):
        return (
            self.__class__ == other.__class__ and self.name == other.name and self.sql == other.sql
        )

    @property
    def install_sql(self) -> str:
        return str(self.sql)

    @property
    def uninstall_sql(self) -> str:
        return str(DropTriggerSql(pgid=self.sql.pgid, table=self.sql.table))

    @property
    def enable_sql(self) -> str:
        return str(EnableTriggerSql(pgid=self.sql.pgid, table=self.sql.table))

    @property
    def disable_sql(self) -> str:
        return str(DisableTriggerSql(pgid=self.sql.pgid, table=self.sql.table))

    @property
    def hash(self) -> str:
        return self.sql.hash

    def deconstruct(self) -> tuple[str, Sequence[Any], dict[str, Any]]:
        """
        Serialize the construction of this class so that it can be used in migrations.
        """
        path = f"{self.__class__.__module__}.{self.__class__.__name__}"
        return path, [], {"name": self.name, "sql": self.sql}
