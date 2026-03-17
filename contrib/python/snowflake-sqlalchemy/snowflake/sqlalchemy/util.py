#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import re
from itertools import chain
from typing import Any
from urllib.parse import quote_plus

from sqlalchemy import exc, inspection, sql
from sqlalchemy.exc import NoForeignKeysError
from sqlalchemy.orm.interfaces import MapperProperty
from sqlalchemy.orm.util import _ORMJoin as sa_orm_util_ORMJoin
from sqlalchemy.orm.util import attributes
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql.base import _expand_cloned, _from_objects
from sqlalchemy.sql.elements import _find_columns
from sqlalchemy.sql.selectable import Join, Lateral, coercions, operators, roles

from snowflake.connector.compat import IS_STR
from snowflake.connector.connection import SnowflakeConnection
from snowflake.sqlalchemy import compat

from ._constants import (
    APPLICATION_NAME,
    PARAM_APPLICATION,
    PARAM_INTERNAL_APPLICATION_NAME,
    PARAM_INTERNAL_APPLICATION_VERSION,
    SNOWFLAKE_SQLALCHEMY_VERSION,
)


def _rfc_1738_quote(text):
    return re.sub(r"[:@/]", lambda m: "%%%X" % ord(m.group(0)), text)


def _url(**db_parameters):
    """
    Composes a SQLAlchemy connect string from the given database connection
    parameters.

    Password containing special characters (e.g., '@', '%') need to be encoded to be parsed correctly.
    Unescaped password containing special characters might lead to authentication failure.
    Please follow the instructions to encode the password:
    https://github.com/snowflakedb/snowflake-sqlalchemy#escaping-special-characters-such-as---signs-in-passwords
    """
    specified_parameters = []
    if "account" not in db_parameters:
        raise exc.ArgumentError("account parameter must be specified.")

    if "host" in db_parameters:
        ret = "snowflake://{user}:{password}@{host}:{port}/".format(
            user=db_parameters.get("user", ""),
            password=_rfc_1738_quote(db_parameters.get("password", "")),
            host=db_parameters["host"],
            port=db_parameters["port"] if "port" in db_parameters else 443,
        )
        specified_parameters += ["user", "password", "host", "port"]
    elif "region" not in db_parameters:
        ret = "snowflake://{user}:{password}@{account}/".format(
            account=db_parameters["account"],
            user=db_parameters.get("user", ""),
            password=_rfc_1738_quote(db_parameters.get("password", "")),
        )
        specified_parameters += ["user", "password", "account"]
    else:
        ret = "snowflake://{user}:{password}@{account}.{region}/".format(
            account=db_parameters["account"],
            user=db_parameters.get("user", ""),
            password=_rfc_1738_quote(db_parameters.get("password", "")),
            region=db_parameters["region"],
        )
        specified_parameters += ["user", "password", "account", "region"]

    if "database" in db_parameters:
        ret += quote_plus(db_parameters["database"])
        specified_parameters += ["database"]
        if "schema" in db_parameters:
            ret += "/" + quote_plus(db_parameters["schema"])
            specified_parameters += ["schema"]
    elif "schema" in db_parameters:
        raise exc.ArgumentError("schema cannot be specified without database")

    def sep(is_first_parameter):
        return "?" if is_first_parameter else "&"

    is_first_parameter = True
    for p in sorted(db_parameters.keys()):
        v = db_parameters[p]
        if p not in specified_parameters:
            encoded_value = quote_plus(v) if IS_STR(v) else str(v)
            ret += sep(is_first_parameter) + p + "=" + encoded_value
            is_first_parameter = False
    return ret


def _set_connection_interpolate_empty_sequences(
    dbapi_connection: SnowflakeConnection, flag: bool
) -> None:
    """set the _interpolate_empty_sequences config of the underlying connection"""
    if hasattr(dbapi_connection, "driver_connection"):
        # _dbapi_connection is a _ConnectionFairy which proxies raw SnowflakeConnection
        dbapi_connection.driver_connection._interpolate_empty_sequences = flag
    else:
        # _dbapi_connection is a raw SnowflakeConnection
        dbapi_connection._interpolate_empty_sequences = flag


def _update_connection_application_name(**conn_kwargs: Any) -> Any:
    if PARAM_APPLICATION not in conn_kwargs:
        conn_kwargs[PARAM_APPLICATION] = APPLICATION_NAME
    if PARAM_INTERNAL_APPLICATION_NAME not in conn_kwargs:
        conn_kwargs[PARAM_INTERNAL_APPLICATION_NAME] = APPLICATION_NAME
    if PARAM_INTERNAL_APPLICATION_VERSION not in conn_kwargs:
        conn_kwargs[PARAM_INTERNAL_APPLICATION_VERSION] = SNOWFLAKE_SQLALCHEMY_VERSION
    return conn_kwargs


def parse_url_boolean(value: str) -> bool:
    if value == "True":
        return True
    elif value == "False":
        return False
    else:
        raise ValueError(f"Invalid boolean value detected: '{value}'")


def parse_url_integer(value: str) -> int:
    try:
        return int(value)
    except ValueError as e:
        raise ValueError(f"Invalid int value detected: '{value}") from e


# handle Snowflake BCR bcr-1057
# the BCR impacts sqlalchemy.orm.context.ORMSelectCompileState and sqlalchemy.sql.selectable.SelectState
# which used the 'sqlalchemy.util.preloaded.sql_util.find_left_clause_to_join_from' method that
# can not handle the BCR change, we implement it in a way that lateral join does not need onclause
def _find_left_clause_to_join_from(clauses, join_to, onclause):
    """Given a list of FROM clauses, a selectable,
    and optional ON clause, return a list of integer indexes from the
    clauses list indicating the clauses that can be joined from.

    The presence of an "onclause" indicates that at least one clause can
    definitely be joined from; if the list of clauses is of length one
    and the onclause is given, returns that index.   If the list of clauses
    is more than length one, and the onclause is given, attempts to locate
    which clauses contain the same columns.

    """
    idx = []
    selectables = set(_from_objects(join_to))

    # if we are given more than one target clause to join
    # from, use the onclause to provide a more specific answer.
    # otherwise, don't try to limit, after all, "ON TRUE" is a valid
    # on clause
    if len(clauses) > 1 and onclause is not None:
        resolve_ambiguity = True
        cols_in_onclause = _find_columns(onclause)
    else:
        resolve_ambiguity = False
        cols_in_onclause = None

    for i, f in enumerate(clauses):
        for s in selectables.difference([f]):
            if resolve_ambiguity:
                if set(f.c).union(s.c).issuperset(cols_in_onclause):
                    idx.append(i)
                    break
            elif onclause is not None or Join._can_join(f, s):
                idx.append(i)
                break
            elif onclause is None and isinstance(s, Lateral):
                # in snowflake, onclause is not accepted for lateral due to BCR change:
                # https://docs.snowflake.com/en/release-notes/bcr-bundles/2023_04/bcr-1057
                # sqlalchemy only allows join with on condition.
                # to adapt to snowflake syntax change,
                # we make the change such that when oncaluse is None and the right part is
                # Lateral, we append the index indicating Lateral clause can be joined from with without onclause.
                idx.append(i)
                break

    if len(idx) > 1:
        # this is the same "hide froms" logic from
        # Selectable._get_display_froms
        toremove = set(chain(*[_expand_cloned(f._hide_froms) for f in clauses]))
        idx = [i for i in idx if clauses[i] not in toremove]

    # onclause was given and none of them resolved, so assume
    # all indexes can match
    if not idx and onclause is not None:
        return range(len(clauses))
    else:
        return idx


class _Snowflake_ORMJoin(sa_orm_util_ORMJoin):
    def __init__(
        self,
        left,
        right,
        onclause=None,
        isouter=False,
        full=False,
        _left_memo=None,
        _right_memo=None,
        _extra_criteria=(),
    ):
        left_info = inspection.inspect(left)

        right_info = inspection.inspect(right)
        adapt_to = right_info.selectable

        # used by joined eager loader
        self._left_memo = _left_memo
        self._right_memo = _right_memo

        # legacy, for string attr name ON clause.  if that's removed
        # then the "_joined_from_info" concept can go
        left_orm_info = getattr(left, "_joined_from_info", left_info)
        self._joined_from_info = right_info
        if isinstance(onclause, compat.string_types):
            onclause = getattr(left_orm_info.entity, onclause)
        # ####

        if isinstance(onclause, attributes.QueryableAttribute):
            on_selectable = onclause.comparator._source_selectable()
            prop = onclause.property
            _extra_criteria += onclause._extra_criteria
        elif isinstance(onclause, MapperProperty):
            # used internally by joined eager loader...possibly not ideal
            prop = onclause
            on_selectable = prop.parent.selectable
        else:
            prop = None

        if prop:
            left_selectable = left_info.selectable

            if sql_util.clause_is_present(on_selectable, left_selectable):
                adapt_from = on_selectable
            else:
                adapt_from = left_selectable

            (
                pj,
                sj,
                source,
                dest,
                secondary,
                target_adapter,
            ) = prop._create_joins(
                source_selectable=adapt_from,
                dest_selectable=adapt_to,
                source_polymorphic=True,
                of_type_entity=right_info,
                alias_secondary=True,
                extra_criteria=_extra_criteria,
            )

            if sj is not None:
                if isouter:
                    # note this is an inner join from secondary->right
                    right = sql.join(secondary, right, sj)
                    onclause = pj
                else:
                    left = sql.join(left, secondary, pj, isouter)
                    onclause = sj
            else:
                onclause = pj

            self._target_adapter = target_adapter

            # we don't use the normal coercions logic for _ORMJoin
            # (probably should), so do some gymnastics to get the entity.
            # logic here is for #8721, which was a major bug in 1.4
            # for almost two years, not reported/fixed until 1.4.43 (!)
            if left_info.is_selectable:
                parententity = left_selectable._annotations.get("parententity", None)
            elif left_info.is_mapper or left_info.is_aliased_class:
                parententity = left_info
            else:
                parententity = None

            if parententity is not None:
                self._annotations = self._annotations.union(
                    {"parententity": parententity}
                )

        augment_onclause = onclause is None and _extra_criteria
        # handle Snowflake BCR bcr-1057
        _Snowflake_Selectable_Join.__init__(self, left, right, onclause, isouter, full)

        if augment_onclause:
            self.onclause &= sql.and_(*_extra_criteria)

        if (
            not prop
            and getattr(right_info, "mapper", None)
            and right_info.mapper.single
        ):
            # if single inheritance target and we are using a manual
            # or implicit ON clause, augment it the same way we'd augment the
            # WHERE.
            single_crit = right_info.mapper._single_table_criterion
            if single_crit is not None:
                if right_info.is_aliased_class:
                    single_crit = right_info._adapter.traverse(single_crit)
                self.onclause = self.onclause & single_crit


class _Snowflake_Selectable_Join(Join):
    def __init__(self, left, right, onclause=None, isouter=False, full=False):
        """Construct a new :class:`_expression.Join`.

        The usual entrypoint here is the :func:`_expression.join`
        function or the :meth:`_expression.FromClause.join` method of any
        :class:`_expression.FromClause` object.

        """
        self.left = coercions.expect(roles.FromClauseRole, left, deannotate=True)
        self.right = coercions.expect(
            roles.FromClauseRole, right, deannotate=True
        ).self_group()

        if onclause is None:
            try:
                self.onclause = self._match_primaries(self.left, self.right)
            except NoForeignKeysError:
                # handle Snowflake BCR bcr-1057
                if isinstance(self.right, Lateral):
                    self.onclause = None
                else:
                    raise
        else:
            # note: taken from If91f61527236fd4d7ae3cad1f24c38be921c90ba
            # not merged yet
            self.onclause = coercions.expect(roles.OnClauseRole, onclause).self_group(
                against=operators._asbool
            )

        self.isouter = isouter
        self.full = full
