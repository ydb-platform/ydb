#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import itertools
import operator
import re

from sqlalchemy import exc as sa_exc
from sqlalchemy import inspect, sql
from sqlalchemy import util as sa_util
from sqlalchemy.engine import default
from sqlalchemy.orm import context
from sqlalchemy.orm.context import _MapperEntity
from sqlalchemy.schema import Sequence, Table
from sqlalchemy.sql import compiler, expression, functions
from sqlalchemy.sql.base import CompileState
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy.sql.selectable import Lateral, SelectState

from .compat import IS_VERSION_20, args_reducer, string_types
from .custom_commands import AWSBucket, AzureContainer, ExternalStage
from .functions import flatten
from .util import (
    _find_left_clause_to_join_from,
    _set_connection_interpolate_empty_sequences,
    _Snowflake_ORMJoin,
    _Snowflake_Selectable_Join,
)

RESERVED_WORDS = frozenset(
    [
        "ALL",  # ANSI Reserved words
        "ALTER",
        "AND",
        "ANY",
        "AS",
        "BETWEEN",
        "BY",
        "CHECK",
        "COLUMN",
        "CONNECT",
        "COPY",
        "CREATE",
        "CURRENT",
        "DELETE",
        "DISTINCT",
        "DROP",
        "ELSE",
        "EXISTS",
        "FOR",
        "FROM",
        "GRANT",
        "GROUP",
        "HAVING",
        "IN",
        "INSERT",
        "INTERSECT",
        "INTO",
        "IS",
        "LIKE",
        "NOT",
        "NULL",
        "OF",
        "ON",
        "OR",
        "ORDER",
        "REVOKE",
        "ROW",
        "ROWS",
        "SAMPLE",
        "SELECT",
        "SET",
        "START",
        "TABLE",
        "THEN",
        "TO",
        "TRIGGER",
        "UNION",
        "UNIQUE",
        "UPDATE",
        "VALUES",
        "WHENEVER",
        "WHERE",
        "WITH",
        "REGEXP",
        "RLIKE",
        "SOME",  # Snowflake Reserved words
        "MINUS",
        "INCREMENT",  # Oracle reserved words
    ]
)

# Snowflake DML:
# - UPDATE
# - INSERT
# - DELETE
# - MERGE
AUTOCOMMIT_REGEXP = re.compile(
    r"\s*(?:UPDATE|INSERT|DELETE|MERGE|COPY)", re.I | re.UNICODE
)


"""
Overwrite methods to handle Snowflake BCR change:
https://docs.snowflake.com/en/release-notes/bcr-bundles/2023_04/bcr-1057
- _join_determine_implicit_left_side
- _join_left_to_right
"""


# handle Snowflake BCR bcr-1057
@CompileState.plugin_for("default", "select")
class SnowflakeSelectState(SelectState):
    def _setup_joins(self, args, raw_columns):
        for right, onclause, left, flags in args:
            isouter = flags["isouter"]
            full = flags["full"]

            if left is None:
                (
                    left,
                    replace_from_obj_index,
                ) = self._join_determine_implicit_left_side(
                    raw_columns, left, right, onclause
                )
            else:
                (replace_from_obj_index) = self._join_place_explicit_left_side(left)

            if replace_from_obj_index is not None:
                # splice into an existing element in the
                # self._from_obj list
                left_clause = self.from_clauses[replace_from_obj_index]

                self.from_clauses = (
                    self.from_clauses[:replace_from_obj_index]
                    + (
                        _Snowflake_Selectable_Join(  # handle Snowflake BCR bcr-1057
                            left_clause,
                            right,
                            onclause,
                            isouter=isouter,
                            full=full,
                        ),
                    )
                    + self.from_clauses[replace_from_obj_index + 1 :]
                )
            else:
                self.from_clauses = self.from_clauses + (
                    # handle Snowflake BCR bcr-1057
                    _Snowflake_Selectable_Join(
                        left, right, onclause, isouter=isouter, full=full
                    ),
                )

    @sa_util.preload_module("sqlalchemy.sql.util")
    def _join_determine_implicit_left_side(self, raw_columns, left, right, onclause):
        """When join conditions don't express the left side explicitly,
        determine if an existing FROM or entity in this query
        can serve as the left hand side.

        """

        replace_from_obj_index = None

        from_clauses = self.from_clauses

        if from_clauses:
            # handle Snowflake BCR bcr-1057
            indexes = _find_left_clause_to_join_from(from_clauses, right, onclause)

            if len(indexes) == 1:
                replace_from_obj_index = indexes[0]
                left = from_clauses[replace_from_obj_index]
        else:
            potential = {}
            statement = self.statement

            for from_clause in itertools.chain(
                itertools.chain.from_iterable(
                    [element._from_objects for element in raw_columns]
                ),
                itertools.chain.from_iterable(
                    [element._from_objects for element in statement._where_criteria]
                ),
            ):

                potential[from_clause] = ()

            all_clauses = list(potential.keys())
            # handle Snowflake BCR bcr-1057
            indexes = _find_left_clause_to_join_from(all_clauses, right, onclause)

            if len(indexes) == 1:
                left = all_clauses[indexes[0]]

        if len(indexes) > 1:
            raise sa_exc.InvalidRequestError(
                "Can't determine which FROM clause to join "
                "from, there are multiple FROMS which can "
                "join to this entity. Please use the .select_from() "
                "method to establish an explicit left side, as well as "
                "providing an explicit ON clause if not present already to "
                "help resolve the ambiguity."
            )
        elif not indexes:
            raise sa_exc.InvalidRequestError(
                "Don't know how to join to %r. "
                "Please use the .select_from() "
                "method to establish an explicit left side, as well as "
                "providing an explicit ON clause if not present already to "
                "help resolve the ambiguity." % (right,)
            )
        return left, replace_from_obj_index


# handle Snowflake BCR bcr-1057
@sql.base.CompileState.plugin_for("orm", "select")
class SnowflakeORMSelectCompileState(context.ORMSelectCompileState):
    def _join_determine_implicit_left_side(
        self, entities_collection, left, right, onclause
    ):
        """When join conditions don't express the left side explicitly,
        determine if an existing FROM or entity in this query
        can serve as the left hand side.

        """

        # when we are here, it means join() was called without an ORM-
        # specific way of telling us what the "left" side is, e.g.:
        #
        # join(RightEntity)
        #
        # or
        #
        # join(RightEntity, RightEntity.foo == LeftEntity.bar)
        #

        r_info = inspect(right)

        replace_from_obj_index = use_entity_index = None

        if self.from_clauses:
            # we have a list of FROMs already.  So by definition this
            # join has to connect to one of those FROMs.

            # handle Snowflake BCR bcr-1057
            indexes = _find_left_clause_to_join_from(
                self.from_clauses, r_info.selectable, onclause
            )

            if len(indexes) == 1:
                replace_from_obj_index = indexes[0]
                left = self.from_clauses[replace_from_obj_index]
            elif len(indexes) > 1:
                raise sa_exc.InvalidRequestError(
                    "Can't determine which FROM clause to join "
                    "from, there are multiple FROMS which can "
                    "join to this entity. Please use the .select_from() "
                    "method to establish an explicit left side, as well as "
                    "providing an explicit ON clause if not present already "
                    "to help resolve the ambiguity."
                )
            else:
                raise sa_exc.InvalidRequestError(
                    "Don't know how to join to %r. "
                    "Please use the .select_from() "
                    "method to establish an explicit left side, as well as "
                    "providing an explicit ON clause if not present already "
                    "to help resolve the ambiguity." % (right,)
                )

        elif entities_collection:
            # we have no explicit FROMs, so the implicit left has to
            # come from our list of entities.

            potential = {}
            for entity_index, ent in enumerate(entities_collection):
                entity = ent.entity_zero_or_selectable
                if entity is None:
                    continue
                ent_info = inspect(entity)
                if ent_info is r_info:  # left and right are the same, skip
                    continue

                # by using a dictionary with the selectables as keys this
                # de-duplicates those selectables as occurs when the query is
                # against a series of columns from the same selectable
                if isinstance(ent, context._MapperEntity):
                    potential[ent.selectable] = (entity_index, entity)
                else:
                    potential[ent_info.selectable] = (None, entity)

            all_clauses = list(potential.keys())
            # handle Snowflake BCR bcr-1057
            indexes = _find_left_clause_to_join_from(
                all_clauses, r_info.selectable, onclause
            )

            if len(indexes) == 1:
                use_entity_index, left = potential[all_clauses[indexes[0]]]
            elif len(indexes) > 1:
                raise sa_exc.InvalidRequestError(
                    "Can't determine which FROM clause to join "
                    "from, there are multiple FROMS which can "
                    "join to this entity. Please use the .select_from() "
                    "method to establish an explicit left side, as well as "
                    "providing an explicit ON clause if not present already "
                    "to help resolve the ambiguity."
                )
            else:
                raise sa_exc.InvalidRequestError(
                    "Don't know how to join to %r. "
                    "Please use the .select_from() "
                    "method to establish an explicit left side, as well as "
                    "providing an explicit ON clause if not present already "
                    "to help resolve the ambiguity." % (right,)
                )
        else:
            raise sa_exc.InvalidRequestError(
                "No entities to join from; please use "
                "select_from() to establish the left "
                "entity/selectable of this join"
            )

        return left, replace_from_obj_index, use_entity_index

    @args_reducer(positions_to_drop=(6, 7))
    def _join_left_to_right(
        self, entities_collection, left, right, onclause, prop, outerjoin, full
    ):
        """given raw "left", "right", "onclause" parameters consumed from
        a particular key within _join(), add a real ORMJoin object to
        our _from_obj list (or augment an existing one)

        """

        if left is None:
            # left not given (e.g. no relationship object/name specified)
            # figure out the best "left" side based on our existing froms /
            # entities
            assert prop is None
            (
                left,
                replace_from_obj_index,
                use_entity_index,
            ) = self._join_determine_implicit_left_side(
                entities_collection, left, right, onclause
            )
        else:
            # left is given via a relationship/name, or as explicit left side.
            # Determine where in our
            # "froms" list it should be spliced/appended as well as what
            # existing entity it corresponds to.
            (
                replace_from_obj_index,
                use_entity_index,
            ) = self._join_place_explicit_left_side(entities_collection, left)

        if left is right:
            raise sa_exc.InvalidRequestError(
                "Can't construct a join from %s to %s, they "
                "are the same entity" % (left, right)
            )

        # the right side as given often needs to be adapted.  additionally
        # a lot of things can be wrong with it.  handle all that and
        # get back the new effective "right" side

        if IS_VERSION_20:
            r_info, right, onclause = self._join_check_and_adapt_right_side(
                left, right, onclause, prop
            )
        else:
            r_info, right, onclause = self._join_check_and_adapt_right_side(
                left, right, onclause, prop, False, False
            )

        if not r_info.is_selectable:
            extra_criteria = self._get_extra_criteria(r_info)
        else:
            extra_criteria = ()

        if replace_from_obj_index is not None:
            # splice into an existing element in the
            # self._from_obj list
            left_clause = self.from_clauses[replace_from_obj_index]

            self.from_clauses = (
                self.from_clauses[:replace_from_obj_index]
                + [
                    _Snowflake_ORMJoin(  # handle Snowflake BCR bcr-1057
                        left_clause,
                        right,
                        onclause,
                        isouter=outerjoin,
                        full=full,
                        _extra_criteria=extra_criteria,
                    )
                ]
                + self.from_clauses[replace_from_obj_index + 1 :]
            )
        else:
            # add a new element to the self._from_obj list
            if use_entity_index is not None:
                # make use of _MapperEntity selectable, which is usually
                # entity_zero.selectable, but if with_polymorphic() were used
                # might be distinct
                assert isinstance(entities_collection[use_entity_index], _MapperEntity)
                left_clause = entities_collection[use_entity_index].selectable
            else:
                left_clause = left

            self.from_clauses = self.from_clauses + [
                _Snowflake_ORMJoin(  # handle Snowflake BCR bcr-1057
                    left_clause,
                    r_info,
                    onclause,
                    isouter=outerjoin,
                    full=full,
                    _extra_criteria=extra_criteria,
                )
            ]


class SnowflakeIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = {x.lower() for x in RESERVED_WORDS}

    def __init__(self, dialect, **kw):
        quote = '"'

        super().__init__(dialect, initial_quote=quote, escape_quote=quote)

    def _quote_free_identifiers(self, *ids):
        """
        Unilaterally identifier-quote any number of strings.
        """
        return tuple(self.quote(i) for i in ids if i is not None)

    def quote_schema(self, schema, force=None):
        """
        Split schema by a dot and merge with required quotes
        """
        idents = self._split_schema_by_dot(schema)
        return ".".join(self._quote_free_identifiers(*idents))

    def format_label(self, label, name=None):
        n = name or label.name
        s = n.replace(self.escape_quote, "")

        if not isinstance(n, quoted_name) or n.quote is None:
            return self.quote(s)

        return self.quote_identifier(s) if n.quote else s

    def _split_schema_by_dot(self, schema):
        ret = []
        idx = 0
        pre_idx = 0
        in_quote = False
        while idx < len(schema):
            if not in_quote:
                if schema[idx] == "." and pre_idx < idx:
                    ret.append(schema[pre_idx:idx])
                    pre_idx = idx + 1
                elif schema[idx] == '"':
                    in_quote = True
                    pre_idx = idx + 1
            else:
                if schema[idx] == '"' and pre_idx < idx:
                    ret.append(schema[pre_idx:idx])
                    in_quote = False
                    pre_idx = idx + 1
            idx += 1
            if pre_idx < len(schema) and schema[pre_idx] == ".":
                pre_idx += 1
        if pre_idx < idx:
            ret.append(schema[pre_idx:idx])

        # convert the returning strings back to quoted_name types, and assign the original 'quote' attribute on it
        quoted_ret = [
            quoted_name(value, quote=getattr(schema, "quote", None)) for value in ret
        ]

        return quoted_ret


class SnowflakeCompiler(compiler.SQLCompiler):
    def visit_sequence(self, sequence, **kw):
        return self.dialect.identifier_preparer.format_sequence(sequence) + ".nextval"

    def visit_now_func(self, now, **kw):
        return "CURRENT_TIMESTAMP"

    def visit_merge_into(self, merge_into, **kw):
        clauses = " ".join(
            clause._compiler_dispatch(self, **kw) for clause in merge_into.clauses
        )
        return (
            f"MERGE INTO {merge_into.target} USING {merge_into.source} ON {merge_into.on}"
            + (" " + clauses if clauses else "")
        )

    def visit_merge_into_clause(self, merge_into_clause, **kw):
        case_predicate = (
            f" AND {str(merge_into_clause.predicate._compiler_dispatch(self, **kw))}"
            if merge_into_clause.predicate is not None
            else ""
        )
        if merge_into_clause.command == "INSERT":
            sets, sets_tos = zip(*merge_into_clause.set.items())
            sets, sets_tos = list(sets), list(sets_tos)
            if kw.get("deterministic", False):
                sets, sets_tos = zip(
                    *sorted(merge_into_clause.set.items(), key=operator.itemgetter(0))
                )
            return "WHEN NOT MATCHED{} THEN {} ({}) VALUES ({})".format(
                case_predicate,
                merge_into_clause.command,
                ", ".join(sets),
                ", ".join(map(lambda e: e._compiler_dispatch(self, **kw), sets_tos)),
            )
        else:
            set_list = list(merge_into_clause.set.items())
            if kw.get("deterministic", False):
                set_list.sort(key=operator.itemgetter(0))
            sets = (
                ", ".join(
                    [
                        f"{set[0]} = {set[1]._compiler_dispatch(self, **kw)}"
                        for set in set_list
                    ]
                )
                if merge_into_clause.set
                else ""
            )
            return "WHEN MATCHED{} THEN {}{}".format(
                case_predicate,
                merge_into_clause.command,
                " SET %s" % sets if merge_into_clause.set else "",
            )

    def visit_copy_into(self, copy_into, **kw):
        if hasattr(copy_into, "formatter") and copy_into.formatter is not None:
            formatter = copy_into.formatter._compiler_dispatch(self, **kw)
        else:
            formatter = ""
        into = (
            copy_into.into
            if isinstance(copy_into.into, Table)
            else copy_into.into._compiler_dispatch(self, **kw)
        )
        from_ = None
        if isinstance(copy_into.from_, Table):
            from_ = copy_into.from_
        # this is intended to catch AWSBucket and AzureContainer
        elif (
            isinstance(copy_into.from_, AWSBucket)
            or isinstance(copy_into.from_, AzureContainer)
            or isinstance(copy_into.from_, ExternalStage)
        ):
            from_ = copy_into.from_._compiler_dispatch(self, **kw)
        # everything else (selects, etc.)
        else:
            from_ = f"({copy_into.from_._compiler_dispatch(self, **kw)})"
        credentials, encryption = "", ""
        if isinstance(into, tuple):
            into, credentials, encryption = into
        elif isinstance(from_, tuple):
            from_, credentials, encryption = from_
        options_list = list(copy_into.copy_options.items())
        if kw.get("deterministic", False):
            options_list.sort(key=operator.itemgetter(0))
        options = (
            (
                " "
                + " ".join(
                    [
                        "{} = {}".format(
                            n,
                            (
                                v._compiler_dispatch(self, **kw)
                                if getattr(v, "compiler_dispatch", False)
                                else str(v)
                            ),
                        )
                        for n, v in options_list
                    ]
                )
            )
            if copy_into.copy_options
            else ""
        )
        if credentials:
            options += f" {credentials}"
        if encryption:
            options += f" {encryption}"
        return f"COPY INTO {into} FROM {from_} {formatter}{options}"

    def visit_copy_formatter(self, formatter, **kw):
        options_list = list(formatter.options.items())
        if kw.get("deterministic", False):
            options_list.sort(key=operator.itemgetter(0))
        if "format_name" in formatter.options:
            return f"FILE_FORMAT=(format_name = {formatter.options['format_name']})"
        return "FILE_FORMAT=(TYPE={}{})".format(
            formatter.file_format,
            (
                " "
                + " ".join(
                    [
                        "{}={}".format(
                            name,
                            (
                                value._compiler_dispatch(self, **kw)
                                if hasattr(value, "_compiler_dispatch")
                                else formatter.value_repr(name, value)
                            ),
                        )
                        for name, value in options_list
                    ]
                )
                if formatter.options
                else ""
            ),
        )

    def visit_aws_bucket(self, aws_bucket, **kw):
        credentials_list = list(aws_bucket.credentials_used.items())
        if kw.get("deterministic", False):
            credentials_list.sort(key=operator.itemgetter(0))
        credentials = "CREDENTIALS=({})".format(
            " ".join(f"{n}='{v}'" for n, v in credentials_list)
        )
        encryption_list = list(aws_bucket.encryption_used.items())
        if kw.get("deterministic", False):
            encryption_list.sort(key=operator.itemgetter(0))
        encryption = "ENCRYPTION=({})".format(
            " ".join(
                ("{}='{}'" if isinstance(v, string_types) else "{}={}").format(n, v)
                for n, v in encryption_list
            )
        )
        uri = "'s3://{}{}'".format(
            aws_bucket.bucket, f"/{aws_bucket.path}" if aws_bucket.path else ""
        )
        return (
            uri,
            credentials if aws_bucket.credentials_used else "",
            encryption if aws_bucket.encryption_used else "",
        )

    def visit_azure_container(self, azure_container, **kw):
        credentials_list = list(azure_container.credentials_used.items())
        if kw.get("deterministic", False):
            credentials_list.sort(key=operator.itemgetter(0))
        credentials = "CREDENTIALS=({})".format(
            " ".join(f"{n}='{v}'" for n, v in credentials_list)
        )
        encryption_list = list(azure_container.encryption_used.items())
        if kw.get("deterministic", False):
            encryption_list.sort(key=operator.itemgetter(0))
        encryption = "ENCRYPTION=({})".format(
            " ".join(
                f"{n}='{v}'" if isinstance(v, string_types) else f"{n}={v}"
                for n, v in encryption_list
            )
        )
        uri = "'azure://{}.blob.core.windows.net/{}{}'".format(
            azure_container.account,
            azure_container.container,
            f"/{azure_container.path}" if azure_container.path else "",
        )
        return (
            uri,
            credentials if azure_container.credentials_used else "",
            encryption if azure_container.encryption_used else "",
        )

    def visit_external_stage(self, external_stage, **kw):
        if external_stage.file_format is None:
            return (
                f"@{external_stage.namespace}{external_stage.name}{external_stage.path}"
            )
        return f"@{external_stage.namespace}{external_stage.name}{external_stage.path} (file_format => {external_stage.file_format})"

    def delete_extra_from_clause(
        self, delete_stmt, from_table, extra_froms, from_hints, **kw
    ):
        return "USING " + ", ".join(
            t._compiler_dispatch(self, asfrom=True, fromhints=from_hints, **kw)
            for t in extra_froms
        )

    def update_from_clause(
        self, update_stmt, from_table, extra_froms, from_hints, **kw
    ):
        return "FROM " + ", ".join(
            t._compiler_dispatch(self, asfrom=True, fromhints=from_hints, **kw)
            for t in extra_froms
        )

    def _get_regexp_args(self, binary, kw):
        string = self.process(binary.left, **kw)
        pattern = self.process(binary.right, **kw)
        flags = binary.modifiers["flags"]
        if flags is not None:
            flags = self.process(flags, **kw)
        return string, pattern, flags

    def visit_regexp_match_op_binary(self, binary, operator, **kw):
        string, pattern, flags = self._get_regexp_args(binary, kw)
        if flags is None:
            return f"REGEXP_LIKE({string}, {pattern})"
        else:
            return f"REGEXP_LIKE({string}, {pattern}, {flags})"

    def visit_regexp_replace_op_binary(self, binary, operator, **kw):
        string, pattern, flags = self._get_regexp_args(binary, kw)
        try:
            replacement = self.process(binary.modifiers["replacement"], **kw)
        except KeyError:
            # in sqlalchemy 1.4.49, the internal structure of the expression is changed
            # that binary.modifiers doesn't have "replacement":
            # https://docs.sqlalchemy.org/en/20/changelog/changelog_14.html#change-1.4.49
            return f"REGEXP_REPLACE({string}, {pattern}{'' if flags is None else f', {flags}'})"

        if flags is None:
            return f"REGEXP_REPLACE({string}, {pattern}, {replacement})"
        else:
            return f"REGEXP_REPLACE({string}, {pattern}, {replacement}, {flags})"

    def visit_not_regexp_match_op_binary(self, binary, operator, **kw):
        return f"NOT {self.visit_regexp_match_op_binary(binary, operator, **kw)}"

    def visit_join(self, join, asfrom=False, from_linter=None, **kwargs):
        if from_linter:
            from_linter.edges.update(
                itertools.product(join.left._from_objects, join.right._from_objects)
            )

        if join.full:
            join_type = " FULL OUTER JOIN "
        elif join.isouter:
            join_type = " LEFT OUTER JOIN "
        else:
            join_type = " JOIN "

        join_statement = (
            join.left._compiler_dispatch(
                self, asfrom=True, from_linter=from_linter, **kwargs
            )
            + join_type
            + join.right._compiler_dispatch(
                self, asfrom=True, from_linter=from_linter, **kwargs
            )
        )

        if join.onclause is None and isinstance(join.right, Lateral):
            # in snowflake, onclause is not accepted for lateral due to BCR change:
            # https://docs.snowflake.com/en/release-notes/bcr-bundles/2023_04/bcr-1057
            # sqlalchemy only allows join with on condition.
            # to adapt to snowflake syntax change,
            # we make the change such that when oncaluse is None and the right part is
            # Lateral, we do not append the on condition
            return join_statement

        return (
            join_statement
            + " ON "
            # TODO: likely need asfrom=True here?
            + join.onclause._compiler_dispatch(self, from_linter=from_linter, **kwargs)
        )

    def render_literal_value(self, value, type_):
        # escape backslash
        return super().render_literal_value(value, type_).replace("\\", "\\\\")


class SnowflakeExecutionContext(default.DefaultExecutionContext):
    INSERT_SQL_RE = re.compile(r"^insert\s+into", flags=re.IGNORECASE)

    def fire_sequence(self, seq, type_):
        return self._execute_scalar(
            f"SELECT {self.identifier_preparer.format_sequence(seq)}.nextval",
            type_,
        )

    def should_autocommit_text(self, statement):
        return AUTOCOMMIT_REGEXP.match(statement)

    @sa_util.memoized_property
    def should_autocommit(self):
        autocommit = self.execution_options.get(
            "autocommit",
            not self.compiled
            and self.statement
            and expression.PARSE_AUTOCOMMIT
            or False,
        )

        if autocommit is expression.PARSE_AUTOCOMMIT:
            return self.should_autocommit_text(self.unicode_statement)
        else:
            return autocommit and not self.isddl

    def pre_exec(self):
        if self.compiled and self.identifier_preparer._double_percents:
            # for compiled statements, percent is doubled for escape, we turn on _interpolate_empty_sequences
            _set_connection_interpolate_empty_sequences(self._dbapi_connection, True)

            # if the statement is executemany insert, setting _interpolate_empty_sequences to True is not enough,
            # because executemany pre-processes the param binding and then pass None params to execute so
            # _interpolate_empty_sequences condition not getting met for the command.
            # Therefore, we manually revert the escape percent in the command here
            if self.executemany and self.INSERT_SQL_RE.match(self.statement):
                self.statement = self.statement.replace("%%", "%")
        else:
            # for other cases, do no interpolate empty sequences as "%" is not double escaped
            _set_connection_interpolate_empty_sequences(self._dbapi_connection, False)

    def post_exec(self):
        if self.compiled and self.identifier_preparer._double_percents:
            # for compiled statements, percent is doubled for escapeafter execution
            # we reset _interpolate_empty_sequences to false which is turned on in pre_exec
            _set_connection_interpolate_empty_sequences(self._dbapi_connection, False)

    @property
    def rowcount(self):
        return self.cursor.rowcount


class SnowflakeDDLCompiler(compiler.DDLCompiler):
    def denormalize_column_name(self, name):
        if name is None:
            return None
        elif name.lower() == name and not self.preparer._requires_quotes(name.lower()):
            # no quote as case insensitive
            return name
        return self.preparer.quote(name)

    def get_column_specification(self, column, **kwargs):
        """
        Gets Column specifications
        """
        colspec = [
            self.preparer.format_column(column),
            self.dialect.type_compiler.process(column.type, type_expression=column),
        ]

        has_identity = (
            column.identity is not None and self.dialect.supports_identity_columns
        )

        if not column.nullable:
            colspec.append("NOT NULL")

        default = self.get_column_default_string(column)
        if default is not None:
            colspec.append("DEFAULT " + default)

        # TODO: This makes the first INTEGER column AUTOINCREMENT.
        # But the column is not really considered so unless
        # postfetch_lastrowid is enabled. But it is very unlikely to happen...
        if (
            column.table is not None
            and column is column.table._autoincrement_column
            and column.server_default is None
        ):
            if isinstance(column.default, Sequence):
                colspec.append(
                    f"DEFAULT {self.dialect.identifier_preparer.format_sequence(column.default)}.nextval"
                )
            else:
                colspec.append("AUTOINCREMENT")

        if has_identity:
            colspec.append(self.process(column.identity))

        return " ".join(colspec)

    def post_create_table(self, table):
        """
        Handles snowflake-specific ``CREATE TABLE ... CLUSTER BY`` syntax.

        Users can specify the `clusterby` property per table
        using the dialect specific syntax.
        For example, to specify a cluster by key you apply the following:

        >>> import sqlalchemy as sa
        >>> from sqlalchemy.schema import CreateTable
        >>> engine = sa.create_engine('snowflake://om1')
        >>> metadata = sa.MetaData()
        >>> user = sa.Table(
        ...     'user',
        ...     metadata,
        ...     sa.Column('id', sa.Integer, primary_key=True),
        ...     sa.Column('name', sa.String),
        ...     snowflake_clusterby=['id', 'name']
        ... )
        >>> print(CreateTable(user).compile(engine))
        <BLANKLINE>
        CREATE TABLE "user" (
            id INTEGER NOT NULL AUTOINCREMENT,
            name VARCHAR,
            PRIMARY KEY (id)
        ) CLUSTER BY (id, name)
        <BLANKLINE>
        <BLANKLINE>
        """
        text = ""
        info = table.dialect_options["snowflake"]
        cluster = info.get("clusterby")
        if cluster:
            text += " CLUSTER BY ({})".format(
                ", ".join(self.denormalize_column_name(key) for key in cluster)
            )
        return text

    def visit_create_stage(self, create_stage, **kw):
        """
        This visitor will create the SQL representation for a CREATE STAGE command.
        """
        return "CREATE {or_replace}{temporary}STAGE {}{} URL={}".format(
            create_stage.stage.namespace,
            create_stage.stage.name,
            repr(create_stage.container),
            or_replace="OR REPLACE " if create_stage.replace_if_exists else "",
            temporary="TEMPORARY " if create_stage.temporary else "",
        )

    def visit_create_file_format(self, file_format, **kw):
        """
        This visitor will create the SQL representation for a CREATE FILE FORMAT
        command.
        """
        return "CREATE {}FILE FORMAT {} TYPE='{}' {}".format(
            "OR REPLACE " if file_format.replace_if_exists else "",
            file_format.format_name,
            file_format.formatter.file_format,
            " ".join(
                [
                    f"{name} = {file_format.formatter.value_repr(name, value)}"
                    for name, value in file_format.formatter.options.items()
                ]
            ),
        )

    def visit_drop_table_comment(self, drop, **kw):
        """Snowflake does not support setting table comments as NULL.

        Reflection has to account for this and convert any empty comments to NULL.
        """
        table_name = self.preparer.format_table(drop.element)
        return f"COMMENT ON TABLE {table_name} IS ''"

    def visit_drop_column_comment(self, drop, **kw):
        """Snowflake does not support directly setting column comments as NULL.

        Instead we are forced to use the ALTER COLUMN ... UNSET COMMENT instead.
        """
        return "ALTER TABLE {} ALTER COLUMN {} UNSET COMMENT".format(
            self.preparer.format_table(drop.element.table),
            self.preparer.format_column(drop.element),
        )

    def visit_identity_column(self, identity, **kw):
        text = "IDENTITY"
        if identity.start is not None or identity.increment is not None:
            start = 1 if identity.start is None else identity.start
            increment = 1 if identity.increment is None else identity.increment
            text += f"({start},{increment})"
        if identity.order is not None:
            order = "ORDER" if identity.order else "NOORDER"
            text += f" {order}"
        return text

    def get_identity_options(self, identity_options):
        text = []
        if identity_options.increment is not None:
            text.append("INCREMENT BY %d" % identity_options.increment)
        if identity_options.start is not None:
            text.append("START WITH %d" % identity_options.start)
        if identity_options.minvalue is not None:
            text.append("MINVALUE %d" % identity_options.minvalue)
        if identity_options.maxvalue is not None:
            text.append("MAXVALUE %d" % identity_options.maxvalue)
        if identity_options.nominvalue is not None:
            text.append("NO MINVALUE")
        if identity_options.nomaxvalue is not None:
            text.append("NO MAXVALUE")
        if identity_options.cache is not None:
            text.append("CACHE %d" % identity_options.cache)
        if identity_options.cycle is not None:
            text.append("CYCLE" if identity_options.cycle else "NO CYCLE")
        if identity_options.order is not None:
            text.append("ORDER" if identity_options.order else "NOORDER")
        return " ".join(text)


class SnowflakeTypeCompiler(compiler.GenericTypeCompiler):
    def visit_BYTEINT(self, type_, **kw):
        return "BYTEINT"

    def visit_CHARACTER(self, type_, **kw):
        return "CHARACTER"

    def visit_DEC(self, type_, **kw):
        return "DEC"

    def visit_DOUBLE(self, type_, **kw):
        return "DOUBLE"

    def visit_FIXED(self, type_, **kw):
        return "FIXED"

    def visit_INT(self, type_, **kw):
        return "INT"

    def visit_NUMBER(self, type_, **kw):
        return "NUMBER"

    def visit_STRING(self, type_, **kw):
        return "STRING"

    def visit_TINYINT(self, type_, **kw):
        return "TINYINT"

    def visit_VARIANT(self, type_, **kw):
        return "VARIANT"

    def visit_ARRAY(self, type_, **kw):
        return "ARRAY"

    def visit_OBJECT(self, type_, **kw):
        return "OBJECT"

    def visit_BLOB(self, type_, **kw):
        return "BINARY"

    def visit_datetime(self, type_, **kw):
        return "datetime"

    def visit_DATETIME(self, type_, **kw):
        return "DATETIME"

    def visit_TIMESTAMP_NTZ(self, type_, **kw):
        return "TIMESTAMP_NTZ"

    def visit_TIMESTAMP_TZ(self, type_, **kw):
        return "TIMESTAMP_TZ"

    def visit_TIMESTAMP_LTZ(self, type_, **kw):
        return "TIMESTAMP_LTZ"

    def visit_TIMESTAMP(self, type_, **kw):
        return "TIMESTAMP"

    def visit_GEOGRAPHY(self, type_, **kw):
        return "GEOGRAPHY"

    def visit_GEOMETRY(self, type_, **kw):
        return "GEOMETRY"


construct_arguments = [(Table, {"clusterby": None})]

functions.register_function("flatten", flatten)
