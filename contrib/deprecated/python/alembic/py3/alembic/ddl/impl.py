from collections import namedtuple
import re

from sqlalchemy import cast
from sqlalchemy import schema
from sqlalchemy import text

from . import base
from .. import util
from ..util import sqla_compat
from ..util.compat import string_types
from ..util.compat import text_type
from ..util.compat import with_metaclass


class ImplMeta(type):
    def __init__(cls, classname, bases, dict_):
        newtype = type.__init__(cls, classname, bases, dict_)
        if "__dialect__" in dict_:
            _impls[dict_["__dialect__"]] = cls
        return newtype


_impls = {}

Params = namedtuple("Params", ["token0", "tokens", "args", "kwargs"])


class DefaultImpl(with_metaclass(ImplMeta)):

    """Provide the entrypoint for major migration operations,
    including database-specific behavioral variances.

    While individual SQL/DDL constructs already provide
    for database-specific implementations, variances here
    allow for entirely different sequences of operations
    to take place for a particular migration, such as
    SQL Server's special 'IDENTITY INSERT' step for
    bulk inserts.

    """

    __dialect__ = "default"

    transactional_ddl = False
    command_terminator = ";"
    type_synonyms = ({"NUMERIC", "DECIMAL"},)
    type_arg_extract = ()
    # on_null is known to be supported only by oracle
    identity_attrs_ignore = ("on_null",)

    def __init__(
        self,
        dialect,
        connection,
        as_sql,
        transactional_ddl,
        output_buffer,
        context_opts,
    ):
        self.dialect = dialect
        self.connection = connection
        self.as_sql = as_sql
        self.literal_binds = context_opts.get("literal_binds", False)

        self.output_buffer = output_buffer
        self.memo = {}
        self.context_opts = context_opts
        if transactional_ddl is not None:
            self.transactional_ddl = transactional_ddl

        if self.literal_binds:
            if not self.as_sql:
                raise util.CommandError(
                    "Can't use literal_binds setting without as_sql mode"
                )

    @classmethod
    def get_by_dialect(cls, dialect):
        return _impls[dialect.name]

    def static_output(self, text):
        self.output_buffer.write(text_type(text + "\n\n"))
        self.output_buffer.flush()

    def requires_recreate_in_batch(self, batch_op):
        """Return True if the given :class:`.BatchOperationsImpl`
        would need the table to be recreated and copied in order to
        proceed.

        Normally, only returns True on SQLite when operations other
        than add_column are present.

        """
        return False

    def prep_table_for_batch(self, batch_impl, table):
        """perform any operations needed on a table before a new
        one is created to replace it in batch mode.

        the PG dialect uses this to drop constraints on the table
        before the new one uses those same names.

        """

    @property
    def bind(self):
        return self.connection

    def _exec(
        self,
        construct,
        execution_options=None,
        multiparams=(),
        params=util.immutabledict(),
    ):
        if isinstance(construct, string_types):
            construct = text(construct)
        if self.as_sql:
            if multiparams or params:
                # TODO: coverage
                raise Exception("Execution arguments not allowed with as_sql")

            if self.literal_binds and not isinstance(
                construct, schema.DDLElement
            ):
                compile_kw = dict(compile_kwargs={"literal_binds": True})
            else:
                compile_kw = {}

            self.static_output(
                text_type(
                    construct.compile(dialect=self.dialect, **compile_kw)
                )
                .replace("\t", "    ")
                .strip()
                + self.command_terminator
            )
        else:
            conn = self.connection
            if execution_options:
                conn = conn.execution_options(**execution_options)
            if params:
                multiparams += (params,)

            return conn.execute(construct, multiparams)

    def execute(self, sql, execution_options=None):
        self._exec(sql, execution_options)

    def alter_column(
        self,
        table_name,
        column_name,
        nullable=None,
        server_default=False,
        name=None,
        type_=None,
        schema=None,
        autoincrement=None,
        comment=False,
        existing_comment=None,
        existing_type=None,
        existing_server_default=None,
        existing_nullable=None,
        existing_autoincrement=None,
    ):
        if autoincrement is not None or existing_autoincrement is not None:
            util.warn(
                "autoincrement and existing_autoincrement "
                "only make sense for MySQL",
                stacklevel=3,
            )
        if nullable is not None:
            self._exec(
                base.ColumnNullable(
                    table_name,
                    column_name,
                    nullable,
                    schema=schema,
                    existing_type=existing_type,
                    existing_server_default=existing_server_default,
                    existing_nullable=existing_nullable,
                    existing_comment=existing_comment,
                )
            )
        if server_default is not False:
            kw = {}
            if sqla_compat._server_default_is_computed(
                server_default, existing_server_default
            ):
                cls_ = base.ComputedColumnDefault
            elif sqla_compat._server_default_is_identity(
                server_default, existing_server_default
            ):
                cls_ = base.IdentityColumnDefault
                kw["impl"] = self
            else:
                cls_ = base.ColumnDefault
            self._exec(
                cls_(
                    table_name,
                    column_name,
                    server_default,
                    schema=schema,
                    existing_type=existing_type,
                    existing_server_default=existing_server_default,
                    existing_nullable=existing_nullable,
                    existing_comment=existing_comment,
                    **kw
                )
            )
        if type_ is not None:
            self._exec(
                base.ColumnType(
                    table_name,
                    column_name,
                    type_,
                    schema=schema,
                    existing_type=existing_type,
                    existing_server_default=existing_server_default,
                    existing_nullable=existing_nullable,
                    existing_comment=existing_comment,
                )
            )

        if comment is not False:
            self._exec(
                base.ColumnComment(
                    table_name,
                    column_name,
                    comment,
                    schema=schema,
                    existing_type=existing_type,
                    existing_server_default=existing_server_default,
                    existing_nullable=existing_nullable,
                    existing_comment=existing_comment,
                )
            )

        # do the new name last ;)
        if name is not None:
            self._exec(
                base.ColumnName(
                    table_name,
                    column_name,
                    name,
                    schema=schema,
                    existing_type=existing_type,
                    existing_server_default=existing_server_default,
                    existing_nullable=existing_nullable,
                )
            )

    def add_column(self, table_name, column, schema=None):
        self._exec(base.AddColumn(table_name, column, schema=schema))

    def drop_column(self, table_name, column, schema=None, **kw):
        self._exec(base.DropColumn(table_name, column, schema=schema))

    def add_constraint(self, const):
        if const._create_rule is None or const._create_rule(self):
            self._exec(schema.AddConstraint(const))

    def drop_constraint(self, const):
        self._exec(schema.DropConstraint(const))

    def rename_table(self, old_table_name, new_table_name, schema=None):
        self._exec(
            base.RenameTable(old_table_name, new_table_name, schema=schema)
        )

    def create_table(self, table):
        table.dispatch.before_create(
            table, self.connection, checkfirst=False, _ddl_runner=self
        )
        self._exec(schema.CreateTable(table))
        table.dispatch.after_create(
            table, self.connection, checkfirst=False, _ddl_runner=self
        )
        for index in table.indexes:
            self._exec(schema.CreateIndex(index))

        with_comment = (
            self.dialect.supports_comments and not self.dialect.inline_comments
        )
        comment = table.comment
        if comment and with_comment:
            self.create_table_comment(table)

        for column in table.columns:
            comment = column.comment
            if comment and with_comment:
                self.create_column_comment(column)

    def drop_table(self, table):
        self._exec(schema.DropTable(table))

    def create_index(self, index):
        self._exec(schema.CreateIndex(index))

    def create_table_comment(self, table):
        self._exec(schema.SetTableComment(table))

    def drop_table_comment(self, table):
        self._exec(schema.DropTableComment(table))

    def create_column_comment(self, column):
        self._exec(schema.SetColumnComment(column))

    def drop_index(self, index):
        self._exec(schema.DropIndex(index))

    def bulk_insert(self, table, rows, multiinsert=True):
        if not isinstance(rows, list):
            raise TypeError("List expected")
        elif rows and not isinstance(rows[0], dict):
            raise TypeError("List of dictionaries expected")
        if self.as_sql:
            for row in rows:
                self._exec(
                    sqla_compat._insert_inline(table).values(
                        **dict(
                            (
                                k,
                                sqla_compat._literal_bindparam(
                                    k, v, type_=table.c[k].type
                                )
                                if not isinstance(
                                    v, sqla_compat._literal_bindparam
                                )
                                else v,
                            )
                            for k, v in row.items()
                        )
                    )
                )
        else:
            # work around http://www.sqlalchemy.org/trac/ticket/2461
            if not hasattr(table, "_autoincrement_column"):
                table._autoincrement_column = None
            if rows:
                if multiinsert:
                    self._exec(
                        sqla_compat._insert_inline(table), multiparams=rows
                    )
                else:
                    for row in rows:
                        self._exec(
                            sqla_compat._insert_inline(table).values(**row)
                        )

    def _tokenize_column_type(self, column):
        definition = self.dialect.type_compiler.process(column.type).lower()

        # tokenize the SQLAlchemy-generated version of a type, so that
        # the two can be compared.
        #
        # examples:
        # NUMERIC(10, 5)
        # TIMESTAMP WITH TIMEZONE
        # INTEGER UNSIGNED
        # INTEGER (10) UNSIGNED
        # INTEGER(10) UNSIGNED
        # varchar character set utf8
        #

        tokens = re.findall(r"[\w\-_]+|\(.+?\)", definition)

        term_tokens = []
        paren_term = None

        for token in tokens:
            if re.match(r"^\(.*\)$", token):
                paren_term = token
            else:
                term_tokens.append(token)

        params = Params(term_tokens[0], term_tokens[1:], [], {})

        if paren_term:
            for term in re.findall("[^(),]+", paren_term):
                if "=" in term:
                    key, val = term.split("=")
                    params.kwargs[key.strip()] = val.strip()
                else:
                    params.args.append(term.strip())

        return params

    def _column_types_match(self, inspector_params, metadata_params):
        if inspector_params.token0 == metadata_params.token0:
            return True

        synonyms = [{t.lower() for t in batch} for batch in self.type_synonyms]
        inspector_all_terms = " ".join(
            [inspector_params.token0] + inspector_params.tokens
        )
        metadata_all_terms = " ".join(
            [metadata_params.token0] + metadata_params.tokens
        )

        for batch in synonyms:
            if {inspector_all_terms, metadata_all_terms}.issubset(batch) or {
                inspector_params.token0,
                metadata_params.token0,
            }.issubset(batch):
                return True
        return False

    def _column_args_match(self, inspected_params, meta_params):
        """We want to compare column parameters. However, we only want
        to compare parameters that are set. If they both have `collation`,
        we want to make sure they are the same. However, if only one
        specifies it, dont flag it for being less specific
        """

        if (
            len(meta_params.tokens) == len(inspected_params.tokens)
            and meta_params.tokens != inspected_params.tokens
        ):
            return False

        if (
            len(meta_params.args) == len(inspected_params.args)
            and meta_params.args != inspected_params.args
        ):
            return False

        insp = " ".join(inspected_params.tokens).lower()
        meta = " ".join(meta_params.tokens).lower()

        for reg in self.type_arg_extract:
            mi = re.search(reg, insp)
            mm = re.search(reg, meta)

            if mi and mm and mi.group(1) != mm.group(1):
                return False

        return True

    def compare_type(self, inspector_column, metadata_column):
        """Returns True if there ARE differences between the types of the two
        columns. Takes impl.type_synonyms into account between retrospected
        and metadata types
        """
        inspector_params = self._tokenize_column_type(inspector_column)
        metadata_params = self._tokenize_column_type(metadata_column)

        if not self._column_types_match(inspector_params, metadata_params):
            return True
        if not self._column_args_match(inspector_params, metadata_params):
            return True
        return False

    def compare_server_default(
        self,
        inspector_column,
        metadata_column,
        rendered_metadata_default,
        rendered_inspector_default,
    ):
        return rendered_inspector_default != rendered_metadata_default

    def correct_for_autogen_constraints(
        self,
        conn_uniques,
        conn_indexes,
        metadata_unique_constraints,
        metadata_indexes,
    ):
        pass

    def cast_for_batch_migrate(self, existing, existing_transfer, new_type):
        if existing.type._type_affinity is not new_type._type_affinity:
            existing_transfer["expr"] = cast(
                existing_transfer["expr"], new_type
            )

    def render_ddl_sql_expr(self, expr, is_server_default=False, **kw):
        """Render a SQL expression that is typically a server default,
        index expression, etc.

        .. versionadded:: 1.0.11

        """

        compile_kw = dict(
            compile_kwargs={"literal_binds": True, "include_table": False}
        )
        return text_type(expr.compile(dialect=self.dialect, **compile_kw))

    def _compat_autogen_column_reflect(self, inspector):
        return self.autogen_column_reflect

    def correct_for_autogen_foreignkeys(self, conn_fks, metadata_fks):
        pass

    def autogen_column_reflect(self, inspector, table, column_info):
        """A hook that is attached to the 'column_reflect' event for when
        a Table is reflected from the database during the autogenerate
        process.

        Dialects can elect to modify the information gathered here.

        """

    def start_migrations(self):
        """A hook called when :meth:`.EnvironmentContext.run_migrations`
        is called.

        Implementations can set up per-migration-run state here.

        """

    def emit_begin(self):
        """Emit the string ``BEGIN``, or the backend-specific
        equivalent, on the current connection context.

        This is used in offline mode and typically
        via :meth:`.EnvironmentContext.begin_transaction`.

        """
        self.static_output("BEGIN" + self.command_terminator)

    def emit_commit(self):
        """Emit the string ``COMMIT``, or the backend-specific
        equivalent, on the current connection context.

        This is used in offline mode and typically
        via :meth:`.EnvironmentContext.begin_transaction`.

        """
        self.static_output("COMMIT" + self.command_terminator)

    def render_type(self, type_obj, autogen_context):
        return False

    def _compare_identity_default(self, metadata_identity, inspector_identity):

        # ignored contains the attributes that were not considered
        # because assumed to their default values in the db.
        diff, ignored = _compare_identity_options(
            sqla_compat._identity_attrs,
            metadata_identity,
            inspector_identity,
            sqla_compat.Identity(),
        )

        meta_always = getattr(metadata_identity, "always", None)
        inspector_always = getattr(inspector_identity, "always", None)
        # None and False are the same in this comparison
        if bool(meta_always) != bool(inspector_always):
            diff.add("always")

        diff.difference_update(self.identity_attrs_ignore)

        # returns 3 values:
        return (
            # different identity attributes
            diff,
            # ignored identity attributes
            ignored,
            # if the two identity should be considered different
            bool(diff) or bool(metadata_identity) != bool(inspector_identity),
        )


def _compare_identity_options(
    attributes, metadata_io, inspector_io, default_io
):
    # this can be used for identity or sequence compare.
    # default_io is an instance of IdentityOption with all attributes to the
    # default value.
    diff = set()
    ignored_attr = set()
    for attr in attributes:
        meta_value = getattr(metadata_io, attr, None)
        default_value = getattr(default_io, attr, None)
        conn_value = getattr(inspector_io, attr, None)
        if conn_value != meta_value:
            if meta_value == default_value:
                ignored_attr.add(attr)
            else:
                diff.add(attr)
    return diff, ignored_attr
