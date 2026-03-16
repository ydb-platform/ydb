import types
import warnings

from sqlalchemy import select
from sqlalchemy.schema import Column
from sqlalchemy.sql.elements import Label

from .declarative import Model


class Loader:
    """The abstract base class of loaders.

    Loaders are used to load raw database rows into expected results.
    :class:`~gino.engine.GinoEngine` will use the loader set on the ``loader`` value of
    the :meth:`~sqlalchemy.sql.expression.Executable.execution_options`, for example::

        from sqlalchemy import text, create_engine
        from gino.loader import ColumnLoader

        e = await create_engine("postgresql://localhost/gino", strategy="gino")
        q = text("SELECT now() as ts")
        loader = ColumnLoader("ts")
        ts = await e.first(q.execution_options(loader=loader))  # datetime

    """

    @classmethod
    def get(cls, value):
        """Automatically create a loader based on the type of the given value.

        +-------------------------------------------+--------------------------+
        | value type                                | loader type              |
        +===========================================+==========================+
        | :class:`tuple`                            | :class:`~TupleLoader`    |
        +-------------------------------------------+--------------------------+
        | :func:`callable`                          | :class:`~CallableLoader` |
        +-------------------------------------------+--------------------------+
        | :class:`~sqlalchemy.schema.Column`,       | :class:`~ColumnLoader`   |
        | :class:`~sqlalchemy.sql.expression.Label` |                          |
        +-------------------------------------------+--------------------------+
        | :class:`~gino.declarative.Model`          | :class:`~ModelLoader`    |
        +-------------------------------------------+--------------------------+
        | :class:`~gino.crud.Alias`                 | :class:`~AliasLoader`    |
        +-------------------------------------------+--------------------------+
        | :class:`~Loader`                          | as is                    |
        +-------------------------------------------+--------------------------+
        | any other types                           | :class:`~ValueLoader`    |
        +-------------------------------------------+--------------------------+

        :param value: Any supported value above.
        :return: A loader instance.
        """
        from .crud import Alias

        if isinstance(value, Loader):
            rv = value
        elif isinstance(value, type) and issubclass(value, Model):
            rv = ModelLoader(value)
        elif isinstance(value, Alias):
            rv = AliasLoader(value)
        elif isinstance(value, Column):
            rv = ColumnLoader(value)
        elif isinstance(value, Label):
            rv = ColumnLoader(value.name)
        elif isinstance(value, tuple):
            rv = TupleLoader(value)
        elif callable(value):
            rv = CallableLoader(value)
        else:
            rv = ValueLoader(value)
        return rv

    @property
    def query(self):
        """Generate a query from this loader.

        This is an experimental feature, not all loaders support this.

        :return: A query instance with the ``loader`` execution option set to self.
        """
        rv = select(self.get_columns())
        from_clause = self.get_from()
        if from_clause is not None:
            rv = rv.select_from(from_clause)
        return rv.execution_options(loader=self)

    def do_load(self, row, context):
        """Interface used by GINO to run the loader.

        Must be implemented in subclasses.

        :param row: A :class:`~sqlalchemy.engine.RowProxy` instance.
        :param context: A :class:`dict` that is reused across all loaders in one query.
        :return: Any result that the loader is supposed to return, followed by a boolean
                 value indicating if the result is distinct.
        """
        raise NotImplementedError

    def get_columns(self):
        """Generate a list of selectables from this loader.

        This is an experimental feature, this method is supposed to be called by
        :attr:`~query`.

        :return: A :class:`list` of SQLAlchemy selectables.
        """
        return []

    def get_from(self):
        """Generate a clause to be used in
        :meth:`~sqlalchemy.sql.expression.Select.select_from` from this loader.

        This is an experimental feature, this method is supposed to be called by
        :attr:`~query`.

        :return: A :class:`~sqlalchemy.sql.expression.FromClause` instance, or ``None``.
        """
        return None

    def __getattr__(self, item):
        return getattr(self.query, item)


_none = object()


def _get_column(model, column_or_name) -> Column:
    if isinstance(column_or_name, str):
        return getattr(model, column_or_name)

    if isinstance(column_or_name, Column):
        if column_or_name in model:
            return column_or_name
        raise AttributeError(
            "Column {} does not belong to model {}".format(column_or_name, model)
        )

    raise TypeError(
        "Unknown column {} with type {}".format(column_or_name, type(column_or_name))
    )


class ModelLoader(Loader):
    """A loader that loads a row into a GINO model instance.

    This loader generates an instance of the given ``model`` type and fills the instance
    with attributes according to the other given parameters:

    * Load each column attribute listed in the given ``columns`` positional arguments.
    * If ``columns`` is not given, all defined columns of the ``model`` will be loaded.
    * For each keyword argument, its value will be used to generate a loader using
      :meth:`Loader.get`, and the loaded value will be :func:`setattr` to the model
      instance under the name of the key.

    .. note:

        The loader does not affect the query. You must have the values in the SQL result
        before you can use loaders to load them into model instances.

    For example, the simplest select and load::

        sqlalchemy.select([User]).execution_options(loader=ModelLoader(User))

    Select only the name column, and still load it into model instances::

        sqlalchemy.select(
            [User.name]
        ).execution_options(
            loader=ModelLoader(User, User.name)
        )

    This would also yield ``User`` instances, with all column attributes as ``None`` but
    ``name``.

    Nest a :class:`~ValueLoader`::

        sqlalchemy.select(
            [User.name]
        ).execution_options(
            loader=ModelLoader(User, id=1)
        )

    ``1`` is then converted into a :class:`~ValueLoader` and mocked the ``id`` attribute
    of all returned ``User`` instances as ``1``.

    Nest another :class:`~ModelLoader`::

        sqlalchemy.select(
            [user.outerjoin(Company)]
        ).execution_options(
            loader=ModelLoader(User, company=Company)
        )

    Likewise, ``Company`` is converted into a :class:`~ModelLoader` to load the
    ``Company`` columns from the joined result, and the ``Company`` instances are set to
    the ``company`` attribute of each ``User`` instance using :func:`setattr`.

    :param model: A subclass of :class:`~gino.declarative.Model` to instantiate.
    :param columns: A list of :class:`~sqlalchemy.schema.Column` or :class:`str` to
                    load, default is all the columns in the model.
    :param extras: Additional attributes to load on the model instance.
    """

    def __init__(self, model, *columns, **extras):
        self.model = model
        self._distinct = None
        if columns:
            self.columns = [_get_column(model, name) for name in columns]
        else:
            self.columns = model
        self.extras = dict((key, self.get(value)) for key, value in extras.items())
        self.on_clause = None

    def _do_load(self, row):
        values = dict((c.name, row[c]) for c in self.columns if c in row)
        if all((v is None) for v in values.values()):
            return None
        rv = self.model()
        for c in self.columns:
            if c in row:
                # noinspection PyProtectedMember
                instance_key = self.model._column_name_map.invert_get(c.name)
                rv.__values__[instance_key] = row[c]
        return rv

    def do_load(self, row, context):
        """Interface used by GINO to run the loader.

        :param row: A :class:`~sqlalchemy.engine.RowProxy` instance.
        :param context: A :class:`dict` that is reused across all loaders in one query.
        :return: The model instance, followed by a boolean value indicating if the
                 result is distinct.
        """

        distinct = True
        if self._distinct:
            if context is None:
                context = {}
            ctx = context.setdefault(self._distinct, {})
            key = tuple(row[col] for col in self._distinct)
            rv = ctx.get(key, _none)
            if rv is _none:
                rv = self._do_load(row)
                ctx[key] = rv
            else:
                distinct = False
        else:
            rv = self._do_load(row)

        if rv is None:
            return None, None
        else:
            for key, value in self.extras.items():
                value, distinct_ = value.do_load(row, context)
                if distinct_ is None:
                    continue

                if isinstance(getattr(self.model, key, None), types.FunctionType):
                    getattr(rv, key)(value)
                else:
                    setattr(rv, key, value)
            return rv, distinct

    def get_columns(self):
        yield from self.columns
        for subloader in self.extras.values():
            yield from subloader.get_columns()

    def get_from(self):
        rv = self.model
        for key, subloader in self.extras.items():
            from_clause = subloader.get_from()
            if from_clause is not None:
                rv = rv.outerjoin(from_clause, getattr(subloader, "on_clause", None))
        return rv

    def load(self, *columns, **extras):
        """Update the loader with new rules.

        After initialization, the rules of this loader can still be updated. This is
        useful when using the model class as a shortcut of :class:`~ModelLoader` where
        possible, chaining with a :meth:`~load` to initialize the rules, for example::

            sqlalchemy.select(
                [user.outerjoin(Company)]
            ).execution_options(
                loader=ModelLoader(User, company=Company.load('name'))
            )

        :param columns: If provided, replace the columns to load with the given ones.
        :param extras: Update the loader with new extras.
        :return: ``self`` for chaining.
        """

        if columns:
            self.columns = [_get_column(self.model, name) for name in columns]

        self.extras.update((key, self.get(value)) for key, value in extras.items())
        return self

    def on(self, on_clause):
        """Specify the ``on_clause`` for generating joined queries.

        This is an experimental feature, used by :meth:`~get_from`.

        :param on_clause: An expression to feed into
                          :func:`~sqlalchemy.sql.expression.join`.
        :return: ``self`` for chaining.
        """

        self.on_clause = on_clause
        return self

    def distinct(self, *columns):
        """Configure this loader to reuse instances that have the same values of all the
        give columns.

        :param columns: Preferably :class:`~sqlalchemy.schema.Column` instances.
        :return: ``self`` for chaining.
        """
        self._distinct = columns
        return self

    def none_as_none(self, enabled=True):
        """Deprecated method for compatibility, does nothing."""
        if not enabled:
            warnings.warn(
                "Disabling none_as_none is not supported.", DeprecationWarning,
            )
        return self


class AliasLoader(ModelLoader):
    """The same as :class:`~ModelLoader`, kept for compatibility."""

    def __init__(self, alias, *columns, **extras):
        super().__init__(alias, *columns, **extras)


class ColumnLoader(Loader):
    """Load a given column in the row.

    :param column: The column name as :class:`str`, or a
                   :class:`~sqlalchemy.schema.Column` instance to avoid name conflict.
    """

    def __init__(self, column):
        self.column = column

    def do_load(self, row, context):
        """Interface used by GINO to run the loader.

        :param row: A :class:`~sqlalchemy.engine.RowProxy` instance.
        :param context: Not used.
        :return: The value of the specified column, followed by ``True``.
        """

        return row[self.column], True


class TupleLoader(Loader):
    """Load multiple values into a tuple.

    :param values: A :class:`tuple`, each item is converted into a loader with
                   :func:`Loader.get`.
    """

    def __init__(self, values):
        self.loaders = tuple(self.get(value) for value in values)

    def do_load(self, row, context):
        """Interface used by GINO to run the loader.

        The arguments are simply passed to sub-loaders.

        :param row: A :class:`~sqlalchemy.engine.RowProxy` instance.
        :param context: A :class:`dict` that is reused across all loaders in one query.
        :return: A :class:`tuple` with loaded results from all sub-loaders, followed by
                 ``True``.
        """

        return tuple(loader.do_load(row, context)[0] for loader in self.loaders), True


class CallableLoader(Loader):
    """Load the row by calling a specified function.

    :param func: A :func:`callable` taking 2 positional arguments ``(row, context)``
                 that will be called in :meth:`~do_load`, returning the loaded result.
    """

    def __init__(self, func):
        self.func = func

    def do_load(self, row, context):
        """Interface used by GINO to run the loader.

        The arguments are simply passed to the given function.

        :param row: A :class:`~sqlalchemy.engine.RowProxy` instance.
        :param context: A :class:`dict` that is reused across all loaders in one query.
        :return: The result calling the given function, followed by ``True``.
        """

        return self.func(row, context), True


class ValueLoader(Loader):
    """A loader that always return the specified value.

    :param value: The value to return on load.
    """

    def __init__(self, value):
        self.value = value

    def do_load(self, row, context):
        """Interface used by GINO to run the loader.

        :param row: Not used.
        :param context: Not used.
        :return: The given value, followed by ``True``.
        """

        return self.value, True
