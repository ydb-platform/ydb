import weakref

import sqlalchemy as sa
from sqlalchemy.engine.url import make_url, URL
from sqlalchemy.sql.base import Executable
from sqlalchemy.sql.schema import SchemaItem

from .crud import CRUDModel
from .declarative import declarative_base, declared_attr
from .exceptions import UninitializedError
from .schema import GinoSchemaVisitor, patch_schema
from . import json_support


class GinoExecutor:
    """
    The default ``gino`` extension on
    :class:`~sqlalchemy.sql.expression.Executable` constructs for implicit
    execution.

    Instances of this class are created when visiting the ``gino`` property of
    :class:`~sqlalchemy.sql.expression.Executable` instances (also referred as
    queries or clause elements), for example::

        await User.query.gino.first()

    This allows GINO to add the asynchronous query APIs (:meth:`all`,
    :meth:`first`, :meth:`one`, :meth:`one_or_none`, :meth:`scalar`,
    :meth:`status`, :meth:`iterate`) to SQLAlchemy query clauses without
    messing up with existing synchronous ones.
    Calling these asynchronous query APIs has the same restriction - the
    relevant metadata (the :class:`Gino` instance) must be bound to an engine,
    or an :exc:`AttributeError` will be raised.

    .. note::

        Executable clause elements that are completely irrelevant with any
        table - for example ``db.select([db.text('now()')])`` - has no
        metadata, hence no engine. Therefore, this will always fail::

            await db.select([db.text('now()')]).gino.scalar()

        You should use :meth:`conn.scalar() <.engine.GinoConnection.scalar>`,
        :meth:`engine.scalar() <.engine.GinoEngine.scalar>` or even
        :meth:`db.scalar() <.Gino.scalar>` in this case.

    """

    __slots__ = ("_query",)

    def __init__(self, query):
        self._query = query

    @property
    def query(self):
        """
        Get back the chained :class:`~sqlalchemy.sql.expression.Executable`.

        In a chained query calls, occasionally the previous query clause is
        needed after a ``.gino.`` chain, you can use ``.query.`` to resume the
        chain back. For example::

            await User.query.gino.model(FOUser).query.where(...).gino.all()

        """
        return self._query

    def model(self, model):
        """
        Shortcut to set execution option ``model`` in a chaining call.

        Read :meth:`~gino.engine.GinoConnection.execution_options` for more
        information.

        """
        if model is not None:
            model = weakref.ref(model)
        self._query = self._query.execution_options(model=model)
        return self

    def return_model(self, switch):
        """
        Shortcut to set execution option ``return_model`` in a chaining call.

        Read :meth:`~gino.engine.GinoConnection.execution_options` for more
        information.

        """
        self._query = self._query.execution_options(return_model=switch)
        return self

    def timeout(self, timeout):
        """
        Shortcut to set execution option ``timeout`` in a chaining call.

        Read :meth:`~gino.engine.GinoConnection.execution_options` for more
        information.

        """
        self._query = self._query.execution_options(timeout=timeout)
        return self

    def load(self, value):
        """
        Shortcut to set execution option ``loader`` in a chaining call.

        For example to load ``Book`` instances with their authors::

            query = Book.join(User).select()
            books = await query.gino.load(Book.load(author=User)).all()

        Read :meth:`~gino.engine.GinoConnection.execution_options` for more
        information.

        """
        self._query = self._query.execution_options(loader=value)
        return self

    async def all(self, *multiparams, **params):
        """
        Returns :meth:`engine.all() <.engine.GinoEngine.all>` with this query
        as the first argument, and other arguments followed, where ``engine``
        is the :class:`~.engine.GinoEngine` to which the metadata
        (:class:`Gino`) is bound, while metadata is found in this query.

        """
        return await self._query.bind.all(self._query, *multiparams, **params)

    async def first(self, *multiparams, **params):
        """
        Returns :meth:`engine.first() <.engine.GinoEngine.first>` with this
        query as the first argument, and other arguments followed, where
        ``engine`` is the :class:`~.engine.GinoEngine` to which the metadata
        (:class:`Gino`) is bound, while metadata is found in this query.

        """
        return await self._query.bind.first(self._query, *multiparams, **params)

    async def one_or_none(self, *multiparams, **params):
        """
        Returns :meth:`engine.one_or_none() <.engine.GinoEngine.one_or_none>`
        with this query as the first argument, and other arguments followed,
        where ``engine`` is the :class:`~.engine.GinoEngine` to which the
        metadata (:class:`Gino`) is bound, while metadata is found in this
        query.

        """
        return await self._query.bind.one_or_none(self._query, *multiparams, **params)

    async def one(self, *multiparams, **params):
        """
        Returns :meth:`engine.one() <.engine.GinoEngine.one>` with this query
        as the first argument, and other arguments followed, where ``engine``
        is the :class:`~.engine.GinoEngine` to which the metadata
        (:class:`Gino`) is bound, while metadata is found in this query.

        """
        return await self._query.bind.one(self._query, *multiparams, **params)

    async def scalar(self, *multiparams, **params):
        """
        Returns :meth:`engine.scalar() <.engine.GinoEngine.scalar>` with this
        query as the first argument, and other arguments followed, where
        ``engine`` is the :class:`~.engine.GinoEngine` to which the metadata
        (:class:`Gino`) is bound, while metadata is found in this query.

        """
        return await self._query.bind.scalar(self._query, *multiparams, **params)

    async def status(self, *multiparams, **params):
        """
        Returns :meth:`engine.status() <.engine.GinoEngine.status>` with this
        query as the first argument, and other arguments followed, where
        ``engine`` is the :class:`~.engine.GinoEngine` to which the metadata
        (:class:`Gino`) is bound, while metadata is found in this query.

        """
        return await self._query.bind.status(self._query, *multiparams, **params)

    def iterate(self, *multiparams, **params):
        """
        Returns :meth:`engine.iterate() <.engine.GinoEngine.iterate>` with this
        query as the first argument, and other arguments followed, where
        ``engine`` is the :class:`~.engine.GinoEngine` to which the metadata
        (:class:`Gino`) is bound, while metadata is found in this query.

        """
        connection = self._query.bind.current_connection
        if connection is None:
            raise ValueError("No Connection in context, please provide one")
        return connection.iterate(self._query, *multiparams, **params)


class _BindContext:
    def __init__(self, *args):
        self._args = args

    async def __aenter__(self):
        api, bind, loop, kwargs = self._args
        return await api.set_bind(bind, loop, **kwargs)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._args[0].pop_bind().close()


class Gino(sa.MetaData):
    """
    All-in-one API class of GINO, providing several shortcuts.

    This class is a subclass of SQLAlchemy
    :class:`~sqlalchemy.schema.MetaData`, therefore its instances can be used
    as a normal :class:`~sqlalchemy.schema.MetaData` object, e.g. used in
    `Alembic <http://alembic.zzzcomputing.com/>`_. In usual cases, you would
    want to define one global :class:`~.Gino` instance, usually under the name
    of ``db``, representing the database used in your application.

    You may define tables in `the official way <http://bit.ly/2G25fdc>`_
    SQLAlchemy core recommended, but more often in GINO we define model classes
    with ``db.Model`` as their parent class to represent tables, for its
    objective interface and CRUD operations. Please read :doc:`/how-to/crud`
    for more information.

    For convenience, :class:`Gino` instance delegated all properties publicly
    exposed by :mod:`sqlalchemy`, so that you can define tables / models
    without importing :mod:`sqlalchemy`::

        id = db.Column(db.BigInteger(), primary_key=True)

    Similar to :class:`~sqlalchemy.schema.MetaData`, a :class:`~.Gino` object
    can bind to a :class:`~gino.engine.GinoEngine` instance, hereby allowing
    `"implicit execution" <http://bit.ly/2oTUcKY>`_ through the ``gino``
    extension on :class:`~sqlalchemy.sql.expression.Executable` or
    :class:`~sqlalchemy.schema.SchemaItem` constructs::

        await User.query.gino.first()
        await db.gino.create_all()

    Differently, GINO encourages the use of implicit execution and manages
    transactional context correctly.

    Binding to a connection object is not supported.

    To set a bind property, you can simply set your
    :class:`~gino.engine.GinoEngine` object on :attr:`db.bind <Gino.bind>`, or
    set it to ``None`` to unbind. However, the creation of engine usually
    happens at the same time. Therefore, GINO provided several convenient ways
    doing so:

    1. :meth:`~Gino.with_bind` returning an asynchronous context manager::

        async with db.with_bind('postgresql://...') as engine:

    2. :meth:`~Gino.set_bind` and :meth:`~Gino.pop_bind`::

        engine = await db.set_bind('postgresql://...')
        await db.pop_bind().close()

    3. Directly ``await`` on :class:`~.Gino` instance::

        db = await gino.Gino('postgresql://...')
        await db.pop_bind().close()

    .. note::

        SQLAlchemy allows creating the engine by::

            metadata.bind = 'postgresql://...'

        While in GINO this only sets a string to :attr:`~.Gino.bind`, because
        creating an engine requires ``await``, which is exactly what
        :meth:`~.Gino.set_bind` does.

    At last, :class:`Gino` delegates all query APIs on the bound
    :class:`~.engine.GinoEngine`.

    """

    model_base_classes = (CRUDModel,)
    """
    Overridable default model classes to build the :attr:`Model`.

    Default is :class:`(CRUDModel, ) <gino.crud.CRUDModel>`.

    """

    query_executor = GinoExecutor
    """
    The overridable ``gino`` extension class on
    :class:`~sqlalchemy.sql.expression.Executable`.

    This class will be set as the getter method of the property ``gino`` on
    :class:`~sqlalchemy.sql.expression.Executable` and its subclasses, if
    ``ext`` and ``query_ext`` arguments are both ``True``. Default is
    :class:`GinoExecutor`.

    """

    schema_visitor = GinoSchemaVisitor
    """
    The overridable ``gino`` extension class on
    :class:`~sqlalchemy.schema.SchemaItem`.

    This class will be set as the getter method of the property ``gino`` on
    :class:`~sqlalchemy.schema.SchemaItem` and its subclasses, if ``ext`` and
    ``schema_ext`` arguments are both ``True``. Default is
    :class:`~gino.schema.GinoSchemaVisitor`.

    """

    no_delegate = {"create_engine", "engine_from_config"}
    """
    A set of symbols from :mod:`sqlalchemy` which is not delegated by
    :class:`Gino`.

    """

    def __init__(
        self,
        bind=None,
        model_classes=None,
        query_ext=True,
        schema_ext=True,
        ext=True,
        **kwargs
    ):
        """
        :param bind: A :class:`~.engine.GinoEngine` instance to bind. Also
                     accepts string or :class:`~sqlalchemy.engine.url.URL`,
                     which will be passed to
                     :func:`~gino.create_engine` when this :class:`Gino`
                     instance is awaited. Default is ``None``.
        :param model_classes: A :class:`tuple` of base class and mixin classes
                              to create the :attr:`~.Gino.Model` class. Default
                              is :class:`(CRUDModel, ) <gino.crud.CRUDModel>`.
        :param query_ext: Boolean value to control the installation of the
                          ``gino`` extension on
                          :class:`~sqlalchemy.sql.expression.Executable` for
                          implicit execution. Default is to install (``True``).
        :param schema_ext: Boolean value to control the installation of the
                           ``gino`` extension on
                           :class:`~sqlalchemy.schema.SchemaItem` for implicit
                           execution. Default is to install (``True``).
        :param ext: Boolean value to control the installation of the two
                    ``gino`` extensions. ``False`` for no extension at all,
                    while it depends on the two individual switches when this
                    is set to ``True`` (default).
        :param kwargs: Other arguments accepted by
                       :class:`~sqlalchemy.schema.MetaData`.

        """
        super().__init__(bind=bind, **kwargs)
        if model_classes is None:
            model_classes = self.model_base_classes
        self._model = declarative_base(self, model_classes)
        self.declared_attr = declared_attr
        self.quoted_name = sa.sql.quoted_name
        for mod in json_support, sa:
            for key in mod.__all__:
                if not hasattr(self, key) and key not in self.no_delegate:
                    setattr(self, key, getattr(mod, key))
        if ext:
            if query_ext:
                Executable.gino = property(self.query_executor)
            if schema_ext:
                SchemaItem.gino = property(self.schema_visitor)
                patch_schema(self)

    # noinspection PyPep8Naming
    @property
    def Model(self):
        """
        Declarative base class for models, subclass of
        :class:`gino.declarative.Model`. Defining subclasses of this class will
        result new tables added to this :class:`Gino` metadata.

        """
        return self._model

    @property
    def bind(self):
        """
        An :class:`~.engine.GinoEngine` to which this :class:`Gino` is bound.

        This is a simple property with no getter or setter hook - what you set
        is what you get. To achieve the same result as it is in SQLAlchemy -
        setting a string or :class:`~sqlalchemy.engine.url.URL` and getting an
        engine instance, use :meth:`set_bind` (or ``await`` on this
        :class:`Gino` object after setting a string or
        :class:`~sqlalchemy.engine.url.URL`).

        """
        if self._bind is None:
            return _PlaceHolder(UninitializedError("Gino engine is not initialized."))
        return self._bind

    # noinspection PyMethodOverriding,PyAttributeOutsideInit
    @bind.setter
    def bind(self, bind):
        self._bind = bind

    async def set_bind(self, bind, loop=None, **kwargs):
        """
        Bind self to the given :class:`~.engine.GinoEngine` and return it.

        If the given ``bind`` is a string or
        :class:`~sqlalchemy.engine.url.URL`, all arguments will be sent to
        :func:`~gino.create_engine` to create a new engine, and return it.

        :return: :class:`~.engine.GinoEngine`

        """
        if isinstance(bind, str):
            bind = make_url(bind)
        if isinstance(bind, URL):
            from . import create_engine

            bind = await create_engine(bind, loop=loop, **kwargs)
        self.bind = bind
        return bind

    def pop_bind(self):
        """
        Unbind self, and return the bound engine.

        This is usually used in a chained call to close the engine::

            await db.pop_bind().close()

        :return: :class:`~.engine.GinoEngine` or ``None`` if self is not bound.

        """
        bind, self.bind = self.bind, None
        return bind

    def with_bind(self, bind, loop=None, **kwargs):
        """
        Shortcut for :meth:`set_bind` and :meth:`pop_bind` plus closing engine.

        This method accepts the same arguments of
        :func:`~gino.create_engine`. This allows inline creating an engine and
        binding self on enter, and unbinding self and closing the engine on
        exit::

            async with db.with_bind('postgresql://...') as engine:
                # play with engine

        :return: An asynchronous context manager.

        """
        return _BindContext(self, bind, loop, kwargs)

    def __await__(self):
        async def init():
            await self.set_bind(self.bind)
            return self

        return init().__await__()

    def compile(self, elem, *multiparams, **params):
        """
        A delegate of :meth:`GinoEngine.compile()
        <.engine.GinoEngine.compile>`.

        """
        return self.bind.compile(elem, *multiparams, **params)

    async def all(self, clause, *multiparams, **params):
        """
        A delegate of :meth:`GinoEngine.all() <.engine.GinoEngine.all>`.

        """
        return await self.bind.all(clause, *multiparams, **params)

    async def first(self, clause, *multiparams, **params):
        """
        A delegate of :meth:`GinoEngine.first() <.engine.GinoEngine.first>`.

        """
        return await self.bind.first(clause, *multiparams, **params)

    async def one_or_none(self, clause, *multiparams, **params):
        """
        A delegate of :meth:`GinoEngine.one_or_none()
        <.engine.GinoEngine.one_or_none>`.

        """
        return await self.bind.one_or_none(clause, *multiparams, **params)

    async def one(self, clause, *multiparams, **params):
        """
        A delegate of :meth:`GinoEngine.one() <.engine.GinoEngine.first>`.

        """
        return await self.bind.one(clause, *multiparams, **params)

    async def scalar(self, clause, *multiparams, **params):
        """
        A delegate of :meth:`GinoEngine.scalar() <.engine.GinoEngine.scalar>`.

        """
        return await self.bind.scalar(clause, *multiparams, **params)

    async def status(self, clause, *multiparams, **params):
        """
        A delegate of :meth:`GinoEngine.status() <.engine.GinoEngine.status>`.

        """
        return await self.bind.status(clause, *multiparams, **params)

    def iterate(self, clause, *multiparams, **params):
        """
        A delegate of :meth:`GinoEngine.iterate()
        <.engine.GinoEngine.iterate>`.

        """
        return self.bind.iterate(clause, *multiparams, **params)

    def acquire(self, *args, **kwargs):
        """
        A delegate of :meth:`GinoEngine.acquire()
        <.engine.GinoEngine.acquire>`.

        """
        return self.bind.acquire(*args, **kwargs)

    def transaction(self, *args, **kwargs):
        """
        A delegate of :meth:`GinoEngine.transaction()
        <.engine.GinoEngine.transaction>`.

        """
        return self.bind.transaction(*args, **kwargs)


class _PlaceHolder:
    __slots__ = "_exception"

    def __init__(self, exception):
        self._exception = exception

    def __getattribute__(self, item):
        if item == "_exception":
            return super().__getattribute__(item)
        raise self._exception

    def __setattr__(self, key, value):
        if key == "_exception":
            return super().__setattr__(key, value)
        raise self._exception
