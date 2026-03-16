import inspect
import itertools
import weakref

import sqlalchemy as sa
from sqlalchemy.sql import ClauseElement

from . import json_support
from .declarative import Model, InvertDict
from .exceptions import NoSuchRowError
from .loader import AliasLoader, ModelLoader

DEFAULT = object()


class _Create:
    def __get__(self, instance, owner):
        if instance is None:
            # noinspection PyProtectedMember
            return owner._create_without_instance
        else:
            # noinspection PyProtectedMember
            return instance._create


class _Query:
    def __get__(self, instance, owner):
        # noinspection PyProtectedMember
        owner._check_abstract()
        q = sa.select([owner.__table__])
        if instance is not None:
            q = q.where(instance.lookup())
        return q.execution_options(model=weakref.ref(owner))


class _Select:
    def __get__(self, instance, owner):
        def select(*args):
            q = sa.select([getattr(owner, x) for x in args])
            if instance is not None:
                q = q.where(instance.lookup())
            return q.execution_options(model=weakref.ref(owner), return_model=False)

        return select


class _Update:
    def __get__(self, instance, owner):
        if instance is None:
            # noinspection PyProtectedMember
            owner._check_abstract()
            q = owner.__table__.update()
            return q.execution_options(model=weakref.ref(owner))
        else:
            # noinspection PyProtectedMember
            return instance._update


class _Delete:
    def __get__(self, instance, owner):
        if instance is None:
            # noinspection PyProtectedMember
            owner._check_abstract()
            q = owner.__table__.delete()
            return q.execution_options(model=weakref.ref(owner))
        else:
            # noinspection PyProtectedMember
            return instance._delete


class UpdateRequest:
    """
    A collection of attributes and their new values to update on one model
    instance.

    :class:`.UpdateRequest` instances are created by :attr:`.CRUDModel.update`,
    don't instantiate manually unless required. Every :class:`.UpdateRequest`
    instance is bound to one model instance, all updates are for that one
    specific model instance and its database row.

    """

    def __init__(self, instance: "CRUDModel"):
        self._instance = instance
        self._values = {}
        self._props = {}
        self._literal = True
        self._locator = None
        if instance.__table__ is not None:
            try:
                self._locator = instance.lookup()
            except LookupError:
                # apply() will fail anyway, but still allow update()
                pass

    def _set(self, key, value):
        self._values[key] = value

    def _set_prop(self, prop, value):
        if isinstance(value, ClauseElement):
            self._literal = False
        self._props[prop] = value

    async def apply(self, bind=None, timeout=DEFAULT):
        """
        Apply pending updates into database by executing an ``UPDATE`` SQL.

        :param bind: A :class:`~gino.engine.GinoEngine` to execute the SQL, or
          ``None`` (default) to use the bound engine in the metadata.

        :param timeout: Seconds to wait for the database to finish executing,
          ``None`` for wait forever. By default it will use the ``timeout``
          execution option value if unspecified.

        :return: ``self`` for chaining calls.

        """
        if self._locator is None:
            raise TypeError(
                "Model {} has no table, primary key or custom lookup()".format(
                    self._instance.__class__.__name__
                )
            )
        cls = type(self._instance)
        values = self._values.copy()

        # handle JSON columns
        json_updates = {}
        for prop, value in self._props.items():
            value = prop.save(self._instance, value)
            updates = json_updates.setdefault(prop.prop_name, {})
            if self._literal:
                updates[prop.name] = value
            else:
                if isinstance(value, int):
                    value = sa.cast(value, sa.BigInteger)
                elif not isinstance(value, ClauseElement):
                    value = sa.cast(value, sa.Unicode)
                updates[sa.cast(prop.name, sa.Unicode)] = value
        for prop_name, updates in json_updates.items():
            prop = getattr(cls, prop_name)
            from .dialects.asyncpg import JSONB

            if isinstance(prop.type, JSONB):
                if self._literal:
                    values[prop_name] = prop.concat(updates)
                else:
                    values[prop_name] = prop.concat(
                        sa.func.jsonb_build_object(*itertools.chain(*updates.items()))
                    )
            else:
                raise TypeError(
                    "{} is not supported to update json "
                    "properties in Gino. Please consider using "
                    "JSONB.".format(prop.type)
                )

        opts = dict(return_model=False)
        if timeout is not DEFAULT:
            opts["timeout"] = timeout
        clause = (
            type(self._instance)
            .update.where(self._locator,)
            .values(**self._instance._get_sa_values(values),)
            .returning(*[getattr(cls, key) for key in values],)
            .execution_options(**opts)
        )
        if bind is None:
            bind = cls.__metadata__.bind
        row = await bind.first(clause, read_only=False, reuse=False)
        if not row:
            raise NoSuchRowError()
        for k, v in row.items():
            self._instance.__values__[self._instance._column_name_map.invert_get(k)] = v
        for prop in self._props:
            prop.reload(self._instance)
        return self

    def update(self, **values):
        """
        Set given attributes on the bound model instance, and add them into
        the update collections for :meth:`.apply`.

        Given keyword-only arguments are pairs of attribute names and values to
        update. This is not a coroutine, calling :meth:`.update` will have
        instant effect on the bound model instance - its in-memory values will
        be updated immediately. Therefore this can be used individually as a
        shortcut to update several attributes in a batch::

            user.update(age=32, disabled=True)

        :meth:`.update` returns ``self`` for chaining calls to either
        :meth:`.apply` or another :meth:`.update`. If one attribute is updated
        several times by the same :class:`.UpdateRequest`, then only the last
        value is remembered for :meth:`.apply`.

        Updated values can be SQLAlchemy expressions, for example an atomic
        increment for user balance looks like this::

            await user.update(balance=User.balance + 100).apply()

        .. note::

            Expression values will not affect the in-memory attribute value on
            :meth:`.update` before :meth:`.apply`, because it has no knowledge
            of the latest value in the database. After :meth:`.apply` the new
            value will be automatically reloaded from database with
            ``RETURNING`` clause.

        """

        cls = type(self._instance)
        for key, value in values.items():
            prop = cls.__dict__.get(key)
            if isinstance(prop, json_support.JSONProperty):
                value_from = "__profile__"
                method = self._set_prop
                k = prop
            else:
                value_from = "__values__"
                method = self._set
                k = key
            if not isinstance(value, ClauseElement):
                setattr(self._instance, key, value)
                value = getattr(self._instance, value_from)[key]
            method(k, value)
        return self


class Alias:
    """
    Experimental proxy for table alias on model.

    """

    def __init__(self, model, *args, **kwargs):
        # noinspection PyProtectedMember
        model._check_abstract()
        self.model = model
        self.alias = model.__table__.alias(*args, **kwargs)

    def __getattr__(self, item):
        rv = getattr(
            self.alias.columns,
            item,
            getattr(self.alias, item, getattr(self.model, item, DEFAULT)),
        )
        if rv is DEFAULT:
            raise AttributeError
        return rv

    def __iter__(self):
        return iter(self.alias.columns)

    def __call__(self, *args, **kwargs):
        return self.model(*args, **kwargs)

    def load(self, *column_names, **relationships):
        return AliasLoader(self, *column_names, **relationships)

    def on(self, on_clause):
        return self.load().on(on_clause)

    def distinct(self, *columns):
        return self.load().distinct(*columns)


# noinspection PyProtectedMember
@sa.inspection._inspects(Alias)
def _inspect_alias(target):
    return sa.inspection.inspect(target.alias)


class CRUDModel(Model):
    """
    The base class for models with CRUD support.

    Don't inherit from this class directly, because it has no metadata. Use
    :attr:`db.Model <gino.api.Gino.Model>` instead.

    """

    create = _Create()
    """
    This ``create`` behaves a bit different on model classes compared to model
    instances.

    On model classes, ``create`` will create a new model instance and insert
    it into database. On model instances, ``create`` will just insert the
    instance into the database.

    Under the hood :meth:`.create` uses ``INSERT ... RETURNING ...`` to
    create the new model instance and load it with database default data if
    not specified.

    Some examples::

        user1 = await User.create(name='fantix', age=32)
        user2 = User(name='gino', age=42)
        await user2.create()

    :param bind: A :class:`~gino.engine.GinoEngine` to execute the
      ``INSERT`` statement with, or ``None`` (default) to use the bound
      engine on the metadata (:class:`~gino.api.Gino`).

    :param timeout: Seconds to wait for the database to finish executing,
      ``None`` for wait forever. By default it will use the ``timeout``
      execution option value if unspecified.

    :param values: Keyword arguments are pairs of attribute names and their
      initial values. Only available when called on a model class.

    :return: The instance of this model class (newly created or existing).

    """

    query = _Query()
    """
    Get a SQLAlchemy query clause of the table behind this model. This equals
    to :func:`sqlalchemy.select([self.__table__])
    <sqlalchemy.sql.expression.select>`. If this attribute is retrieved on a
    model instance, then a where clause to locate this instance by its primary
    key is appended to the returning query clause. This model type is set as
    the execution option ``model`` in the returning clause, so by default the
    query yields instances of this model instead of database rows.

    """

    update = _Update()
    """
    This ``update`` behaves quite different on model classes rather than model
    instances.

    On model classes, ``update`` is an attribute of type
    :class:`~sqlalchemy.sql.expression.Update` for massive updates, for
    example::

        await User.update.values(enabled=True).where(...).gino.status()

    Like :attr:`.query`, the update query also has the ``model`` execution
    option of this model, so if you use the
    :meth:`~sqlalchemy.sql.expression.Update.returning` clause, the query shall
    return model objects.

    However on model instances, ``update()`` is a method which accepts keyword
    arguments only and returns an :class:`.UpdateRequest` to update this single
    model instance. The keyword arguments are pairs of attribute names and new
    values. This is the same as :meth:`.UpdateRequest.update`, feel free to
    read more about it. A normal usage example would be like this::

        await user.update(name='new name', age=32).apply()

    Here, the :meth:`await ... apply() <.UpdateRequest.apply>` executes the
    actual ``UPDATE`` SQL in the database, while ``user.update()`` only makes
    changes in the memory, and collect all changes into an
    :class:`.UpdateRequest` instance.

    """

    delete = _Delete()
    """
    Similar to :meth:`.update`, this ``delete`` is also different on model
    classes than on model instances.

    On model classes ``delete`` is an attribute of type
    :class:`~sqlalchemy.sql.expression.Delete` for massive deletes, for
    example::

        await User.delete.where(User.enabled.is_(False)).gino.status()

    Similarly you can add a :meth:`~sqlalchemy.sql.expression.Delete.returning`
    clause to the query and it shall return the deleted rows as model objects.

    And on model instances, ``delete()`` is a method to remove the
    corresponding row in the database of this model instance. and returns the
    status returned from the database::

        print(await user.delete())  # e.g. prints DELETE 1

    .. note::

        ``delete()`` only removes the row from database, it does not affect the
        current model instance.

    :param bind: An optional :class:`~gino.engine.GinoEngine` if current
      metadata (:class:`~gino.api.Gino`) has no bound engine, or specifying a
      different :class:`~gino.engine.GinoEngine` to execute the ``DELETE``.

    :param timeout: Seconds to wait for the database to finish executing,
      ``None`` for wait forever. By default it will use the ``timeout``
      execution option value if unspecified.

    """

    select = _Select()
    """
    Build a query to retrieve only specified columns from this table.

    This method accepts positional string arguments as names of attributes to
    retrieve, and returns a :class:`~sqlalchemy.sql.expression.Select` for
    query. The returning query object is always set with two execution options:

    1. ``model`` is set to this model type
    2. ``return_model`` is set to ``False``

    So that by default it always return rows instead of model instances, while
    column types can be inferred correctly by the ``model`` option.

    For example::

        async for row in User.select('id', 'name').gino.iterate():
            print(row['id'], row['name'])

    If :meth:`.select` is invoked on a model instance, then a ``WHERE`` clause
    to locate this instance by its primary key is appended to the returning
    query clause. This is useful when you want to retrieve a latest value of a
    field on current model instance from database::

        db_age = await user.select('age').gino.scalar()

    .. seealso::

        :meth:`~gino.engine.GinoConnection.execution_options`

    """

    _update_request_cls = UpdateRequest
    _column_name_map = InvertDict()

    def __init__(self, **values):
        super().__init__()
        self.__profile__ = None
        self._update_request_cls(self).update(**values)

    @classmethod
    def _init_table(cls, sub_cls):
        rv = Model._init_table(sub_cls)
        if rv is not None:
            rv.__model__ = weakref.ref(sub_cls)
        return rv

    @classmethod
    async def _create_without_instance(cls, bind=None, timeout=DEFAULT, **values):
        return await cls(**values)._create(bind=bind, timeout=timeout)

    async def _create(self, bind=None, timeout=DEFAULT):
        # handle JSON properties
        cls = type(self)
        # noinspection PyUnresolvedReferences,PyProtectedMember
        cls._check_abstract()
        profile_keys = set(self.__profile__.keys() if self.__profile__ else [])
        for key in profile_keys:
            cls.__dict__.get(key).save(self)
        # initialize default values
        for key, prop in cls.__dict__.items():
            if key in profile_keys:
                continue
            if isinstance(prop, json_support.JSONProperty):
                if prop.default is None or prop.after_get.method is not None:
                    continue
                setattr(self, key, getattr(self, key))
                prop.save(self)

        # insert into database
        opts = dict(return_model=False, model=cls)
        if timeout is not DEFAULT:
            opts["timeout"] = timeout
        # noinspection PyArgumentList
        q = (
            cls.__table__.insert()
            .values(**self._get_sa_values(self.__values__))
            .returning(*cls)
            .execution_options(**opts)
        )
        if bind is None:
            bind = cls.__metadata__.bind
        row = await bind.first(q, read_only=False, reuse=False)
        for k, v in row.items():
            self.__values__[self._column_name_map.invert_get(k)] = v
        self.__profile__ = None
        return self

    def _get_sa_values(self, instance_values: dict) -> dict:
        values = {}
        for k, v in instance_values.items():
            values[self._column_name_map[k]] = v
        return values

    @classmethod
    async def get(cls, ident, bind=None, timeout=DEFAULT):
        """
        Get an instance of this model class by primary key.

        For example::

            user = await User.get(request.args.get('user_id'))

        :param ident: Value of the primary key. For composite primary keys this
          should be a tuple of values for all keys in database order, or a dict
          of names (or position numbers in database order starting from zero)
          of all primary keys to their values.

        :param bind: A :class:`~gino.engine.GinoEngine` to execute the
          ``INSERT`` statement with, or ``None`` (default) to use the bound
          engine on the metadata (:class:`~gino.api.Gino`).

        :param timeout: Seconds to wait for the database to finish executing,
          ``None`` for wait forever. By default it will use the ``timeout``
          execution option value if unspecified.

        :return: An instance of this model class, or ``None`` if no such row.

        """
        # noinspection PyUnresolvedReferences,PyProtectedMember
        cls._check_abstract()
        if not isinstance(ident, (list, tuple, dict)):
            ident_ = [ident]
        else:
            ident_ = ident
        columns = cls.__table__.primary_key.columns
        if len(ident_) != len(columns):
            raise ValueError(
                "Incorrect number of values as primary key: "
                "expected {}, got {}.".format(len(columns), len(ident_))
            )
        clause = cls.query
        for i, c in enumerate(columns):
            try:
                val = ident_[i]
            except KeyError:
                val = ident_[cls._column_name_map.invert_get(c.name)]
            clause = clause.where(c == val)
        if timeout is not DEFAULT:
            clause = clause.execution_options(timeout=timeout)
        if bind is None:
            bind = cls.__metadata__.bind
        return await bind.first(clause)

    def append_where_primary_key(self, q):
        """
        Append where clause to locate this model instance by primary on the
        given query, and return the new query.

        This is mostly used internally in GINO, but also available for such
        usage::

            await user.append_where_primary_key(User.query).gino.first()

        which is identical to::

            await user.query.gino.first()

        .. deprecated:: 0.7.6
            Use :meth:`.lookup` instead.

        """
        return q.where(self.lookup())  # pragma: no cover

    def lookup(self):
        """
        Generate where-clause expression to locate this model instance.

        By default this method uses current values of all primary keys, and you
        can override it to behave differently. Most instance-level CRUD
        operations depend on this method internally. Particularly while
        :meth:`.lookup` is called in :meth:`.update`, the where condition is
        used in :meth:`.UpdateRequest.apply`, so that queries like ``UPDATE ...
        SET id = NEW WHERE id = OLD`` could work correctly.

        :return:

        .. versionadded:: 0.7.6

        """
        exps = []
        for c in self.__table__.primary_key.columns:
            exps.append(c == getattr(self, self._column_name_map.invert_get(c.name)))
        if exps:
            return sa.and_(*exps)
        else:
            raise LookupError(
                "Instance-level CRUD operations not allowed on "
                "models without primary keys or lookup(), please"
                " use model-level CRUD operations instead."
            )

    def _update(self, **values):
        return self._update_request_cls(self).update(**values)

    async def _delete(self, bind=None, timeout=DEFAULT):
        cls = type(self)
        # noinspection PyUnresolvedReferences,PyProtectedMember
        cls._check_abstract()
        clause = cls.delete.where(self.lookup())
        if timeout is not DEFAULT:
            clause = clause.execution_options(timeout=timeout)
        if bind is None:
            bind = self.__metadata__.bind
        return (await bind.status(clause, read_only=False, reuse=False))[0]

    def to_dict(self):
        """
        Convenient method to generate a dict from this model instance.

        Keys will be attribute names, while values are loaded from memory (not
        from database). If there are :class:`~gino.json_support.JSONProperty`
        attributes in this model, their source JSON field will not be included
        in the returning dict - instead the JSON attributes will be.

        .. seealso::

            :mod:`.json_support`

        """
        cls = type(self)
        # noinspection PyTypeChecker
        keys = set(cls._column_name_map.invert_get(c.name) for c in cls)
        for key, prop in cls.__dict__.items():
            if isinstance(prop, json_support.JSONProperty):
                keys.add(key)
                keys.discard(prop.prop_name)
        return dict((k, getattr(self, k)) for k in keys)

    @classmethod
    def load(cls, *column_names, **relationships):
        """
        Populates a :class:`.loader.Loader` instance to be used by the
        ``loader`` execution option in order to customize the loading behavior
        to load specified fields into instances of this model.

        The basic usage of this method is to provide the ``loader`` execution
        option (if you are looking for reloading
        the instance from database, check :meth:`.get` or :attr:`.query`) for a
        given query.

        This method takes both positional arguments and keyword arguments with
        very different meanings. The positional arguments should be column
        names as strings, specifying only these columns should be loaded into
        the model instance (other values are discarded even if they are
        retrieved from database). Meanwhile, the keyword arguments should be
        loaders for instance attributes. For example::

            u = await User.query.gino.load(User.load('id', 'name')).first()

        .. tip::

            ``gino.load`` is a shortcut for setting the execution option
            ``loader``.

        This will populate a ``User`` instance with only ``id`` and ``name``
        values, all the rest are simply ``None`` even if the query actually
        returned all the column values.

        ::

            q = User.join(Team).select()
            u = await q.gino.load(User.load(team=Team)).first()

        This will load two instances of model ``User`` and ``Team``, returning
        the ``User`` instance with ``u.team`` set to the ``Team`` instance.

        Both positional and keyword arguments can be used ath the same time. If
        they are both omitted, like ``Team.load()``, it is equivalent to just
        ``Team`` as a loader.

        Additionally, a :class:`.loader.Loader` instance can also be used to
        generate queries, as its structure is usually the same as the query::

            u = await User.load(team=Team).query.gino.first()

        This generates a query like this::

            SELECT users.xxx, ..., teams.xxx, ...
              FROM users LEFT JOIN teams
                ON ...

        The :class:`~.loader.Loader` delegates attributes on the ``query``, so
        ``.query`` can be omitted. The ``LEFT JOIN`` is built-in behavior,
        while the ``ON`` clause is generated based on foreign key. If there is
        no foreign key, or the condition should be customized, you can use
        this::

            u = await User.load(
                team=Team.on(User.team_id == Team.id)).gino.first()

        And you can use both :meth:`~.load` and :meth:`~.on` at the same time
        in a chain, in whatever order suits you.

        .. seealso::

            :meth:`~gino.engine.GinoConnection.execution_options`

        """
        return ModelLoader(cls, *column_names, **relationships)

    @classmethod
    def on(cls, on_clause):
        """
        Customize the on-clause for the auto-generated outer join query.

        .. note::

            This has no effect when provided as the ``loader`` execution option
            for a given query.

        .. seealso::

            :meth:`.load`

        """
        return cls.load().on(on_clause)

    @classmethod
    def distinct(cls, *columns):
        """
        Experimental loader feature to yield only distinct instances by given
        columns.

        """
        return cls.load().distinct(*columns)

    @classmethod
    def none_as_none(cls, enabled=True):
        return cls.load().none_as_none(enabled)

    @classmethod
    def alias(cls, *args, **kwargs):
        """
        Experimental proxy for table alias on model.

        """
        return Alias(cls, *args, **kwargs)

    @classmethod
    def in_query(cls, query):
        """
        Convenient method to get a Model object when using subqueries.

        Though with filters and aggregations, subqueries often return same
        columns as the original table, but SQLAlchemy could not recognize them
        as the columns are in subqueries, so technically they're columns in the
        new "table".

        With this method, the columns are loaded into the original models when
        being used in subqueries. For example::

            query = query.alias('users')
            MyUser = User.in_query(query)

            loader = MyUser.distinct(User1.id).load()
            users = await query.gino.load(loader).all()

        """
        return _get_query_model(cls, query)


def _get_query_model(model, query):
    return QueryModel(model.__name__, (), dict(_model=model, _query=query))


class QueryModel(type):
    """
    Metaclass of Model classes used for subqueries.

    """

    def __getattr__(self, item):
        rv = getattr(
            self._query.columns,
            item,
            getattr(
                self._model.__table__.columns, item, getattr(self._model, item, DEFAULT)
            ),
        )
        # replace `cls` in classmethod in models to `self`
        if inspect.ismethod(rv) and inspect.isclass(rv.__self__):
            return lambda *args, **kwargs: rv.__func__(self, *args, **kwargs)
        if rv is DEFAULT:
            raise AttributeError
        return rv

    def __iter__(self):
        return iter(self._query.columns)

    def __call__(self, *args, **kwargs):
        return self._model(*args, **kwargs)
