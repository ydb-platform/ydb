# -*- coding: utf-8 -*-
import marshmallow as ma

from .compat import with_metaclass, iteritems
from .convert import ModelConverter
from .fields import get_primary_keys


class TableSchemaOpts(ma.SchemaOpts):
    """Options class for `TableSchema`.
    Adds the following options:

    - ``table``: The SQLAlchemy table to generate the `Schema` from (required).
    - ``model_converter``: `ModelConverter` class to use for converting the SQLAlchemy table to
        marshmallow fields.
    - ``include_fk``: Whether to include foreign fields; defaults to `False`.
    """

    def __init__(self, meta, *args, **kwargs):
        super(TableSchemaOpts, self).__init__(meta, *args, **kwargs)
        self.table = getattr(meta, "table", None)
        self.model_converter = getattr(meta, "model_converter", ModelConverter)
        self.include_fk = getattr(meta, "include_fk", False)


class ModelSchemaOpts(ma.SchemaOpts):
    """Options class for `ModelSchema`.
    Adds the following options:

    - ``model``: The SQLAlchemy model to generate the `Schema` from (required).
    - ``sqla_session``: SQLAlchemy session to be used for deserialization. This is optional; you
        can also pass a session to the Schema's `load` method.
    - ``model_converter``: `ModelConverter` class to use for converting the SQLAlchemy model to
        marshmallow fields.
    - ``include_fk``: Whether to include foreign fields; defaults to `False`.
    - ``transient``: Whether load model instances in a transient state (effectively ignoring
        the session).
    """

    def __init__(self, meta, *args, **kwargs):
        super(ModelSchemaOpts, self).__init__(meta, *args, **kwargs)
        self.model = getattr(meta, "model", None)
        self.sqla_session = getattr(meta, "sqla_session", None)
        self.model_converter = getattr(meta, "model_converter", ModelConverter)
        self.include_fk = getattr(meta, "include_fk", False)
        self.transient = getattr(meta, "transient", False)


class SchemaMeta(ma.schema.SchemaMeta):
    """Metaclass for `ModelSchema`."""

    # override SchemaMeta
    @classmethod
    def get_declared_fields(mcs, klass, cls_fields, inherited_fields, dict_cls):
        """Updates declared fields with fields converted from the SQLAlchemy model
        passed as the `model` class Meta option.
        """
        opts = klass.opts
        Converter = opts.model_converter
        converter = Converter(schema_cls=klass)
        declared_fields = super(SchemaMeta, mcs).get_declared_fields(
            klass, cls_fields, inherited_fields, dict_cls
        )
        fields = mcs.get_fields(converter, opts, declared_fields, dict_cls)
        fields.update(declared_fields)
        return fields

    @classmethod
    def get_fields(mcs, converter, base_fields, opts):
        pass


class TableSchemaMeta(SchemaMeta):
    @classmethod
    def get_fields(mcs, converter, opts, base_fields, dict_cls):
        if opts.table is not None:
            return converter.fields_for_table(
                opts.table,
                fields=opts.fields,
                exclude=opts.exclude,
                include_fk=opts.include_fk,
                base_fields=base_fields,
                dict_cls=dict_cls,
            )
        return dict_cls()


class ModelSchemaMeta(SchemaMeta):
    @classmethod
    def get_fields(mcs, converter, opts, base_fields, dict_cls):
        if opts.model is not None:
            return converter.fields_for_model(
                opts.model,
                fields=opts.fields,
                exclude=opts.exclude,
                include_fk=opts.include_fk,
                base_fields=base_fields,
                dict_cls=dict_cls,
            )
        return dict_cls()


class TableSchema(with_metaclass(TableSchemaMeta, ma.Schema)):
    """Base class for SQLAlchemy model-based Schemas.

    Example: ::

        from marshmallow_sqlalchemy import TableSchema
        from mymodels import engine, users

        class UserSchema(TableSchema):
            class Meta:
                table = users

        schema = UserSchema()

        select = users.select().limit(1)
        user = engine.execute(select).fetchone()
        serialized = schema.dump(user)
    """

    OPTIONS_CLASS = TableSchemaOpts


class ModelSchema(with_metaclass(ModelSchemaMeta, ma.Schema)):
    """Base class for SQLAlchemy model-based Schemas.

    Example: ::

        from marshmallow_sqlalchemy import ModelSchema
        from mymodels import User, session

        class UserSchema(ModelSchema):
            class Meta:
                model = User

        schema = UserSchema()

        user = schema.load({'name': 'Bill'}, session=session)
        existing_user = schema.load({'name': 'Bill'}, instance=User.query.first())

    :param session: Optional SQLAlchemy session; may be overridden in `load.`
    :param instance: Optional existing instance to modify; may be overridden in `load`.
    """

    OPTIONS_CLASS = ModelSchemaOpts

    @property
    def session(self):
        return self._session or self.opts.sqla_session

    @session.setter
    def session(self, session):
        self._session = session

    @property
    def transient(self):
        return self._transient or self.opts.transient

    @transient.setter
    def transient(self, transient):
        self._transient = transient

    def __init__(self, *args, **kwargs):
        self._session = kwargs.pop("session", None)
        self.instance = kwargs.pop("instance", None)
        self._transient = kwargs.pop("transient", None)
        super(ModelSchema, self).__init__(*args, **kwargs)

    def get_instance(self, data):
        """Retrieve an existing record by primary key(s). If the schema instance
        is transient, return None.

        :param data: Serialized data to inform lookup.
        """
        if self.transient:
            return None
        props = get_primary_keys(self.opts.model)
        filters = {prop.key: data.get(prop.key) for prop in props}
        if None not in filters.values():
            return self.session.query(self.opts.model).filter_by(**filters).first()
        return None

    @ma.post_load
    def make_instance(self, data, **kwargs):
        """Deserialize data to an instance of the model. Update an existing row
        if specified in `self.instance` or loaded by primary key(s) in the data;
        else create a new row.

        :param data: Data to deserialize.
        """
        instance = self.instance or self.get_instance(data)
        if instance is not None:
            for key, value in iteritems(data):
                setattr(instance, key, value)
            return instance
        kwargs, association_attrs = self._split_model_kwargs_association(data)
        instance = self.opts.model(**kwargs)
        for attr, value in iteritems(association_attrs):
            setattr(instance, attr, value)
        return instance

    def load(self, data, session=None, instance=None, transient=False, *args, **kwargs):
        """Deserialize data to internal representation.

        :param session: Optional SQLAlchemy session.
        :param instance: Optional existing instance to modify.
        :param transient: Optional switch to allow transient instantiation.
        """
        self._session = session or self._session
        self._transient = transient or self._transient
        if not (self.transient or self.session):
            raise ValueError("Deserialization requires a session")
        self.instance = instance or self.instance
        try:
            return super(ModelSchema, self).load(data, *args, **kwargs)
        finally:
            self.instance = None

    def validate(self, data, session=None, *args, **kwargs):
        self._session = session or self._session
        if not (self.transient or self.session):
            raise ValueError("Validation requires a session")
        return super(ModelSchema, self).validate(data, *args, **kwargs)

    def _split_model_kwargs_association(self, data):
        """Split serialized attrs to ensure association proxies are passed separately.

        This is necessary for Python < 3.6.0, as the order in which kwargs are passed
        is non-deterministic, and associations must be parsed by sqlalchemy after their
        intermediate relationship, unless their `creator` has been set.

        Ignore invalid keys at this point - behaviour for unknowns should be
        handled elsewhere.

        :param data: serialized dictionary of attrs to split on association_proxy.
        """
        association_attrs = {
            key: value
            for key, value in iteritems(data)
            # association proxy
            if hasattr(getattr(self.opts.model, key, None), "remote_attr")
        }
        kwargs = {
            key: value
            for key, value in iteritems(data)
            if (hasattr(self.opts.model, key) and key not in association_attrs)
        }
        return kwargs, association_attrs
