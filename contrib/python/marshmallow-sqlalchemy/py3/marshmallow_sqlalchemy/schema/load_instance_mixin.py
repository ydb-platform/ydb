"""Mixin that adds model instance loading behavior.

.. warning::

    This module is treated as private API.
    Users should not need to use this module directly.
"""
import marshmallow as ma

from ..fields import get_primary_keys


class LoadInstanceMixin:
    class Opts:
        def __init__(self, meta, *args, **kwargs):
            super().__init__(meta, *args, **kwargs)
            self.sqla_session = getattr(meta, "sqla_session", None)
            self.load_instance = getattr(meta, "load_instance", False)
            self.transient = getattr(meta, "transient", False)

    class Schema:
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
            self._load_instance = kwargs.pop("load_instance", self.opts.load_instance)
            super().__init__(*args, **kwargs)

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
            """Deserialize data to an instance of the model if self.load_instance is True.

            Update an existing row if specified in `self.instance` or loaded by primary
            key(s) in the data; else create a new row.

            :param data: Data to deserialize.
            """
            if not self._load_instance:
                return data
            instance = self.instance or self.get_instance(data)
            if instance is not None:
                for key, value in data.items():
                    setattr(instance, key, value)
                return instance
            kwargs, association_attrs = self._split_model_kwargs_association(data)
            instance = self.opts.model(**kwargs)
            for attr, value in association_attrs.items():
                setattr(instance, attr, value)
            return instance

        def load(self, data, *, session=None, instance=None, transient=False, **kwargs):
            """Deserialize data to internal representation.

            :param session: Optional SQLAlchemy session.
            :param instance: Optional existing instance to modify.
            :param transient: Optional switch to allow transient instantiation.
            """
            self._session = session or self._session
            self._transient = transient or self._transient
            if self._load_instance and not (self.transient or self.session):
                raise ValueError("Deserialization requires a session")
            self.instance = instance or self.instance
            try:
                return super().load(data, **kwargs)
            finally:
                self.instance = None

        def validate(self, data, *, session=None, **kwargs):
            self._session = session or self._session
            if not (self.transient or self.session):
                raise ValueError("Validation requires a session")
            return super().validate(data, **kwargs)

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
                for key, value in data.items()
                # association proxy
                if hasattr(getattr(self.opts.model, key, None), "remote_attr")
            }
            kwargs = {
                key: value
                for key, value in data.items()
                if (hasattr(self.opts.model, key) and key not in association_attrs)
            }
            return kwargs, association_attrs
