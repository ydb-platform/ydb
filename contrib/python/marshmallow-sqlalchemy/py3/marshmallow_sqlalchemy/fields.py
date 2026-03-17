from marshmallow import fields
from marshmallow.utils import is_iterable_but_not_string

from sqlalchemy import inspect
from sqlalchemy.orm.exc import NoResultFound


def get_primary_keys(model):
    """Get primary key properties for a SQLAlchemy model.

    :param model: SQLAlchemy model class
    """
    mapper = model.__mapper__
    return [mapper.get_property_by_column(column) for column in mapper.primary_key]


def ensure_list(value):
    return value if is_iterable_but_not_string(value) else [value]


class RelatedList(fields.List):
    def get_value(self, obj, attr, accessor=None):
        # Do not call `fields.List`'s get_value as it calls the container's
        # `get_value` if the container has `attribute`.
        # Instead call the `get_value` from the parent of `fields.List`
        # so the special handling is avoided.
        return super(fields.List, self).get_value(obj, attr, accessor=accessor)


class Related(fields.Field):
    """Related data represented by a SQLAlchemy `relationship`. Must be attached
    to a :class:`Schema` class whose options includes a SQLAlchemy `model`, such
    as :class:`ModelSchema`.

    :param list columns: Optional column names on related model. If not provided,
        the primary key(s) of the related model will be used.
    """

    default_error_messages = {
        "invalid": "Could not deserialize related value {value!r}; "
        "expected a dictionary with keys {keys!r}"
    }

    def __init__(self, column=None, **kwargs):
        super().__init__(**kwargs)
        self.columns = ensure_list(column or [])

    @property
    def model(self):
        return self.root.opts.model

    @property
    def related_model(self):
        model_attr = getattr(self.model, self.attribute or self.name)
        if hasattr(model_attr, "remote_attr"):  # handle association proxies
            model_attr = model_attr.remote_attr
        return model_attr.property.mapper.class_

    @property
    def related_keys(self):
        if self.columns:
            insp = inspect(self.related_model)
            return [insp.attrs[column] for column in self.columns]
        return get_primary_keys(self.related_model)

    @property
    def session(self):
        return self.root.session

    @property
    def transient(self):
        return self.root.transient

    def _serialize(self, value, attr, obj):
        ret = {prop.key: getattr(value, prop.key, None) for prop in self.related_keys}
        return ret if len(ret) > 1 else list(ret.values())[0]

    def _deserialize(self, value, *args, **kwargs):
        """Deserialize a serialized value to a model instance.

        If the parent schema is transient, create a new (transient) instance.
        Otherwise, attempt to find an existing instance in the database.
        :param value: The value to deserialize.
        """
        if not isinstance(value, dict):
            if len(self.related_keys) != 1:
                keys = [prop.key for prop in self.related_keys]
                raise self.make_error("invalid", value=value, keys=keys)
            value = {self.related_keys[0].key: value}
        if self.transient:
            return self.related_model(**value)
        try:
            result = self._get_existing_instance(
                self.session.query(self.related_model), value
            )
        except NoResultFound:
            # The related-object DNE in the DB, but we still want to deserialize it
            # ...perhaps we want to add it to the DB later
            return self.related_model(**value)
        return result

    def _get_existing_instance(self, query, value):
        """Retrieve the related object from an existing instance in the DB.

        :param query: A SQLAlchemy `Query <sqlalchemy.orm.query.Query>` object.
        :param value: The serialized value to mapto an existing instance.
        :raises NoResultFound: if there is no matching record.
        """
        if self.columns:
            result = query.filter_by(
                **{prop.key: value.get(prop.key) for prop in self.related_keys}
            ).one()
        else:
            # Use a faster path if the related key is the primary key.
            lookup_values = [value.get(prop.key) for prop in self.related_keys]
            try:
                result = query.get(lookup_values)
            except TypeError:
                keys = [prop.key for prop in self.related_keys]
                raise self.make_error("invalid", value=value, keys=keys)
            if result is None:
                raise NoResultFound
        return result


class Nested(fields.Nested):
    """Nested field that inherits the session from its parent."""

    def _deserialize(self, *args, **kwargs):
        if hasattr(self.schema, "session"):
            self.schema.session = self.root.session
            self.schema.transient = self.root.transient
        return super()._deserialize(*args, **kwargs)
