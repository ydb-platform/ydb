# -*- coding: utf-8 -*-
import copy

from mongoengine.base import BaseDocument
import marshmallow as ma
from marshmallow_mongoengine.convert import ModelConverter


DEFAULT_SKIP_VALUES = (None, [], {})


class SchemaOpts(ma.SchemaOpts):
    """Options class for `ModelSchema`.
    Adds the following options:

    - ``model``: The Mongoengine Document model to generate the `Schema`
        from (required).
    - ``model_fields_kwargs``: Dict of {field: kwargs} to provide as
        additionals argument during fields creation.
    - ``model_build_obj``: If true, :Schema load: returns a :model: objects
        instead of a dict (default: True).
    - ``model_converter``: `ModelConverter` class to use for converting the
        Mongoengine Document model to marshmallow fields.
    - ``model_dump_only_pk``: If the document autogenerate it primary_key
        (default behaviour in Mongoengine), ignore it from the incomming data
        (default: False)
    - ``model_skip_values``: Skip the field if it contains one of the given
        values (default: None, [] and {})
    """

    def __init__(self, meta, *args, **kwargs):
        super(SchemaOpts, self).__init__(meta, *args, **kwargs)
        self.model = getattr(meta, "model", None)
        if self.model and not issubclass(self.model, BaseDocument):
            raise ValueError(
                "`model` must be a subclass of mongoengine.base.BaseDocument"
            )
        self.model_fields_kwargs = getattr(meta, "model_fields_kwargs", {})
        self.model_dump_only_pk = getattr(meta, "model_dump_only_pk", False)
        self.model_converter = getattr(meta, "model_converter", ModelConverter)
        self.model_build_obj = getattr(meta, "model_build_obj", True)
        self.model_skip_values = getattr(meta, "model_skip_values", DEFAULT_SKIP_VALUES)


class SchemaMeta(ma.schema.SchemaMeta):
    """Metaclass for `ModelSchema`."""

    # override SchemaMeta
    @classmethod
    def get_declared_fields(mcs, klass, *args, **kwargs):
        """Updates declared fields with fields converted from the
        Mongoengine model passed as the `model` class Meta option.
        """
        declared_fields = kwargs.get("dict_class", dict)()
        # Generate the fields provided through inheritance
        opts = klass.opts
        model = getattr(opts, "model", None)
        if model:
            converter = opts.model_converter()
            declared_fields.update(
                converter.fields_for_model(model, fields=opts.fields)
            )
        # Generate the fields provided in the current class
        base_fields = super(SchemaMeta, mcs).get_declared_fields(klass, *args, **kwargs)
        declared_fields.update(base_fields)
        # Customize fields with provided kwargs
        for field_name, field_kwargs in klass.opts.model_fields_kwargs.items():
            field = declared_fields.get(field_name, None)
            if field:
                # Copy to prevent alteration of a possible parent class's field
                field = copy.copy(field)
                for key, value in field_kwargs.items():
                    setattr(field, key, value)
                declared_fields[field_name] = field
        if opts.model_dump_only_pk and opts.model:
            # If primary key is automatically generated (nominal case), we
            # must make sure this field is read-only
            if opts.model._auto_id_field is True:
                field_name = opts.model._meta["id_field"]
                id_field = declared_fields.get(field_name)
                if id_field:
                    # Copy to prevent alteration of a possible parent class's field
                    id_field = copy.copy(id_field)
                    id_field.dump_only = True
                    declared_fields[field_name] = id_field
        return declared_fields


class ModelSchema(ma.Schema, metaclass=SchemaMeta):
    """Base class for Mongoengine model-based Schemas.

    Example: ::

        from marshmallow_mongoengine import ModelSchema
        from mymodels import User

        class UserSchema(ModelSchema):
            class Meta:
                model = User
    """

    OPTIONS_CLASS = SchemaOpts

    @ma.post_dump
    def _remove_skip_values(self, data, **kwargs):
        to_skip = self.opts.model_skip_values
        return {key: value for key, value in data.items() if value not in to_skip}

    @ma.post_load
    def _make_object(self, data, **kwargs):
        if self.opts.model_build_obj and self.opts.model:
            return self.opts.model(**data)
        else:
            return data

    def update(self, obj, data):
        """Helper function to update an already existing document
        instead of creating a new one.
        :param obj: Mongoengine Document to update
        :param data: incomming payload to deserialize
        :return: an :class UnmarshallResult:

        Example: ::

            from marshmallow_mongoengine import ModelSchema
            from mymodels import User

            class UserSchema(ModelSchema):
                class Meta:
                    model = User

            def update_obj(id, payload):
                user = User.objects(id=id).first()
                result = UserSchema().update(user, payload)
                result.data is user # True

        Note:

            Given the update is done on a existing object, the required param
            on the fields is ignored
        """
        # TODO: find a cleaner way to skip required validation on update
        required_fields = [k for k, f in self.fields.items() if f.required]
        for field in required_fields:
            self.fields[field].required = False
        loaded_data = self._do_load(data, postprocess=False)
        for field in required_fields:
            self.fields[field].required = True
        # Update the given obj fields
        for k, v in loaded_data.items():
            # Skip default values that have been automatically
            # added during unserialization
            if k in data:
                setattr(obj, k, v)
        return obj
