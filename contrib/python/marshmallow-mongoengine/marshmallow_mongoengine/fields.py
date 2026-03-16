import bson
import uuid
from bson.errors import BSONError
from marshmallow import ValidationError, fields, missing
from mongoengine import ValidationError as MongoValidationError, NotRegistered
from mongoengine.base import get_document

# Republish default fields...
from marshmallow.fields import *  # noqa


class ObjectId(fields.Field):
    def _deserialize(self, value, attr, data, **kwargs):
        try:
            return bson.ObjectId(value)
        except BSONError:
            raise ValidationError("invalid ObjectId `%s`" % value)

    def _serialize(self, value, attr, obj, **kwargs):
        if value is None:
            return missing
        return str(value)


class Point(fields.Field):
    def _deserialize(self, value, attr, data, **kwargs):
        try:
            return dict(
                type="Point", coordinates=[float(value["x"]), float(value["y"])]
            )
        except Exception:
            raise ValidationError("invalid Point `%s`" % value)

    def _serialize(self, value, attr, obj, **kwargs):
        if value is None:
            return missing
        return dict(x=value["coordinates"][0], y=value["coordinates"][1])


class LineString(fields.Field):

    """
    Marshmallow custom field to map with :class Mongoengine.LineStringField:
    """

    def _deserialize(self, value, attr, data, **kwargs):
        try:
            coordinates = []

            for x, y in value["coordinates"]:
                coordinates.append([float(x), float(y)])
            return dict(type="LineString", coordinates=coordinates)
        except Exception:
            raise ValidationError("invalid value data `%s`" % value)

    def _serialize(self, value, attr, obj):
        if value is None:
            return missing
        return dict(coordinates=value["coordinates"])


class Reference(fields.Field):

    """
    Marshmallow custom field to map with :class Mongoengine.ReferenceField:
    """

    def __init__(self, document_type_obj, *args, **kwargs):
        self.document_type_obj = document_type_obj
        super(Reference, self).__init__(*args, **kwargs)

    @property
    def document_type(self):
        if isinstance(self.document_type_obj, str):
            self.document_type_obj = get_document(self.document_type_obj)
        return self.document_type_obj

    def _deserialize(self, value, attr, data, **kwargs):
        document_type = self.document_type
        try:
            if isinstance(value, dict):
                return document_type(**value)

            return document_type.objects.get(pk=value)
        except (
            document_type.DoesNotExist,
            MongoValidationError,
            ValueError,
            TypeError,
        ):
            raise ValidationError(
                "unknown document %s `%s`" % (document_type._class_name, value)
            )
        return value

    def _serialize(self, value, attr, obj, **kwargs):
        # Only return the pk of the document for serialization
        if value is None:
            return missing
        return (
            str(value.pk)
            if isinstance(value.pk, (bson.ObjectId, uuid.UUID))
            else value.pk
        )


class GenericReference(fields.Field):

    """
    Marshmallow custom field to map with :class Mongoengine.GenericReferenceField:

    :param choices: List of Mongoengine document class (or class name) allowed

    .. note:: Without `choices` param, this field allow to reference to
        any document in the application which can be a security issue.
    """

    def __init__(self, *args, **kwargs):
        self.document_class_choices = []
        choices = kwargs.pop("choices", None)
        if choices:
            # Temporary fix for https://github.com/MongoEngine/mongoengine/pull/1060
            for choice in choices:
                if hasattr(choice, "_class_name"):
                    self.document_class_choices.append(choice._class_name)
                else:
                    self.document_class_choices.append(choice)
        super(GenericReference, self).__init__(*args, **kwargs)

    def _deserialize(self, value, attr, data, **kwargs):
        # To deserialize a generic reference, we need a _cls field in addition
        # with the id field
        if not isinstance(value, dict) or not value.get("id") or not value.get("_cls"):
            raise ValidationError("Need a dict with 'id' and '_cls' fields")
        doc_id = value["id"]
        doc_cls_name = value["_cls"]
        if (
            self.document_class_choices
            and doc_cls_name not in self.document_class_choices
        ):
            raise ValidationError(
                "Invalid _cls field `%s`, must be one of %s"
                % (doc_cls_name, self.document_class_choices)
            )
        try:
            doc_cls = get_document(doc_cls_name)
        except NotRegistered:
            raise ValidationError("Invalid _cls field `%s`" % doc_cls_name)
        try:
            doc = doc_cls.objects.get(pk=doc_id)
        except (doc_cls.DoesNotExist, MongoValidationError, ValueError, TypeError):
            raise ValidationError("unknown document %s `%s`" % (doc_cls_name, value))
        return doc

    def _serialize(self, value, attr, obj, **kwargs):
        # Only return the pk of the document for serialization
        if value is None:
            return missing
        return str(value.pk) if isinstance(value.pk, bson.ObjectId) else value.pk


class GenericEmbeddedDocument(fields.Field):

    """
    Dynamic embedded document
    """

    def _deserialize(self, value, attr, data, **kwargs):
        # Cannot deserialize given we have no way knowing wich kind of
        # document is given...
        return missing

    def _serialize(self, value, attr, obj, **kwargs):
        # Create the schema at serialize time to be dynamic
        from marshmallow_mongoengine.schema import ModelSchema

        class NestedSchema(ModelSchema):
            class Meta:
                model = type(value)

        return NestedSchema().dump(value)


class Map(fields.Field):

    """
    Marshmallow custom field to map with :class Mongoengine.Map:
    """

    def __init__(self, mapped, **kwargs):
        self.mapped = mapped
        self.schema = getattr(mapped, "schema", None)
        super(Map, self).__init__(**kwargs)

    def _schema_process(self, action, value):
        func = getattr(self.schema, action)
        total = {}
        for k, v in value.items():
            total[k] = func(v)
        return total

    def _serialize(self, value, attr, obj, **kwargs):
        if self.schema:
            return self._schema_process("dump", value)
        else:
            return value

    def _deserialize(self, value, attr, data, **kwargs):
        if self.schema:
            return self._schema_process("load", value)
        else:
            return value


class Skip(fields.Field):

    """
    Marshmallow custom field that just ignore the current field
    """

    def _deserialize(self, value, attr, data, **kwargs):
        return missing

    def _serialize(self, value, attr, obj, **kwargs):
        return missing
