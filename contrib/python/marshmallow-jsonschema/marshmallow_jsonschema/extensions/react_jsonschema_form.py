from marshmallow_jsonschema.base import JSONSchema


class ReactJsonSchemaFormJSONSchema(JSONSchema):
    """
    Usage (assuming marshmallow v3):

    class MySchema(Schema):
        first_name = fields.String(
            metadata={
                'ui:autofocus': True,
            }
        )
        last_name = fields.String()

        class Meta:
            react_uischema_extra = {
                'ui:order': [
                    'first_name',
                    'last_name',
                ]
            }


    json_schema_obj = ReactJsonSchemaFormJSONSchema()
    json_schema, uischema = json_schema_obj.dump_with_uischema(MySchema())
    """

    def dump_with_uischema(self, obj, many=None, *args):
        """Runs both dump and dump_uischema"""
        dump = self.dump(obj, *args, many=many)
        uischema = self.dump_uischema(obj, *args, many=many)
        return dump, uischema

    def dump_uischema(self, obj, many=None, *args):
        """
        Attempt to return something resembling a uiSchema compliant with
        react-jsonschema-form

        See: https://react-jsonschema-form.readthedocs.io/en/latest/form-customization/#the-uischema-object
        """
        return dict(self._dump_uischema_iter(obj, *args, many=many))

    def _dump_uischema_iter(self, obj, many=None, *args):
        """
        This is simply implementing a Dictionary Iterator for
        ReactJsonSchemaFormJSONSchema.dump_uischema
        """

        for k, v in getattr(obj.Meta, "react_uischema_extra", {}).items():
            yield k, v

        for field_name, field in obj.fields.items():
            # NOTE: doubled up to maintain backwards compatibility
            metadata = field.metadata.get("metadata", {})
            metadata.update(field.metadata)
            yield field_name, {k: v for k, v in metadata.items() if k.startswith("ui:")}
