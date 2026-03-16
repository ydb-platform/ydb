from enumfields.drf.fields import EnumField as EnumSerializerField
from enumfields.fields import EnumFieldMixin
from rest_framework.fields import CharField, ChoiceField, IntegerField


class EnumSupportSerializerMixin:
    enumfield_options = {}
    enumfield_classes_to_replace = (ChoiceField, CharField, IntegerField)

    def build_standard_field(self, field_name, model_field):
        field_class, field_kwargs = (
            super().build_standard_field(field_name, model_field)
        )
        if isinstance(model_field, EnumFieldMixin) and field_class in self.enumfield_classes_to_replace:
            field_class = EnumSerializerField
            field_kwargs['enum'] = model_field.enum
            field_kwargs.update(self.enumfield_options)
        return field_class, field_kwargs
