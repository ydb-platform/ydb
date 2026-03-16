import graphene
from graphene import ID
from graphene.types.inputobjecttype import InputObjectType
from graphene.utils.str_converters import to_camel_case

from ..converter import EnumValueField
from ..types import ErrorType  # noqa Import ErrorType for backwards compatibility
from .mutation import fields_for_form


class DjangoFormInputObjectType(InputObjectType):
    @classmethod
    def __init_subclass_with_meta__(
        cls,
        container=None,
        _meta=None,
        only_fields=(),
        exclude_fields=(),
        form_class=None,
        object_type=None,
        add_id_field_name=None,
        add_id_field_type=None,
        **options,
    ):
        """Retrieve fields from django form (Meta.form_class). Received
        fields are set to cls (they will be converted to input fields
        by InputObjectType). Type of fields with choices (converted
        to enum) is set to custom scalar type (using Meta.object_type)
        to dynamically convert enum values back.

        class MyDjangoFormInput(DjangoFormInputObjectType):
            # any other fields can be placed here and other inputobjectforms as well

            class Meta:
                form_class = MyDjangoModelForm
                object_type = MyModelType

        class SomeMutation(graphene.Mutation):
            class Arguments:
                data = MyDjangoFormInput(required=True)

            @staticmethod
            def mutate(_root, _info, data):
                form_inst = MyDjangoModelForm(data=data)
                if form_inst.is_valid():
                    django_model_instance = form_inst.save(commit=False)
                # ... etc ...
        """

        if not form_class:
            raise Exception("form_class is required for DjangoFormInputObjectType")

        form = form_class()
        form_fields = fields_for_form(form, only_fields, exclude_fields)

        for name, field in form_fields.items():
            if (
                object_type
                and name in object_type._meta.fields
                and isinstance(object_type._meta.fields[name], EnumValueField)
            ):
                # Field type EnumValueField here means that field
                # with choices have been converted to enum
                setattr(cls, name, cls.get_enum_cnv_cls_instance(name, object_type))
            elif (
                object_type
                and name in object_type._meta.fields
                and object_type._meta.convert_choices_to_enum is False
                and form.fields[name].__class__.__name__ == "TypedChoiceField"
            ):
                # FIXME
                # in case if convert_choices_to_enum is False
                # form field class is converted to String but original
                # model field type is needed here... (.converter.py bug?)
                # This is temp workaround to get field type from ObjectType field
                # TEST: test_enum_not_converted_and_field_type_as_in_model
                setattr(cls, name, object_type._meta.fields[name].type())
            else:
                # set input field according to django form field
                setattr(cls, name, field)

        # explicitly adding id field (absent in django form fields)
        # with name and type from Meta or 'id' with graphene.ID by default
        if add_id_field_name:
            setattr(cls, add_id_field_name, add_id_field_type or ID(required=False))
        elif "id" not in exclude_fields:
            cls.id = ID(required=False)

        super().__init_subclass_with_meta__(container=container, _meta=_meta, **options)

    @staticmethod
    def get_enum_cnv_cls_instance(field_name, object_type):
        """Saves args in context to convert enum values in
        Dynamically created Scalar derived class
        """

        @staticmethod
        def parse_value(value):
            # field_name & object_type have been saved in context (closure)
            field = object_type._meta.fields[field_name]
            if isinstance(field.type, graphene.NonNull):
                val_before_convert = field.type._of_type[value].value
            else:
                val_before_convert = field.type[value].value
            return graphene.String.parse_value(val_before_convert)

        cls_doc = "String scalar to convert choice value back from enum to original"
        scalar_type = type(
            (
                f"{field_name[0].upper()}{to_camel_case(field_name[1:])}"
                "EnumBackConvString"
            ),
            (graphene.String,),
            {"parse_value": parse_value, "__doc__": cls_doc},
        )
        return scalar_type()
