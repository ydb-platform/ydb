from django_filters import Filter

from graphene.types.utils import get_type


class TypedFilter(Filter):
    """
    Filter class for which the input GraphQL type can explicitly be provided.
    If it is not provided, when building the schema, it will try to guess
    it from the field.
    """

    def __init__(self, input_type=None, *args, **kwargs):
        self._input_type = input_type
        super().__init__(*args, **kwargs)

    @property
    def input_type(self):
        input_type = get_type(self._input_type)
        if input_type is not None:
            if not callable(getattr(input_type, "get_type", None)):
                raise ValueError(
                    "Wrong `input_type` for {}: it only accepts graphene types, got {}".format(
                        self.__class__.__name__, input_type
                    )
                )
        return input_type
