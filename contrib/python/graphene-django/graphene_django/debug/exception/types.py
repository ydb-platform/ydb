from graphene import ObjectType, String


class DjangoDebugException(ObjectType):
    class Meta:
        description = "Represents a single exception raised."

    exc_type = String(required=True, description="The class of the exception")
    message = String(required=True, description="The message of the exception")
    stack = String(required=True, description="The stack trace")
