import graphene
from graphene.types.unmountedtype import UnmountedType


class DictType(UnmountedType):
    key = graphene.String()
    value = graphene.String()
