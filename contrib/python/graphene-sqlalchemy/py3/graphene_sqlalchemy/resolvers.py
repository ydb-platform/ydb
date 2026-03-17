from graphene.utils.get_unbound_function import get_unbound_function


def get_custom_resolver(obj_type, orm_field_name):
    """
    Since `graphene` will call `resolve_<field_name>` on a field only if it
    does not have a `resolver`, we need to re-implement that logic here so
    users are able to override the default resolvers that we provide.
    """
    resolver = getattr(obj_type, 'resolve_{}'.format(orm_field_name), None)
    if resolver:
        return get_unbound_function(resolver)

    return None


def get_attr_resolver(obj_type, model_attr):
    """
    In order to support field renaming via `ORMField.model_attr`,
    we need to define resolver functions for each field.

    :param SQLAlchemyObjectType obj_type:
    :param str model_attr: the name of the SQLAlchemy attribute
    :rtype: Callable
    """
    return lambda root, _info: getattr(root, model_attr, None)
