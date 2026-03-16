from collections import OrderedDict

from promise import Promise

from graphql.type import (
    GraphQLArgument,
    GraphQLInputObjectField,
    GraphQLInputObjectType,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLString,
    GraphQLField,
)
from graphql.error import GraphQLError

from ..utils import resolve_maybe_thunk


def mutation_with_client_mutation_id(
        name, input_fields, output_fields, mutate_and_get_payload):
    augmented_input_fields = OrderedDict(
        resolve_maybe_thunk(input_fields),
        clientMutationId=GraphQLInputObjectField(
            GraphQLNonNull(GraphQLString)
        )
    )
    augmented_output_fields = OrderedDict(
        resolve_maybe_thunk(output_fields),
        clientMutationId=GraphQLField(
            GraphQLNonNull(GraphQLString)
        )
    )

    input_type = GraphQLInputObjectType(
        name + 'Input',
        fields=augmented_input_fields,
    )
    output_type = GraphQLObjectType(
        name + 'Payload',
        fields=augmented_output_fields,
    )

    def resolver(_root, info, **args):
        input_ = args.get('input')

        def on_resolve(payload):
            try:
                payload.clientMutationId = input_['clientMutationId']
            except Exception:
                raise GraphQLError(
                    'Cannot set clientMutationId in the payload object {}'.format(
                        repr(payload)))
            return payload

        return Promise.resolve(mutate_and_get_payload(info, **input_)).then(on_resolve)

    return GraphQLField(
        output_type,
        args=OrderedDict((
            ('input', GraphQLArgument(GraphQLNonNull(input_type))),
        )),
        resolver=resolver
    )
