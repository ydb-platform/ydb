from typing import Type

from pydantic import BaseModel
from werkzeug.datastructures import ImmutableMultiDict


def convert_query_params(
    query_params: ImmutableMultiDict, model: Type[BaseModel]
) -> dict:
    """
    group query parameters into lists if model defines them

    :param query_params: flasks request.args
    :param model: query parameter's model
    :return: resulting parameters
    """
    return {
        **query_params.to_dict(),
        **{
            key: value
            for key, value in query_params.to_dict(flat=False).items()
            if key in model.__fields__ and model.__fields__[key].is_complex()
        },
    }
