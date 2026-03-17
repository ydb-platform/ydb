from jsonquerylang.compile import compile
from jsonquerylang.parse import parse
from jsonquerylang.types import JsonType, JsonQueryType, JsonQueryOptions


def jsonquery(
    data: JsonType, query: JsonQueryType | str, options: JsonQueryOptions | None = None
) -> JsonType:
    """
    Compile and evaluate a JSON query.

    Example:

        from pprint import pprint
        from jsonquerylang import jsonquery

        input = [
            {"name": "Chris", "age": 23, "scores": [7.2, 5, 8.0]},
            {"name": "Joe", "age": 32, "scores": [6.1, 8.1]},
            {"name": "Emily", "age": 19},
        ]
        query = ["sort", ["get", "age"], "desc"]
        output = jsonquery(query)
        pprint(output)
        # [{'age': 32, 'name': 'Joe', 'scores': [6.1, 8.1]},
        #  {'age': 23, 'name': 'Chris', 'scores': [7.2, 5, 8.0]},
        #  {'age': 19, 'name': 'Emily'}]

    :param data: The JSON document to be queried
    :param query: A JSON Query
    :param options: Can an object with custom functions
    :return: Returns the result of the query applied to the data
    """

    _query = parse(query, options) if type(query) is str else query

    evaluate = compile(_query, options)

    return evaluate(data)
