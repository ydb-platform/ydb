import json
import warnings

from django.test import Client, TestCase, TransactionTestCase

from graphene_django.settings import graphene_settings
from graphene_django.utils.utils import _DJANGO_VERSION_AT_LEAST_4_2

DEFAULT_GRAPHQL_URL = "/graphql"


def graphql_query(
    query,
    operation_name=None,
    input_data=None,
    variables=None,
    headers=None,
    client=None,
    graphql_url=None,
):
    """
    Args:
        query (string)              - GraphQL query to run
        operation_name (string)     - If the query is a mutation or named query, you must
                                      supply the operation_name.  For annon queries ("{ ... }"),
                                      should be None (default).
        input_data (dict)           - If provided, the $input variable in GraphQL will be set
                                      to this value. If both ``input_data`` and ``variables``,
                                      are provided, the ``input`` field in the ``variables``
                                      dict will be overwritten with this value.
        variables (dict)            - If provided, the "variables" field in GraphQL will be
                                      set to this value.
        headers (dict)              - If provided, the headers in POST request to GRAPHQL_URL
                                      will be set to this value. Keys should be prepended with
                                      "HTTP_" (e.g. to specify the "Authorization" HTTP header,
                                      use "HTTP_AUTHORIZATION" as the key).
        client (django.test.Client) - Test client. Defaults to django.test.Client.
        graphql_url (string)        - URL to graphql endpoint. Defaults to "/graphql".

    Returns:
        Response object from client
    """
    if client is None:
        client = Client()
    if not graphql_url:
        graphql_url = graphene_settings.TESTING_ENDPOINT

    body = {"query": query}
    if operation_name:
        body["operationName"] = operation_name
    if variables:
        body["variables"] = variables
    if input_data:
        if "variables" in body:
            body["variables"]["input"] = input_data
        else:
            body["variables"] = {"input": input_data}
    if headers:
        header_params = (
            {"headers": headers} if _DJANGO_VERSION_AT_LEAST_4_2 else headers
        )
        resp = client.post(
            graphql_url,
            json.dumps(body),
            content_type="application/json",
            **header_params,
        )
    else:
        resp = client.post(
            graphql_url, json.dumps(body), content_type="application/json"
        )
    return resp


class GraphQLTestMixin:
    """
    Based on: https://www.sam.today/blog/testing-graphql-with-graphene-django/
    """

    # URL to graphql endpoint
    GRAPHQL_URL = graphene_settings.TESTING_ENDPOINT

    def query(
        self, query, operation_name=None, input_data=None, variables=None, headers=None
    ):
        """
        Args:
            query (string)    - GraphQL query to run
            operation_name (string)  - If the query is a mutation or named query, you must
                                supply the operation_name.  For annon queries ("{ ... }"),
                                should be None (default).
            input_data (dict) - If provided, the $input variable in GraphQL will be set
                                to this value. If both ``input_data`` and ``variables``,
                                are provided, the ``input`` field in the ``variables``
                                dict will be overwritten with this value.
            variables (dict)  - If provided, the "variables" field in GraphQL will be
                                set to this value.
            headers (dict)    - If provided, the headers in POST request to GRAPHQL_URL
                                will be set to this value. Keys should be prepended with
                                "HTTP_" (e.g. to specify the "Authorization" HTTP header,
                                use "HTTP_AUTHORIZATION" as the key).

        Returns:
            Response object from client
        """
        return graphql_query(
            query,
            operation_name=operation_name,
            input_data=input_data,
            variables=variables,
            headers=headers,
            client=self.client,
            graphql_url=self.GRAPHQL_URL,
        )

    @property
    def _client(self):
        pass

    @_client.getter
    def _client(self):
        warnings.warn(
            "Using `_client` is deprecated in favour of `client`.",
            PendingDeprecationWarning,
            stacklevel=2,
        )
        return self.client

    @_client.setter
    def _client(self, client):
        warnings.warn(
            "Using `_client` is deprecated in favour of `client`.",
            PendingDeprecationWarning,
            stacklevel=2,
        )
        self.client = client

    def assertResponseNoErrors(self, resp, msg=None):
        """
        Assert that the call went through correctly. 200 means the syntax is ok, if there are no `errors`,
        the call was fine.
        :resp HttpResponse: Response
        """
        content = json.loads(resp.content)
        self.assertEqual(resp.status_code, 200, msg or content)
        self.assertNotIn("errors", list(content.keys()), msg or content)

    def assertResponseHasErrors(self, resp, msg=None):
        """
        Assert that the call was failing. Take care: Even with errors, GraphQL returns status 200!
        :resp HttpResponse: Response
        """
        content = json.loads(resp.content)
        self.assertIn("errors", list(content.keys()), msg or content)


class GraphQLTestCase(GraphQLTestMixin, TestCase):
    pass


class GraphQLTransactionTestCase(GraphQLTestMixin, TransactionTestCase):
    pass
