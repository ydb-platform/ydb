import json
import logging

from .endpoint import Endpoint, api
from .exceptions import GraphQLError, InvalidGraphQLQuery

from tableauserverclient.helpers.logging import logger


def is_valid_paged_query(parsed_query):
    """Check that the required $first and $afterToken variables are present in the query.
    Also check that we are asking for the pageInfo object, so we get the endCursor. There
    is no way to do this relilably without writing a GraphQL parser, so simply check that
    that the string contains 'hasNextPage' and 'endCursor'"""
    return (
        all(k in parsed_query["variables"] for k in ("first", "afterToken"))
        and "hasNextPage" in parsed_query["query"]
        and "endCursor" in parsed_query["query"]
    )


def extract_values(obj, key):
    """Pull all values of specified key from nested JSON.
    Taken from: https://hackersandslackers.com/extract-data-from-complex-json-python/"""
    arr = []

    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    results = extract(obj, arr, key)
    return results


def get_page_info(result):
    next_page = extract_values(result, "hasNextPage")
    cursor = extract_values(result, "endCursor")
    return next_page.pop() if next_page else None, cursor.pop() if cursor else None


class Metadata(Endpoint):
    @property
    def baseurl(self):
        return f"{self.parent_srv.server_address}/api/metadata/graphql"

    @property
    def control_baseurl(self):
        return f"{self.parent_srv.server_address}/api/metadata/v1/control"

    @api("3.5")
    def query(self, query, variables=None, abort_on_error=False, parameters=None):
        logger.info("Querying Metadata API")

        url = self.baseurl

        try:
            graphql_query = json.dumps({"query": query, "variables": variables})
        except Exception as e:
            raise InvalidGraphQLQuery("Must provide a string")

        # Setting content type because post_reuqest defaults to text/xml
        server_response = self.post_request(url, graphql_query, content_type="application/json", parameters=parameters)
        results = server_response.json()

        if abort_on_error and results.get("errors", None):
            raise GraphQLError(results["errors"])

        return results

    @api("3.9")
    def backfill_status(self):
        url = self.control_baseurl + "/backfill/status"
        response = self.get_request(url)
        return response.json()

    @api("3.9")
    def eventing_status(self):
        url = self.control_baseurl + "/eventing/status"
        response = self.get_request(url)
        return response.json()

    @api("3.5")
    def paginated_query(self, query, variables=None, abort_on_error=False):
        logger.info("Querying Metadata API using a Paged Query")
        url = self.baseurl

        if variables is None:
            # default paramaters
            variables = {"first": 100, "afterToken": None}
        elif ("first" in variables) and ("afterToken" not in variables):
            # they passed a page size but not a token, probably because they're starting at `null` token
            variables.update({"afterToken": None})

        graphql_query = json.dumps({"query": query, "variables": variables})
        parsed_query = json.loads(graphql_query)

        if not is_valid_paged_query(parsed_query):
            raise InvalidGraphQLQuery(
                "Paged queries must have a `$first` and `$afterToken` variables as well as "
                "a pageInfo object with `endCursor` and `hasNextPage`"
            )

        results_dict = {"pages": []}
        paginated_results = results_dict["pages"]

        # get first page
        server_response = self.post_request(url, graphql_query, content_type="application/json")
        results = server_response.json()

        if abort_on_error and results.get("errors", None):
            raise GraphQLError(results["errors"])

        paginated_results.append(results)

        # repeat
        has_another_page, cursor = get_page_info(results)

        while has_another_page:
            # Update the page
            variables.update({"afterToken": cursor})
            # make the call
            logger.debug("Calling Token: " + cursor)
            graphql_query = json.dumps({"query": query, "variables": variables})
            server_response = self.post_request(url, graphql_query, content_type="application/json")
            results = server_response.json()
            # verify response
            if abort_on_error and results.get("errors", None):
                raise GraphQLError(results["errors"])
            # save results and repeat
            paginated_results.append(results)
            has_another_page, cursor = get_page_info(results)

        logger.info("Sucessfully got all results for paged query")
        return results_dict
