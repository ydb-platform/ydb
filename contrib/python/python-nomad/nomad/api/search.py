"""Nomad Search API: https://developer.hashicorp.com/nomad/api-docs/search"""

import nomad.api.exceptions

from nomad.api.base import Requester


class Search(Requester):
    """
    The endpoint returns matches for a given prefix and context, where a context can be jobs, allocations, evaluations,
    nodes, deployments, plugins, namespaces, or volumes.
    When using Nomad Enterprise, the allowed contexts include quotas.
    Additionally, a prefix can be searched for within every context.

    https://developer.hashicorp.com/nomad/api-docs/search
    """

    ENDPOINT = "search"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __str__(self):
        return f"{self.__dict__}"

    def __repr__(self):
        return f"{self.__dict__}"

    def __getattr__(self, item):
        msg = f"{item} does not exist"
        raise AttributeError(msg)

    def search(self, prefix, context):
        """The endpoint returns matches for a given prefix and context, where a context can be jobs,
        allocations, evaluations, nodes, deployments, plugins, namespaces, or volumes.

        https://developer.hashicorp.com/nomad/api-docs/search
        arguments:
          - prefix:(str) required, specifies the identifier against which matches will be found.
            For example, if the given prefix were "a", potential matches might be "abcd", or "aabb".
          - context:(str) defines the scope in which a search for a prefix operates.
            Contexts can be: "jobs", "evals", "allocs", "nodes", "deployment", "plugins",
            "volumes" or "all", where "all" means every context will be searched.
        returns: dict
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
          - nomad.api.exceptions.InvalidParameters
        """
        accetaple_contexts = (
            "jobs",
            "evals",
            "allocs",
            "nodes",
            "deployment",
            "plugins",
            "volumes",
            "all",
        )
        if context not in accetaple_contexts:
            raise nomad.api.exceptions.InvalidParameters(
                "context is invalid " f"(expected values are {accetaple_contexts} but got {context})"
            )
        params = {"Prefix": prefix, "Context": context}

        return self.request(json=params, method="post").json()

    def fuzzy_search(self, text, context):
        """The /search/fuzzy endpoint returns partial substring matches for a given search term and context,
        where a context can be jobs, allocations, nodes, plugins, or namespaces. Additionally,
        fuzzy searching can be done across all contexts.

        https://developer.hashicorp.com/nomad/api-docs/search#fuzzy-searching
        arguments:
          - text:(str) required, specifies the identifier against which matches will be found.
            For example, if the given text were "py", potential fuzzy matches might be "python", "spying",
            or "happy".
          - context:(str) defines the scope in which a search for a prefix operates. Contexts can be:
            "jobs", "allocs", "nodes", "plugins", or "all", where "all" means every context will
            be searched.
            When "all" is selected, additional prefix matches will be included for the "deployments",
            "evals", and "volumes" types. When searching in the "jobs" context, results that fuzzy match
            "groups", "services", "tasks", "images", "commands", and "classes" are also included in the results.
        returns: dict
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """

        params = {"Text": text, "Context": context}

        accetaple_contexts = ("jobs", "allocs", "nodes", "plugins", "all")
        if context not in accetaple_contexts:
            raise nomad.api.exceptions.InvalidParameters(
                "context is invalid " f"(expected values are {accetaple_contexts} but got {context})"
            )

        return self.request("fuzzy", json=params, method="post").json()
