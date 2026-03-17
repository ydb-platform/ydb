from office365.search.answer import SearchAnswer


class Acronym(SearchAnswer):
    """Represents an acronym that is an administrative answer in Microsoft Search results to define common
    acronyms in an organization."""

    @property
    def entity_type_name(self):
        return "microsoft.graph.search.acronym"
