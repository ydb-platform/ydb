from office365.sharepoint.taxonomy.groups.group import TermGroup
from office365.sharepoint.taxonomy.item_collection import TaxonomyItemCollection


class TermGroupCollection(TaxonomyItemCollection[TermGroup]):
    """A collection of TermGroup (section 3.1.5.18) objects"""

    def __init__(self, context, resource_path=None):
        super(TermGroupCollection, self).__init__(context, TermGroup, resource_path)

    def get_by_name(self, name):
        """
        Returns the term group with the specified name.
        :param str name: The name of the TermGroup.
        """
        return self.single("name eq '{0}'".format(name))
