from office365.sharepoint.taxonomy.item_collection import TaxonomyItemCollection
from office365.sharepoint.taxonomy.sets.set import TermSet


class TermSetCollection(TaxonomyItemCollection[TermSet]):
    def __init__(self, context, resource_path=None):
        super(TermSetCollection, self).__init__(context, TermSet, resource_path)
