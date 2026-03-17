from office365.entity_collection import EntityCollection
from office365.onedrive.termstore.terms.label import LocalizedLabel
from office365.onedrive.termstore.terms.term import Term
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.queries.create_entity import CreateEntityQuery


class TermCollection(EntityCollection[Term]):
    def __init__(self, context, resource_path=None, parent_set=None):
        """
        :param office365.onedrive.termstore.sets.set.Set parent_set: The parent set that contains the term
        """
        super(TermCollection, self).__init__(context, Term, resource_path)
        self._parent_set = parent_set

    def add(self, label):
        """Create a new term object.

        :param str label: The name of the label.
        """
        return_type = Term(self.context)
        self.add_child(return_type)

        def _set_loaded():
            term_create_info = {
                "labels": ClientValueCollection(LocalizedLabel, [LocalizedLabel(label)])
            }
            qry = CreateEntityQuery(self, term_create_info, return_type)
            self.context.add_query(qry)

        self._parent_set.ensure_property("id", _set_loaded)
        return return_type
