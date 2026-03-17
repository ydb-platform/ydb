from office365.entity_collection import EntityCollection
from office365.onedrive.lists.list import List
from office365.runtime.paths.v4.entity import EntityPath
from office365.runtime.queries.create_entity import CreateEntityQuery


class ListCollection(EntityCollection[List]):
    """Drive list's collection"""

    def __init__(self, context, resource_path=None):
        super(ListCollection, self).__init__(context, List, resource_path)

    def add(self, display_name, list_template="genericList"):
        """
        Create a new list.

        :param str display_name: The displayable title of the list.
        :param str list_template: The base list template used in creating the list
        """
        return_type = List(self.context, EntityPath(None, self.resource_path))
        self.add_child(return_type)
        payload = {
            "displayName": display_name,
            "list": {"template": list_template},
        }
        qry = CreateEntityQuery(self, payload, return_type)
        self.context.add_query(qry)
        return return_type

    def get_by_name(self, name):
        return List(self.context, EntityPath(name, self.resource_path))

    def get_by_id(self, list_id):
        return List(self.context, EntityPath(list_id, self.resource_path))
