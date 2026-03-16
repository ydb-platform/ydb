from office365.sharepoint.changes.change import Change
from office365.sharepoint.entity_collection import EntityCollection


class ChangeCollection(EntityCollection[Change]):
    """Represents a collection of Change objects"""

    def __init__(self, context, resource_path=None):
        super(ChangeCollection, self).__init__(context, Change, resource_path)

    def set_property(self, key, value, persist_changes=False):
        self._resolve_change_type(value)
        super(ChangeCollection, self).set_property(key, value)

    def _resolve_change_type(self, properties):
        # type: (dict) -> None
        """Resolves a change type"""
        from office365.sharepoint.changes.alert import ChangeAlert
        from office365.sharepoint.changes.content_type import ChangeContentType
        from office365.sharepoint.changes.field import ChangeField
        from office365.sharepoint.changes.group import ChangeGroup
        from office365.sharepoint.changes.item import ChangeItem
        from office365.sharepoint.changes.list import ChangeList
        from office365.sharepoint.changes.user import ChangeUser
        from office365.sharepoint.changes.view import ChangeView
        from office365.sharepoint.changes.web import ChangeWeb

        if "ItemId" in properties and "ListId" in properties:
            self._item_type = ChangeItem
        elif "ListId" in properties and "WebId" in properties:
            self._item_type = ChangeList
        elif "WebId" in properties:
            self._item_type = ChangeWeb
        elif "UserId" in properties:
            self._item_type = ChangeUser
        elif "GroupId" in properties:
            self._item_type = ChangeGroup
        elif "ContentTypeId" in properties:
            self._item_type = ChangeContentType
        elif "AlertId" in properties:
            self._item_type = ChangeAlert
        elif "FieldId" in properties:
            self._item_type = ChangeField
        elif "ViewId" in properties:
            self._item_type = ChangeView
