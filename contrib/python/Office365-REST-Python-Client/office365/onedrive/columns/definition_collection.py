from typing import Optional

from typing_extensions import TYPE_CHECKING

from office365.entity_collection import EntityCollection
from office365.onedrive.columns.definition import ColumnDefinition
from office365.runtime.queries.create_entity import CreateEntityQuery

if TYPE_CHECKING:
    from office365.onedrive.lists.list import List


class ColumnDefinitionCollection(EntityCollection[ColumnDefinition]):
    def __init__(self, context, resource_path, parent):
        super(ColumnDefinitionCollection, self).__init__(
            context, ColumnDefinition, resource_path, parent
        )

    def add_number(self, name, minimum=None, maximum=None):
        """
        Creates a number column
        :param str name: The API-facing name of the column as it appears in the fields on a listItem
        :param float minimum: The minimum permitted value.
        :param float maximum: The maximum permitted value.
        """
        from office365.onedrive.columns.number import NumberColumn

        return self.add(name=name, number=NumberColumn(minimum, maximum))

    def add_text(self, name, max_length=None, text_type=None):
        """
        Creates a text column

        :param str name: The API-facing name of the column as it appears in the fields on a listItem
        :param int or None max_length: The maximum number of characters for the value.
        :param str or None text_type: The type of text being stored
        """
        from office365.onedrive.columns.text import TextColumn

        return self.add(
            name=name, text=TextColumn(max_length=max_length, text_type=text_type)
        )

    def add_hyperlink_or_picture(self, name, is_picture=None):
        """
        Creates a hyperlink or picture column

        :param str name: The API-facing name of the column as it appears in the fields on a listItem
        :param bool is_picture: Specifies whether the display format used for URL columns is an image or a hyperlink.
        """
        from office365.onedrive.columns.hyperlink_or_picture import (
            HyperlinkOrPictureColumn,
        )

        return self.add(
            name=name,
            hyperlinkOrPicture=HyperlinkOrPictureColumn(is_picture=is_picture),
        )

    def add_lookup(self, name, lookup_list, column_name=None):
        # type: (str, List|str, Optional[str]) -> ColumnDefinition
        """
        Creates a lookup column

        :param str name: The API-facing name of the column as it appears in the fields on a listItem
        :param office365.onedrive.lists.list.List or str lookup_list: Lookup source list or identifier
        :param str column_name: The name of the lookup source column.
        """
        from office365.onedrive.columns.lookup import LookupColumn
        from office365.onedrive.lists.list import List  # noqa

        if isinstance(lookup_list, List):
            return_type = ColumnDefinition(self.context)
            self.add_child(return_type)

            def _list_loaded():
                params = {
                    "name": name,
                    "lookup": LookupColumn(lookup_list.id, column_name),
                }
                qry = CreateEntityQuery(self, params, return_type)
                self.context.add_query(qry)

            lookup_list.ensure_property("id", _list_loaded)
            return return_type
        else:
            return self.add(name=name, lookup=LookupColumn(lookup_list, column_name))
