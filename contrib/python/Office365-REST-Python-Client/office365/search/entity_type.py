from typing import Type

from office365.onedrive.driveitems.driveItem import DriveItem
from office365.onedrive.drives.drive import Drive
from office365.onedrive.listitems.list_item import ListItem
from office365.onedrive.lists.list import List
from office365.onedrive.sites.site import Site
from office365.outlook.calendar.events.event import Event
from office365.outlook.mail.messages.message import Message
from office365.outlook.person import Person
from office365.search.external.item import ExternalItem


class EntityType:
    """Search resource type"""

    _types = {
        "event": Event,
        "list": List,
        "site": Site,
        "listItem": ListItem,
        "message": Message,
        "drive": Drive,
        "driveItem": DriveItem,
        "externalItem": ExternalItem,
        "person": Person,
    }

    def __init__(self):
        pass

    @staticmethod
    def resolve(name):
        # type: (str) -> Type[Event | List | Site | ListItem | Message | Drive | DriveItem]
        class_name = name.split(".")[-1]
        return EntityType._types.get(class_name, None)

    event = "event"

    list = "list"

    site = "site"

    listItem = "listItem"

    message = "message"

    drive = "drive"

    driveItem = "driveItem"

    externalItem = "externalItem"

    person = "person"
