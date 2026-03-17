from typing import AnyStr, Optional

from office365.todo.attachments.base import AttachmentBase


class TaskFileAttachment(AttachmentBase):
    """
    Represents a file, such as a text file or Word document, attached to a todoTask.
    When you create a file attachment on a task, include "@odata.type": "#microsoft.graph.taskFileAttachment"
    and the properties name and contentBytes.
    """

    @property
    def content_bytes(self):
        # type: () -> Optional[AnyStr]
        """The base64-encoded contents of the file."""
        return self.properties.get("contentBytes", None)
