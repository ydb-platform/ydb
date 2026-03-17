from office365.entity_collection import EntityCollection
from office365.todo.attachments.base import AttachmentBase


class AttachmentBaseCollection(EntityCollection):
    def __init__(self, context, resource_path=None):
        super(AttachmentBaseCollection, self).__init__(
            context, AttachmentBase, resource_path
        )

    def create_upload_session(self):
        pass
