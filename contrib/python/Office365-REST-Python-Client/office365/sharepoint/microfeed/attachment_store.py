from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity


class MicrofeedAttachmentStore(Entity):
    def __init__(self, context):
        super(MicrofeedAttachmentStore, self).__init__(
            context, ResourcePath("SP.Microfeed.MicrofeedAttachmentStore")
        )
