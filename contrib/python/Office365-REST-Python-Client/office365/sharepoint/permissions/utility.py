from office365.sharepoint.entity import Entity


class Utility(Entity):
    def __init__(self, context, resource_path):
        super(Utility, self).__init__(context, resource_path, "SP.Utilities")
