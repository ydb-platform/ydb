from office365.entity import Entity


class WorkbookFunctionResult(Entity):
    """"""

    @property
    def value(self):
        return self.properties.get("value", None)
