from office365.runtime.client_value import ClientValue


class RenderListDataOverrideParameters(ClientValue):
    def __init__(self):
        super(RenderListDataOverrideParameters, self).__init__()

    @property
    def entity_type_name(self):
        return "SP.RenderListDataOverrideParameters"
