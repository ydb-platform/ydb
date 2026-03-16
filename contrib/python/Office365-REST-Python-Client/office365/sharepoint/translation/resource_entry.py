from office365.runtime.client_value import ClientValue


class SPResourceEntry(ClientValue):
    def __init__(self, lcid=None, value=None):
        self.LCID = lcid
        self.Value = value
