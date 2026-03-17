from office365.runtime.client_value import ClientValue


class AlternativeSecurityId(ClientValue):
    """"""

    def __init__(self, type_=None, key=None):
        self.type = type_
        self.key = key
