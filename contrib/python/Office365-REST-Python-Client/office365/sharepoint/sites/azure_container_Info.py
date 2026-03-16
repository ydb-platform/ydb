from office365.runtime.client_value import ClientValue


class ProvisionedTemporaryAzureContainerInfo(ClientValue):

    def __init__(self, encryption_key=None, uri=None):
        self.EncryptionKey = encryption_key
        self.Uri = uri
