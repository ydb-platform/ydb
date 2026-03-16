class ResultMock(object):

    def __init__(
            self, body=None, parameters=None, data=None, error_to_raise=None):
        self.body = body
        self.parameters = parameters
        self.data = data
        self.error_to_raise = error_to_raise

    def raise_for_errors(self):
        if self.error_to_raise is not None:
            raise self.error_to_raise

        if self.parameters is not None:
            return self.parameters

        if self.data is not None:
            return self.data
