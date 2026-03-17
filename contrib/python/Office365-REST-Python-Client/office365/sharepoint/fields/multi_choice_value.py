from office365.runtime.client_value_collection import ClientValueCollection


class FieldMultiChoiceValue(ClientValueCollection):
    def __init__(self, choices):
        """
        :param list[str] choices: Choice values
        """
        super(FieldMultiChoiceValue, self).__init__(str)
        [self.add(choice) for choice in choices]
