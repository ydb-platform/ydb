from office365.runtime.client_value import ClientValue


class DefaultColumnValue(ClientValue):
    """The defaultColumnValue on a columnDefinition resource specifies the default value for this column.
    The default value can either be specified directly or as a formula."""

    def __init__(self, formula=None, value=None):
        """
        :param str formula: The formula used to compute the default value for the column.
        :param str value: The direct value to use as the default value for the column.
        """
        super(DefaultColumnValue, self).__init__()
        self.formula = formula
        self.value = value
