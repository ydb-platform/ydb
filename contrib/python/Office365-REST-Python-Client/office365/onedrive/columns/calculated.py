from office365.runtime.client_value import ClientValue


class CalculatedColumn(ClientValue):
    """The calculatedColumn on a columnDefinition resource indicates that the column's
    data is calculated based on other columns in the site."""

    def __init__(self, format_name=None, formula=None, output_type=None):
        """
        :param str format_name: For dateTime output types, the format of the value. Must be one of dateOnly or dateTime
        :param str formula: The formula used to compute the value for this column.
        :param str output_type: The output type used to format values in this column. Must be one of boolean,
            currency, dateTime, number, or text.
        """
        self.format = format_name
        self.formula = formula
        self.outputType = output_type
