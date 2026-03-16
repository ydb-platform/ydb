from office365.runtime.client_value import ClientValue


class NumberColumn(ClientValue):
    """The numberColumn on a columnDefinition resource indicates that the column's values are numbers."""

    def __init__(
        self, minimum=None, maximum=None, display_as=None, decimal_places=None
    ):
        """
        :param float minimum: The minimum permitted value.
        :param float maximum: The maximum permitted value.
        :param str display_as: How the value should be presented in the UX. Must be one of number or percentage.
            If unspecified, treated as number.
        :param str decimal_places: How many decimal places to display.
            See below for information about the possible values:
            - automatic	Default. Automatically display decimal places as needed.
            - none	Do not display any decimal places.
            - one	Always display one decimal place.
            - two	Always display two decimal places.
            - three	Always display three decimal places.
            - four	Always display four decimal places.
            - five	Always display five decimal places.
        """
        self.minimum = minimum
        self.maximum = maximum
        self.displayAs = display_as
        self.decimalPlaces = decimal_places
