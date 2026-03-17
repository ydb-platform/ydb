from typing import TYPE_CHECKING

from office365.runtime.client_value import ClientValue

if TYPE_CHECKING:
    from typing import Optional  # noqa


class PhysicalAddress(ClientValue):
    """The physical address of a contact."""

    def __init__(
        self,
        city=None,
        country_or_region=None,
        postal_code=None,
        state=None,
        street=None,
    ):
        # type: (Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]) -> None
        """
        :param city: The city.
        :param country_or_region: The country or region. It's a free-format string value, for example, "United States".
        :param postal_code: The postal code.
        :param state:
        :param street:
        """
        super(PhysicalAddress, self).__init__()
        self.city = city
        self.countryOrRegion = country_or_region
        self.postalCode = postal_code
        self.state = state
        self.street = street
