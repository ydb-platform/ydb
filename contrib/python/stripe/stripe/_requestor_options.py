# using global variables
import stripe  # noqa: IMP101
from stripe._base_address import BaseAddresses

from typing import Optional, Union
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._stripe_context import StripeContext


class RequestorOptions(object):
    api_key: Optional[str]
    stripe_account: Optional[str]
    stripe_context: "Optional[Union[str, StripeContext]]"
    stripe_version: Optional[str]
    base_addresses: BaseAddresses
    max_network_retries: Optional[int]

    def __init__(
        self,
        api_key: Optional[str] = None,
        stripe_account: Optional[str] = None,
        stripe_context: "Optional[Union[str, StripeContext]]" = None,
        stripe_version: Optional[str] = None,
        base_addresses: Optional[BaseAddresses] = None,
        max_network_retries: Optional[int] = None,
    ):
        self.api_key = api_key
        self.stripe_account = stripe_account
        self.stripe_context = stripe_context
        self.stripe_version = stripe_version
        self.base_addresses = {}

        if base_addresses:
            # Base addresses can be unset (for correct merging).
            # If they are not set, then we will use default API bases defined on stripe.
            if base_addresses.get("api"):
                self.base_addresses["api"] = base_addresses.get("api")
            if base_addresses.get("connect") is not None:
                self.base_addresses["connect"] = base_addresses.get("connect")
            if base_addresses.get("files") is not None:
                self.base_addresses["files"] = base_addresses.get("files")
            if base_addresses.get("meter_events") is not None:
                self.base_addresses["meter_events"] = base_addresses.get(
                    "meter_events"
                )

        self.max_network_retries = max_network_retries

    def to_dict(self):
        """
        Returns a dict representation of the object.
        """
        return {
            "api_key": self.api_key,
            "stripe_account": self.stripe_account,
            "stripe_context": self.stripe_context,
            "stripe_version": self.stripe_version,
            "base_addresses": self.base_addresses,
            "max_network_retries": self.max_network_retries,
        }


class _GlobalRequestorOptions(RequestorOptions):
    def __init__(self):
        pass

    @property
    def base_addresses(self):
        return {
            "api": stripe.api_base,
            "connect": stripe.connect_api_base,
            "files": stripe.upload_api_base,
            "meter_events": stripe.meter_events_api_base,
        }

    @property
    def api_key(self):
        return stripe.api_key

    @property
    def stripe_version(self):
        return stripe.api_version

    @property
    def stripe_account(self):
        return None

    @property
    def stripe_context(self):
        return None

    @property
    def max_network_retries(self):
        return stripe.max_network_retries
