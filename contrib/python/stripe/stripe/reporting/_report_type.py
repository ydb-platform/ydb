# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from typing import ClassVar, List, Optional
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.reporting._report_type_list_params import (
        ReportTypeListParams,
    )
    from stripe.params.reporting._report_type_retrieve_params import (
        ReportTypeRetrieveParams,
    )


class ReportType(ListableAPIResource["ReportType"]):
    """
    The Report Type resource corresponds to a particular type of report, such as
    the "Activity summary" or "Itemized payouts" reports. These objects are
    identified by an ID belonging to a set of enumerated values. See
    [API Access to Reports documentation](https://docs.stripe.com/reporting/statements/api)
    for those Report Type IDs, along with required and optional parameters.

    Note that certain report types can only be run based on your live-mode data (not test-mode
    data), and will error when queried without a [live-mode API key](https://docs.stripe.com/keys#test-live-modes).
    """

    OBJECT_NAME: ClassVar[Literal["reporting.report_type"]] = (
        "reporting.report_type"
    )
    data_available_end: int
    """
    Most recent time for which this Report Type is available. Measured in seconds since the Unix epoch.
    """
    data_available_start: int
    """
    Earliest time for which this Report Type is available. Measured in seconds since the Unix epoch.
    """
    default_columns: Optional[List[str]]
    """
    List of column names that are included by default when this Report Type gets run. (If the Report Type doesn't support the `columns` parameter, this will be null.)
    """
    id: str
    """
    The [ID of the Report Type](https://docs.stripe.com/reporting/statements/api#available-report-types), such as `balance.summary.1`.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    name: str
    """
    Human-readable name of the Report Type
    """
    object: Literal["reporting.report_type"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    updated: int
    """
    When this Report Type was latest updated. Measured in seconds since the Unix epoch.
    """
    version: int
    """
    Version of the Report Type. Different versions report with the same ID will have the same purpose, but may take different run parameters or have different result schemas.
    """

    @classmethod
    def list(
        cls, **params: Unpack["ReportTypeListParams"]
    ) -> ListObject["ReportType"]:
        """
        Returns a full list of Report Types.
        """
        result = cls._static_request(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    async def list_async(
        cls, **params: Unpack["ReportTypeListParams"]
    ) -> ListObject["ReportType"]:
        """
        Returns a full list of Report Types.
        """
        result = await cls._static_request_async(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["ReportTypeRetrieveParams"]
    ) -> "ReportType":
        """
        Retrieves the details of a Report Type. (Certain report types require a [live-mode API key](https://stripe.com/docs/keys#test-live-modes).)
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["ReportTypeRetrieveParams"]
    ) -> "ReportType":
        """
        Retrieves the details of a Report Type. (Certain report types require a [live-mode API key](https://stripe.com/docs/keys#test-live-modes).)
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance
