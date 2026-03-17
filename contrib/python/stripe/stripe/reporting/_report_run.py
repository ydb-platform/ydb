# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from typing import ClassVar, List, Optional, cast
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._file import File
    from stripe.params.reporting._report_run_create_params import (
        ReportRunCreateParams,
    )
    from stripe.params.reporting._report_run_list_params import (
        ReportRunListParams,
    )
    from stripe.params.reporting._report_run_retrieve_params import (
        ReportRunRetrieveParams,
    )


class ReportRun(
    CreateableAPIResource["ReportRun"],
    ListableAPIResource["ReportRun"],
):
    """
    The Report Run object represents an instance of a report type generated with
    specific run parameters. Once the object is created, Stripe begins processing the report.
    When the report has finished running, it will give you a reference to a file
    where you can retrieve your results. For an overview, see
    [API Access to Reports](https://docs.stripe.com/reporting/statements/api).

    Note that certain report types can only be run based on your live-mode data (not test-mode
    data), and will error when queried without a [live-mode API key](https://docs.stripe.com/keys#test-live-modes).
    """

    OBJECT_NAME: ClassVar[Literal["reporting.report_run"]] = (
        "reporting.report_run"
    )

    class Parameters(StripeObject):
        columns: Optional[List[str]]
        """
        The set of output columns requested for inclusion in the report run.
        """
        connected_account: Optional[str]
        """
        Connected account ID by which to filter the report run.
        """
        currency: Optional[str]
        """
        Currency of objects to be included in the report run.
        """
        interval_end: Optional[int]
        """
        Ending timestamp of data to be included in the report run. Can be any UTC timestamp between 1 second after the user specified `interval_start` and 1 second before this report's last `data_available_end` value.
        """
        interval_start: Optional[int]
        """
        Starting timestamp of data to be included in the report run. Can be any UTC timestamp between 1 second after this report's `data_available_start` and 1 second before the user specified `interval_end` value.
        """
        payout: Optional[str]
        """
        Payout ID by which to filter the report run.
        """
        reporting_category: Optional[str]
        """
        Category of balance transactions to be included in the report run.
        """
        timezone: Optional[str]
        """
        Defaults to `Etc/UTC`. The output timezone for all timestamps in the report. A list of possible time zone values is maintained at the [IANA Time Zone Database](http://www.iana.org/time-zones). Has no effect on `interval_start` or `interval_end`.
        """

    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    error: Optional[str]
    """
    If something should go wrong during the run, a message about the failure (populated when
     `status=failed`).
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    `true` if the report is run on live mode data and `false` if it is run on test mode data.
    """
    object: Literal["reporting.report_run"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    parameters: Parameters
    report_type: str
    """
    The ID of the [report type](https://docs.stripe.com/reports/report-types) to run, such as `"balance.summary.1"`.
    """
    result: Optional["File"]
    """
    The file object representing the result of the report run (populated when
     `status=succeeded`).
    """
    status: str
    """
    Status of this report run. This will be `pending` when the run is initially created.
     When the run finishes, this will be set to `succeeded` and the `result` field will be populated.
     Rarely, we may encounter an error, at which point this will be set to `failed` and the `error` field will be populated.
    """
    succeeded_at: Optional[int]
    """
    Timestamp at which this run successfully finished (populated when
     `status=succeeded`). Measured in seconds since the Unix epoch.
    """

    @classmethod
    def create(cls, **params: Unpack["ReportRunCreateParams"]) -> "ReportRun":
        """
        Creates a new object and begin running the report. (Certain report types require a [live-mode API key](https://stripe.com/docs/keys#test-live-modes).)
        """
        return cast(
            "ReportRun",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["ReportRunCreateParams"]
    ) -> "ReportRun":
        """
        Creates a new object and begin running the report. (Certain report types require a [live-mode API key](https://stripe.com/docs/keys#test-live-modes).)
        """
        return cast(
            "ReportRun",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["ReportRunListParams"]
    ) -> ListObject["ReportRun"]:
        """
        Returns a list of Report Runs, with the most recent appearing first.
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
        cls, **params: Unpack["ReportRunListParams"]
    ) -> ListObject["ReportRun"]:
        """
        Returns a list of Report Runs, with the most recent appearing first.
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
        cls, id: str, **params: Unpack["ReportRunRetrieveParams"]
    ) -> "ReportRun":
        """
        Retrieves the details of an existing Report Run.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["ReportRunRetrieveParams"]
    ) -> "ReportRun":
        """
        Retrieves the details of an existing Report Run.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {"parameters": Parameters}
