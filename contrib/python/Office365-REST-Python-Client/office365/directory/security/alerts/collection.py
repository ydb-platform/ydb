from office365.directory.security.alerts.alert import Alert
from office365.entity_collection import EntityCollection
from office365.runtime.http.request_options import RequestOptions


class AlertCollection(EntityCollection[Alert]):
    """Service Principal's collection"""

    def __init__(self, context, resource_path=None):
        super(AlertCollection, self).__init__(context, Alert, resource_path)

    def add(
        self,
        title,
        description=None,
        severity=None,
        category=None,
        status=None,
        source=None,
        vendor_information=None,
    ):
        """
        Creates an alert object.
        :param str title:
        :param str description:
        :param str severity:
        :param str category:
        :param str status:
        :param str source:
        :param str vendor_information:
        """

        def _construct_request(request):
            # type: (RequestOptions) -> None
            request.set_header("Content-Type", "application/json")

        return (
            super(AlertCollection, self)
            .add(
                title=title,
                description=description,
                severity=severity,
                category=category,
                status=status,
                source=source,
                vendorInformation=vendor_information,
            )
            .before_execute(_construct_request)
        )
