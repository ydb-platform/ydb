import datetime
from typing import Optional

from office365.entity import Entity
from office365.runtime.queries.service_operation import ServiceOperationQuery


class Subscription(Entity):
    """A subscription allows a client app to receive change notifications about changes to data in Microsoft Graph"""

    def reauthorize(self):
        """Reauthorize a subscription when you receive a reauthorizationRequired challenge."""
        qry = ServiceOperationQuery(self, "reauthorize")
        self.context.add_query(qry)
        return self

    @property
    def application_id(self):
        # type: () -> Optional[str]
        """
        Identifier of the application used to create the subscription.
        """
        return self.properties.get("applicationId", None)

    @property
    def change_type(self):
        # type: () -> Optional[str]
        """
        Required. Indicates the type of change in the subscribed resource that will raise a change notification.
        The supported values are: created, updated, deleted.
        Multiple values can be combined using a comma-separated list.

        Note:
        Drive root item and list change notifications support only the updated changeType.
        User and group change notifications support updated and deleted changeType. Use updated to receive
        notifications when user or group is created, updated or soft deleted. Use deleted to receive notifications
        when user or group is permanently deleted.
        """
        return self.properties.get("changeType", None)

    @property
    def client_state(self):
        # type: () -> Optional[str]
        """
        Required. Specifies the value of the clientState property sent by the service in each change notification.
        The maximum length is 128 characters. The client can check that the change notification came from the service
        by comparing the value of the clientState property sent with the subscription with the value of the clientState
        property received with each change notification.
        """
        return self.properties.get("clientState", None)

    @property
    def creator_id(self):
        # type: () -> Optional[str]
        """
        Optional. Identifier of the user or service principal that created the subscription. If the app used
        delegated permissions to create the subscription, this field contains the id of the signed-in user the app
        called on behalf of. If the app used application permissions, this field contains the id of the service
        principal corresponding to the app. Read-only.
        """
        return self.properties.get("creatorId", None)

    @property
    def encryption_certificate(self):
        # type: () -> Optional[str]
        """
        Optional. A base64-encoded representation of a certificate with a public key used to encrypt resource data in
        change notifications. Optional but required when includeResourceData is true.
        """
        return self.properties.get("encryptionCertificate", None)

    @property
    def encryption_certificate_id(self):
        # type: () -> Optional[str]
        """
        Optional. A custom app-provided identifier to help identify the certificate needed to decrypt resource data.
        """
        return self.properties.get("encryptionCertificateId", None)

    @property
    def expiration_datetime(self):
        # type: () -> Optional[datetime.datetime]
        """
        Required. Specifies the date and time when the webhook subscription expires. The time is in UTC, and can be
        an amount of time from subscription creation that varies for the resource subscribed to
        """
        return self.properties.get("expirationDateTime", datetime.datetime)

    @property
    def include_resource_data(self):
        # type: () -> Optional[bool]
        """
        Optional. When set to true, change notifications include resource data (such as content of a chat message).
        """
        return self.properties.get("includeResourceData", None)

    @property
    def latest_supported_tls_version(self):
        # type: () -> Optional[str]
        """
        Optional. Specifies the latest version of Transport Layer Security (TLS) that the notification endpoint,
        specified by notificationUrl, supports. The possible values are: v1_0, v1_1, v1_2, v1_3.

        For subscribers whose notification endpoint supports a version lower than the currently recommended
        version (TLS 1.2), specifying this property by a set timeline allows them to temporarily use their deprecated
        version of TLS before completing their upgrade to TLS 1.2. For these subscribers, not setting this property
        per the timeline would result in subscription operations failing.

        For subscribers whose notification endpoint already supports TLS 1.2, setting this property is optional.
        In such cases, Microsoft Graph defaults the property to v1_2.
        """
        return self.properties.get("latestSupportedTlsVersion", None)

    @property
    def lifecycle_notification_url(self):
        # type: () -> Optional[str]
        """
        Required for Teams resources if the expirationDateTime value is more than 1 hour from now; optional otherwise.
        The URL of the endpoint that receives lifecycle notifications, including subscriptionRemoved,
        reauthorizationRequired, and missed notifications. This URL must make use of the HTTPS protocol.
        For more information, see Reduce missing subscriptions and change notifications.
        """
        return self.properties.get("lifecycleNotificationUrl", None)

    @property
    def notification_url(self):
        # type: () -> Optional[str]
        """
        Required. The URL of the endpoint that will receive the change notifications. This URL must make use of
        the HTTPS protocol. Any query string parameter included in the notificationUrl property will be included
        in the HTTP POST request when Microsoft Graph sends the change notifications.
        """
        return self.properties.get("notificationUrl", None)

    @property
    def resource(self):
        # type: () -> Optional[str]
        """
        Specifies the resource that will be monitored for changes.
        Do not include the base URL (https://graph.microsoft.com/v1.0/). See the possible resource path values for
        each supported resource.
        """
        return self.properties.get("resource", None)

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "expirationDateTime": self.expiration_datetime,
            }
            default_value = property_mapping.get(name, None)
        return super(Subscription, self).get_property(name, default_value)
