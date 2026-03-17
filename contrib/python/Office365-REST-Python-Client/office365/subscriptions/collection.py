from office365.entity_collection import EntityCollection
from office365.runtime.queries.create_entity import CreateEntityQuery
from office365.subscriptions.subscription import Subscription


class SubscriptionCollection(EntityCollection[Subscription]):
    def __init__(self, context, resource_path=None):
        super(SubscriptionCollection, self).__init__(
            context, Subscription, resource_path
        )

    def add(
        self,
        change_type,
        notification_url,
        resource_path,
        expiration,
        client_state=None,
        latest_supported_tls_version=None,
        include_resource_data=None,
        encryption_certificate=None,
        encryption_certificate_id=None,
    ):
        """
        Subscribes a listener application to receive change notifications when the requested type of changes occur
        to the specified resource in Microsoft Graph.

        :param str change_type: Indicates the type of change in the subscribed resource that will raise a change
           notification
        :param str notification_url: The URL of the endpoint that will receive the change notifications.
            This URL must make use of the HTTPS protocol. Any query string parameter included in the notificationUrl
            property will be included in the HTTP POST request when Microsoft Graph sends the change notifications.
        :param office365.runtime.paths.resource_path.ResourcePath or str resource_path:
        :param datetime.datetime expiration:  Specifies the date and time when the webhook subscription expires.
        :param str client_state: Specifies the value of the clientState property sent by the service in each change
            notification
        :param str latest_supported_tls_version:  Specifies the latest version of Transport Layer Security (TLS) that
            the notification endpoint, specified by notificationUrl, supports.
            The possible values are: v1_0, v1_1, v1_2, v1_3.
        :param bool include_resource_data: Indicates whether the resource data for the resource that generated the
            change notification should be included in the payload of the notification.
        :param str encryption_certificate: Specifies the public key certificate which contains only the public key
            that Microsoft Graph uses to encrypt the resource data it returns to your app. For security, Microsoft
            Graph encrypts the resource data returned in a rich notification. You must provide a public encryption
            key as part of creating the subscription.
        :param str encryption_certificate_id: Specifies the identifier of the certificate used to encrypt the content
            of the change notification. Use this ID to match in each change notification, which certificate to use
            for decryption.
        :return: Subscription
        """
        return_type = Subscription(self.context)
        self.add_child(return_type)
        payload = {
            "changeType": change_type,
            "notificationUrl": notification_url,
            "resource": str(resource_path),
            "expirationDateTime": expiration.isoformat() + "Z",
            "clientState": client_state,
            "latestSupportedTlsVersion": latest_supported_tls_version,
            "includeResourceData": include_resource_data,
            "encryptionCertificate": encryption_certificate,
            "encryptionCertificateId": encryption_certificate_id,
        }
        qry = CreateEntityQuery(self, payload, return_type)
        self.context.add_query(qry)
        return return_type
