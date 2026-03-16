from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.runtime.types.collections import StringCollection
from office365.sharepoint.entity import Entity


class RichSharing(Entity):
    def __init__(self, context, resource_path=None):
        if resource_path is None:
            resource_path = ResourcePath("SP.Publishing.RichSharing")
        super(RichSharing, self).__init__(context, resource_path)

    def share_page_by_email(
        self, url, message, recipient_emails, page_content, subject
    ):
        """
        :param str url:
        :param str message:
        :param list[str] recipient_emails:
        :param str page_content:
        :param str subject:
        """
        payload = {
            "url": url,
            "message": message,
            "recipientEmails": StringCollection(recipient_emails),
            "pageContent": page_content,
            "subject": subject,
        }
        qry = ServiceOperationQuery(self, "SharePageByEmail", None, payload)
        self.context.add_query(qry)
        return self

    def share_site_by_email(
        self, custom_description, custom_title, message, url, recipient_emails
    ):
        """
        :param str url:
        :param str message:
        :param list[str] recipient_emails:
        :param str custom_description:
        :param str custom_title:
        """
        payload = {
            "Url": url,
            "Message": message,
            "recipientEmails": StringCollection(recipient_emails),
            "CustomTitle": custom_title,
            "CustomDescription": custom_description,
        }
        qry = ServiceOperationQuery(self, "ShareSiteByEmail", None, payload)
        self.context.add_query(qry)
        return self

    @property
    def entity_type_name(self):
        return "SP.Publishing.RichSharing"
