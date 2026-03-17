from typing import TYPE_CHECKING, Optional

from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.runtime.types.collections import StringCollection
from office365.sharepoint.compliance.tag import ComplianceTag
from office365.sharepoint.entity import Entity

if TYPE_CHECKING:
    from office365.sharepoint.client_context import ClientContext


class SPPolicyStoreProxy(Entity):
    """ """

    @staticmethod
    def check_site_is_deletable_by_id(context, site_id, return_type=None):
        # type: (ClientContext, str, Optional[ClientResult[bool]]) -> ClientResult[bool]
        """ """
        if return_type is None:
            return_type = ClientResult(context, bool())
        payload = {"siteId": site_id}
        qry = ServiceOperationQuery(
            SPPolicyStoreProxy(context),
            "CheckSiteIsDeletableById",
            None,
            payload,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def is_site_deletable(context, site_url, return_type=None):
        # type: (ClientContext, str, Optional[ClientResult[bool]]) -> ClientResult[bool]
        """ """
        if return_type is None:
            return_type = ClientResult(context, bool())
        payload = {"siteUrl": site_url}
        qry = ServiceOperationQuery(
            SPPolicyStoreProxy(context),
            "IsSiteDeletable",
            None,
            payload,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def get_available_tags_for_site(context, site_url, return_type=None):
        """
        :param office365.sharepoint.client_context.ClientContext context: SharePoint client context
        :param str site_url:
        :param ClientResult return_type:
        """
        if return_type is None:
            return_type = ClientResult(context, ClientValueCollection(ComplianceTag))
        payload = {"siteUrl": site_url}
        qry = ServiceOperationQuery(
            SPPolicyStoreProxy(context),
            "GetAvailableTagsForSite",
            None,
            payload,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    def get_dynamic_scope_binding_by_site_id(self, site_id):
        """
        :param str site_id:
        """
        return_type = ClientResult(self.context, StringCollection())
        payload = {"siteId": site_id}
        qry = ServiceOperationQuery(
            self, "GetDynamicScopeBindingBySiteId", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @staticmethod
    def get_list_compliance_tag(context, list_url, return_type=None):
        """ """
        if return_type is None:
            return_type = ClientResult(context, ComplianceTag())
        payload = {"listUrl": list_url}
        qry = ServiceOperationQuery(
            SPPolicyStoreProxy(context),
            "GetListComplianceTag",
            None,
            payload,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def set_list_compliance_tag(
        context,
        list_url,
        compliance_tag_value,
        block_delete=None,
        block_edit=None,
        sync_to_items=None,
    ):
        """ """
        payload = {
            "listUrl": list_url,
            "complianceTagValue": compliance_tag_value,
            "blockDelete": block_delete,
            "blockEdit": block_edit,
            "syncToItems": sync_to_items,
        }
        binding_type = SPPolicyStoreProxy(context)
        qry = ServiceOperationQuery(
            binding_type,
            "SetListComplianceTag",
            None,
            payload,
            None,
            None,
            True,
        )
        context.add_query(qry)
        return binding_type

    @staticmethod
    def lock_record_item(
        context, list_url, item_id, refresh_labeled_time, return_type=None
    ):
        """ """
        if return_type is None:
            return_type = ClientResult(context, int())
        payload = {
            "listUrl": list_url,
            "itemId": item_id,
            "refreshLabeledTime": refresh_labeled_time,
        }

        qry = ServiceOperationQuery(
            SPPolicyStoreProxy(context),
            "LockRecordItem",
            None,
            payload,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "SP.CompliancePolicy.SPPolicyStoreProxy"
