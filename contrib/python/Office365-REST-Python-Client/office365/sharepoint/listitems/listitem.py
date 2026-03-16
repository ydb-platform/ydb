import json
from typing import TYPE_CHECKING

from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.http.http_method import HttpMethod
from office365.runtime.http.request_options import RequestOptions
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.runtime.paths.v3.entity import EntityPath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.attachments.collection import AttachmentCollection
from office365.sharepoint.changes.collection import ChangeCollection
from office365.sharepoint.changes.query import ChangeQuery
from office365.sharepoint.comments.collection import CommentCollection
from office365.sharepoint.fields.image_value import ImageFieldValue
from office365.sharepoint.fields.lookup_value import FieldLookupValue
from office365.sharepoint.fields.multi_lookup_value import FieldMultiLookupValue
from office365.sharepoint.fields.string_values import FieldStringValues
from office365.sharepoint.fields.url_value import FieldUrlValue
from office365.sharepoint.likes.liked_by_information import LikedByInformation
from office365.sharepoint.listitems.compliance_info import ListItemComplianceInfo
from office365.sharepoint.listitems.form_update_value import ListItemFormUpdateValue
from office365.sharepoint.listitems.update_parameters import ListItemUpdateParameters
from office365.sharepoint.listitems.versions.collection import ListItemVersionCollection
from office365.sharepoint.permissions.securable_object import SecurableObject
from office365.sharepoint.policy.dlp_policy_tip import DlpPolicyTip
from office365.sharepoint.reputationmodel.reputation import Reputation
from office365.sharepoint.sharing.external_site_option import ExternalSharingSiteOption
from office365.sharepoint.sharing.links.share_request import ShareLinkRequest
from office365.sharepoint.sharing.links.share_response import ShareLinkResponse
from office365.sharepoint.sharing.links.share_settings import ShareLinkSettings
from office365.sharepoint.sharing.object_sharing_information import (
    ObjectSharingInformation,
)
from office365.sharepoint.sharing.result import SharingResult
from office365.sharepoint.taxonomy.field_value import TaxonomyFieldValueCollection
from office365.sharepoint.ui.applicationpages.peoplepicker.web_service_interface import (
    ClientPeoplePickerWebServiceInterface,
)

if TYPE_CHECKING:
    import datetime
    from typing import Optional


class ListItem(SecurableObject):
    """An individual entry within a SharePoint list. Each list item has a schema that maps to fields in the list
    that contains the item, depending on the content type of the item."""

    def __init__(self, context, resource_path=None, parent_list=None):
        """

        :type context: office365.sharepoint.client_context.ClientContext
        :type resource_path: office365.runtime.paths.resource_path.ResourcePath or None
        :type parent_list: office365.sharepoint.lists.list.List or None
        """
        super(ListItem, self).__init__(context, resource_path)
        if parent_list is not None:
            self.set_property("ParentList", parent_list, False)

    def archive(self):
        """Archives the list item."""
        return_type = ClientResult(self.context, str())
        qry = ServiceOperationQuery(self, "Archive", None, None, None, return_type)
        self.context.add_query(qry)
        return return_type

    def share_link(self, link_kind, expiration=None, role=None, password=None):
        # type: (int, Optional[datetime.datetime], Optional[int], Optional[str]) -> ClientResult[ShareLinkResponse]
        """Creates a tokenized sharing link for a list item based on the specified parameters and optionally
        sends an email to the people that are listed in the specified parameters.

        :param link_kind: The kind of the tokenized sharing link to be created/updated or retrieved.
        :param expiration: A date/time string for which the format conforms to the ISO 8601:2004(E)
            complete representation for calendar date and time of day and which represents the time and date of expiry
            for the tokenized sharing link. Both the minutes and hour value MUST be specified for the difference
            between the local and UTC time. Midnight is represented as 00:00:00. A null value indicates no expiry.
            This value is only applicable to tokenized sharing links that are anonymous access links.
        :param role: The role to be used for the tokenized sharing link. This is required for Flexible links
            and ignored for all other kinds.
        :param password: Optional password value to apply to the tokenized sharing link,
            if it can support password protection.
        """
        return_type = ClientResult(self.context, ShareLinkResponse())
        request = ShareLinkRequest(
            settings=ShareLinkSettings(
                link_kind=link_kind, expiration=expiration, role=role, password=password
            )
        )
        if password:
            request.settings.allowAnonymousAccess = True
            request.settings.updatePassword = True

        payload = {"request": request}
        qry = ServiceOperationQuery(self, "ShareLink", None, payload, None, return_type)
        self.context.add_query(qry)
        return return_type

    def unshare_link(self, link_kind, share_id=None):
        """
        Removes the specified tokenized sharing link of the list item.

        :param int link_kind: This optional value specifies the globally unique identifier (GUID) of the tokenized
            sharing link that is intended to be removed.
        :param str or None share_id: The kind of tokenized sharing link that is intended to be removed.
        """
        payload = {"linkKind": link_kind, "shareId": share_id}
        qry = ServiceOperationQuery(self, "UnshareLink", None, payload)
        self.context.add_query(qry)
        return self

    def delete_link_by_kind(self, link_kind):
        """Removes the specified tokenized sharing link of the list item

        :param int link_kind: The kind of tokenized sharing link that is intended to be removed.
            This MUST be set to one of the following values:
            OrganizationView (section 3.2.5.315.1.3)
            OrganizationEdit (section 3.2.5.315.1.4)
            AnonymousView (section 3.2.5.315.1.5)
            AnonymousEdit (section 3.2.5.315.1.6)
        """
        payload = {"linkKind": link_kind}
        qry = ServiceOperationQuery(self, "DeleteLinkByKind", None, payload)
        self.context.add_query(qry)
        return self

    def set_rating(self, value):
        """
        Rates an item within the specified list. The return value is the average rating for the specified list item.

        :param int value: An integer value for the rating to be submitted.
            The rating value SHOULD be between 1 and 5; otherwise, the server SHOULD return an exception.
        """
        return_value = ClientResult(self.context)

        def _list_item_loaded():
            Reputation.set_rating(
                self.context, self.parent_list.id, self.id, value, return_value
            )

        self.parent_list.ensure_properties(["Id", "ParentList"], _list_item_loaded)
        return return_value

    def set_like(self, value):
        """
        Sets or unsets the like quality for the current user for an item within
           the specified list. The return value is the total number of likes for the specified list item.

        :param bool value: A Boolean value that indicates the operation being either like or unlike.
            A True value indicates like.
        """
        return_value = ClientResult(self.context)

        def _list_item_loaded():
            Reputation.set_like(
                self.context, self.parent_list.id, self.id, value, return_value
            )

        self.parent_list.ensure_properties(["Id", "ParentList"], _list_item_loaded)
        return return_value

    def get_wopi_frame_url(self, action):
        """
        Gets the full URL to the SharePoint frame page that initiates the SPWOPIAction object with the WOPI
            application associated with the list item.

        :param int action: Indicates which user action is indicated in the returned WOPIFrameUrl.
        """
        result = ClientResult(self.context)
        qry = ServiceOperationQuery(
            self, "GetWOPIFrameUrl", [action], None, None, result
        )
        self.context.add_query(qry)
        return result

    def recycle(self):
        """Moves the listItem to the Recycle Bin and returns the identifier of the new Recycle Bin item."""

        result = ClientResult(self.context)
        qry = ServiceOperationQuery(self, "Recycle", None, None, None, result)
        self.context.add_query(qry)
        return result

    def get_changes(self, query=None):
        """Returns the collection of changes from the change log that have occurred within the ListItem,
           based on the specified query.

        :param office365.sharepoint.changeQuery.ChangeQuery query: Specifies which changes to return
        """
        if query is None:
            query = ChangeQuery(item=True)
        return_type = ChangeCollection(self.context)
        payload = {"query": query}
        qry = ServiceOperationQuery(
            self, "getChanges", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def share(
        self,
        user_principal_name,
        share_option=ExternalSharingSiteOption.View,
        send_email=True,
        email_subject=None,
        email_body=None,
    ):
        """
        Share a ListItem (file or folder facet)

        :param str user_principal_name: User identifier
        :param ExternalSharingSiteOption share_option: The sharing type of permission to grant on the object.
        :param bool send_email: A flag to determine if an email notification SHOULD be sent (if email is configured).
        :param str email_subject: The email subject.
        :param str email_body: The email subject.
        """

        return_type = SharingResult(self.context)
        role_values = {
            ExternalSharingSiteOption.View: "role:1073741826",
            ExternalSharingSiteOption.Edit: "role:1073741827",
        }

        def _picker_value_resolved(picker_result):
            # type: (ClientResult) -> None
            abs_url = self.get_property("EncodedAbsUrl")
            picker_value = "[{0}]".format(picker_result.value)
            from office365.sharepoint.webs.web import Web

            Web.share_object(
                self.context,
                abs_url,
                picker_value,
                role_values[share_option],
                0,
                False,
                send_email,
                False,
                email_subject,
                email_body,
                return_type=return_type,
            )

        def _url_resolved():
            ClientPeoplePickerWebServiceInterface.client_people_picker_resolve_user(
                self.context, user_principal_name
            ).after_execute(_picker_value_resolved)

        self.ensure_property("EncodedAbsUrl", _url_resolved)
        return return_type

    def unshare(self):
        """Unshare a ListItem (file or folder facet)"""
        return_type = SharingResult(self.context)

        def _property_resolved():
            abs_url = self.get_property("EncodedAbsUrl")
            from office365.sharepoint.webs.web import Web

            Web.unshare_object(self.context, abs_url, return_type=return_type)

        self.ensure_property("EncodedAbsUrl", _property_resolved)
        return return_type

    def get_sharing_information(self):
        """
        Retrieves information about the sharing state for a given list item.
        """
        return_type = ObjectSharingInformation(self.context)

        def _item_resolved():
            ObjectSharingInformation.get_list_item_sharing_information(
                self.context, self.parent_list.id, self.id, return_type=return_type
            )

        self.ensure_properties(["Id", "ParentList"], _item_resolved)
        return return_type

    def validate_update_list_item(
        self,
        form_values,
        new_document_update=False,
        checkin_comment=None,
        dates_in_utc=None,
    ):
        """Validates and sets the values of the specified collection of fields for the list item.

        :param dict form_values: Specifies a collection of field internal names and values for the given field
        :param bool new_document_update: Specifies whether the list item is a document being updated after upload.
        :param str checkin_comment: Check-in comment, if any. This parameter is only applicable when the list item
             is checked out.
        :param bool or None dates_in_utc:
        """
        payload = {
            "formValues": [
                ListItemFormUpdateValue(k, v) for k, v in form_values.items()
            ],
            "bNewDocumentUpdate": new_document_update,
            "checkInComment": checkin_comment,
            "datesInUTC": dates_in_utc,
        }
        return_type = ClientResult(
            self.context, ClientValueCollection(ListItemFormUpdateValue)
        )
        qry = ServiceOperationQuery(
            self, "ValidateUpdateListItem", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def update(self):
        """
        Updates the item without creating another version of the item.
        Exceptions:
        - 2130575305 Microsoft.SharePoint.SPException List item was modified on the server in a way that prevents
            changes from being committed, as determined by the protocol server.
        -1 System.InvalidOperationException List does not support this operation.

        """
        self.ensure_type_name(self.parent_list)
        super(ListItem, self).update()
        return self

    def update_ex(self, bypass_quota_check=None, bypass_shared_lock=None):
        """

        :param bool bypass_quota_check:
        :param bool bypass_shared_lock:
        """
        payload = {
            "parameters": ListItemUpdateParameters(
                bypass_quota_check, bypass_shared_lock
            )
        }
        qry = ServiceOperationQuery(self, "UpdateEx", None, payload)
        self.context.add_query(qry)
        return self

    def system_update(self):
        """Update the list item."""

        sys_metadata = ["EditorId", "Modified"]

        def _after_system_update(result):
            # type: (ClientResult[ClientValueCollection[ListItemFormUpdateValue]]) -> None
            has_any_error = any(item.HasException for item in result.value)
            if has_any_error:
                raise ValueError("Update ListItem failed")

        def _system_update():
            from office365.sharepoint.fields.user_value import FieldUserValue

            form_values = self.persistable_properties
            for n in sys_metadata:
                if n == "Id":
                    pass
                elif n.endswith("Id"):
                    user = self.context.web.site_users.get_by_id(self.get_property(n))
                    form_values[n[:-2]] = FieldUserValue.from_user(user)
                else:
                    form_values[n] = self.get_property(n)

            self.validate_update_list_item(
                form_values=form_values,
                dates_in_utc=True,
                new_document_update=True,
            ).after_execute(_after_system_update)

        def _list_loaded():
            if self.parent_list.base_template == 101:
                self.ensure_properties(sys_metadata, _system_update)
            else:
                next_qry = ServiceOperationQuery(self, "SystemUpdate")
                self.context.add_query(next_qry)

        self.parent_list.ensure_properties(["BaseTemplate"], _list_loaded)
        return self

    def update_overwrite_version(self):
        """Updates the item without creating another version of the item."""
        qry = ServiceOperationQuery(self, "UpdateOverwriteVersion")
        self.context.add_query(qry)
        return self

    def set_comments_disabled(self, value):
        """
        Sets the value of CommentsDisabled for the item.

        :param bool value: Indicates whether comments for this item are disabled or not.
        """
        qry = ServiceOperationQuery(self, "SetCommentsDisabled", [value])
        self.context.add_query(qry)
        return self

    def set_compliance_tag_with_hold(self, compliance_tag):
        """
        Sets a compliance tag with a hold

        :param str compliance_tag: The applying label (tag) to the list item
        """
        payload = {"complianceTag": compliance_tag}
        qry = ServiceOperationQuery(self, "SetComplianceTagWithHold", None, payload)
        self.context.add_query(qry)
        return self

    def get_comments(self):
        """Retrieve ListItem comments"""
        return_type = CommentCollection(
            self.context, ServiceOperationPath("GetComments", [], self.resource_path)
        )
        qry = ServiceOperationQuery(self, "GetComments", [], None, None, return_type)

        def _create_request(request):
            # type: (RequestOptions) -> None
            request.method = HttpMethod.Get

        self.context.add_query(qry).before_query_execute(_create_request)
        return return_type

    def override_policy_tip(self, user_action, justification):
        """
        Overrides the policy tip on this list item.

        :param int user_action: The user action to take.
        :param str justification: The reason why the override is being done.
        """
        return_type = ClientResult(self.context, int())
        payload = {"userAction": user_action, "justification": justification}
        qry = ServiceOperationQuery(
            self, "OverridePolicyTip", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def parse_and_set_field_value(self, field_name, value):
        """Sets the value of the field (2) for the list item based on an implementation-specific transformation
           of the value.

        :param str field_name: Specifies the field internal name.
        :param str value: Specifies the new value for the field (2).
        """
        payload = {"fieldName": field_name, "value": value}
        qry = ServiceOperationQuery(self, "ParseAndSetFieldValue", None, payload)
        self.context.add_query(qry)
        return self

    @property
    def display_name(self):
        # type: () -> Optional[str]
        """Specifies the display name of the list item."""
        return self.properties.get("DisplayName", None)

    @property
    def parent_list(self):
        """Get parent List"""
        from office365.sharepoint.lists.list import List

        return self.properties.setdefault(
            "ParentList",
            List(self.context, ResourcePath("ParentList", self.resource_path)),
        )

    @property
    def file(self):
        """Get file"""
        from office365.sharepoint.files.file import File

        return self.properties.get(
            "File", File(self.context, ResourcePath("File", self.resource_path))
        )

    @property
    def folder(self):
        """Get folder"""
        from office365.sharepoint.folders.folder import Folder

        return self.properties.get(
            "Folder", Folder(self.context, ResourcePath("Folder", self.resource_path))
        )

    @property
    def attachment_files(self):
        # type: () -> AttachmentCollection
        """Specifies the collection of attachments that are associated with the list item.<62>"""
        from office365.sharepoint.attachments.collection import (
            AttachmentCollection,  # noqa
        )

        return self.properties.get(
            "AttachmentFiles",
            AttachmentCollection(
                self.context, ResourcePath("AttachmentFiles", self.resource_path), self
            ),
        )

    @property
    def content_type(self):
        """Gets a value that specifies the content type of the list item."""
        from office365.sharepoint.contenttypes.content_type import ContentType

        return self.properties.get(
            "ContentType",
            ContentType(self.context, ResourcePath("ContentType", self.resource_path)),
        )

    @property
    def effective_base_permissions(self):
        """Gets a value that specifies the effective permissions on the list item that are assigned
        to the current user."""
        from office365.sharepoint.permissions.base_permissions import BasePermissions

        return self.properties.get("EffectiveBasePermissions", BasePermissions())

    @property
    def effective_base_permissions_for_ui(self):
        """Specifies the effective base permissions for the current user, as they SHOULD be displayed in the user
        interface (UI). If the list is not in read-only UI mode, the value of EffectiveBasePermissionsForUI
        MUST be the same as the value of EffectiveBasePermissions (section 3.2.5.87.1.1.2).
        If the list is in read-only UI mode, the value of EffectiveBasePermissionsForUI MUST be a subset of the
        value of EffectiveBasePermissions."""
        from office365.sharepoint.permissions.base_permissions import BasePermissions

        return self.properties.get("EffectiveBasePermissionsForUI", BasePermissions())

    @property
    def field_values(self):
        # type: () -> Optional[dict]
        """Gets a collection of key/value pairs containing the names and values for the fields of the list item."""
        return self.properties.get("FieldValues", None)

    @property
    def comments_disabled(self):
        # type: () -> Optional[bool]
        """Indicates whether comments for this item are disabled or not."""
        return self.properties.get("CommentsDisabled", None)

    @property
    def file_system_object_type(self):
        # type: () -> Optional[str]
        """Gets a value that specifies whether the list item is a file or a list folder"""
        return self.properties.get("FileSystemObjectType", None)

    @property
    def icon_overlay(self):
        # type: () -> Optional[str]
        """This is an overlay icon for the item. If the parent list of the item does not already have the IconOverlay
        field and The user setting the property does not have rights to add the field to the list then the property
        will not be set for the item."""
        return self.properties.get("IconOverlay", None)

    @property
    def id(self):
        # type: () -> Optional[int]
        """Gets a value that specifies the list item identifier."""
        return self.properties.get("Id", None)

    @property
    def server_redirected_embed_uri(self):
        # type: () -> Optional[str]
        """Returns the path for previewing a document in the browser, often in an interactive way, if
        that feature exists."""
        return self.properties.get("ServerRedirectedEmbedUri", None)

    @property
    def server_redirected_embed_url(self):
        # type: () -> Optional[str]
        """Returns the URL for previewing a document in the browser, often in an interactive way, if that feature
        exists. This is currently used in the hovering panel of search results and document library.
        """
        return self.properties.get("ServerRedirectedEmbedUri", None)

    @property
    def client_title(self):
        # type: () -> Optional[str]
        """ """
        return self.properties.get("Client_Title", None)

    @property
    def compliance_info(self):
        return self.properties.get("ComplianceInfo", ListItemComplianceInfo())

    @property
    def comments_disabled_scope(self):
        # type: () -> Optional[str]
        """Indicates at what scope comments are disabled."""
        return self.properties.get("CommentsDisabledScope", None)

    @property
    def get_dlp_policy_tip(self):
        """Gets the Data Loss Protection policy tip notification for this item."""
        return self.properties.get(
            "GetDlpPolicyTip",
            DlpPolicyTip(
                self.context, ResourcePath("GetDlpPolicyTip", self.resource_path)
            ),
        )

    @property
    def field_values_as_html(self):
        """Specifies the values for the list item as Hypertext Markup Language (HTML)."""
        return self.properties.get(
            "FieldValuesAsHtml",
            FieldStringValues(
                self.context, ResourcePath("FieldValuesAsHtml", self.resource_path)
            ),
        )

    @property
    def liked_by_information(self):
        """Gets a value that specifies the list item identifier."""
        return self.properties.get(
            "LikedByInformation",
            LikedByInformation(
                self.context, ResourcePath("likedByInformation", self.resource_path)
            ),
        )

    @property
    def versions(self):
        """Gets the collection of item version objects that represent the versions of the item."""
        return self.properties.get(
            "Versions",
            ListItemVersionCollection(
                self.context, ResourcePath("versions", self.resource_path)
            ),
        )

    @property
    def doc_id(self):
        # type: () -> Optional[str]
        """Document ID fora document"""
        return self.properties.get("OData__dlc_DocId", None)

    @property
    def doc_id_url(self):
        # type: () -> Optional[FieldUrlValue]
        """Document ID fora document"""
        return self.properties.get("OData__dlc_DocIdUrl", FieldUrlValue())

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "AttachmentFiles": self.attachment_files,
                "ContentType": self.content_type,
                "ComplianceInfo": self.compliance_info,
                "OData__dlc_DocIdUrl": self.doc_id_url,
                "EffectiveBasePermissions": self.effective_base_permissions,
                "EffectiveBasePermissionsForUI": self.effective_base_permissions_for_ui,
                "GetDlpPolicyTip": self.get_dlp_policy_tip,
                "FieldValuesAsHtml": self.field_values_as_html,
                "LikedByInformation": self.liked_by_information,
                "ParentList": self.parent_list,
            }
            default_value = property_mapping.get(name, None)

        value = super(ListItem, self).get_property(name, default_value)
        if self.is_property_available(name[:-2]):
            lookup_value = super(ListItem, self).get_property(name[:-2], default_value)
            if isinstance(lookup_value, FieldMultiLookupValue):
                return ClientValueCollection(int, [v.LookupId for v in lookup_value])
            elif isinstance(lookup_value, FieldLookupValue):
                return lookup_value.LookupId
        return value

    def set_property(self, name, value, persist_changes=True):
        if persist_changes:
            if isinstance(value, TaxonomyFieldValueCollection):
                self._set_taxonomy_field_value(name, value)
            elif isinstance(value, ImageFieldValue):
                super(ListItem, self).set_property(
                    name, json.dumps(value.to_json()), persist_changes
                )
            elif isinstance(value, FieldMultiLookupValue):
                collection = ClientValueCollection(int, [v.LookupId for v in value])
                super(ListItem, self).set_property(
                    "{name}Id".format(name=name), collection
                )
                super(ListItem, self).set_property(name, value, False)
            elif isinstance(value, FieldLookupValue):
                super(ListItem, self).set_property(
                    "{name}Id".format(name=name), value.LookupId
                )
                super(ListItem, self).set_property(name, value, False)
            else:
                super(ListItem, self).set_property(name, value, persist_changes)
        else:
            super(ListItem, self).set_property(name, value, persist_changes)

        # fallback: create a new resource path
        if name == "Id":
            if self._resource_path is None and self.parent_collection is not None:
                self._resource_path = EntityPath(
                    value, self.parent_collection.resource_path
                )
            else:
                self._resource_path.patch(value)
        return self

    def _set_taxonomy_field_value(self, name, value):
        # type: (str, TaxonomyFieldValueCollection) -> None
        """
        Sets taxonomy field value
        :param str name: Taxonomy field name
        :param TaxonomyFieldValueCollection value: Taxonomy field value
        """
        tax_field = self.parent_list.fields.get_by_internal_name_or_title(name)

        def _tax_field_loaded():
            tax_text_field = self.parent_list.fields.get_by_id(
                tax_field.properties["TextField"]
            )

            def _tax_text_field_loaded(return_type):
                self.set_property(tax_text_field.properties["StaticName"], str(value))

            tax_text_field.select(["StaticName"]).get().after_execute(
                _tax_text_field_loaded, execute_first=True
            )

        tax_field.ensure_property("TextField", _tax_field_loaded)

    def ensure_type_name(self, target_list, action=None):
        """
        Determine metadata annotation for ListItem entity

        :param office365.sharepoint.lists.list.List target_list: List resource
        :param () -> None action: Event handler
        """
        if self._entity_type_name is None:

            def _list_loaded():
                self._entity_type_name = target_list.properties[
                    "ListItemEntityTypeFullName"
                ]
                if callable(action):
                    action()

            target_list.ensure_property("ListItemEntityTypeFullName", _list_loaded)
        else:
            if callable(action):
                action()
        return self
