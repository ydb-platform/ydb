import warnings
import xml.etree.ElementTree as ET

from defusedxml.ElementTree import fromstring

from .property_decorators import (
    property_is_enum,
    property_is_boolean,
    property_matches,
    property_not_empty,
    property_not_nullable,
    property_is_int,
)

VALID_CONTENT_URL_RE = r"^[a-zA-Z0-9_\-]*$"

from typing import Optional, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from tableauserverclient.server import Server


class SiteItem:
    """
    The SiteItem class contains the members or attributes for the site resources
    on Tableau Server or Tableau Cloud. The SiteItem class defines the
    information you can request or query from Tableau Server or Tableau Cloud.
    The class members correspond to the attributes of a server request or
    response payload.

    Attributes
    ----------
    name: str
        The name of the site. The name of the default site is "".

    content_url: str
        The path to the site.

    admin_mode: str
        (Optional) For Tableau Server only. Specify ContentAndUsers to allow
        site administrators to use the server interface and tabcmd commands to
        add and remove users. (Specifying this option does not give site
        administrators permissions to manage users using the REST API.) Specify
        ContentOnly to prevent site administrators from adding or removing
        users. (Server administrators can always add or remove users.)

    user_quota: int
        (Optional) Specifies the total number of users for the site. The number
        can't exceed the number of licenses activated for the site; and if
        tiered capacity attributes are set, then user_quota will equal the sum
        of the tiered capacity values, and attempting to set user_quota will
        cause an error.

    tier_explorer_capacity: int
    tier_creator_capacity: int
    tier_viewer_capacity: int
        (Optional) The maximum number of licenses for users with the Creator,
        Explorer, or Viewer role, respectively, allowed on a site.

    storage_quota: int
        (Optional) Specifies the maximum amount of space for the new site, in
        megabytes. If you set a quota and the site exceeds it, publishers will
        be prevented from uploading new content until the site is under the
        limit again.

    disable_subscriptions: bool
        (Optional) Specify true to prevent users from being able to subscribe
        to workbooks on the specified site. The default is False.

    subscribe_others_enabled: bool
        (Optional) Specify false to prevent server administrators, site
        administrators, and project or content owners from being able to
        subscribe other users to workbooks on the specified site. The default
        is True.

    revision_history_enabled: bool
        (Optional) Specify true to enable revision history for content resources
        (workbooks and datasources). The default is False.

    revision_limit: int
        (Optional) Specifies the number of revisions of a content source
        (workbook or data source) to allow. On Tableau Server, the default is
        25.

    state: str
        Shows the current state of the site (Active or Suspended).

    attribute_capture_enabled: Optional[str]
        Enables user attributes for all Tableau Server embedding workflows.

    """

    _user_quota: Optional[int] = None
    _tier_creator_capacity: Optional[int] = None
    _tier_explorer_capacity: Optional[int] = None
    _tier_viewer_capacity: Optional[int] = None

    def __str__(self):
        return (
            "<"
            + __name__
            + ": "
            + (self.name or "unnamed")
            + ", "
            + (self.id or "unknown-id")
            + ", "
            + (self.state or "unknown-state")
            + ">"
        )

    def __repr__(self):
        return self.__str__() + "  { " + ", ".join(" % s: % s" % item for item in vars(self).items()) + "}"

    class AdminMode:
        ContentAndUsers: str = "ContentAndUsers"
        ContentOnly: str = "ContentOnly"

    class State:
        Active: str = "Active"
        Suspended: str = "Suspended"

    def __init__(
        self,
        name: str,
        content_url: str,
        admin_mode: Optional[str] = None,
        user_quota: Optional[int] = None,
        storage_quota: Optional[int] = None,
        disable_subscriptions: bool = False,
        subscribe_others_enabled: bool = True,
        revision_history_enabled: bool = False,
        revision_limit: int = 25,
        data_acceleration_mode: Optional[str] = None,
        flows_enabled: bool = True,
        cataloging_enabled: bool = True,
        editing_flows_enabled: bool = True,
        scheduling_flows_enabled: bool = True,
        allow_subscription_attachments: bool = True,
        guest_access_enabled: bool = False,
        cache_warmup_enabled: bool = True,
        commenting_enabled: bool = True,
        extract_encryption_mode: Optional[str] = None,
        request_access_enabled: bool = False,
        run_now_enabled: bool = True,
        tier_explorer_capacity: Optional[int] = None,
        tier_creator_capacity: Optional[int] = None,
        tier_viewer_capacity: Optional[int] = None,
        data_alerts_enabled: bool = True,
        commenting_mentions_enabled: bool = True,
        catalog_obfuscation_enabled: bool = True,
        flow_auto_save_enabled: bool = True,
        web_extraction_enabled: bool = True,
        metrics_content_type_enabled: bool = True,
        notify_site_admins_on_throttle: bool = False,
        authoring_enabled: bool = True,
        custom_subscription_email_enabled: bool = False,
        custom_subscription_email: Union[str, bool] = False,
        custom_subscription_footer_enabled: bool = False,
        custom_subscription_footer: Union[str, bool] = False,
        ask_data_mode: str = "EnabledByDefault",
        named_sharing_enabled: bool = True,
        mobile_biometrics_enabled: bool = False,
        sheet_image_enabled: bool = True,
        derived_permissions_enabled: bool = False,
        user_visibility_mode: str = "FULL",
        use_default_time_zone: bool = True,
        time_zone=None,
        auto_suspend_refresh_enabled: bool = True,
        auto_suspend_refresh_inactivity_window: int = 30,
        attribute_capture_enabled: Optional[bool] = None,
    ):
        self._admin_mode = None
        self._id: Optional[str] = None
        self._num_users = None
        self._state = None
        self._status_reason = None
        self._storage: Optional[str] = None
        self.user_quota = user_quota
        self.storage_quota = storage_quota
        self.content_url = content_url
        self.disable_subscriptions = disable_subscriptions
        self.name = name
        self.revision_history_enabled = revision_history_enabled
        self.revision_limit = revision_limit
        self.subscribe_others_enabled = subscribe_others_enabled
        self.admin_mode = admin_mode
        self.data_acceleration_mode = data_acceleration_mode
        self.cataloging_enabled = cataloging_enabled
        self.flows_enabled = flows_enabled
        self.editing_flows_enabled = editing_flows_enabled
        self.scheduling_flows_enabled = scheduling_flows_enabled
        self.allow_subscription_attachments = allow_subscription_attachments
        self.guest_access_enabled = guest_access_enabled
        self.cache_warmup_enabled = cache_warmup_enabled
        self.commenting_enabled = commenting_enabled
        self.extract_encryption_mode = extract_encryption_mode
        self.request_access_enabled = request_access_enabled
        self.run_now_enabled = run_now_enabled
        self.tier_explorer_capacity = tier_explorer_capacity
        self.tier_creator_capacity = tier_creator_capacity
        self.tier_viewer_capacity = tier_viewer_capacity
        self.data_alerts_enabled = data_alerts_enabled
        self.commenting_mentions_enabled = commenting_mentions_enabled
        self.catalog_obfuscation_enabled = catalog_obfuscation_enabled
        self.flow_auto_save_enabled = flow_auto_save_enabled
        self.web_extraction_enabled = web_extraction_enabled
        self.metrics_content_type_enabled = metrics_content_type_enabled
        self.notify_site_admins_on_throttle = notify_site_admins_on_throttle
        self.authoring_enabled = authoring_enabled
        self.custom_subscription_footer_enabled = custom_subscription_footer_enabled
        self.custom_subscription_email_enabled = custom_subscription_email_enabled
        self.custom_subscription_email = custom_subscription_email
        self.custom_subscription_footer = custom_subscription_footer
        self.ask_data_mode = ask_data_mode
        self.named_sharing_enabled = named_sharing_enabled
        self.mobile_biometrics_enabled = mobile_biometrics_enabled
        self.sheet_image_enabled = sheet_image_enabled
        self.derived_permissions_enabled = derived_permissions_enabled
        self.user_visibility_mode = user_visibility_mode
        self.use_default_time_zone = use_default_time_zone
        self.time_zone = time_zone
        self.auto_suspend_refresh_enabled = auto_suspend_refresh_enabled
        self.auto_suspend_refresh_inactivity_window = auto_suspend_refresh_inactivity_window
        self.attribute_capture_enabled = attribute_capture_enabled

    @property
    def admin_mode(self) -> Optional[str]:
        return self._admin_mode

    @admin_mode.setter
    @property_is_enum(AdminMode)
    def admin_mode(self, value: Optional[str]) -> None:
        self._admin_mode = value

    @property
    def content_url(self) -> str:
        return self._content_url

    @content_url.setter
    @property_not_nullable
    @property_matches(
        VALID_CONTENT_URL_RE,
        "content_url can contain only letters, numbers, dashes, and underscores",
    )
    def content_url(self, value: str) -> None:
        self._content_url = value

    @property
    def disable_subscriptions(self) -> bool:
        return self._disable_subscriptions

    @disable_subscriptions.setter
    @property_is_boolean
    def disable_subscriptions(self, value: bool):
        self._disable_subscriptions = value

    @property
    def id(self) -> Optional[str]:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    @property_not_empty
    def name(self, value: str):
        self._name = value

    @property
    def num_users(self):
        return self._num_users

    @property
    def revision_history_enabled(self) -> bool:
        return self._revision_history_enabled

    @revision_history_enabled.setter
    @property_is_boolean
    def revision_history_enabled(self, value: bool):
        self._revision_history_enabled = value

    @property
    def revision_limit(self) -> int:
        return self._revision_limit

    @revision_limit.setter
    @property_is_int((2, 10000), allowed=[-1])
    def revision_limit(self, value: int):
        self._revision_limit = value

    @property
    def state(self) -> Optional[str]:
        return self._state

    @state.setter
    @property_is_enum(State)
    def state(self, value: Optional[str]) -> None:
        self._state = value

    @property
    def status_reason(self) -> Optional[str]:
        return self._status_reason

    @property
    def storage(self) -> Optional[str]:
        return self._storage

    @property
    def user_quota(self) -> Optional[int]:
        if any((self.tier_creator_capacity, self.tier_explorer_capacity, self.tier_viewer_capacity)):
            warnings.warn("Tiered license level is set. Returning None for user_quota")
            return None
        else:
            return self._user_quota

    @user_quota.setter
    def user_quota(self, value: Optional[int]) -> None:
        if value is not None and any(
            (self.tier_creator_capacity, self.tier_explorer_capacity, self.tier_viewer_capacity)
        ):
            raise ValueError(
                "User quota conflicts with setting tiered license levels. "
                "Use replace_license_tiers_with_user_quota to set those to None, "
                "and set user_quota to the desired value."
            )
        self._user_quota = value

    @property
    def subscribe_others_enabled(self) -> bool:
        return self._subscribe_others_enabled

    @subscribe_others_enabled.setter
    @property_is_boolean
    def subscribe_others_enabled(self, value: bool) -> None:
        self._subscribe_others_enabled = value

    @property
    def data_acceleration_mode(self) -> Optional[str]:
        return self._data_acceleration_mode

    @data_acceleration_mode.setter
    def data_acceleration_mode(self, value: Optional[str]):
        self._data_acceleration_mode = value

    @property
    def cataloging_enabled(self) -> bool:
        return self._cataloging_enabled

    @cataloging_enabled.setter
    def cataloging_enabled(self, value: bool):
        self._cataloging_enabled = value

    def is_default(self) -> bool:
        return self.name.lower() == "default"

    @staticmethod
    def use_new_flow_settings(parent_srv: "Server") -> bool:
        return parent_srv is not None and parent_srv.check_at_least_version("3.10")

    @property
    def flows_enabled(self) -> bool:
        return self._flows_enabled

    @flows_enabled.setter
    @property_is_boolean
    def flows_enabled(self, value: bool) -> None:
        # Flows Enabled' is not a supported site setting in API Version [3.17].
        # In Version 3.10+ use the more granular settings 'Editing Flows Enabled' and/or 'Scheduling Flows Enabled'
        self._flows_enabled = value

    @property
    def editing_flows_enabled(self) -> bool:
        return self._editing_flows_enabled

    @editing_flows_enabled.setter
    @property_is_boolean
    def editing_flows_enabled(self, value: bool) -> None:
        self._editing_flows_enabled = value

    @property
    def scheduling_flows_enabled(self) -> bool:
        return self._scheduling_flows_enabled

    @scheduling_flows_enabled.setter
    @property_is_boolean
    def scheduling_flows_enabled(self, value: bool):
        self._scheduling_flows_enabled = value

    @property
    def allow_subscription_attachments(self) -> bool:
        return self._allow_subscription_attachments

    @allow_subscription_attachments.setter
    @property_is_boolean
    def allow_subscription_attachments(self, value: bool):
        self._allow_subscription_attachments = value

    @property
    def guest_access_enabled(self) -> bool:
        return self._guest_access_enabled

    @guest_access_enabled.setter
    @property_is_boolean
    def guest_access_enabled(self, value: bool) -> None:
        self._guest_access_enabled = value

    @property
    def cache_warmup_enabled(self) -> bool:
        return self._cache_warmup_enabled

    @cache_warmup_enabled.setter
    @property_is_boolean
    def cache_warmup_enabled(self, value: bool):
        self._cache_warmup_enabled = value

    @property
    def commenting_enabled(self) -> bool:
        return self._commenting_enabled

    @commenting_enabled.setter
    @property_is_boolean
    def commenting_enabled(self, value: bool):
        self._commenting_enabled = value

    @property
    def extract_encryption_mode(self) -> Optional[str]:
        return self._extract_encryption_mode

    @extract_encryption_mode.setter
    def extract_encryption_mode(self, value: Optional[str]):
        self._extract_encryption_mode = value

    @property
    def request_access_enabled(self) -> bool:
        return self._request_access_enabled

    @request_access_enabled.setter
    @property_is_boolean
    def request_access_enabled(self, value: bool) -> None:
        self._request_access_enabled = value

    @property
    def run_now_enabled(self) -> bool:
        return self._run_now_enabled

    @run_now_enabled.setter
    @property_is_boolean
    def run_now_enabled(self, value: bool):
        self._run_now_enabled = value

    @property
    def tier_explorer_capacity(self) -> Optional[int]:
        return self._tier_explorer_capacity

    @tier_explorer_capacity.setter
    def tier_explorer_capacity(self, value: Optional[int]) -> None:
        self._tier_explorer_capacity = value

    @property
    def tier_creator_capacity(self) -> Optional[int]:
        return self._tier_creator_capacity

    @tier_creator_capacity.setter
    def tier_creator_capacity(self, value: Optional[int]) -> None:
        self._tier_creator_capacity = value

    @property
    def tier_viewer_capacity(self) -> Optional[int]:
        return self._tier_viewer_capacity

    @tier_viewer_capacity.setter
    def tier_viewer_capacity(self, value: Optional[int]):
        self._tier_viewer_capacity = value

    @property
    def data_alerts_enabled(self) -> bool:
        return self._data_alerts_enabled

    @data_alerts_enabled.setter
    @property_is_boolean
    def data_alerts_enabled(self, value: bool) -> None:
        self._data_alerts_enabled = value

    @property
    def commenting_mentions_enabled(self) -> bool:
        return self._commenting_mentions_enabled

    @commenting_mentions_enabled.setter
    @property_is_boolean
    def commenting_mentions_enabled(self, value: bool) -> None:
        self._commenting_mentions_enabled = value

    @property
    def catalog_obfuscation_enabled(self) -> bool:
        return self._catalog_obfuscation_enabled

    @catalog_obfuscation_enabled.setter
    @property_is_boolean
    def catalog_obfuscation_enabled(self, value: bool) -> None:
        self._catalog_obfuscation_enabled = value

    @property
    def flow_auto_save_enabled(self) -> bool:
        return self._flow_auto_save_enabled

    @flow_auto_save_enabled.setter
    @property_is_boolean
    def flow_auto_save_enabled(self, value: bool) -> None:
        self._flow_auto_save_enabled = value

    @property
    def web_extraction_enabled(self) -> bool:
        return self._web_extraction_enabled

    @web_extraction_enabled.setter
    @property_is_boolean
    def web_extraction_enabled(self, value: bool) -> None:
        self._web_extraction_enabled = value

    @property
    def metrics_content_type_enabled(self) -> bool:
        return self._metrics_content_type_enabled

    @metrics_content_type_enabled.setter
    @property_is_boolean
    def metrics_content_type_enabled(self, value: bool) -> None:
        self._metrics_content_type_enabled = value

    @property
    def notify_site_admins_on_throttle(self) -> bool:
        return self._notify_site_admins_on_throttle

    @notify_site_admins_on_throttle.setter
    @property_is_boolean
    def notify_site_admins_on_throttle(self, value: bool) -> None:
        self._notify_site_admins_on_throttle = value

    @property
    def authoring_enabled(self) -> bool:
        return self._authoring_enabled

    @authoring_enabled.setter
    @property_is_boolean
    def authoring_enabled(self, value: bool) -> None:
        self._authoring_enabled = value

    @property
    def custom_subscription_email_enabled(self) -> bool:
        return self._custom_subscription_email_enabled

    @custom_subscription_email_enabled.setter
    @property_is_boolean
    def custom_subscription_email_enabled(self, value: bool) -> None:
        self._custom_subscription_email_enabled = value

    @property
    def custom_subscription_email(self) -> Union[str, bool]:
        return self._custom_subscription_email

    @custom_subscription_email.setter
    def custom_subscription_email(self, value: Union[str, bool]):
        self._custom_subscription_email = value

    @property
    def custom_subscription_footer_enabled(self) -> bool:
        return self._custom_subscription_footer_enabled

    @custom_subscription_footer_enabled.setter
    @property_is_boolean
    def custom_subscription_footer_enabled(self, value: bool) -> None:
        self._custom_subscription_footer_enabled = value

    @property
    def custom_subscription_footer(self) -> Union[str, bool]:
        return self._custom_subscription_footer

    @custom_subscription_footer.setter
    def custom_subscription_footer(self, value: Union[str, bool]) -> None:
        self._custom_subscription_footer = value

    @property
    def ask_data_mode(self) -> str:
        return self._ask_data_mode

    @ask_data_mode.setter
    def ask_data_mode(self, value: str) -> None:
        self._ask_data_mode = value

    @property
    def named_sharing_enabled(self) -> bool:
        return self._named_sharing_enabled

    @named_sharing_enabled.setter
    @property_is_boolean
    def named_sharing_enabled(self, value: bool) -> None:
        self._named_sharing_enabled = value

    @property
    def mobile_biometrics_enabled(self) -> bool:
        return self._mobile_biometrics_enabled

    @mobile_biometrics_enabled.setter
    @property_is_boolean
    def mobile_biometrics_enabled(self, value: bool) -> None:
        self._mobile_biometrics_enabled = value

    @property
    def sheet_image_enabled(self) -> bool:
        return self._sheet_image_enabled

    @sheet_image_enabled.setter
    @property_is_boolean
    def sheet_image_enabled(self, value: bool) -> None:
        self._sheet_image_enabled = value

    @property
    def derived_permissions_enabled(self) -> bool:
        return self._derived_permissions_enabled

    @derived_permissions_enabled.setter
    @property_is_boolean
    def derived_permissions_enabled(self, value: bool) -> None:
        self._derived_permissions_enabled = value

    @property
    def user_visibility_mode(self) -> str:
        return self._user_visibility_mode

    @user_visibility_mode.setter
    def user_visibility_mode(self, value: str):
        self._user_visibility_mode = value

    @property
    def use_default_time_zone(self):
        return self._use_default_time_zone

    @use_default_time_zone.setter
    def use_default_time_zone(self, value):
        self._use_default_time_zone = value

    @property
    def time_zone(self):
        return self._time_zone

    @time_zone.setter
    def time_zone(self, value):
        self._time_zone = value

    @property
    def auto_suspend_refresh_inactivity_window(self):
        return self._auto_suspend_refresh_inactivity_window

    @auto_suspend_refresh_inactivity_window.setter
    def auto_suspend_refresh_inactivity_window(self, value):
        self._auto_suspend_refresh_inactivity_window = value

    @property
    def auto_suspend_refresh_enabled(self) -> bool:
        return self._auto_suspend_refresh_enabled

    @auto_suspend_refresh_enabled.setter
    def auto_suspend_refresh_enabled(self, value: bool):
        self._auto_suspend_refresh_enabled = value

    def replace_license_tiers_with_user_quota(self, value: int) -> None:
        self.tier_creator_capacity = None
        self.tier_explorer_capacity = None
        self.tier_viewer_capacity = None
        self.user_quota = value

    def _parse_common_tags(self, site_xml, ns):
        if not isinstance(site_xml, ET.Element):
            site_xml = fromstring(site_xml).find(".//t:site", namespaces=ns)
        if site_xml is not None:
            (
                _,
                name,
                content_url,
                _,
                admin_mode,
                state,
                subscribe_others_enabled,
                disable_subscriptions,
                revision_history_enabled,
                user_quota,
                storage_quota,
                revision_limit,
                num_users,
                storage,
                data_acceleration_mode,
                flows_enabled,
                cataloging_enabled,
                editing_flows_enabled,
                scheduling_flows_enabled,
                allow_subscription_attachments,
                guest_access_enabled,
                cache_warmup_enabled,
                commenting_enabled,
                extract_encryption_mode,
                request_access_enabled,
                run_now_enabled,
                tier_explorer_capacity,
                tier_creator_capacity,
                tier_viewer_capacity,
                data_alerts_enabled,
                commenting_mentions_enabled,
                catalog_obfuscation_enabled,
                flow_auto_save_enabled,
                web_extraction_enabled,
                metrics_content_type_enabled,
                notify_site_admins_on_throttle,
                authoring_enabled,
                custom_subscription_email_enabled,
                custom_subscription_email,
                custom_subscription_footer_enabled,
                custom_subscription_footer,
                ask_data_mode,
                named_sharing_enabled,
                mobile_biometrics_enabled,
                sheet_image_enabled,
                derived_permissions_enabled,
                user_visibility_mode,
                use_default_time_zone,
                time_zone,
                auto_suspend_refresh_enabled,
                auto_suspend_refresh_inactivity_window,
                attribute_capture_enabled,
            ) = self._parse_element(site_xml, ns)

            self._set_values(
                None,
                name,
                content_url,
                None,
                admin_mode,
                state,
                subscribe_others_enabled,
                disable_subscriptions,
                revision_history_enabled,
                user_quota,
                storage_quota,
                revision_limit,
                num_users,
                storage,
                data_acceleration_mode,
                flows_enabled,
                cataloging_enabled,
                editing_flows_enabled,
                scheduling_flows_enabled,
                allow_subscription_attachments,
                guest_access_enabled,
                cache_warmup_enabled,
                commenting_enabled,
                extract_encryption_mode,
                request_access_enabled,
                run_now_enabled,
                tier_explorer_capacity,
                tier_creator_capacity,
                tier_viewer_capacity,
                data_alerts_enabled,
                commenting_mentions_enabled,
                catalog_obfuscation_enabled,
                flow_auto_save_enabled,
                web_extraction_enabled,
                metrics_content_type_enabled,
                notify_site_admins_on_throttle,
                authoring_enabled,
                custom_subscription_email_enabled,
                custom_subscription_email,
                custom_subscription_footer_enabled,
                custom_subscription_footer,
                ask_data_mode,
                named_sharing_enabled,
                mobile_biometrics_enabled,
                sheet_image_enabled,
                derived_permissions_enabled,
                user_visibility_mode,
                use_default_time_zone,
                time_zone,
                auto_suspend_refresh_enabled,
                auto_suspend_refresh_inactivity_window,
                attribute_capture_enabled,
            )
        return self

    def _set_values(
        self,
        id,
        name,
        content_url,
        status_reason,
        admin_mode,
        state,
        subscribe_others_enabled,
        disable_subscriptions,
        revision_history_enabled,
        user_quota,
        storage_quota,
        revision_limit,
        num_users,
        storage,
        data_acceleration_mode,
        flows_enabled,
        cataloging_enabled,
        editing_flows_enabled,
        scheduling_flows_enabled,
        allow_subscription_attachments,
        guest_access_enabled,
        cache_warmup_enabled,
        commenting_enabled,
        extract_encryption_mode,
        request_access_enabled,
        run_now_enabled,
        tier_explorer_capacity,
        tier_creator_capacity,
        tier_viewer_capacity,
        data_alerts_enabled,
        commenting_mentions_enabled,
        catalog_obfuscation_enabled,
        flow_auto_save_enabled,
        web_extraction_enabled,
        metrics_content_type_enabled,
        notify_site_admins_on_throttle,
        authoring_enabled,
        custom_subscription_email_enabled,
        custom_subscription_email,
        custom_subscription_footer_enabled,
        custom_subscription_footer,
        ask_data_mode,
        named_sharing_enabled,
        mobile_biometrics_enabled,
        sheet_image_enabled,
        derived_permissions_enabled,
        user_visibility_mode,
        use_default_time_zone,
        time_zone,
        auto_suspend_refresh_enabled,
        auto_suspend_refresh_inactivity_window,
        attribute_capture_enabled,
    ):
        if id is not None:
            self._id = id
        if name:
            self._name = name
        if content_url:
            self._content_url = content_url
        if status_reason:
            self._status_reason = status_reason
        if admin_mode:
            self._admin_mode = admin_mode
        if state:
            self._state = state
        if subscribe_others_enabled is not None:
            self._subscribe_others_enabled = subscribe_others_enabled
        if disable_subscriptions is not None:
            self._disable_subscriptions = disable_subscriptions
        if revision_history_enabled is not None:
            self._revision_history_enabled = revision_history_enabled
        if user_quota:
            try:
                self.user_quota = user_quota
            except ValueError:
                warnings.warn("Tiered license level is set. Setting user_quota to None.")
                self.user_quota = None
        if storage_quota:
            self.storage_quota = storage_quota
        if revision_limit:
            self.revision_limit = revision_limit
        if num_users:
            self._num_users = num_users
        if storage:
            self._storage = storage
        if data_acceleration_mode:
            self._data_acceleration_mode = data_acceleration_mode
        if flows_enabled is not None:
            self.flows_enabled = flows_enabled
        if cataloging_enabled is not None:
            self.cataloging_enabled = cataloging_enabled
        if editing_flows_enabled is not None:
            self.editing_flows_enabled = editing_flows_enabled
        if scheduling_flows_enabled is not None:
            self.scheduling_flows_enabled = scheduling_flows_enabled
        if allow_subscription_attachments is not None:
            self.allow_subscription_attachments = allow_subscription_attachments
        if guest_access_enabled is not None:
            self.guest_access_enabled = guest_access_enabled
        if cache_warmup_enabled is not None:
            self.cache_warmup_enabled = cache_warmup_enabled
        if commenting_enabled is not None:
            self.commenting_enabled = commenting_enabled
        if extract_encryption_mode is not None:
            self.extract_encryption_mode = extract_encryption_mode
        if request_access_enabled is not None:
            self.request_access_enabled = request_access_enabled
        if run_now_enabled is not None:
            self.run_now_enabled = run_now_enabled
        if tier_explorer_capacity:
            self.tier_explorer_capacity = tier_explorer_capacity
        if tier_creator_capacity:
            self.tier_creator_capacity = tier_creator_capacity
        if tier_viewer_capacity:
            self.tier_viewer_capacity = tier_viewer_capacity
        if data_alerts_enabled is not None:
            self.data_alerts_enabled = data_alerts_enabled
        if commenting_mentions_enabled is not None:
            self.commenting_mentions_enabled = commenting_mentions_enabled
        if catalog_obfuscation_enabled is not None:
            self.catalog_obfuscation_enabled = catalog_obfuscation_enabled
        if flow_auto_save_enabled is not None:
            self.flow_auto_save_enabled = flow_auto_save_enabled
        if web_extraction_enabled is not None:
            self.web_extraction_enabled = web_extraction_enabled
        if metrics_content_type_enabled is not None:
            self.metrics_content_type_enabled = metrics_content_type_enabled
        if notify_site_admins_on_throttle is not None:
            self.notify_site_admins_on_throttle = notify_site_admins_on_throttle
        if authoring_enabled is not None:
            self.authoring_enabled = authoring_enabled
        if custom_subscription_email_enabled is not None:
            self.custom_subscription_email_enabled = custom_subscription_email_enabled
        if custom_subscription_email is not None:
            self.custom_subscription_email = custom_subscription_email
        if custom_subscription_footer_enabled is not None:
            self.custom_subscription_footer_enabled = custom_subscription_footer_enabled
        if custom_subscription_footer is not None:
            self.custom_subscription_footer = custom_subscription_footer
        if ask_data_mode is not None:
            self.ask_data_mode = ask_data_mode
        if named_sharing_enabled is not None:
            self.named_sharing_enabled = named_sharing_enabled
        if mobile_biometrics_enabled is not None:
            self.mobile_biometrics_enabled = mobile_biometrics_enabled
        if sheet_image_enabled is not None:
            self.sheet_image_enabled = sheet_image_enabled
        if derived_permissions_enabled is not None:
            self.derived_permissions_enabled = derived_permissions_enabled
        if user_visibility_mode is not None:
            self.user_visibility_mode = user_visibility_mode
        if use_default_time_zone is not None:
            self.use_default_time_zone = use_default_time_zone
        if time_zone is not None:
            self.time_zone = time_zone
        if auto_suspend_refresh_enabled is not None:
            self.auto_suspend_refresh_enabled = auto_suspend_refresh_enabled
        if auto_suspend_refresh_inactivity_window is not None:
            self.auto_suspend_refresh_inactivity_window = auto_suspend_refresh_inactivity_window
        self.attribute_capture_enabled = attribute_capture_enabled

    @classmethod
    def from_response(cls, resp, ns) -> list["SiteItem"]:
        all_site_items = list()
        parsed_response = fromstring(resp)
        all_site_xml = parsed_response.findall(".//t:site", namespaces=ns)
        for site_xml in all_site_xml:
            (
                id,
                name,
                content_url,
                status_reason,
                admin_mode,
                state,
                subscribe_others_enabled,
                disable_subscriptions,
                revision_history_enabled,
                user_quota,
                storage_quota,
                revision_limit,
                num_users,
                storage,
                data_acceleration_mode,
                flows_enabled,
                cataloging_enabled,
                editing_flows_enabled,
                scheduling_flows_enabled,
                allow_subscription_attachments,
                guest_access_enabled,
                cache_warmup_enabled,
                commenting_enabled,
                extract_encryption_mode,
                request_access_enabled,
                run_now_enabled,
                tier_explorer_capacity,
                tier_creator_capacity,
                tier_viewer_capacity,
                data_alerts_enabled,
                commenting_mentions_enabled,
                catalog_obfuscation_enabled,
                flow_auto_save_enabled,
                web_extraction_enabled,
                metrics_content_type_enabled,
                notify_site_admins_on_throttle,
                authoring_enabled,
                custom_subscription_email_enabled,
                custom_subscription_email,
                custom_subscription_footer_enabled,
                custom_subscription_footer,
                ask_data_mode,
                named_sharing_enabled,
                mobile_biometrics_enabled,
                sheet_image_enabled,
                derived_permissions_enabled,
                user_visibility_mode,
                use_default_time_zone,
                time_zone,
                auto_suspend_refresh_enabled,
                auto_suspend_refresh_inactivity_window,
                attribute_capture_enabled,
            ) = cls._parse_element(site_xml, ns)

            site_item = cls(name, content_url)
            site_item._set_values(
                id,
                name,
                content_url,
                status_reason,
                admin_mode,
                state,
                subscribe_others_enabled,
                disable_subscriptions,
                revision_history_enabled,
                user_quota,
                storage_quota,
                revision_limit,
                num_users,
                storage,
                data_acceleration_mode,
                flows_enabled,
                cataloging_enabled,
                editing_flows_enabled,
                scheduling_flows_enabled,
                allow_subscription_attachments,
                guest_access_enabled,
                cache_warmup_enabled,
                commenting_enabled,
                extract_encryption_mode,
                request_access_enabled,
                run_now_enabled,
                tier_explorer_capacity,
                tier_creator_capacity,
                tier_viewer_capacity,
                data_alerts_enabled,
                commenting_mentions_enabled,
                catalog_obfuscation_enabled,
                flow_auto_save_enabled,
                web_extraction_enabled,
                metrics_content_type_enabled,
                notify_site_admins_on_throttle,
                authoring_enabled,
                custom_subscription_email_enabled,
                custom_subscription_email,
                custom_subscription_footer_enabled,
                custom_subscription_footer,
                ask_data_mode,
                named_sharing_enabled,
                mobile_biometrics_enabled,
                sheet_image_enabled,
                derived_permissions_enabled,
                user_visibility_mode,
                use_default_time_zone,
                time_zone,
                auto_suspend_refresh_enabled,
                auto_suspend_refresh_inactivity_window,
                attribute_capture_enabled,
            )
            all_site_items.append(site_item)
        return all_site_items

    @staticmethod
    def _parse_element(site_xml, ns):
        id = site_xml.get("id", None)
        name = site_xml.get("name", None)
        content_url = site_xml.get("contentUrl", None)
        status_reason = site_xml.get("statusReason", None)
        admin_mode = site_xml.get("adminMode", None)
        state = site_xml.get("state", None)
        subscribe_others_enabled = string_to_bool(site_xml.get("subscribeOthersEnabled", ""))
        disable_subscriptions = string_to_bool(site_xml.get("disableSubscriptions", ""))
        revision_history_enabled = string_to_bool(site_xml.get("revisionHistoryEnabled", ""))
        editing_flows_enabled = string_to_bool(site_xml.get("editingFlowsEnabled", ""))
        scheduling_flows_enabled = string_to_bool(site_xml.get("schedulingFlowsEnabled", ""))
        allow_subscription_attachments = string_to_bool(site_xml.get("allowSubscriptionAttachments", ""))
        guest_access_enabled = string_to_bool(site_xml.get("guestAccessEnabled", ""))
        cache_warmup_enabled = string_to_bool(site_xml.get("cacheWarmupEnabled", ""))
        commenting_enabled = string_to_bool(site_xml.get("commentingEnabled", ""))
        extract_encryption_mode = site_xml.get("extractEncryptionMode", None)
        request_access_enabled = string_to_bool(site_xml.get("requestAccessEnabled", ""))
        run_now_enabled = string_to_bool(site_xml.get("runNowEnabled", ""))
        tier_explorer_capacity = site_xml.get("tierExplorerCapacity", None)
        if tier_explorer_capacity:
            tier_explorer_capacity = int(tier_explorer_capacity)
        tier_creator_capacity = site_xml.get("tierCreatorCapacity", None)
        if tier_creator_capacity:
            tier_creator_capacity = int(tier_creator_capacity)
        tier_viewer_capacity = site_xml.get("tierViewerCapacity", None)
        if tier_viewer_capacity:
            tier_viewer_capacity = int(tier_viewer_capacity)
        data_alerts_enabled = string_to_bool(site_xml.get("dataAlertsEnabled", ""))
        commenting_mentions_enabled = string_to_bool(site_xml.get("commentingMentionsEnabled", ""))
        catalog_obfuscation_enabled = string_to_bool(site_xml.get("catalogObfuscationEnabled", ""))
        flow_auto_save_enabled = string_to_bool(site_xml.get("flowAutoSaveEnabled", ""))
        web_extraction_enabled = string_to_bool(site_xml.get("webExtractionEnabled", ""))
        metrics_content_type_enabled = string_to_bool(site_xml.get("metricsContentTypeEnabled", ""))
        notify_site_admins_on_throttle = string_to_bool(site_xml.get("notifySiteAdminsOnThrottle", ""))
        authoring_enabled = string_to_bool(site_xml.get("authoringEnabled", ""))
        custom_subscription_email_enabled = string_to_bool(site_xml.get("customSubscriptionEmailEnabled", ""))
        custom_subscription_email = site_xml.get("customSubscriptionEmail", None)
        custom_subscription_footer_enabled = string_to_bool(site_xml.get("customSubscriptionFooterEnabled", ""))
        custom_subscription_footer = site_xml.get("customSubscriptionFooter", None)
        ask_data_mode = site_xml.get("askDataMode", None)
        named_sharing_enabled = string_to_bool(site_xml.get("namedSharingEnabled", ""))
        mobile_biometrics_enabled = string_to_bool(site_xml.get("mobileBiometricsEnabled", ""))
        sheet_image_enabled = string_to_bool(site_xml.get("sheetImageEnabled", ""))
        derived_permissions_enabled = string_to_bool(site_xml.get("derivedPermissionsEnabled", ""))
        user_visibility_mode = site_xml.get("userVisibilityMode", "")
        use_default_time_zone = string_to_bool(site_xml.get("useDefaultTimeZone", ""))
        time_zone = site_xml.get("timeZone", None)
        auto_suspend_refresh_enabled = string_to_bool(site_xml.get("autoSuspendRefreshEnabled", ""))
        auto_suspend_refresh_inactivity_window = site_xml.get("autoSuspendRefreshInactivityWindow", None)
        if auto_suspend_refresh_inactivity_window:
            auto_suspend_refresh_inactivity_window = int(auto_suspend_refresh_inactivity_window)

        user_quota = site_xml.get("userQuota", None)
        if user_quota:
            user_quota = int(user_quota)

        storage_quota = site_xml.get("storageQuota", None)
        if storage_quota:
            storage_quota = int(storage_quota)

        revision_limit = site_xml.get("revisionLimit", None)
        if revision_limit:
            revision_limit = int(revision_limit)

        num_users = None
        storage = None
        usage_elem = site_xml.find(".//t:usage", namespaces=ns)
        if usage_elem is not None:
            num_users = usage_elem.get("numUsers", None)
            storage = usage_elem.get("storage", None)

        data_acceleration_mode = site_xml.get("dataAccelerationMode", "")

        flows_enabled = string_to_bool(site_xml.get("flowsEnabled", ""))
        cataloging_enabled = string_to_bool(site_xml.get("catalogingEnabled", ""))
        attribute_capture_enabled = (
            string_to_bool(ace) if (ace := site_xml.get("attributeCaptureEnabled")) is not None else None
        )

        return (
            id,
            name,
            content_url,
            status_reason,
            admin_mode,
            state,
            subscribe_others_enabled,
            disable_subscriptions,
            revision_history_enabled,
            user_quota,
            storage_quota,
            revision_limit,
            num_users,
            storage,
            data_acceleration_mode,
            flows_enabled,
            cataloging_enabled,
            editing_flows_enabled,
            scheduling_flows_enabled,
            allow_subscription_attachments,
            guest_access_enabled,
            cache_warmup_enabled,
            commenting_enabled,
            extract_encryption_mode,
            request_access_enabled,
            run_now_enabled,
            tier_explorer_capacity,
            tier_creator_capacity,
            tier_viewer_capacity,
            data_alerts_enabled,
            commenting_mentions_enabled,
            catalog_obfuscation_enabled,
            flow_auto_save_enabled,
            web_extraction_enabled,
            metrics_content_type_enabled,
            notify_site_admins_on_throttle,
            authoring_enabled,
            custom_subscription_email_enabled,
            custom_subscription_email,
            custom_subscription_footer_enabled,
            custom_subscription_footer,
            ask_data_mode,
            named_sharing_enabled,
            mobile_biometrics_enabled,
            sheet_image_enabled,
            derived_permissions_enabled,
            user_visibility_mode,
            use_default_time_zone,
            time_zone,
            auto_suspend_refresh_enabled,
            auto_suspend_refresh_inactivity_window,
            attribute_capture_enabled,
        )


class SiteAuthConfiguration:
    """
    Authentication configuration for a site.
    """

    def __init__(self):
        self.auth_setting: Optional[str] = None
        self.enabled: Optional[bool] = None
        self.idp_configuration_id: Optional[str] = None
        self.idp_configuration_name: Optional[str] = None
        self.known_provider_alias: Optional[str] = None

    @classmethod
    def from_response(cls, resp: bytes, ns: dict) -> list["SiteAuthConfiguration"]:
        all_auth_configs = list()
        parsed_response = fromstring(resp)
        all_auth_xml = parsed_response.findall(".//t:siteAuthConfiguration", namespaces=ns)
        for auth_xml in all_auth_xml:
            auth_config = cls()
            auth_config.auth_setting = auth_xml.get("authSetting", None)
            auth_config.enabled = string_to_bool(auth_xml.get("enabled", ""))
            auth_config.idp_configuration_id = auth_xml.get("idpConfigurationId", None)
            auth_config.idp_configuration_name = auth_xml.get("idpConfigurationName", None)
            auth_config.known_provider_alias = auth_xml.get("knownProviderAlias", None)
            all_auth_configs.append(auth_config)
        return all_auth_configs

    def __str__(self):
        return (
            f"{self.__class__.__qualname__}(auth_setting={self.auth_setting}, "
            f"enabled={self.enabled}, "
            f"idp_configuration_id={self.idp_configuration_id}, "
            f"idp_configuration_name={self.idp_configuration_name})"
        )

    def __repr__(self):
        return f"<{str(self)}>"


# Used to convert string represented boolean to a boolean type
def string_to_bool(s: str) -> bool:
    return s.lower() == "true"
