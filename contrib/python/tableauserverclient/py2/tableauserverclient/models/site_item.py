import xml.etree.ElementTree as ET
from .property_decorators import (property_is_enum, property_is_boolean, property_matches,
                                  property_not_empty, property_not_nullable, property_is_int)


VALID_CONTENT_URL_RE = r"^[a-zA-Z0-9_\-]*$"


class SiteItem(object):
    class AdminMode:
        ContentAndUsers = 'ContentAndUsers'
        ContentOnly = 'ContentOnly'

    class State:
        Active = 'Active'
        Suspended = 'Suspended'

    def __init__(self, name, content_url, admin_mode=None, user_quota=None, storage_quota=None,
                 disable_subscriptions=False, subscribe_others_enabled=True, revision_history_enabled=False,
                 revision_limit=None, data_acceleration_mode=None, flows_enabled=None, cataloging_enabled=None):
        self._admin_mode = None
        self._id = None
        self._num_users = None
        self._state = None
        self._status_reason = None
        self._storage = None
        self._revision_limit = None
        self.user_quota = user_quota
        self.storage_quota = storage_quota
        self.content_url = content_url
        self.disable_subscriptions = disable_subscriptions
        self.name = name
        self.revision_history_enabled = revision_history_enabled
        self.subscribe_others_enabled = subscribe_others_enabled
        self.admin_mode = admin_mode
        self.data_acceleration_mode = data_acceleration_mode
        self.cataloging_enabled = cataloging_enabled
        self.flows_enabled = flows_enabled

    @property
    def admin_mode(self):
        return self._admin_mode

    @admin_mode.setter
    @property_is_enum(AdminMode)
    def admin_mode(self, value):
        self._admin_mode = value

    @property
    def content_url(self):
        return self._content_url

    @content_url.setter
    @property_not_nullable
    @property_matches(VALID_CONTENT_URL_RE, "content_url can contain only letters, numbers, dashes, and underscores")
    def content_url(self, value):
        self._content_url = value

    @property
    def disable_subscriptions(self):
        return self._disable_subscriptions

    @disable_subscriptions.setter
    @property_is_boolean
    def disable_subscriptions(self, value):
        self._disable_subscriptions = value

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @name.setter
    @property_not_empty
    def name(self, value):
        self._name = value

    @property
    def num_users(self):
        return self._num_users

    @property
    def revision_history_enabled(self):
        return self._revision_history_enabled

    @revision_history_enabled.setter
    @property_is_boolean
    def revision_history_enabled(self, value):
        self._revision_history_enabled = value

    @property
    def revision_limit(self):
        return self._revision_limit

    @revision_limit.setter
    @property_is_int((2, 10000), allowed=[-1])
    def revision_limit(self, value):
        self._revision_limit = value

    @property
    def state(self):
        return self._state

    @state.setter
    @property_is_enum(State)
    def state(self, value):
        self._state = value

    @property
    def status_reason(self):
        return self._status_reason

    @property
    def storage(self):
        return self._storage

    @property
    def subscribe_others_enabled(self):
        return self._subscribe_others_enabled

    @subscribe_others_enabled.setter
    @property_is_boolean
    def subscribe_others_enabled(self, value):
        self._subscribe_others_enabled = value

    @property
    def data_acceleration_mode(self):
        return self._data_acceleration_mode

    @data_acceleration_mode.setter
    def data_acceleration_mode(self, value):
        self._data_acceleration_mode = value

    @property
    def cataloging_enabled(self):
        return self._cataloging_enabled

    @cataloging_enabled.setter
    def cataloging_enabled(self, value):
        self._cataloging_enabled = value

    @property
    def flows_enabled(self):
        return self._flows_enabled

    @flows_enabled.setter
    def flows_enabled(self, value):
        self._flows_enabled = value

    def is_default(self):
        return self.name.lower() == 'default'

    def _parse_common_tags(self, site_xml, ns):
        if not isinstance(site_xml, ET.Element):
            site_xml = ET.fromstring(site_xml).find('.//t:site', namespaces=ns)
        if site_xml is not None:
            (_, name, content_url, _, admin_mode, state,
             subscribe_others_enabled, disable_subscriptions, revision_history_enabled,
             user_quota, storage_quota, revision_limit, num_users, storage,
             data_acceleration_mode, cataloging_enabled, flows_enabled) = self._parse_element(site_xml, ns)

            self._set_values(None, name, content_url, None, admin_mode, state, subscribe_others_enabled,
                             disable_subscriptions, revision_history_enabled, user_quota, storage_quota,
                             revision_limit, num_users, storage, data_acceleration_mode, cataloging_enabled,
                             flows_enabled)
        return self

    def _set_values(self, id, name, content_url, status_reason, admin_mode, state,
                    subscribe_others_enabled, disable_subscriptions, revision_history_enabled,
                    user_quota, storage_quota, revision_limit, num_users, storage, data_acceleration_mode,
                    flows_enabled, cataloging_enabled):
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
            self.user_quota = user_quota
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

    @classmethod
    def from_response(cls, resp, ns):
        all_site_items = list()
        parsed_response = ET.fromstring(resp)
        all_site_xml = parsed_response.findall('.//t:site', namespaces=ns)
        for site_xml in all_site_xml:
            (id, name, content_url, status_reason, admin_mode, state, subscribe_others_enabled,
                disable_subscriptions, revision_history_enabled, user_quota, storage_quota,
                revision_limit, num_users, storage, data_acceleration_mode, flows_enabled,
                cataloging_enabled) = cls._parse_element(site_xml, ns)

            site_item = cls(name, content_url)
            site_item._set_values(id, name, content_url, status_reason, admin_mode, state,
                                  subscribe_others_enabled, disable_subscriptions, revision_history_enabled,
                                  user_quota, storage_quota, revision_limit, num_users, storage,
                                  data_acceleration_mode, flows_enabled, cataloging_enabled)
            all_site_items.append(site_item)
        return all_site_items

    @staticmethod
    def _parse_element(site_xml, ns):
        id = site_xml.get('id', None)
        name = site_xml.get('name', None)
        content_url = site_xml.get('contentUrl', None)
        status_reason = site_xml.get('statusReason', None)
        admin_mode = site_xml.get('adminMode', None)
        state = site_xml.get('state', None)
        subscribe_others_enabled = string_to_bool(site_xml.get('subscribeOthersEnabled', ''))
        disable_subscriptions = string_to_bool(site_xml.get('disableSubscriptions', ''))
        revision_history_enabled = string_to_bool(site_xml.get('revisionHistoryEnabled', ''))

        user_quota = site_xml.get('userQuota', None)
        if user_quota:
            user_quota = int(user_quota)

        storage_quota = site_xml.get('storageQuota', None)
        if storage_quota:
            storage_quota = int(storage_quota)

        revision_limit = site_xml.get('revisionLimit', None)
        if revision_limit:
            revision_limit = int(revision_limit)

        num_users = None
        storage = None
        usage_elem = site_xml.find('.//t:usage', namespaces=ns)
        if usage_elem is not None:
            num_users = usage_elem.get('numUsers', None)
            storage = usage_elem.get('storage', None)

        data_acceleration_mode = site_xml.get('dataAccelerationMode', '')

        flows_enabled = string_to_bool(site_xml.get('flowsEnabled', ''))
        cataloging_enabled = string_to_bool(site_xml.get('catalogingEnabled', ''))

        return id, name, content_url, status_reason, admin_mode, state, subscribe_others_enabled,\
            disable_subscriptions, revision_history_enabled, user_quota, storage_quota,\
            revision_limit, num_users, storage, data_acceleration_mode, flows_enabled, cataloging_enabled


# Used to convert string represented boolean to a boolean type
def string_to_bool(s):
    return s.lower() == 'true'
