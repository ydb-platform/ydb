from defusedxml.ElementTree import fromstring

from tableauserverclient.datetime_helpers import parse_datetime


class DQWItem:
    class WarningType:
        WARNING = "WARNING"
        DEPRECATED = "DEPRECATED"
        STALE = "STALE"
        SENSITIVE_DATA = "SENSITIVE_DATA"
        MAINTENANCE = "MAINTENANCE"

    def __init__(self, warning_type="WARNING", message=None, active=True, severe=False):
        self._id = None
        # content related
        self._content_id = None
        self._content_type = None

        # DQW related
        self.warning_type = warning_type
        self.message = message
        self.active = active
        self.severe = severe
        self._created_at = None
        self._updated_at = None

        # owner
        self._owner_display_name = None
        self._owner_id = None

    @property
    def id(self):
        return self._id

    @property
    def content_id(self):
        return self._content_id

    @property
    def content_type(self):
        return self._content_type

    @property
    def owner_display_name(self):
        return self._owner_display_name

    @property
    def owner_id(self):
        return self._owner_id

    @property
    def warning_type(self):
        return self._warning_type

    @warning_type.setter
    def warning_type(self, value):
        self._warning_type = value

    @property
    def message(self):
        return self._message

    @message.setter
    def message(self, value):
        self._message = value

    @property
    def active(self):
        return self._active

    @active.setter
    def active(self, value):
        self._active = value

    @property
    def severe(self):
        return self._severe

    @severe.setter
    def severe(self, value):
        self._severe = value

    @property
    def created_at(self):
        return self._created_at

    @created_at.setter
    def created_at(self, value):
        self._created_at = value

    @property
    def updated_at(self):
        return self._updated_at

    @updated_at.setter
    def updated_at(self, value):
        self._updated_at = value

    @classmethod
    def from_response(cls, resp, ns):
        return cls.from_xml_element(fromstring(resp), ns)

    @classmethod
    def from_xml_element(cls, parsed_response, ns):
        all_dqws = []
        dqw_elem_list = parsed_response.findall(".//t:dataQualityWarning", namespaces=ns)
        for dqw_elem in dqw_elem_list:
            dqw = DQWItem()
            dqw._id = dqw_elem.get("id", None)
            dqw._owner_display_name = dqw_elem.get("userDisplayName", None)
            dqw._content_id = dqw_elem.get("contentId", None)
            dqw._content_type = dqw_elem.get("contentType", None)
            dqw.message = dqw_elem.get("message", None)
            dqw.warning_type = dqw_elem.get("type", None)

            is_active = dqw_elem.get("isActive", None)
            if is_active is not None:
                dqw._active = string_to_bool(is_active)

            is_severe = dqw_elem.get("isSevere", None)
            if is_severe is not None:
                dqw._severe = string_to_bool(is_severe)

            dqw._created_at = parse_datetime(dqw_elem.get("createdAt", None))
            dqw._updated_at = parse_datetime(dqw_elem.get("updatedAt", None))

            owner_id = None
            owner_tag = dqw_elem.find(".//t:owner", namespaces=ns)
            if owner_tag is not None:
                owner_id = owner_tag.get("id", None)
            dqw._owner_id = owner_id

            all_dqws.append(dqw)

        return all_dqws


# Used to convert string represented boolean to a boolean type
def string_to_bool(s):
    return s.lower() == "true"
