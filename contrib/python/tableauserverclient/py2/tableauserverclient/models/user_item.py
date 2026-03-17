import xml.etree.ElementTree as ET
from .exceptions import UnpopulatedPropertyError
from .property_decorators import property_is_enum, property_not_empty, property_not_nullable
from ..datetime_helpers import parse_datetime
from .reference_item import ResourceReference


class UserItem(object):

    tag_name = 'user'

    class Roles:
        Interactor = 'Interactor'
        Publisher = 'Publisher'
        ServerAdministrator = 'ServerAdministrator'
        SiteAdministrator = 'SiteAdministrator'
        Unlicensed = 'Unlicensed'
        UnlicensedWithPublish = 'UnlicensedWithPublish'
        Viewer = 'Viewer'
        ViewerWithPublish = 'ViewerWithPublish'
        Guest = 'Guest'

        Creator = 'Creator'
        Explorer = 'Explorer'
        ExplorerCanPublish = 'ExplorerCanPublish'
        ReadOnly = 'ReadOnly'
        SiteAdministratorCreator = 'SiteAdministratorCreator'
        SiteAdministratorExplorer = 'SiteAdministratorExplorer'

        # Online only
        SupportUser = 'SupportUser'

    class Auth:
        OpenID = 'OpenID'
        SAML = 'SAML'
        ServerDefault = 'ServerDefault'

    def __init__(self, name=None, site_role=None, auth_setting=None):
        self._auth_setting = None
        self._domain_name = None
        self._external_auth_user_id = None
        self._id = None
        self._last_login = None
        self._workbooks = None
        self._favorites = None
        self.email = None
        self.fullname = None
        self.name = name
        self.site_role = site_role
        self.auth_setting = auth_setting

    @property
    def auth_setting(self):
        return self._auth_setting

    @auth_setting.setter
    @property_is_enum(Auth)
    def auth_setting(self, value):
        self._auth_setting = value

    @property
    def domain_name(self):
        return self._domain_name

    @property
    def external_auth_user_id(self):
        return self._external_auth_user_id

    @property
    def id(self):
        return self._id

    @property
    def last_login(self):
        return self._last_login

    @property
    def name(self):
        return self._name

    @name.setter
    @property_not_empty
    def name(self, value):
        self._name = value

    @property
    def site_role(self):
        return self._site_role

    @site_role.setter
    @property_not_nullable
    @property_is_enum(Roles)
    def site_role(self, value):
        self._site_role = value

    @property
    def workbooks(self):
        if self._workbooks is None:
            error = "User item must be populated with workbooks first."
            raise UnpopulatedPropertyError(error)
        return self._workbooks()

    @property
    def favorites(self):
        if self._favorites is None:
            error = "User item must be populated with favorites first."
            raise UnpopulatedPropertyError(error)
        return self._favorites

    def to_reference(self):
        return ResourceReference(id_=self.id, tag_name=self.tag_name)

    def _set_workbooks(self, workbooks):
        self._workbooks = workbooks

    def _parse_common_tags(self, user_xml, ns):
        if not isinstance(user_xml, ET.Element):
            user_xml = ET.fromstring(user_xml).find('.//t:user', namespaces=ns)
        if user_xml is not None:
            (_, _, site_role, _, _, fullname, email, auth_setting, _) = self._parse_element(user_xml, ns)
            self._set_values(None, None, site_role, None, None, fullname, email, auth_setting, None)
        return self

    def _set_values(self, id, name, site_role, last_login,
                    external_auth_user_id, fullname, email, auth_setting, domain_name):
        if id is not None:
            self._id = id
        if name:
            self._name = name
        if site_role:
            self._site_role = site_role
        if last_login:
            self._last_login = last_login
        if external_auth_user_id:
            self._external_auth_user_id = external_auth_user_id
        if fullname:
            self.fullname = fullname
        if email:
            self.email = email
        if auth_setting:
            self._auth_setting = auth_setting
        if domain_name:
            self._domain_name = domain_name

    @classmethod
    def from_response(cls, resp, ns):
        all_user_items = []
        parsed_response = ET.fromstring(resp)
        all_user_xml = parsed_response.findall('.//t:user', namespaces=ns)
        for user_xml in all_user_xml:
            (id, name, site_role, last_login, external_auth_user_id,
             fullname, email, auth_setting, domain_name) = cls._parse_element(user_xml, ns)
            user_item = cls(name, site_role)
            user_item._set_values(id, name, site_role, last_login, external_auth_user_id,
                                  fullname, email, auth_setting, domain_name)
            all_user_items.append(user_item)
        return all_user_items

    @staticmethod
    def as_reference(id_):
        return ResourceReference(id_, UserItem.tag_name)

    @staticmethod
    def _parse_element(user_xml, ns):
        id = user_xml.get('id', None)
        name = user_xml.get('name', None)
        site_role = user_xml.get('siteRole', None)
        last_login = parse_datetime(user_xml.get('lastLogin', None))
        external_auth_user_id = user_xml.get('externalAuthUserId', None)
        fullname = user_xml.get('fullName', None)
        email = user_xml.get('email', None)
        auth_setting = user_xml.get('authSetting', None)

        domain_name = None
        domain_elem = user_xml.find('.//t:domain', namespaces=ns)
        if domain_elem is not None:
            domain_name = domain_elem.get('name', None)

        return id, name, site_role, last_login, external_auth_user_id, fullname, email, auth_setting, domain_name

    def __repr__(self):
        return "<User {} name={} role={}>".format(self.id, self.name, self.site_role)
