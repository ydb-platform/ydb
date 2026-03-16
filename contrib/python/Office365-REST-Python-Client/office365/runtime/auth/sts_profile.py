from datetime import datetime, timedelta

from office365.azure_env import AzureEnvironment
from office365.runtime.compat import timezone, urlparse


class STSProfile(object):
    def __init__(self, site_url, environment=None):
        # type: (str, str) -> None
        self._site_url = site_url
        self._environment = environment
        self._created = datetime.now(tz=timezone.utc)
        self._expires = self._created + timedelta(minutes=30)

    def reset(self):
        """Renew the expiration time."""
        self._created = datetime.now(tz=timezone.utc)
        self._expires = self._created + timedelta(minutes=30)

    @property
    def tenant(self):
        return urlparse(self._site_url).netloc

    @property
    def security_token_service_url(self):
        return "{0}/extSTS.srf".format(
            AzureEnvironment.get_login_authority(self._environment),
        )

    @property
    def signin_page_url(self):
        site_info = urlparse(self._site_url)
        return "{0}://{1}/_forms/default.aspx?wa=wsignin1.0".format(
            site_info.scheme, site_info.netloc
        )

    @property
    def user_realm_service_url(self):
        return "{0}/GetUserRealm.srf".format(
            AzureEnvironment.get_login_authority(self._environment),
        )

    @property
    def token_issuer(self):
        return "urn:federation:MicrosoftOnline"

    @property
    def created(self):
        return self._created

    @property
    def expires(self):
        return self._expires

    @property
    def site_url(self):
        return self._site_url
