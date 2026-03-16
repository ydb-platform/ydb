import logging
import uuid

from django.conf import settings
from django.http import HttpResponseForbidden
from django.utils import timezone

from localshop.apps.accounts.models import AccessKey
from localshop.apps.permissions.utils import get_basic_auth_data
from localshop.http import HttpResponseUnauthorized

logger = logging.getLogger(__name__)

SAFE_METHODS = ('GET', 'HEAD', 'OPTIONS')


class RepositoryAccessMixin(object):

    def dispatch(self, request, *args, **kwargs):
        request.credentials = None
        if request.user.is_authenticated:
            return super().dispatch(request, *args, **kwargs)

        ip_addr = self._get_client_ip_address(request)

        logger.info("Package request from %s", ip_addr)
        access_key, secret_key = get_basic_auth_data(request)

        if not (access_key and secret_key) and request.method == 'POST':
            # post means register or upload,
            # distutils for register do not sent the auth by default
            # so force it to send HTTP_AUTHORIZATION header
            return HttpResponseUnauthorized()

        if access_key and secret_key:
            is_authenticated = self._validate_credentials(
                request, access_key, secret_key)

            if not is_authenticated:
                return HttpResponseUnauthorized()

        if self._allow_request(request, ip_addr):
            return super().dispatch(request, *args, **kwargs)
        else:
            logger.info(
                "Denied upload to %s from %s with access key %s",
                request.path, ip_addr, access_key)

        return HttpResponseUnauthorized("No permission")

    def _allow_request(self, request, ip_addr):
        # If the user is already logged in then continue
        if request.user.is_authenticated:
            return True

        # If this view doesn't require upload permissions then we allow access
        # purely based on the cidr. Otherwise credentials are required
        if self.repository.cidr_list.has_access(ip_addr, with_credentials=False):
            return True

        elif self.repository.cidr_list.has_access(ip_addr, with_credentials=True):
            return bool(request.credentials)

        return False

    def _validate_credentials(self, request, access_key, secret_key):
        try:
            access_key = uuid.UUID(access_key)
            secret_key = uuid.UUID(secret_key)
        except ValueError:
            return False

        key = (
            AccessKey.objects
            .filter(
                access_key=access_key,
                secret_key=secret_key)
            .select_related('user')
            .first())

        if key and key.user.is_active:
            if request.method not in SAFE_METHODS:
                key.last_usage = timezone.now()
                key.save(update_fields=['last_usage'])

            if self.repository.user_has_access(key.user):
                request.credentials = key
                request.user = key.user
                return True

        # Check for repository based credentials
        credential = self.repository.credentials.authenticate(
            access_key, secret_key)
        if credential:
            request.credentials = credential
            return True

        return False

    def _get_client_ip_address(self, request):
        # TODO: Should be handled in middleware
        if settings.LOCALSHOP_USE_PROXIED_IP:
            try:
                ip_addr = request.META['HTTP_X_FORWARDED_FOR']
            except KeyError:
                return HttpResponseForbidden('No permission')
            else:
                # HTTP_X_FORWARDED_FOR can be a comma-separated list of IPs.
                # The client's IP will be the first one.
                return ip_addr.split(",")[0].strip()
        return request.META['REMOTE_ADDR']
