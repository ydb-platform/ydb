from allauth.account import app_settings as allauth_account_settings
from allauth.account.adapter import get_adapter
from allauth.account.utils import complete_signup
from allauth.account.views import ConfirmEmailView
from allauth.account.models import EmailAddress
from allauth.socialaccount import signals
from allauth.socialaccount.adapter import get_adapter as get_social_adapter
from allauth.socialaccount.models import SocialAccount
from django.utils.decorators import method_decorator
from django.utils.translation import gettext_lazy as _
from django.views.decorators.debug import sensitive_post_parameters
from rest_framework import status
from rest_framework.exceptions import MethodNotAllowed, NotFound
from rest_framework.generics import CreateAPIView, GenericAPIView, ListAPIView
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from dj_rest_auth.app_settings import api_settings
from dj_rest_auth.models import TokenModel
from dj_rest_auth.registration.serializers import (
    SocialAccountSerializer, SocialConnectSerializer, SocialLoginSerializer,
    VerifyEmailSerializer, ResendEmailVerificationSerializer
)
from dj_rest_auth.utils import jwt_encode
from dj_rest_auth.views import LoginView


sensitive_post_parameters_m = method_decorator(
    sensitive_post_parameters('password1', 'password2'),
)


class RegisterView(CreateAPIView):
    """
    Registers a new user.

    Accepts the following POST parameters: username, email, password1, password2.
    """
    serializer_class = api_settings.REGISTER_SERIALIZER
    permission_classes = api_settings.REGISTER_PERMISSION_CLASSES
    token_model = TokenModel
    throttle_scope = 'dj_rest_auth'

    @sensitive_post_parameters_m
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)

    def get_response_data(self, user):
        if allauth_account_settings.EMAIL_VERIFICATION == \
                allauth_account_settings.EmailVerificationMethod.MANDATORY:
            return {'detail': _('Verification e-mail sent.')}

        if api_settings.USE_JWT:
            data = {
                'user': user,
                'access': self.access_token,
                'refresh': self.refresh_token,
            }
            return api_settings.JWT_SERIALIZER(data, context=self.get_serializer_context()).data
        elif self.token_model:
            return api_settings.TOKEN_SERIALIZER(user.auth_token, context=self.get_serializer_context()).data
        return None

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        user = self.perform_create(serializer)
        headers = self.get_success_headers(serializer.data)
        data = self.get_response_data(user)

        if data:
            response = Response(
                data,
                status=status.HTTP_201_CREATED,
                headers=headers,
            )
        else:
            response = Response(status=status.HTTP_204_NO_CONTENT, headers=headers)

        return response

    def perform_create(self, serializer):
        user = serializer.save(self.request)
        if allauth_account_settings.EMAIL_VERIFICATION != \
                allauth_account_settings.EmailVerificationMethod.MANDATORY:
            if api_settings.USE_JWT:
                self.access_token, self.refresh_token = jwt_encode(user)
            elif self.token_model:
                api_settings.TOKEN_CREATOR(self.token_model, user, serializer)

        complete_signup(
            self.request._request, user,
            allauth_account_settings.EMAIL_VERIFICATION,
            None,
        )
        return user


class VerifyEmailView(APIView, ConfirmEmailView):
    """
    Verifies the email associated with the provided key.

    Accepts the following POST parameter: key.
    """
    permission_classes = (AllowAny,)
    allowed_methods = ('POST', 'OPTIONS', 'HEAD')

    def get_serializer(self, *args, **kwargs):
        return VerifyEmailSerializer(*args, **kwargs)

    def get(self, *args, **kwargs):
        raise MethodNotAllowed('GET')

    def post(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        self.kwargs['key'] = serializer.validated_data['key']
        confirmation = self.get_object()
        confirmation.confirm(self.request)
        return Response({'detail': _('ok')}, status=status.HTTP_200_OK)


class ResendEmailVerificationView(CreateAPIView):
    """
    Resends another email to an unverified email.

    Accepts the following POST parameter: email.
    """
    permission_classes = (AllowAny,)
    serializer_class = ResendEmailVerificationSerializer
    queryset = EmailAddress.objects.all()

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        email = self.get_queryset().filter(**serializer.validated_data).first()
        if email and not email.verified:
            email.send_confirmation(request)

        return Response({'detail': _('ok')}, status=status.HTTP_200_OK)


class SocialLoginView(LoginView):
    """
    class used for social authentications
    example usage for facebook with access_token
    -------------
    from allauth.socialaccount.providers.facebook.views import FacebookOAuth2Adapter

    class FacebookLogin(SocialLoginView):
        adapter_class = FacebookOAuth2Adapter
    -------------

    example usage for facebook with code

    -------------
    from allauth.socialaccount.providers.facebook.views import FacebookOAuth2Adapter
    from allauth.socialaccount.providers.oauth2.client import OAuth2Client

    class FacebookLogin(SocialLoginView):
        adapter_class = FacebookOAuth2Adapter
        client_class = OAuth2Client
        callback_url = 'localhost:8000'
    -------------
    """
    serializer_class = SocialLoginSerializer

    def process_login(self):
        get_adapter(self.request).login(self.request, self.user)


class SocialConnectView(LoginView):
    """
    class used for social account linking

    example usage for facebook with access_token
    -------------
    from allauth.socialaccount.providers.facebook.views import FacebookOAuth2Adapter

    class FacebookConnect(SocialConnectView):
        adapter_class = FacebookOAuth2Adapter
    -------------
    """
    serializer_class = SocialConnectSerializer
    permission_classes = (IsAuthenticated,)

    def process_login(self):
        get_adapter(self.request).login(self.request, self.user)


class SocialAccountListView(ListAPIView):
    """
    List SocialAccounts for the currently logged in user
    """
    serializer_class = SocialAccountSerializer
    permission_classes = (IsAuthenticated,)

    def get_queryset(self):
        return SocialAccount.objects.filter(user=self.request.user)


class SocialAccountDisconnectView(GenericAPIView):
    """
    Disconnect SocialAccount from remote service for
    the currently logged in user
    """
    serializer_class = SocialConnectSerializer
    permission_classes = (IsAuthenticated,)

    def get_queryset(self):
        return SocialAccount.objects.filter(user=self.request.user)

    def post(self, request, *args, **kwargs):
        accounts = self.get_queryset()
        account = accounts.filter(pk=kwargs['pk']).first()
        if not account:
            raise NotFound

        get_social_adapter(self.request).validate_disconnect(account, accounts)

        account.delete()
        signals.social_account_removed.send(
            sender=SocialAccount,
            request=self.request,
            socialaccount=account,
        )

        return Response(self.get_serializer(account).data)
