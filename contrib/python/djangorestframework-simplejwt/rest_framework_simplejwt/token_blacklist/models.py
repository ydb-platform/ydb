from django.conf import settings
from django.db import models
from django.utils.translation import gettext_lazy as _


class OutstandingToken(models.Model):
    id = models.BigAutoField(primary_key=True, serialize=False)
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True
    )

    jti = models.CharField(unique=True, max_length=255)
    token = models.TextField()

    created_at = models.DateTimeField(null=True, blank=True)
    expires_at = models.DateTimeField()

    class Meta:
        verbose_name = _("Outstanding Token")
        verbose_name_plural = _("Outstanding Tokens")
        # Work around for a bug in Django:
        # https://code.djangoproject.com/ticket/19422
        #
        # Also see corresponding ticket:
        # https://github.com/encode/django-rest-framework/issues/705
        abstract = (
            "rest_framework_simplejwt.token_blacklist" not in settings.INSTALLED_APPS
        )
        ordering = ("user",)

    def __str__(self) -> str:
        return _("Token for %(user)s (%(jti)s)") % {
            "user": self.user,
            "jti": self.jti,
        }


class BlacklistedToken(models.Model):
    id = models.BigAutoField(primary_key=True, serialize=False)
    token = models.OneToOneField(OutstandingToken, on_delete=models.CASCADE)

    blacklisted_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = _("Blacklisted Token")
        verbose_name_plural = _("Blacklisted Tokens")
        # Work around for a bug in Django:
        # https://code.djangoproject.com/ticket/19422
        #
        # Also see corresponding ticket:
        # https://github.com/encode/django-rest-framework/issues/705
        abstract = (
            "rest_framework_simplejwt.token_blacklist" not in settings.INSTALLED_APPS
        )

    def __str__(self) -> str:
        return _("Blacklisted token for %(user)s") % {"user": self.token.user}
