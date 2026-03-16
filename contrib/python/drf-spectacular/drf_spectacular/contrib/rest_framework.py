from drf_spectacular.extensions import OpenApiViewExtension


class ObtainAuthTokenView(OpenApiViewExtension):
    target_class = 'rest_framework.authtoken.views.ObtainAuthToken'
    match_subclasses = True

    def view_replacement(self):
        """
        Prior to DRF 3.12.0, usage of ObtainAuthToken resulted in AssertionError

            Incompatible AutoSchema used on View "ObtainAuthToken". Is DRF's DEFAULT_SCHEMA_CLASS ...

        This is because DRF had a bug which made it NOT honor DEFAULT_SCHEMA_CLASS and instead
        injected an unsolicited coreschema class for this view and this view only. This extension
        fixes the view before the wrong schema class is used.

        Bug in DRF that was fixed in later versions:
        https://github.com/encode/django-rest-framework/blob/4121b01b912668c049b26194a9a107c27a332429/rest_framework/authtoken/views.py#L16
        """
        from rest_framework import VERSION

        from drf_spectacular.openapi import AutoSchema

        # no intervention needed
        if VERSION >= '3.12':
            return self.target

        class FixedObtainAuthToken(self.target):
            schema = AutoSchema()

        return FixedObtainAuthToken
