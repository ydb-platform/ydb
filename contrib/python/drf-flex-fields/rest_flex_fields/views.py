"""
    This class helps provide control over which fields can be expanded when a
    collection is request via the list method.
"""

from rest_framework import viewsets


class FlexFieldsMixin(object):
    permit_list_expands = []

    def get_serializer_context(self):
        default_context = super(FlexFieldsMixin, self).get_serializer_context()

        if hasattr(self, "action") and self.action == "list":
            default_context["permitted_expands"] = self.permit_list_expands

        return default_context


class FlexFieldsModelViewSet(FlexFieldsMixin, viewsets.ModelViewSet):
    pass

