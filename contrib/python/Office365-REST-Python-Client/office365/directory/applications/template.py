from typing import Optional

from office365.entity import Entity
from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.runtime.types.collections import StringCollection


class ApplicationTemplate(Entity):
    """Represents an application in the Azure AD application gallery."""

    def instantiate(self, display_name):
        """
        Add an instance of an application from the Azure AD application gallery into your directory. You can also use
        this API to instantiate non-gallery apps.
        Use the following ID for the applicationTemplate object: 8adf8e6e-67b2-4cf2-a259-e3dc5476c621.

        :param str display_name: Custom name of the application
        """
        return_type = ClientResult(self.context)
        payload = {"displayName": display_name}
        qry = ServiceOperationQuery(
            self, "instantiate", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @property
    def display_name(self):
        # type: () -> Optional[str]
        """The name of the application."""
        return self.properties.get("displayName", None)

    @property
    def categories(self):
        """
        The list of categories for the application. Supported values can be: Collaboration, Business Management,
        Consumer, Content management, CRM, Data services, Developer services, E-commerce, Education, ERP, Finance,
        Health, Human resources, IT infrastructure, Mail, Management, Marketing, Media, Productivity,
        Project management, Telecommunications, Tools, Travel, and Web design & hosting.
        """
        return self.properties.get("categories", StringCollection())

    @property
    def supported_provisioning_types(self):
        """The list of provisioning modes supported by this application"""
        return self.properties.get("supportedProvisioningTypes", StringCollection())

    @property
    def supported_single_signon_modes(self):
        """
        The list of single sign-on modes supported by this application.
        The supported values are oidc, password, saml, and notSupported.
        """
        return self.properties.get("supportedSingleSignOnModes", StringCollection())
