from office365.entity import Entity


class ApplicationServicePrincipal(Entity):
    """
    When an instance of an application from the Azure AD application gallery is added, application and servicePrincipal
     objects are created in the directory. The applicationServicePrincipal represents the concatenation of the
     application and servicePrincipal object.
    """
