from office365.runtime.client_value import ClientValue


class ObjectIdentity(ClientValue):
    """
    Represents an identity used to sign in to a user account.
    """

    def __init__(self, sign_in_type=None, issuer=None, issuer_assigned_id=None):
        """

        :param str sign_in_type: Specifies the user sign-in types in your directory, such as emailAddress, userName
            or federated. Here, federated represents a unique identifier for a user from an issuer, that can be in
            any format chosen by the issuer. Additional validation is enforced on issuerAssignedId when the sign-in
            type is set to emailAddress or userName. This property can also be set to any custom string.
        :param str issuer: Specifies the issuer of the identity, for example facebook.com.
            For local accounts (where signInType is not federated), this property is the local B2C tenant default
            domain name, for example contoso.onmicrosoft.com.
            For external users from other Azure AD organization, this will be the domain of the federated organization,
            for example contoso.com.
        :param str issuer_assigned_id: Specifies the unique identifier assigned to the user by the issuer.
            The combination of issuer and issuerAssignedId must be unique within the organization. Represents
            the sign-in name for the user, when signInType is set to emailAddress or userName
            (also known as local accounts).
        """
        super(ObjectIdentity, self).__init__()
        self.signInType = sign_in_type
        self.issuer = issuer
        self.issuerAssignedId = issuer_assigned_id
