class WebAuthnException(Exception):
    pass


class InvalidRegistrationOptions(WebAuthnException):
    pass


class InvalidRegistrationResponse(WebAuthnException):
    pass


class InvalidAuthenticationOptions(WebAuthnException):
    pass


class InvalidAuthenticationResponse(WebAuthnException):
    pass


class InvalidPublicKeyStructure(WebAuthnException):
    pass


class UnsupportedPublicKeyType(WebAuthnException):
    pass


class InvalidJSONStructure(WebAuthnException):
    pass


class InvalidAuthenticatorDataStructure(WebAuthnException):
    pass


class SignatureVerificationException(WebAuthnException):
    pass


class UnsupportedAlgorithm(WebAuthnException):
    pass


class UnsupportedPublicKey(WebAuthnException):
    pass


class UnsupportedEC2Curve(WebAuthnException):
    pass


class InvalidTPMPubAreaStructure(WebAuthnException):
    pass


class InvalidTPMCertInfoStructure(WebAuthnException):
    pass


class InvalidCertificateChain(WebAuthnException):
    pass


class InvalidBackupFlags(WebAuthnException):
    pass


class InvalidCBORData(WebAuthnException):
    pass
