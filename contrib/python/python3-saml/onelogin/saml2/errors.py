# -*- coding: utf-8 -*-

""" OneLogin_Saml2_Error class


Error class of SAML Python Toolkit.

Defines common Error codes and has a custom initializator.

"""


class OneLogin_Saml2_Error(Exception):
    """

    This class implements a custom Exception handler.
    Defines custom error codes.

    """

    # Errors
    SETTINGS_FILE_NOT_FOUND = 0
    SETTINGS_INVALID_SYNTAX = 1
    SETTINGS_INVALID = 2
    METADATA_SP_INVALID = 3
    # SP_CERTS_NOT_FOUND is deprecated, use CERT_NOT_FOUND instead
    SP_CERTS_NOT_FOUND = 4
    CERT_NOT_FOUND = 4
    REDIRECT_INVALID_URL = 5
    PUBLIC_CERT_FILE_NOT_FOUND = 6
    PRIVATE_KEY_FILE_NOT_FOUND = 7
    SAML_RESPONSE_NOT_FOUND = 8
    SAML_LOGOUTMESSAGE_NOT_FOUND = 9
    SAML_LOGOUTREQUEST_INVALID = 10
    SAML_LOGOUTRESPONSE_INVALID = 11
    SAML_SINGLE_LOGOUT_NOT_SUPPORTED = 12
    PRIVATE_KEY_NOT_FOUND = 13
    UNSUPPORTED_SETTINGS_OBJECT = 14

    def __init__(self, message, code=0, errors=None):
        """
        Initializes the Exception instance.

        Arguments are:
            * (str)   message.   Describes the error.
            * (int)   code.      The code error (defined in the error class).
        """
        assert isinstance(code, int)

        if errors is not None:
            message = message % errors

        Exception.__init__(self, message)
        self.code = code


class OneLogin_Saml2_ValidationError(Exception):
    """
    This class implements another custom Exception handler, related
    to exceptions that happens during validation process.
    Defines custom error codes .
    """

    # Validation Errors
    UNSUPPORTED_SAML_VERSION = 0
    MISSING_ID = 1
    WRONG_NUMBER_OF_ASSERTIONS = 2
    MISSING_STATUS = 3
    MISSING_STATUS_CODE = 4
    STATUS_CODE_IS_NOT_SUCCESS = 5
    WRONG_SIGNED_ELEMENT = 6
    ID_NOT_FOUND_IN_SIGNED_ELEMENT = 7
    DUPLICATED_ID_IN_SIGNED_ELEMENTS = 8
    INVALID_SIGNED_ELEMENT = 9
    DUPLICATED_REFERENCE_IN_SIGNED_ELEMENTS = 10
    UNEXPECTED_SIGNED_ELEMENTS = 11
    WRONG_NUMBER_OF_SIGNATURES_IN_RESPONSE = 12
    WRONG_NUMBER_OF_SIGNATURES_IN_ASSERTION = 13
    INVALID_XML_FORMAT = 14
    WRONG_INRESPONSETO = 15
    NO_ENCRYPTED_ASSERTION = 16
    NO_ENCRYPTED_NAMEID = 17
    MISSING_CONDITIONS = 18
    ASSERTION_TOO_EARLY = 19
    ASSERTION_EXPIRED = 20
    WRONG_NUMBER_OF_AUTHSTATEMENTS = 21
    NO_ATTRIBUTESTATEMENT = 22
    ENCRYPTED_ATTRIBUTES = 23
    WRONG_DESTINATION = 24
    EMPTY_DESTINATION = 25
    WRONG_AUDIENCE = 26
    ISSUER_MULTIPLE_IN_RESPONSE = 27
    ISSUER_NOT_FOUND_IN_ASSERTION = 28
    WRONG_ISSUER = 29
    SESSION_EXPIRED = 30
    WRONG_SUBJECTCONFIRMATION = 31
    NO_SIGNED_MESSAGE = 32
    NO_SIGNED_ASSERTION = 33
    NO_SIGNATURE_FOUND = 34
    KEYINFO_NOT_FOUND_IN_ENCRYPTED_DATA = 35
    CHILDREN_NODE_NOT_FOUND_IN_KEYINFO = 36
    UNSUPPORTED_RETRIEVAL_METHOD = 37
    NO_NAMEID = 38
    EMPTY_NAMEID = 39
    SP_NAME_QUALIFIER_NAME_MISMATCH = 40
    DUPLICATED_ATTRIBUTE_NAME_FOUND = 41
    INVALID_SIGNATURE = 42
    WRONG_NUMBER_OF_SIGNATURES = 43
    RESPONSE_EXPIRED = 44
    AUTHN_CONTEXT_MISMATCH = 45
    DEPRECATED_SIGNATURE_METHOD = 46
    DEPRECATED_DIGEST_METHOD = 47

    def __init__(self, message, code=0, errors=None):
        """
        Initializes the Exception instance.
        Arguments are:
            * (str)   message.   Describes the error.
            * (int)   code.      The code error (defined in the error class).
        """
        assert isinstance(code, int)

        if errors is not None:
            message = message % errors

        Exception.__init__(self, message)
        self.code = code
