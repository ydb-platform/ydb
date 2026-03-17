# -*- coding: utf-8 -*-

""" OneLogin_Saml2_Response class


SAML Response class of SAML Python Toolkit.

"""

from copy import deepcopy
from onelogin.saml2.constants import OneLogin_Saml2_Constants
from onelogin.saml2.utils import OneLogin_Saml2_Utils, OneLogin_Saml2_Error, OneLogin_Saml2_ValidationError, return_false_on_exception
from onelogin.saml2.xml_utils import OneLogin_Saml2_XML


class OneLogin_Saml2_Response(object):
    """

    This class handles a SAML Response. It parses or validates
    a Logout Response object.

    """

    def __init__(self, settings, response):
        """
        Constructs the response object.

        :param settings: The setting info
        :type settings: OneLogin_Saml2_Setting object

        :param response: The base64 encoded, XML string containing the samlp:Response
        :type response: string
        """
        self._settings = settings
        self._error = None
        self.response = OneLogin_Saml2_Utils.b64decode(response)
        self.document = OneLogin_Saml2_XML.to_etree(self.response)
        self.decrypted_document = None
        self.encrypted = None
        self.valid_scd_not_on_or_after = None

        # Quick check for the presence of EncryptedAssertion
        encrypted_assertion_nodes = self._query('/samlp:Response/saml:EncryptedAssertion')
        if encrypted_assertion_nodes:
            decrypted_document = deepcopy(self.document)
            self.encrypted = True
            self.decrypted_document = self._decrypt_assertion(decrypted_document)

    def is_valid(self, request_data, request_id=None, raise_exceptions=False):
        """
        Validates the response object.

        :param request_data: Request Data
        :type request_data: dict

        :param request_id: Optional argument. The ID of the AuthNRequest sent by this SP to the IdP
        :type request_id: string

        :param raise_exceptions: Whether to return false on failure or raise an exception
        :type raise_exceptions: Boolean

        :returns: True if the SAML Response is valid, False if not
        :rtype: bool
        """
        self._error = None
        try:
            # Checks SAML version
            if self.document.get('Version', None) != '2.0':
                raise OneLogin_Saml2_ValidationError(
                    'Unsupported SAML version',
                    OneLogin_Saml2_ValidationError.UNSUPPORTED_SAML_VERSION
                )

            # Checks that ID exists
            if self.document.get('ID', None) is None:
                raise OneLogin_Saml2_ValidationError(
                    'Missing ID attribute on SAML Response',
                    OneLogin_Saml2_ValidationError.MISSING_ID
                )

            # Checks that the response has the SUCCESS status
            self.check_status()

            # Checks that the response only has one assertion
            if not self.validate_num_assertions():
                raise OneLogin_Saml2_ValidationError(
                    'SAML Response must contain 1 assertion',
                    OneLogin_Saml2_ValidationError.WRONG_NUMBER_OF_ASSERTIONS
                )

            idp_data = self._settings.get_idp_data()
            idp_entity_id = idp_data['entityId']
            sp_data = self._settings.get_sp_data()
            sp_entity_id = sp_data['entityId']

            signed_elements = self.process_signed_elements()

            has_signed_response = '{%s}Response' % OneLogin_Saml2_Constants.NS_SAMLP in signed_elements
            has_signed_assertion = '{%s}Assertion' % OneLogin_Saml2_Constants.NS_SAML in signed_elements

            if self._settings.is_strict():
                no_valid_xml_msg = 'Invalid SAML Response. Not match the saml-schema-protocol-2.0.xsd'
                res = OneLogin_Saml2_XML.validate_xml(self.document, 'saml-schema-protocol-2.0.xsd', self._settings.is_debug_active())
                if isinstance(res, str):
                    raise OneLogin_Saml2_ValidationError(
                        no_valid_xml_msg,
                        OneLogin_Saml2_ValidationError.INVALID_XML_FORMAT
                    )

                # If encrypted, check also the decrypted document
                if self.encrypted:
                    res = OneLogin_Saml2_XML.validate_xml(self.decrypted_document, 'saml-schema-protocol-2.0.xsd', self._settings.is_debug_active())
                    if isinstance(res, str):
                        raise OneLogin_Saml2_ValidationError(
                            no_valid_xml_msg,
                            OneLogin_Saml2_ValidationError.INVALID_XML_FORMAT
                        )

                security = self._settings.get_security_data()
                current_url = OneLogin_Saml2_Utils.get_self_url_no_query(request_data)

                # Check if the InResponseTo of the Response matchs the ID of the AuthNRequest (requestId) if provided
                in_response_to = self.get_in_response_to()
                if in_response_to is not None and request_id is not None:
                    if in_response_to != request_id:
                        raise OneLogin_Saml2_ValidationError(
                            'The InResponseTo of the Response: %s, does not match the ID of the AuthNRequest sent by the SP: %s' % (in_response_to, request_id),
                            OneLogin_Saml2_ValidationError.WRONG_INRESPONSETO
                        )

                if not self.encrypted and security['wantAssertionsEncrypted']:
                    raise OneLogin_Saml2_ValidationError(
                        'The assertion of the Response is not encrypted and the SP require it',
                        OneLogin_Saml2_ValidationError.NO_ENCRYPTED_ASSERTION
                    )

                if security['wantNameIdEncrypted']:
                    encrypted_nameid_nodes = self._query_assertion('/saml:Subject/saml:EncryptedID/xenc:EncryptedData')
                    if len(encrypted_nameid_nodes) != 1:
                        raise OneLogin_Saml2_ValidationError(
                            'The NameID of the Response is not encrypted and the SP require it',
                            OneLogin_Saml2_ValidationError.NO_ENCRYPTED_NAMEID
                        )

                # Checks that a Conditions element exists
                if not self.check_one_condition():
                    raise OneLogin_Saml2_ValidationError(
                        'The Assertion must include a Conditions element',
                        OneLogin_Saml2_ValidationError.MISSING_CONDITIONS
                    )

                # Validates Assertion timestamps
                self.validate_timestamps(raise_exceptions=True)

                # Checks that an AuthnStatement element exists and is unique
                if not self.check_one_authnstatement():
                    raise OneLogin_Saml2_ValidationError(
                        'The Assertion must include an AuthnStatement element',
                        OneLogin_Saml2_ValidationError.WRONG_NUMBER_OF_AUTHSTATEMENTS
                    )

                # Checks that the response has all of the AuthnContexts that we provided in the request.
                # Only check if failOnAuthnContextMismatch is true and requestedAuthnContext is set to a list.
                requested_authn_contexts = security['requestedAuthnContext']
                if security['failOnAuthnContextMismatch'] and requested_authn_contexts and requested_authn_contexts is not True:
                    authn_contexts = self.get_authn_contexts()
                    unmatched_contexts = set(authn_contexts).difference(requested_authn_contexts)
                    if unmatched_contexts:
                        raise OneLogin_Saml2_ValidationError(
                            'The AuthnContext "%s" was not a requested context "%s"' % (', '.join(unmatched_contexts), ', '.join(requested_authn_contexts)),
                            OneLogin_Saml2_ValidationError.AUTHN_CONTEXT_MISMATCH
                        )

                # Checks that there is at least one AttributeStatement if required
                attribute_statement_nodes = self._query_assertion('/saml:AttributeStatement')
                if security.get('wantAttributeStatement', True) and not attribute_statement_nodes:
                    raise OneLogin_Saml2_ValidationError(
                        'There is no AttributeStatement on the Response',
                        OneLogin_Saml2_ValidationError.NO_ATTRIBUTESTATEMENT
                    )

                encrypted_attributes_nodes = self._query_assertion('/saml:AttributeStatement/saml:EncryptedAttribute')
                if encrypted_attributes_nodes:
                    raise OneLogin_Saml2_ValidationError(
                        'There is an EncryptedAttribute in the Response and this SP not support them',
                        OneLogin_Saml2_ValidationError.ENCRYPTED_ATTRIBUTES
                    )

                # Checks destination
                destination = self.document.get('Destination', None)
                if destination:
                    if not OneLogin_Saml2_Utils.normalize_url(url=destination).startswith(OneLogin_Saml2_Utils.normalize_url(url=current_url)):
                        # TODO: Review if following lines are required, since we can control the
                        # request_data
                        #  current_url_routed = OneLogin_Saml2_Utils.get_self_routed_url_no_query(request_data)
                        #  if not destination.startswith(current_url_routed):
                        raise OneLogin_Saml2_ValidationError(
                            'The response was received at %s instead of %s' % (current_url, destination),
                            OneLogin_Saml2_ValidationError.WRONG_DESTINATION
                        )
                elif destination == '':
                    raise OneLogin_Saml2_ValidationError(
                        'The response has an empty Destination value',
                        OneLogin_Saml2_ValidationError.EMPTY_DESTINATION
                    )
                # Checks audience
                valid_audiences = self.get_audiences()
                if valid_audiences and sp_entity_id not in valid_audiences:
                    raise OneLogin_Saml2_ValidationError(
                        '%s is not a valid audience for this Response' % sp_entity_id,
                        OneLogin_Saml2_ValidationError.WRONG_AUDIENCE
                    )

                # Checks the issuers
                issuers = self.get_issuers()
                for issuer in issuers:
                    if issuer is None or issuer != idp_entity_id:
                        raise OneLogin_Saml2_ValidationError(
                            'Invalid issuer in the Assertion/Response (expected %(idpEntityId)s, got %(issuer)s)' %
                            {
                                'idpEntityId': idp_entity_id,
                                'issuer': issuer
                            },
                            OneLogin_Saml2_ValidationError.WRONG_ISSUER
                        )

                # Checks the session Expiration
                session_expiration = self.get_session_not_on_or_after()
                if session_expiration and session_expiration <= OneLogin_Saml2_Utils.now():
                    raise OneLogin_Saml2_ValidationError(
                        'The attributes have expired, based on the SessionNotOnOrAfter of the AttributeStatement of this Response',
                        OneLogin_Saml2_ValidationError.SESSION_EXPIRED
                    )

                # Checks the SubjectConfirmation, at least one SubjectConfirmation must be valid
                any_subject_confirmation = False
                subject_confirmation_nodes = self._query_assertion('/saml:Subject/saml:SubjectConfirmation')

                for scn in subject_confirmation_nodes:
                    method = scn.get('Method', None)
                    if method and method != OneLogin_Saml2_Constants.CM_BEARER:
                        continue
                    sc_data = scn.find('saml:SubjectConfirmationData', namespaces=OneLogin_Saml2_Constants.NSMAP)
                    if sc_data is None:
                        continue
                    else:
                        irt = sc_data.get('InResponseTo', None)
                        if in_response_to and irt and irt != in_response_to:
                            continue
                        recipient = sc_data.get('Recipient', None)
                        if recipient and current_url not in recipient:
                            continue
                        nooa = sc_data.get('NotOnOrAfter', None)
                        if nooa:
                            parsed_nooa = OneLogin_Saml2_Utils.parse_SAML_to_time(nooa)
                            if parsed_nooa <= OneLogin_Saml2_Utils.now():
                                continue
                        nb = sc_data.get('NotBefore', None)
                        if nb:
                            parsed_nb = OneLogin_Saml2_Utils.parse_SAML_to_time(nb)
                            if parsed_nb > OneLogin_Saml2_Utils.now():
                                continue

                        if nooa:
                            self.valid_scd_not_on_or_after = OneLogin_Saml2_Utils.parse_SAML_to_time(nooa)

                        any_subject_confirmation = True
                        break

                if not any_subject_confirmation:
                    raise OneLogin_Saml2_ValidationError(
                        'A valid SubjectConfirmation was not found on this Response',
                        OneLogin_Saml2_ValidationError.WRONG_SUBJECTCONFIRMATION
                    )

                if security['wantAssertionsSigned'] and not has_signed_assertion:
                    raise OneLogin_Saml2_ValidationError(
                        'The Assertion of the Response is not signed and the SP require it',
                        OneLogin_Saml2_ValidationError.NO_SIGNED_ASSERTION
                    )

                if security['wantMessagesSigned'] and not has_signed_response:
                    raise OneLogin_Saml2_ValidationError(
                        'The Message of the Response is not signed and the SP require it',
                        OneLogin_Saml2_ValidationError.NO_SIGNED_MESSAGE
                    )

            if not signed_elements or (not has_signed_response and not has_signed_assertion):
                raise OneLogin_Saml2_ValidationError(
                    'No Signature found. SAML Response rejected',
                    OneLogin_Saml2_ValidationError.NO_SIGNATURE_FOUND
                )
            else:
                cert = self._settings.get_idp_cert()
                fingerprint = idp_data.get('certFingerprint', None)
                if fingerprint:
                    fingerprint = OneLogin_Saml2_Utils.format_finger_print(fingerprint)
                fingerprintalg = idp_data.get('certFingerprintAlgorithm', None)

                multicerts = None
                if 'x509certMulti' in idp_data and 'signing' in idp_data['x509certMulti'] and idp_data['x509certMulti']['signing']:
                    multicerts = idp_data['x509certMulti']['signing']

                # If find a Signature on the Response, validates it checking the original response
                if has_signed_response and not OneLogin_Saml2_Utils.validate_sign(self.document, cert, fingerprint, fingerprintalg, xpath=OneLogin_Saml2_Utils.RESPONSE_SIGNATURE_XPATH, multicerts=multicerts, raise_exceptions=False):
                    raise OneLogin_Saml2_ValidationError(
                        'Signature validation failed. SAML Response rejected',
                        OneLogin_Saml2_ValidationError.INVALID_SIGNATURE
                    )

                document_check_assertion = self.decrypted_document if self.encrypted else self.document
                if has_signed_assertion and not OneLogin_Saml2_Utils.validate_sign(document_check_assertion, cert, fingerprint, fingerprintalg, xpath=OneLogin_Saml2_Utils.ASSERTION_SIGNATURE_XPATH, multicerts=multicerts, raise_exceptions=False):
                    raise OneLogin_Saml2_ValidationError(
                        'Signature validation failed. SAML Response rejected',
                        OneLogin_Saml2_ValidationError.INVALID_SIGNATURE
                    )

            return True
        except Exception as err:
            self._error = str(err)
            debug = self._settings.is_debug_active()
            if debug:
                print(err)
            if raise_exceptions:
                raise
            return False

    def check_status(self):
        """
        Check if the status of the response is success or not

        :raises: Exception. If the status is not success
        """
        status = OneLogin_Saml2_Utils.get_status(self.document)
        code = status.get('code', None)
        if code and code != OneLogin_Saml2_Constants.STATUS_SUCCESS:
            splited_code = code.split(':')
            printable_code = splited_code.pop()
            status_exception_msg = 'The status code of the Response was not Success, was %s' % printable_code
            status_msg = status.get('msg', None)
            if status_msg:
                status_exception_msg += ' -> ' + status_msg
            raise OneLogin_Saml2_ValidationError(
                status_exception_msg,
                OneLogin_Saml2_ValidationError.STATUS_CODE_IS_NOT_SUCCESS
            )

    def check_one_condition(self):
        """
        Checks that the samlp:Response/saml:Assertion/saml:Conditions element exists and is unique.
        """
        condition_nodes = self._query_assertion('/saml:Conditions')
        if len(condition_nodes) == 1:
            return True
        else:
            return False

    def check_one_authnstatement(self):
        """
        Checks that the samlp:Response/saml:Assertion/saml:AuthnStatement element exists and is unique.
        """
        authnstatement_nodes = self._query_assertion('/saml:AuthnStatement')
        if len(authnstatement_nodes) == 1:
            return True
        else:
            return False

    def get_audiences(self):
        """
        Gets the audiences

        :returns: The valid audiences for the SAML Response
        :rtype: list
        """
        audience_nodes = self._query_assertion('/saml:Conditions/saml:AudienceRestriction/saml:Audience')
        return [OneLogin_Saml2_XML.element_text(node) for node in audience_nodes if OneLogin_Saml2_XML.element_text(node) is not None]

    def get_authn_contexts(self):
        """
        Gets the authentication contexts

        :returns: The authentication classes for the SAML Response
        :rtype: list
        """
        authn_context_nodes = self._query_assertion('/saml:AuthnStatement/saml:AuthnContext/saml:AuthnContextClassRef')
        return [OneLogin_Saml2_XML.element_text(node) for node in authn_context_nodes]

    def get_in_response_to(self):
        """
        Gets the ID of the request which this response is in response to
        :returns: ID of AuthNRequest this Response is in response to or None if it is not present
        :rtype: str
        """
        return self.document.get('InResponseTo')

    def get_issuers(self):
        """
        Gets the issuers (from message and from assertion)

        :returns: The issuers
        :rtype: list
        """
        issuers = set()

        message_issuer_nodes = OneLogin_Saml2_XML.query(self.document, '/samlp:Response/saml:Issuer')
        if len(message_issuer_nodes) > 0:
            if len(message_issuer_nodes) == 1:
                issuer_value = OneLogin_Saml2_XML.element_text(message_issuer_nodes[0])
                if issuer_value:
                    issuers.add(issuer_value)
            else:
                raise OneLogin_Saml2_ValidationError(
                    'Issuer of the Response is multiple.',
                    OneLogin_Saml2_ValidationError.ISSUER_MULTIPLE_IN_RESPONSE
                )

        assertion_issuer_nodes = self._query_assertion('/saml:Issuer')
        if len(assertion_issuer_nodes) == 1:
            issuer_value = OneLogin_Saml2_XML.element_text(assertion_issuer_nodes[0])
            if issuer_value:
                issuers.add(issuer_value)
        else:
            raise OneLogin_Saml2_ValidationError(
                'Issuer of the Assertion not found or multiple.',
                OneLogin_Saml2_ValidationError.ISSUER_NOT_FOUND_IN_ASSERTION
            )

        return list(set(issuers))

    def get_nameid_data(self):
        """
        Gets the NameID Data provided by the SAML Response from the IdP

        :returns: Name ID Data (Value, Format, NameQualifier, SPNameQualifier)
        :rtype: dict
        """
        nameid = None
        nameid_data = {}

        encrypted_id_data_nodes = self._query_assertion('/saml:Subject/saml:EncryptedID/xenc:EncryptedData')
        if encrypted_id_data_nodes:
            encrypted_data = encrypted_id_data_nodes[0]
            key = self._settings.get_sp_key()
            nameid = OneLogin_Saml2_Utils.decrypt_element(encrypted_data, key)
        else:
            nameid_nodes = self._query_assertion('/saml:Subject/saml:NameID')
            if nameid_nodes:
                nameid = nameid_nodes[0]

        is_strict = self._settings.is_strict()
        want_nameid = self._settings.get_security_data().get('wantNameId', True)
        if nameid is None:
            if is_strict and want_nameid:
                raise OneLogin_Saml2_ValidationError(
                    'NameID not found in the assertion of the Response',
                    OneLogin_Saml2_ValidationError.NO_NAMEID
                )
        else:
            if is_strict and want_nameid and not OneLogin_Saml2_XML.element_text(nameid):
                raise OneLogin_Saml2_ValidationError(
                    'An empty NameID value found',
                    OneLogin_Saml2_ValidationError.EMPTY_NAMEID
                )

            nameid_data = {'Value': OneLogin_Saml2_XML.element_text(nameid)}
            for attr in ['Format', 'SPNameQualifier', 'NameQualifier']:
                value = nameid.get(attr, None)
                if value:
                    if is_strict and attr == 'SPNameQualifier':
                        sp_data = self._settings.get_sp_data()
                        sp_entity_id = sp_data.get('entityId', '')
                        if sp_entity_id != value:
                            raise OneLogin_Saml2_ValidationError(
                                'The SPNameQualifier value mistmatch the SP entityID value.',
                                OneLogin_Saml2_ValidationError.SP_NAME_QUALIFIER_NAME_MISMATCH
                            )

                    nameid_data[attr] = value
        return nameid_data

    def get_nameid(self):
        """
        Gets the NameID provided by the SAML Response from the IdP

        :returns: NameID (value)
        :rtype: string|None
        """
        nameid_value = None
        nameid_data = self.get_nameid_data()
        if nameid_data and 'Value' in nameid_data.keys():
            nameid_value = nameid_data['Value']
        return nameid_value

    def get_nameid_format(self):
        """
        Gets the NameID Format provided by the SAML Response from the IdP

        :returns: NameID Format
        :rtype: string|None
        """
        nameid_format = None
        nameid_data = self.get_nameid_data()
        if nameid_data and 'Format' in nameid_data.keys():
            nameid_format = nameid_data['Format']
        return nameid_format

    def get_nameid_nq(self):
        """
        Gets the NameID NameQualifier provided by the SAML Response from the IdP

        :returns: NameID NameQualifier
        :rtype: string|None
        """
        nameid_nq = None
        nameid_data = self.get_nameid_data()
        if nameid_data and 'NameQualifier' in nameid_data.keys():
            nameid_nq = nameid_data['NameQualifier']
        return nameid_nq

    def get_nameid_spnq(self):
        """
        Gets the NameID SP NameQualifier provided by the SAML response from the IdP.

        :returns: NameID SP NameQualifier
        :rtype: string|None
        """
        nameid_spnq = None
        nameid_data = self.get_nameid_data()
        if nameid_data and 'SPNameQualifier' in nameid_data.keys():
            nameid_spnq = nameid_data['SPNameQualifier']
        return nameid_spnq

    def get_session_not_on_or_after(self):
        """
        Gets the SessionNotOnOrAfter from the AuthnStatement
        Could be used to set the local session expiration

        :returns: The SessionNotOnOrAfter value
        :rtype: time|None
        """
        not_on_or_after = None
        authn_statement_nodes = self._query_assertion('/saml:AuthnStatement[@SessionNotOnOrAfter]')
        if authn_statement_nodes:
            not_on_or_after = OneLogin_Saml2_Utils.parse_SAML_to_time(authn_statement_nodes[0].get('SessionNotOnOrAfter'))
        return not_on_or_after

    def get_assertion_not_on_or_after(self):
        """
        Returns the NotOnOrAfter value of the valid SubjectConfirmationData node if any
        """
        return self.valid_scd_not_on_or_after

    def get_session_index(self):
        """
        Gets the SessionIndex from the AuthnStatement
        Could be used to be stored in the local session in order
        to be used in a future Logout Request that the SP could
        send to the SP, to set what specific session must be deleted

        :returns: The SessionIndex value
        :rtype: string|None
        """
        session_index = None
        authn_statement_nodes = self._query_assertion('/saml:AuthnStatement[@SessionIndex]')
        if authn_statement_nodes:
            session_index = authn_statement_nodes[0].get('SessionIndex')
        return session_index

    def get_attributes(self):
        """
        Gets the Attributes from the AttributeStatement element.
        EncryptedAttributes are not supported
        """
        return self._get_attributes('Name')

    def get_friendlyname_attributes(self):
        """
        Gets the Attributes from the AttributeStatement element indexed by FiendlyName.
        EncryptedAttributes are not supported
        """
        return self._get_attributes('FriendlyName')

    def _get_attributes(self, attr_name):
        allow_duplicates = self._settings.get_security_data().get('allowRepeatAttributeName', False)
        attributes = {}
        attribute_nodes = self._query_assertion('/saml:AttributeStatement/saml:Attribute')
        for attribute_node in attribute_nodes:
            attr_key = attribute_node.get(attr_name)
            if attr_key:
                if not allow_duplicates and attr_key in attributes:
                    raise OneLogin_Saml2_ValidationError(
                        'Found an Attribute element with duplicated ' + attr_name,
                        OneLogin_Saml2_ValidationError.DUPLICATED_ATTRIBUTE_NAME_FOUND
                    )

                values = []
                for attr in attribute_node.iterchildren('{%s}AttributeValue' % OneLogin_Saml2_Constants.NSMAP['saml']):
                    attr_text = OneLogin_Saml2_XML.element_text(attr)
                    if attr_text:
                        attr_text = attr_text.strip()
                        if attr_text:
                            values.append(attr_text)

                    # Parse any nested NameID children
                    for nameid in attr.iterchildren('{%s}NameID' % OneLogin_Saml2_Constants.NSMAP['saml']):
                        values.append({
                            'NameID': {
                                'Format': nameid.get('Format'),
                                'NameQualifier': nameid.get('NameQualifier'),
                                'value': nameid.text
                            }
                        })
                if attr_key in attributes:
                    attributes[attr_key].extend(values)
                else:
                    attributes[attr_key] = values
        return attributes

    def validate_num_assertions(self):
        """
        Verifies that the document only contains a single Assertion (encrypted or not)

        :returns: True if only 1 assertion encrypted or not
        :rtype: bool
        """
        encrypted_assertion_nodes = OneLogin_Saml2_XML.query(self.document, '//saml:EncryptedAssertion')
        assertion_nodes = OneLogin_Saml2_XML.query(self.document, '//saml:Assertion')

        valid = len(encrypted_assertion_nodes) + len(assertion_nodes) == 1

        if (self.encrypted):
            assertion_nodes = OneLogin_Saml2_XML.query(self.decrypted_document, '//saml:Assertion')
            valid = valid and len(assertion_nodes) == 1

        return valid

    def process_signed_elements(self):
        """
        Verifies the signature nodes:
         - Checks that are Response or Assertion
         - Check that IDs and reference URI are unique and consistent.

        :returns: The signed elements tag names
        :rtype: list
        """
        sign_nodes = self._query('//ds:Signature')

        signed_elements = []
        verified_seis = []
        verified_ids = []
        response_tag = '{%s}Response' % OneLogin_Saml2_Constants.NS_SAMLP
        assertion_tag = '{%s}Assertion' % OneLogin_Saml2_Constants.NS_SAML

        security = self._settings.get_security_data()
        reject_deprecated_alg = security.get('rejectDeprecatedAlgorithm', False)

        for sign_node in sign_nodes:
            signed_element = sign_node.getparent().tag
            if signed_element != response_tag and signed_element != assertion_tag:
                raise OneLogin_Saml2_ValidationError(
                    'Invalid Signature Element %s SAML Response rejected' % signed_element,
                    OneLogin_Saml2_ValidationError.WRONG_SIGNED_ELEMENT
                )

            if not sign_node.getparent().get('ID'):
                raise OneLogin_Saml2_ValidationError(
                    'Signed Element must contain an ID. SAML Response rejected',
                    OneLogin_Saml2_ValidationError.ID_NOT_FOUND_IN_SIGNED_ELEMENT
                )

            id_value = sign_node.getparent().get('ID')
            if id_value in verified_ids:
                raise OneLogin_Saml2_ValidationError(
                    'Duplicated ID. SAML Response rejected',
                    OneLogin_Saml2_ValidationError.DUPLICATED_ID_IN_SIGNED_ELEMENTS
                )
            verified_ids.append(id_value)

            # Check that reference URI matches the parent ID and no duplicate References or IDs
            ref = OneLogin_Saml2_XML.query(sign_node, './/ds:Reference')
            if ref:
                ref = ref[0]
                if ref.get('URI'):
                    sei = ref.get('URI')[1:]

                    if sei != id_value:
                        raise OneLogin_Saml2_ValidationError(
                            'Found an invalid Signed Element. SAML Response rejected',
                            OneLogin_Saml2_ValidationError.INVALID_SIGNED_ELEMENT
                        )

                    if sei in verified_seis:
                        raise OneLogin_Saml2_ValidationError(
                            'Duplicated Reference URI. SAML Response rejected',
                            OneLogin_Saml2_ValidationError.DUPLICATED_REFERENCE_IN_SIGNED_ELEMENTS
                        )
                    verified_seis.append(sei)

            # Check the signature and digest algorithm
            if reject_deprecated_alg:
                sig_method_node = OneLogin_Saml2_XML.query(sign_node, './/ds:SignatureMethod')
                if sig_method_node:
                    sig_method = sig_method_node[0].get("Algorithm")
                    if sig_method in OneLogin_Saml2_Constants.DEPRECATED_ALGORITHMS:
                        raise OneLogin_Saml2_ValidationError(
                            'Deprecated signature algorithm found: %s' % sig_method,
                            OneLogin_Saml2_ValidationError.DEPRECATED_SIGNATURE_METHOD
                        )

                dig_method_node = OneLogin_Saml2_XML.query(sign_node, './/ds:DigestMethod')
                if dig_method_node:
                    dig_method = dig_method_node[0].get("Algorithm")
                    if dig_method in OneLogin_Saml2_Constants.DEPRECATED_ALGORITHMS:
                        raise OneLogin_Saml2_ValidationError(
                            'Deprecated digest algorithm found: %s' % dig_method,
                            OneLogin_Saml2_ValidationError.DEPRECATED_DIGEST_METHOD
                        )

            signed_elements.append(signed_element)

        if signed_elements:
            if not self.validate_signed_elements(signed_elements, raise_exceptions=True):
                raise OneLogin_Saml2_ValidationError(
                    'Found an unexpected Signature Element. SAML Response rejected',
                    OneLogin_Saml2_ValidationError.UNEXPECTED_SIGNED_ELEMENTS
                )
        return signed_elements

    @return_false_on_exception
    def validate_signed_elements(self, signed_elements):
        """
        Verifies that the document has the expected signed nodes.

        :param signed_elements: The signed elements to be checked
        :type signed_elements: list
        :param raise_exceptions: Whether to return false on failure or raise an exception
        :type raise_exceptions: Boolean
        """
        if len(signed_elements) > 2:
            return False

        response_tag = '{%s}Response' % OneLogin_Saml2_Constants.NS_SAMLP
        assertion_tag = '{%s}Assertion' % OneLogin_Saml2_Constants.NS_SAML

        if (response_tag in signed_elements and signed_elements.count(response_tag) > 1) or \
           (assertion_tag in signed_elements and signed_elements.count(assertion_tag) > 1) or \
           (response_tag not in signed_elements and assertion_tag not in signed_elements):
            return False

        # Check that the signed elements found here, are the ones that will be verified
        # by OneLogin_Saml2_Utils.validate_sign
        if response_tag in signed_elements:
            expected_signature_nodes = OneLogin_Saml2_XML.query(self.document, OneLogin_Saml2_Utils.RESPONSE_SIGNATURE_XPATH)
            if len(expected_signature_nodes) != 1:
                raise OneLogin_Saml2_ValidationError(
                    'Unexpected number of Response signatures found. SAML Response rejected.',
                    OneLogin_Saml2_ValidationError.WRONG_NUMBER_OF_SIGNATURES_IN_RESPONSE
                )

        if assertion_tag in signed_elements:
            expected_signature_nodes = self._query(OneLogin_Saml2_Utils.ASSERTION_SIGNATURE_XPATH)
            if len(expected_signature_nodes) != 1:
                raise OneLogin_Saml2_ValidationError(
                    'Unexpected number of Assertion signatures found. SAML Response rejected.',
                    OneLogin_Saml2_ValidationError.WRONG_NUMBER_OF_SIGNATURES_IN_ASSERTION
                )

        return True

    @return_false_on_exception
    def validate_timestamps(self):
        """
        Verifies that the document is valid according to Conditions Element

        :returns: True if the condition is valid, False otherwise
        :rtype: bool
        """
        conditions_nodes = self._query_assertion('/saml:Conditions')

        for conditions_node in conditions_nodes:
            nb_attr = conditions_node.get('NotBefore')
            nooa_attr = conditions_node.get('NotOnOrAfter')
            if nb_attr and OneLogin_Saml2_Utils.parse_SAML_to_time(nb_attr) > OneLogin_Saml2_Utils.now() + OneLogin_Saml2_Constants.ALLOWED_CLOCK_DRIFT:
                raise OneLogin_Saml2_ValidationError(
                    'Could not validate timestamp: not yet valid. Check system clock.',
                    OneLogin_Saml2_ValidationError.ASSERTION_TOO_EARLY
                )
            if nooa_attr and OneLogin_Saml2_Utils.parse_SAML_to_time(nooa_attr) + OneLogin_Saml2_Constants.ALLOWED_CLOCK_DRIFT <= OneLogin_Saml2_Utils.now():
                raise OneLogin_Saml2_ValidationError(
                    'Could not validate timestamp: expired. Check system clock.',
                    OneLogin_Saml2_ValidationError.ASSERTION_EXPIRED
                )
        return True

    def _query_assertion(self, xpath_expr):
        """
        Extracts nodes that match the query from the Assertion

        :param xpath_expr: Xpath Expresion
        :type xpath_expr: String

        :returns: The queried nodes
        :rtype: list
        """

        assertion_expr = '/saml:Assertion'
        signature_expr = '/ds:Signature/ds:SignedInfo/ds:Reference'
        signed_assertion_query = '/samlp:Response' + assertion_expr + signature_expr
        assertion_reference_nodes = self._query(signed_assertion_query)
        tagid = None

        if not assertion_reference_nodes:
            # Check if the message is signed
            signed_message_query = '/samlp:Response' + signature_expr
            message_reference_nodes = self._query(signed_message_query)
            if message_reference_nodes:
                message_id = message_reference_nodes[0].get('URI')
                final_query = "/samlp:Response[@ID=$tagid]/"
                tagid = message_id[1:]
            else:
                final_query = "/samlp:Response"
            final_query += assertion_expr
        else:
            assertion_id = assertion_reference_nodes[0].get('URI')
            final_query = '/samlp:Response' + assertion_expr + "[@ID=$tagid]"
            tagid = assertion_id[1:]
        final_query += xpath_expr
        return self._query(final_query, tagid)

    def _query(self, query, tagid=None):
        """
        Extracts nodes that match the query from the Response

        :param query: Xpath Expresion
        :type query: String

        :param tagid: Tag ID
        :type query: String

        :returns: The queried nodes
        :rtype: list
        """
        if self.encrypted:
            document = self.decrypted_document
        else:
            document = self.document
        return OneLogin_Saml2_XML.query(document, query, None, tagid)

    def _decrypt_assertion(self, xml):
        """
        Decrypts the Assertion

        :raises: Exception if no private key available
        :param xml: Encrypted Assertion
        :type xml: Element
        :returns: Decrypted Assertion
        :rtype: Element
        """
        key = self._settings.get_sp_key()
        debug = self._settings.is_debug_active()

        if not key:
            raise OneLogin_Saml2_Error(
                'No private key available to decrypt the assertion, check settings',
                OneLogin_Saml2_Error.PRIVATE_KEY_NOT_FOUND
            )

        encrypted_assertion_nodes = OneLogin_Saml2_XML.query(xml, '/samlp:Response/saml:EncryptedAssertion')
        if encrypted_assertion_nodes:
            encrypted_data_nodes = OneLogin_Saml2_XML.query(encrypted_assertion_nodes[0], '//saml:EncryptedAssertion/xenc:EncryptedData')
            if encrypted_data_nodes:
                keyinfo = OneLogin_Saml2_XML.query(encrypted_assertion_nodes[0], '//saml:EncryptedAssertion/xenc:EncryptedData/ds:KeyInfo')
                if not keyinfo:
                    raise OneLogin_Saml2_ValidationError(
                        'No KeyInfo present, invalid Assertion',
                        OneLogin_Saml2_ValidationError.KEYINFO_NOT_FOUND_IN_ENCRYPTED_DATA
                    )
                keyinfo = keyinfo[0]
                children = keyinfo.getchildren()
                if not children:
                    raise OneLogin_Saml2_ValidationError(
                        'KeyInfo has no children nodes, invalid Assertion',
                        OneLogin_Saml2_ValidationError.CHILDREN_NODE_NOT_FOUND_IN_KEYINFO
                    )
                for child in children:
                    if 'RetrievalMethod' in child.tag:
                        if child.attrib['Type'] != 'http://www.w3.org/2001/04/xmlenc#EncryptedKey':
                            raise OneLogin_Saml2_ValidationError(
                                'Unsupported Retrieval Method found',
                                OneLogin_Saml2_ValidationError.UNSUPPORTED_RETRIEVAL_METHOD
                            )
                        uri = child.attrib['URI']
                        if not uri.startswith('#'):
                            break
                        uri = uri.split('#')[1]
                        encrypted_key = OneLogin_Saml2_XML.query(encrypted_assertion_nodes[0], './xenc:EncryptedKey[@Id=$tagid]', None, uri)
                        if encrypted_key:
                            keyinfo.append(encrypted_key[0])

                encrypted_data = encrypted_data_nodes[0]
                decrypted = OneLogin_Saml2_Utils.decrypt_element(encrypted_data, key, debug=debug, inplace=True)
                xml.replace(encrypted_assertion_nodes[0], decrypted)
        return xml

    def get_error(self):
        """
        After executing a validation process, if it fails this method returns the cause
        """
        return self._error

    def get_xml_document(self):
        """
        Returns the SAML Response document (If contains an encrypted assertion, decrypts it)

        :return: Decrypted XML response document
        :rtype: DOMDocument
        """
        if self.encrypted:
            return self.decrypted_document
        else:
            return self.document

    def get_id(self):
        """
        :returns: the ID of the response
        :rtype: string
        """
        return self.document.get('ID', None)

    def get_assertion_id(self):
        """
        :returns: the ID of the assertion in the response
        :rtype: string
        """
        if not self.validate_num_assertions():
            raise OneLogin_Saml2_ValidationError(
                'SAML Response must contain 1 assertion',
                OneLogin_Saml2_ValidationError.WRONG_NUMBER_OF_ASSERTIONS
            )
        return self._query_assertion('')[0].get('ID', None)

    def get_assertion_issue_instant(self):
        """
        :returns: the IssueInstant of the assertion in the response
        :rtype: unix/posix timestamp|None
        """
        if not self.validate_num_assertions():
            raise OneLogin_Saml2_ValidationError(
                'SAML Response must contain 1 assertion',
                OneLogin_Saml2_ValidationError.WRONG_NUMBER_OF_ASSERTIONS
            )
        issue_instant = self._query_assertion('')[0].get('IssueInstant', None)
        return OneLogin_Saml2_Utils.parse_SAML_to_time(issue_instant)
