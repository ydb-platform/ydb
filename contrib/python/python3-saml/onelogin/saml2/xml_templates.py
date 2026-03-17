# -*- coding: utf-8 -*-

""" OneLogin_Saml2_Auth class


Main class of SAML Python Toolkit.

Initializes the SP SAML instance

"""


class OneLogin_Saml2_Templates(object):

    ATTRIBUTE = """
        <saml:Attribute Name="%s" NameFormat="urn:oasis:names:tc:SAML:2.0:attrname-format:basic">
            <saml:AttributeValue xsi:type="xs:string">%s</saml:AttributeValue>
        </saml:Attribute>"""

    AUTHN_REQUEST = """\
<samlp:AuthnRequest
  xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"
  xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"
  ID="%(id)s"
  Version="2.0"%(provider_name)s%(force_authn_str)s%(is_passive_str)s
  IssueInstant="%(issue_instant)s"
  Destination="%(destination)s"
  ProtocolBinding="%(acs_binding)s"
  AssertionConsumerServiceURL="%(assertion_url)s"%(attr_consuming_service_str)s>
    <saml:Issuer>%(entity_id)s</saml:Issuer>%(subject_str)s%(nameid_policy_str)s
%(requested_authn_context_str)s
</samlp:AuthnRequest>"""

    LOGOUT_REQUEST = """\
<samlp:LogoutRequest
  xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"
  xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"
  ID="%(id)s"
  Version="2.0"
  IssueInstant="%(issue_instant)s"
  Destination="%(single_logout_url)s">
    <saml:Issuer>%(entity_id)s</saml:Issuer>
    %(name_id)s
    %(session_index)s
</samlp:LogoutRequest>"""

    LOGOUT_RESPONSE = """\
<samlp:LogoutResponse
  xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"
  xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"
  ID="%(id)s"
  Version="2.0"
  IssueInstant="%(issue_instant)s"
  Destination="%(destination)s"
  InResponseTo="%(in_response_to)s">
    <saml:Issuer>%(entity_id)s</saml:Issuer>
    <samlp:Status>
        <samlp:StatusCode Value="%(status)s" />
    </samlp:Status>
</samlp:LogoutResponse>"""

    MD_CONTACT_PERSON = """\
    <md:ContactPerson contactType="%(type)s">
        <md:GivenName>%(name)s</md:GivenName>
        <md:EmailAddress>%(email)s</md:EmailAddress>
    </md:ContactPerson>"""

    MD_SLS = """\
        <md:SingleLogoutService Binding="%(binding)s"
                                Location="%(location)s" />\n"""

    MD_REQUESTED_ATTRIBUTE = """\
            <md:RequestedAttribute Name="%(req_attr_name)s"%(req_attr_nameformat_str)s%(req_attr_isrequired_str)s%(req_attr_aux_str)s"""

    MD_ATTR_CONSUMER_SERVICE = """\
        <md:AttributeConsumingService index="1">
            <md:ServiceName xml:lang="en">%(service_name)s</md:ServiceName>
%(attr_cs_desc)s%(requested_attribute_str)s
        </md:AttributeConsumingService>\n"""

    MD_ENTITY_DESCRIPTOR = """\
<?xml version="1.0"?>
<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata"
                     %(valid)s
                     %(cache)s
                     entityID="%(entity_id)s">
    <md:SPSSODescriptor AuthnRequestsSigned="%(authnsign)s" WantAssertionsSigned="%(wsign)s" protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">
%(sls)s        <md:NameIDFormat>%(name_id_format)s</md:NameIDFormat>
        <md:AssertionConsumerService Binding="%(binding)s"
                                     Location="%(location)s"
                                     index="1" />
%(attribute_consuming_service)s    </md:SPSSODescriptor>
%(organization)s
%(contacts)s
</md:EntityDescriptor>"""

    MD_ORGANISATION = """\
    <md:Organization>
        <md:OrganizationName xml:lang="%(lang)s">%(name)s</md:OrganizationName>
        <md:OrganizationDisplayName xml:lang="%(lang)s">%(display_name)s</md:OrganizationDisplayName>
        <md:OrganizationURL xml:lang="%(lang)s">%(url)s</md:OrganizationURL>
    </md:Organization>"""

    RESPONSE = """\
<samlp:Response
  xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"
  xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"
  ID="%(id)s"
  InResponseTo="%(in_response_to)s"
  Version="2.0"
  IssueInstant="%(issue_instant)s"
  Destination="%(destination)s">
    <saml:Issuer>%(entity_id)s</saml:Issuer>
    <samlp:Status xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol">
        <samlp:StatusCode
          xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"
          Value="%(status)s">
        </samlp:StatusCode>
    </samlp:Status>
    <saml:Assertion
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xmlns:xs="http://www.w3.org/2001/XMLSchema"
        Version="2.0"
        ID="%(assertion_id)s"
        IssueInstant="%(issue_instant)s">
        <saml:Issuer>%(entity_id)s</saml:Issuer>
        <saml:Subject>
            <saml:NameID
              NameQualifier="%(entity_id)s"
              SPNameQualifier="%(requester)s"
              Format="%(name_id_policy)s">%(name_id)s</saml:NameID>
            <saml:SubjectConfirmation Method="%(cm)s">
                <saml:SubjectConfirmationData
                  NotOnOrAfter="%(not_after)s"
                  InResponseTo="%(in_response_to)s"
                  Recipient="%(destination)s">
                </saml:SubjectConfirmationData>
            </saml:SubjectConfirmation>
        </saml:Subject>
        <saml:Conditions NotBefore="%(not_before)s" NotOnOrAfter="%(not_after)s">
            <saml:AudienceRestriction>
                <saml:Audience>%(requester)s</saml:Audience>
            </saml:AudienceRestriction>
        </saml:Conditions>
        <saml:AuthnStatement
          AuthnInstant="%(issue_instant)s"
          SessionIndex="%(session_index)s"
          SessionNotOnOrAfter="%(not_after)s">
%(authn_context)s
        </saml:AuthnStatement>
        <saml:AttributeStatement>
%(attributes)s
        </saml:AttributeStatement>
    </saml:Assertion>
</samlp:Response>"""
