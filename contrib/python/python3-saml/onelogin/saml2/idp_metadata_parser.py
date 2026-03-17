# -*- coding: utf-8 -*-

""" OneLogin_Saml2_IdPMetadataParser class
Metadata class of SAML Python Toolkit.
"""


from copy import deepcopy

try:
    import urllib.request as urllib2
except ImportError:
    import urllib2

import ssl

from onelogin.saml2.constants import OneLogin_Saml2_Constants
from onelogin.saml2.xml_utils import OneLogin_Saml2_XML


class OneLogin_Saml2_IdPMetadataParser(object):
    """
    A class that contain methods related to obtaining and parsing metadata from IdP

    This class does not validate in any way the URL that is introduced,
    make sure to validate it properly before use it in a get_metadata method.
    """

    @classmethod
    def get_metadata(cls, url, validate_cert=True, timeout=None, headers=None):
        """
        Gets the metadata XML from the provided URL
        :param url: Url where the XML of the Identity Provider Metadata is published.
        :type url: string

        :param validate_cert: If the url uses https schema, that flag enables or not the verification of the associated certificate.
        :type validate_cert: bool

        :param timeout: Timeout in seconds to wait for metadata response
        :type timeout: int
        :param headers: Extra headers to send in the request
        :type headers: dict

        :returns: metadata XML
        :rtype: string
        """
        valid = False

        request = urllib2.Request(url, headers=headers or {})

        if validate_cert:
            response = urllib2.urlopen(request, timeout=timeout)
        else:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            response = urllib2.urlopen(request, context=ctx, timeout=timeout)
        xml = response.read()

        if xml:
            try:
                dom = OneLogin_Saml2_XML.to_etree(xml)
                idp_descriptor_nodes = OneLogin_Saml2_XML.query(dom, '//md:IDPSSODescriptor')
                if idp_descriptor_nodes:
                    valid = True
            except Exception:
                pass

        if not valid:
            raise Exception('Not valid IdP XML found from URL: %s' % (url))

        return xml

    @classmethod
    def parse_remote(cls, url, validate_cert=True, entity_id=None, timeout=None, **kwargs):
        """
        Gets the metadata XML from the provided URL and parse it, returning a dict with extracted data
        :param url: Url where the XML of the Identity Provider Metadata is published.
        :type url: string

        :param validate_cert: If the url uses https schema, that flag enables or not the verification of the associated certificate.
        :type validate_cert: bool

        :param entity_id: Specify the entity_id of the EntityDescriptor that you want to parse a XML
                          that contains multiple EntityDescriptor.
        :type entity_id: string

        :param timeout: Timeout in seconds to wait for metadata response
        :type timeout: int

        :returns: settings dict with extracted data
        :rtype: dict
        """
        idp_metadata = cls.get_metadata(url, validate_cert, timeout, headers=kwargs.pop('headers', None))
        return cls.parse(idp_metadata, entity_id=entity_id, **kwargs)

    @classmethod
    def parse(
            cls,
            idp_metadata,
            required_sso_binding=OneLogin_Saml2_Constants.BINDING_HTTP_REDIRECT,
            required_slo_binding=OneLogin_Saml2_Constants.BINDING_HTTP_REDIRECT,
            entity_id=None):
        """
        Parses the Identity Provider metadata and return a dict with extracted data.

        If there are multiple <IDPSSODescriptor> tags, parse only the first.

        Parses only those SSO endpoints with the same binding as given by
        the `required_sso_binding` parameter.

        Parses only those SLO endpoints with the same binding as given by
        the `required_slo_binding` parameter.

        If the metadata specifies multiple SSO endpoints with the required
        binding, extract only the first (the same holds true for SLO
        endpoints).

        :param idp_metadata: XML of the Identity Provider Metadata.
        :type idp_metadata: string

        :param required_sso_binding: Parse only POST or REDIRECT SSO endpoints.
        :type required_sso_binding: one of OneLogin_Saml2_Constants.BINDING_HTTP_REDIRECT
            or OneLogin_Saml2_Constants.BINDING_HTTP_POST

        :param required_slo_binding: Parse only POST or REDIRECT SLO endpoints.
        :type required_slo_binding: one of OneLogin_Saml2_Constants.BINDING_HTTP_REDIRECT
            or OneLogin_Saml2_Constants.BINDING_HTTP_POST

        :param entity_id: Specify the entity_id of the EntityDescriptor that you want to parse a XML
                          that contains multiple EntityDescriptor.
        :type entity_id: string

        :returns: settings dict with extracted data
        :rtype: dict
        """
        data = {}

        dom = OneLogin_Saml2_XML.to_etree(idp_metadata)
        idp_entity_id = want_authn_requests_signed = idp_name_id_format = idp_sso_url = idp_slo_url = certs = None

        entity_desc_path = '//md:EntityDescriptor'
        if entity_id:
            entity_desc_path += "[@entityID='%s']" % entity_id
        entity_descriptor_nodes = OneLogin_Saml2_XML.query(dom, entity_desc_path)

        if len(entity_descriptor_nodes) > 0:
            entity_descriptor_node = entity_descriptor_nodes[0]
            idp_descriptor_nodes = OneLogin_Saml2_XML.query(entity_descriptor_node, './md:IDPSSODescriptor')
            if len(idp_descriptor_nodes) > 0:
                idp_descriptor_node = idp_descriptor_nodes[0]

                idp_entity_id = entity_descriptor_node.get('entityID', None)

                want_authn_requests_signed = idp_descriptor_node.get('WantAuthnRequestsSigned', None)

                name_id_format_nodes = OneLogin_Saml2_XML.query(idp_descriptor_node, './md:NameIDFormat')
                if len(name_id_format_nodes) > 0:
                    idp_name_id_format = OneLogin_Saml2_XML.element_text(name_id_format_nodes[0])

                sso_nodes = OneLogin_Saml2_XML.query(
                    idp_descriptor_node,
                    "./md:SingleSignOnService[@Binding='%s']" % required_sso_binding
                )

                if len(sso_nodes) > 0:
                    idp_sso_url = sso_nodes[0].get('Location', None)

                slo_nodes = OneLogin_Saml2_XML.query(
                    idp_descriptor_node,
                    "./md:SingleLogoutService[@Binding='%s']" % required_slo_binding
                )

                if len(slo_nodes) > 0:
                    idp_slo_url = slo_nodes[0].get('Location', None)

                signing_nodes = OneLogin_Saml2_XML.query(idp_descriptor_node, "./md:KeyDescriptor[not(contains(@use, 'encryption'))]/ds:KeyInfo/ds:X509Data/ds:X509Certificate")
                encryption_nodes = OneLogin_Saml2_XML.query(idp_descriptor_node, "./md:KeyDescriptor[not(contains(@use, 'signing'))]/ds:KeyInfo/ds:X509Data/ds:X509Certificate")

                if len(signing_nodes) > 0 or len(encryption_nodes) > 0:
                    certs = {}
                    if len(signing_nodes) > 0:
                        certs['signing'] = []
                        for cert_node in signing_nodes:
                            certs['signing'].append(''.join(OneLogin_Saml2_XML.element_text(cert_node).split()))
                    if len(encryption_nodes) > 0:
                        certs['encryption'] = []
                        for cert_node in encryption_nodes:
                            certs['encryption'].append(''.join(OneLogin_Saml2_XML.element_text(cert_node).split()))

                data['idp'] = {}

                if idp_entity_id is not None:
                    data['idp']['entityId'] = idp_entity_id

                if idp_sso_url is not None:
                    data['idp']['singleSignOnService'] = {}
                    data['idp']['singleSignOnService']['url'] = idp_sso_url
                    data['idp']['singleSignOnService']['binding'] = required_sso_binding

                if idp_slo_url is not None:
                    data['idp']['singleLogoutService'] = {}
                    data['idp']['singleLogoutService']['url'] = idp_slo_url
                    data['idp']['singleLogoutService']['binding'] = required_slo_binding

                if want_authn_requests_signed is not None:
                    data['security'] = {}
                    data['security']['authnRequestsSigned'] = want_authn_requests_signed == "true"

                if idp_name_id_format:
                    data['sp'] = {}
                    data['sp']['NameIDFormat'] = idp_name_id_format

                if certs is not None:
                    if (len(certs) == 1 and
                        (('signing' in certs and len(certs['signing']) == 1) or
                         ('encryption' in certs and len(certs['encryption']) == 1))) or \
                        (('signing' in certs and len(certs['signing']) == 1) and
                         ('encryption' in certs and len(certs['encryption']) == 1 and
                         certs['signing'][0] == certs['encryption'][0])):
                        if 'signing' in certs:
                            data['idp']['x509cert'] = certs['signing'][0]
                        else:
                            data['idp']['x509cert'] = certs['encryption'][0]
                    else:
                        data['idp']['x509certMulti'] = certs
        return data

    @staticmethod
    def merge_settings(settings, new_metadata_settings):
        """
        Will update the settings with the provided new settings data extracted from the IdP metadata
        :param settings: Current settings dict data
        :type settings: dict
        :param new_metadata_settings: Settings to be merged (extracted from IdP metadata after parsing)
        :type new_metadata_settings: dict
        :returns: merged settings
        :rtype: dict
        """
        for d in (settings, new_metadata_settings):
            if not isinstance(d, dict):
                raise TypeError('Both arguments must be dictionaries.')

        # Guarantee to not modify original data (`settings.copy()` would not
        # be sufficient, as it's just a shallow copy).
        result_settings = deepcopy(settings)

        # previously I will take care of cert stuff
        if 'idp' in new_metadata_settings and 'idp' in result_settings:
            if new_metadata_settings['idp'].get('x509cert', None) and result_settings['idp'].get('x509certMulti', None):
                del result_settings['idp']['x509certMulti']
            if new_metadata_settings['idp'].get('x509certMulti', None) and result_settings['idp'].get('x509cert', None):
                del result_settings['idp']['x509cert']

        # Merge `new_metadata_settings` into `result_settings`.
        dict_deep_merge(result_settings, new_metadata_settings)
        return result_settings


def dict_deep_merge(a, b, path=None):
    """Deep-merge dictionary `b` into dictionary `a`.

    Kudos to http://stackoverflow.com/a/7205107/145400
    """
    if path is None:
        path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                dict_deep_merge(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                # Key conflict, but equal value.
                pass
            else:
                # Key/value conflict. Prioritize b over a.
                a[key] = b[key]
        else:
            a[key] = b[key]
    return a
