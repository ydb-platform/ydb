from ..request_factory import RequestFactory

from .endpoint import Endpoint, api
import xml.etree.ElementTree as ET
import logging

logger = logging.getLogger('tableau.endpoint.auth')


class Auth(Endpoint):
    class contextmgr(object):
        def __init__(self, callback):
            self._callback = callback

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            self._callback()

    @property
    def baseurl(self):
        return "{0}/auth".format(self.parent_srv.baseurl)

    @api(version="2.0")
    def sign_in(self, auth_req):
        url = "{0}/{1}".format(self.baseurl, 'signin')
        signin_req = RequestFactory.Auth.signin_req(auth_req)
        server_response = self.parent_srv.session.post(url, data=signin_req,
                                                       **self.parent_srv.http_options)
        self.parent_srv._namespace.detect(server_response.content)
        self._check_status(server_response)
        parsed_response = ET.fromstring(server_response.content)
        site_id = parsed_response.find('.//t:site', namespaces=self.parent_srv.namespace).get('id', None)
        user_id = parsed_response.find('.//t:user', namespaces=self.parent_srv.namespace).get('id', None)
        auth_token = parsed_response.find('t:credentials', namespaces=self.parent_srv.namespace).get('token', None)
        self.parent_srv._set_auth(site_id, user_id, auth_token)
        logger.info('Signed into {0} as user with id {1}'.format(self.parent_srv.server_address, user_id))
        return Auth.contextmgr(self.sign_out)

    @api(version="3.6")
    def sign_in_with_personal_access_token(self, auth_req):
        # We use the same request that username/password login uses.
        return self.sign_in(auth_req)

    @api(version="2.0")
    def sign_out(self):
        url = "{0}/{1}".format(self.baseurl, 'signout')
        # If there are no auth tokens you're already signed out. No-op
        if not self.parent_srv.is_signed_in():
            return
        self.post_request(url, '')
        self.parent_srv._clear_auth()
        logger.info('Signed out')
