import time

from django.conf import settings
from django.test.client import Client

from tastypie.compat import force_str
from tastypie.serializers import Serializer


class TestApiClient(object):
    def __init__(self, serializer=None):
        """
        Sets up a fresh ``TestApiClient`` instance.

        If you are employing a custom serializer, you can pass the class to the
        ``serializer=`` kwarg.
        """
        self.client = Client()
        self.serializer = serializer

        if not self.serializer:
            self.serializer = Serializer()

    def get_content_type(self, short_format):
        """
        Given a short name (such as ``json`` or ``xml``), returns the full
        content-type for it (``application/json`` or ``application/xml`` in
        this case).
        """
        return self.serializer.content_types.get(short_format, 'json')

    def get(self, uri, format='json', data=None, authentication=None,
            **kwargs):
        """
        Performs a simulated ``GET`` request to the provided URI.

        Optionally accepts a ``data`` kwarg, which in the case of ``GET``, lets
        you send along ``GET`` parameters. This is useful when testing
        filtering or other things that read off the ``GET`` params. Example::

            from tastypie.test import TestApiClient
            client = TestApiClient()

            response = client.get('/api/v1/entry/1/', data={
                'format': 'json',
                'title__startswith': 'a',
                'limit': 20,
                'offset': 60
            })

        Optionally accepts an ``authentication`` kwarg, which should be an HTTP
        header with the correct authentication data already setup.

        All other ``**kwargs`` passed in get passed through to the Django
        ``TestClient``. See
https://docs.djangoproject.com/en/dev/topics/testing/#module-django.test.client
        for details.
        """
        content_type = self.get_content_type(format)
        kwargs['HTTP_ACCEPT'] = content_type

        # GET & DELETE are the only times we don't serialize the data.
        if data is not None:
            kwargs['data'] = data

        if authentication is not None:
            kwargs['HTTP_AUTHORIZATION'] = authentication

        return self.client.get(uri, **kwargs)

    def post(self, uri, format='json', data=None, authentication=None,
            **kwargs):
        """
        Performs a simulated ``POST`` request to the provided URI.

        Optionally accepts a ``data`` kwarg. **Unlike** ``GET``, in ``POST``
        the ``data`` gets serialized & sent as the body instead of becoming
        part of the URI.
        Example::

            from tastypie.test import TestApiClient
            client = TestApiClient()

            response = client.post('/api/v1/entry/', data={
                'created': '2012-05-01T20:02:36',
                'slug': 'another-post',
                'title': 'Another Post',
                'user': '/api/v1/user/1/',
            })

        Optionally accepts an ``authentication`` kwarg, which should be an HTTP
        header with the correct authentication data already setup.

        All other ``**kwargs`` passed in get passed through to the Django
        ``TestClient``. See
https://docs.djangoproject.com/en/dev/topics/testing/#module-django.test.client
        for details.
        """
        content_type = self.get_content_type(format)
        kwargs['content_type'] = content_type

        if data is not None:
            kwargs['data'] = self.serializer.serialize(
                data, format=content_type)

        if authentication is not None:
            kwargs['HTTP_AUTHORIZATION'] = authentication

        return self.client.post(uri, **kwargs)

    def put(self, uri, format='json', data=None, authentication=None,
            **kwargs):
        """
        Performs a simulated ``PUT`` request to the provided URI.

        Optionally accepts a ``data`` kwarg. **Unlike** ``GET``, in ``PUT`` the
        ``data`` gets serialized & sent as the body instead of becoming part of
        the URI.
        Example::

            from tastypie.test import TestApiClient
            client = TestApiClient()

            response = client.put('/api/v1/entry/1/', data={
                'created': '2012-05-01T20:02:36',
                'slug': 'another-post',
                'title': 'Another Post',
                'user': '/api/v1/user/1/',
            })

        Optionally accepts an ``authentication`` kwarg, which should be an HTTP
        header with the correct authentication data already setup.

        All other ``**kwargs`` passed in get passed through to the Django
        ``TestClient``. See
https://docs.djangoproject.com/en/dev/topics/testing/#module-django.test.client
        for details.
        """
        content_type = self.get_content_type(format)
        kwargs['content_type'] = content_type

        if data is not None:
            kwargs['data'] = self.serializer.serialize(
                data, format=content_type)

        if authentication is not None:
            kwargs['HTTP_AUTHORIZATION'] = authentication

        return self.client.put(uri, **kwargs)

    def patch(self, uri, format='json', data=None, authentication=None,
            **kwargs):
        """
        Performs a simulated ``PATCH`` request to the provided URI.

        Optionally accepts a ``data`` kwarg. **Unlike** ``GET``, in ``PATCH``
        the ``data`` gets serialized & sent as the body instead of becoming
        part of the URI.
        Example::

            from tastypie.test import TestApiClient
            client = TestApiClient()

            response = client.patch('/api/v1/entry/1/', data={
                'created': '2012-05-01T20:02:36',
                'slug': 'another-post',
                'title': 'Another Post',
                'user': '/api/v1/user/1/',
            })

        Optionally accepts an ``authentication`` kwarg, which should be an HTTP
        header with the correct authentication data already setup.

        All other ``**kwargs`` passed in get passed through to the Django
        ``TestClient``. See
https://docs.djangoproject.com/en/dev/topics/testing/#module-django.test.client
        for details.
        """
        content_type = self.get_content_type(format)
        kwargs['content_type'] = content_type

        if data is not None:
            kwargs['data'] = self.serializer.serialize(
                data, format=content_type)

        if authentication is not None:
            kwargs['HTTP_AUTHORIZATION'] = authentication

        return self.client.patch(uri, **kwargs)

    def delete(self, uri, format='json', data=None, authentication=None,
            **kwargs):
        """
        Performs a simulated ``DELETE`` request to the provided URI.

        Optionally accepts a ``data`` kwarg, which in the case of ``DELETE``,
        lets you send along ``DELETE`` parameters. This is useful when testing
        filtering or other things that read off the ``DELETE`` params.
        Example::

            from tastypie.test import TestApiClient
            client = TestApiClient()

            response = client.delete('/api/v1/entry/1/',
                data={'format': 'json'})

        Optionally accepts an ``authentication`` kwarg, which should be an HTTP
        header with the correct authentication data already setup.

        All other ``**kwargs`` passed in get passed through to the Django
        ``TestClient``. See
https://docs.djangoproject.com/en/dev/topics/testing/#module-django.test.client
        for details.
        """
        content_type = self.get_content_type(format)
        kwargs['content_type'] = content_type

        # GET & DELETE are the only times we don't serialize the data.
        if data is not None:
            kwargs['data'] = data

        if authentication is not None:
            kwargs['HTTP_AUTHORIZATION'] = authentication

        return self.client.delete(uri, **kwargs)


class ResourceTestCaseMixin(object):
    """
    A mixin of useful methods for testing Tastypie APIs.
    Below we use this to subclass Django's TestCase and TransactionTestCase classes.
    """
    def setUp(self):
        super(ResourceTestCaseMixin, self).setUp()
        self.serializer = Serializer()
        self.api_client = TestApiClient()

    def get_credentials(self):
        """
        A convenience method for the user as a way to shorten up the
        often repetitious calls to create the same authentication.

        Raises ``NotImplementedError`` by default.

        Usage::

            class MyResourceTestCase(ResourceTestCase):
                def get_credentials(self):
                    return self.create_basic('daniel', 'pass')

                # Then the usual tests...

        """
        raise NotImplementedError(
            "You must return the class for your Resource to test.")

    def create_basic(self, username, password):
        """
        Creates & returns the HTTP ``Authorization`` header for use with BASIC
        Auth.
        """
        import base64
        return 'Basic %s' % base64.b64encode(
            ':'.join([username, password]).encode('utf-8')).decode('utf-8')

    def create_apikey(self, username, api_key):
        """
        Creates & returns the HTTP ``Authorization`` header for use with
        ``ApiKeyAuthentication``.
        """
        return 'ApiKey %s:%s' % (username, api_key)

    def create_digest(self, username, api_key, method, uri):
        """
        Creates & returns the HTTP ``Authorization`` header for use with Digest
        Auth.
        """
        from tastypie.authentication import hmac, sha1, uuid, python_digest

        new_uuid = uuid.uuid4()
        opaque = hmac.new(
            str(new_uuid).encode('utf-8'), digestmod=sha1
        ).hexdigest().decode('utf-8')
        return python_digest.build_authorization_request(
            username,
            method.upper(),
            uri,
            1,  # nonce_count
            digest_challenge=python_digest.build_digest_challenge(
                time.time(),
                settings.SECRET_KEY,
                'django-tastypie',
                opaque,
                False
            ),
            password=api_key
        )

    def create_oauth(self, user):
        """
        Creates & returns the HTTP ``Authorization`` header for use with Oauth.
        """
        from oauth_provider.models import Consumer, Token, Resource

        # Necessary setup for ``oauth_provider``.
        resource, _ = Resource.objects.get_or_create(url='test', defaults={
            'name': 'Test Resource'
        })
        consumer, _ = Consumer.objects.get_or_create(key='123', defaults={
            'name': 'Test',
            'description': 'Testing...'
        })
        token, _ = Token.objects.get_or_create(
            key='foo',
            token_type=Token.ACCESS,
            defaults={
                'consumer': consumer,
                'resource': resource,
                'secret': '',
                'user': user,
            }
        )

        # Then generate the header.
        oauth_data = {
            'oauth_consumer_key': '123',
            'oauth_nonce': 'abc',
            'oauth_signature': '&',
            'oauth_signature_method': 'PLAINTEXT',
            'oauth_timestamp': str(int(time.time())),
            'oauth_token': 'foo',
        }
        return 'OAuth %s' % ','.join([
            key + '=' + value for key, value in oauth_data.items()
        ])

    def assertHttpOK(self, resp):
        """
        Ensures the response is returning a HTTP 200.
        """
        return self.assertEqual(resp.status_code, 200)

    def assertHttpCreated(self, resp):
        """
        Ensures the response is returning a HTTP 201.
        """
        return self.assertEqual(resp.status_code, 201)

    def assertHttpAccepted(self, resp):
        """
        Ensures the response is returning either a HTTP 202 or a HTTP 204.
        """
        self.assertIn(resp.status_code, [202, 204])

    def assertHttpMultipleChoices(self, resp):
        """
        Ensures the response is returning a HTTP 300.
        """
        return self.assertEqual(resp.status_code, 300)

    def assertHttpSeeOther(self, resp):
        """
        Ensures the response is returning a HTTP 303.
        """
        return self.assertEqual(resp.status_code, 303)

    def assertHttpNotModified(self, resp):
        """
        Ensures the response is returning a HTTP 304.
        """
        return self.assertEqual(resp.status_code, 304)

    def assertHttpBadRequest(self, resp):
        """
        Ensures the response is returning a HTTP 400.
        """
        return self.assertEqual(resp.status_code, 400)

    def assertHttpUnauthorized(self, resp):
        """
        Ensures the response is returning a HTTP 401.
        """
        return self.assertEqual(resp.status_code, 401)

    def assertHttpForbidden(self, resp):
        """
        Ensures the response is returning a HTTP 403.
        """
        return self.assertEqual(resp.status_code, 403)

    def assertHttpNotFound(self, resp):
        """
        Ensures the response is returning a HTTP 404.
        """
        return self.assertEqual(resp.status_code, 404)

    def assertHttpMethodNotAllowed(self, resp):
        """
        Ensures the response is returning a HTTP 405.
        """
        return self.assertEqual(resp.status_code, 405)

    def assertHttpConflict(self, resp):
        """
        Ensures the response is returning a HTTP 409.
        """
        return self.assertEqual(resp.status_code, 409)

    def assertHttpGone(self, resp):
        """
        Ensures the response is returning a HTTP 410.
        """
        return self.assertEqual(resp.status_code, 410)

    def assertHttpUnprocessableEntity(self, resp):
        """
        Ensures the response is returning a HTTP 422.
        """
        return self.assertEqual(resp.status_code, 422)

    def assertHttpTooManyRequests(self, resp):
        """
        Ensures the response is returning a HTTP 429.
        """
        return self.assertEqual(resp.status_code, 429)

    def assertHttpApplicationError(self, resp):
        """
        Ensures the response is returning a HTTP 500.
        """
        return self.assertEqual(resp.status_code, 500)

    def assertHttpNotImplemented(self, resp):
        """
        Ensures the response is returning a HTTP 501.
        """
        return self.assertEqual(resp.status_code, 501)

    def assertValidJSON(self, data):
        """
        Given the provided ``data`` as a string, ensures that it is valid JSON
        & can be loaded properly.
        """
        # Just try the load. If it throws an exception, the test case will
        # fail.
        self.serializer.from_json(data)

    def assertValidXML(self, data):
        """
        Given the provided ``data`` as a string, ensures that it is valid XML &
        can be loaded properly.
        """
        # Just try the load. If it throws an exception, the test case will
        # fail.
        self.serializer.from_xml(data)

    def assertValidYAML(self, data):
        """
        Given the provided ``data`` as a string, ensures that it is valid YAML
        & can be loaded properly.
        """
        # Just try the load. If it throws an exception, the test case will
        # fail.
        self.serializer.from_yaml(data)

    def assertValidPlist(self, data):
        """
        Given the provided ``data`` as a string, ensures that it is valid
        binary plist & can be loaded properly.
        """
        # Just try the load. If it throws an exception, the test case will
        # fail.
        self.serializer.from_plist(data)

    def assertValidJSONResponse(self, resp):
        """
        Given a ``HttpResponse`` coming back from using the ``client``, assert
        that you get back:

        * An HTTP 200
        * The correct content-type (``application/json``)
        * The content is valid JSON
        """
        self.assertHttpOK(resp)
        self.assertTrue(resp['Content-Type'].startswith('application/json'))
        self.assertValidJSON(force_str(resp.content))

    def assertValidXMLResponse(self, resp):
        """
        Given a ``HttpResponse`` coming back from using the ``client``, assert
        that you get back:

        * An HTTP 200
        * The correct content-type (``application/xml``)
        * The content is valid XML
        """
        self.assertHttpOK(resp)
        self.assertTrue(resp['Content-Type'].startswith('application/xml'))
        self.assertValidXML(force_str(resp.content))

    def assertValidYAMLResponse(self, resp):
        """
        Given a ``HttpResponse`` coming back from using the ``client``, assert
        that you get back:

        * An HTTP 200
        * The correct content-type (``text/yaml``)
        * The content is valid YAML
        """
        self.assertHttpOK(resp)
        self.assertTrue(resp['Content-Type'].startswith('text/yaml'))
        self.assertValidYAML(force_str(resp.content))

    def assertValidPlistResponse(self, resp):
        """
        Given a ``HttpResponse`` coming back from using the ``client``, assert
        that you get back:

        * An HTTP 200
        * The correct content-type (``application/x-plist``)
        * The content is valid binary plist data
        """
        self.assertHttpOK(resp)
        self.assertTrue(resp['Content-Type'].startswith('application/x-plist'))
        self.assertValidPlist(force_str(resp.content))

    def deserialize(self, resp):
        """
        Given a ``HttpResponse`` coming back from using the ``client``, this
        method checks the ``Content-Type`` header & attempts to deserialize the
        data based on that.

        It returns a Python datastructure (typically a ``dict``) of the
        serialized data.
        """
        return self.serializer.deserialize(
            resp.content, format=resp['Content-Type'])

    def serialize(self, data, format='application/json'):
        """
        Given a Python datastructure (typically a ``dict``) & a desired
        content-type, this method will return a serialized string of that data.
        """
        return self.serializer.serialize(data, format=format)

    def assertKeys(self, data, expected):
        """
        This method ensures that the keys of the ``data`` match up to the keys
        of ``expected``.

        It covers the (extremely) common case where you want to make sure the
        keys of a response match up to what is expected. This is typically less
        fragile than testing the full structure, which can be prone to data
        changes.
        """
        self.assertEqual(sorted(data.keys()), sorted(expected))
