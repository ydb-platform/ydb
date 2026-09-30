"""Browser and event helpers for the Keycloak integration tests."""

from html.parser import HTMLParser
import os
from urllib.parse import urljoin, urlsplit

import requests


class _Forms(HTMLParser):
    def __init__(self, html):
        super().__init__(convert_charrefs=True)
        self.forms = []
        self.current = None
        self.feed(html)

    def handle_starttag(self, tag, attributes):
        attrs = dict(attributes)
        if tag == 'form':
            self.current = {'action': attrs.get('action', ''), 'fields': {}}
            self.forms.append(self.current)
        elif tag in ('input', 'button') and self.current is not None and attrs.get('name'):
            if attrs.get('type') not in ('checkbox', 'radio') or 'checked' in attrs:
                self.current['fields'][attrs['name']] = attrs.get('value', '')

    def handle_endtag(self, tag):
        if tag == 'form':
            self.current = None


def approve_device(url):
    """Log in and approve the real Keycloak device authorization HTML flow."""
    issuer = os.environ['OIDC_ISSUER']
    if urlsplit(url).netloc != urlsplit(issuer).netloc:
        raise ValueError('Device verification URL is not on the test Keycloak server')
    with requests.Session() as session:
        session.verify = os.environ['OIDC_CA_FILE']
        response = session.get(url, timeout=15)
        for _ in range(8):
            response.raise_for_status()
            forms = _Forms(response.text).forms
            if not forms:
                if 'Device Login Successful' in response.text:
                    return
                raise RuntimeError('Keycloak device approval did not reach its success page')
            form = next((item for item in forms if 'password' in item['fields']), forms[0])
            fields = form['fields']
            if 'password' in fields:
                fields.update(username='oidc-user', password='password')
            # Consent forms have accept and cancel submit controls. A browser
            # sends only the clicked control, so never submit the cancel button.
            fields.pop('cancel', None)
            target = urljoin(response.url, form['action'])
            if urlsplit(target).netloc != urlsplit(issuer).netloc:
                raise RuntimeError('Keycloak form points outside the test server')
            response = session.post(target, data=fields, timeout=15)
    raise RuntimeError('Too many pages in Keycloak device approval')


def grant_counts():
    """Count successful grants from Keycloak's persisted realm events."""
    event_types = {
        'CLIENT_LOGIN': 'client_credentials',
        # Keycloak 26 sets OAUTH2_DEVICE_AUTH without persisting a success
        # event. Successful user-code verification records entry into each
        # device login flow, including the client that requested it.
        'OAUTH2_DEVICE_VERIFY_USER_CODE': 'device_verification',
        'OAUTH2_DEVICE_CODE_TO_TOKEN': 'device_token',
        'REFRESH_TOKEN': 'refresh_token',
    }
    counts = dict.fromkeys(event_types.values(), 0)
    first = 0
    with requests.Session() as session:
        session.verify = os.environ['OIDC_CA_FILE']
        session.headers['Authorization'] = 'Bearer ' + os.environ['OIDC_ADMIN_TOKEN']
        while True:
            response = session.get(os.environ['OIDC_ADMIN_EVENTS_URL'], params={'first': first, 'max': 100}, timeout=15)
            response.raise_for_status()
            events = response.json()
            for event in events:
                name = event_types.get(event['type'])
                if name is not None and event.get('clientId') in (
                    os.environ['OIDC_CLIENT_ID'],
                    os.environ['OIDC_DEVICE_CLIENT_ID'],
                ):
                    counts[name] += 1
            if len(events) < 100:
                return counts
            first += len(events)
