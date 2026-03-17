import xml.etree.ElementTree as ET

import re


NAMESPACE_RE = re.compile(r'^{.*}')


def _parse_event(events):
    event = events[0]
    # Strip out the namespace from the tag name
    return NAMESPACE_RE.sub('', event.tag)


class WebhookItem(object):
    def __init__(self):
        self._id = None
        self.name = None
        self.url = None
        self._event = None
        self.owner_id = None

    def _set_values(self, id, name, url, event, owner_id):
        if id is not None:
            self._id = id
        if name:
            self.name = name
        if url:
            self.url = url
        if event:
            self.event = event
        if owner_id:
            self.owner_id = owner_id

    @property
    def id(self):
        return self._id

    @property
    def event(self):
        if self._event:
            return self._event.replace("webhook-source-event-", "")
        return None

    @event.setter
    def event(self, value):
        self._event = "webhook-source-event-{}".format(value)

    @classmethod
    def from_response(cls, resp, ns):
        all_webhooks_items = list()
        parsed_response = ET.fromstring(resp)
        all_webhooks_xml = parsed_response.findall('.//t:webhook', namespaces=ns)
        for webhook_xml in all_webhooks_xml:
            values = cls._parse_element(webhook_xml, ns)

            webhook_item = cls()
            webhook_item._set_values(*values)
            all_webhooks_items.append(webhook_item)
        return all_webhooks_items

    @staticmethod
    def _parse_element(webhook_xml, ns):
        id = webhook_xml.get('id', None)
        name = webhook_xml.get('name', None)

        url = None
        url_tag = webhook_xml.find('.//t:webhook-destination-http', namespaces=ns)
        if url_tag is not None:
            url = url_tag.get('url', None)

        event = webhook_xml.findall('.//t:webhook-source/*', namespaces=ns)
        if event is not None and len(event) > 0:
            event = _parse_event(event)

        owner_id = None
        owner_tag = webhook_xml.find('.//t:owner', namespaces=ns)
        if owner_tag is not None:
            owner_id = owner_tag.get('id', None)

        return id, name, url, event, owner_id

    def __repr__(self):
        return "<Webhook id={} name={} url={} event={}>".format(
            self.id, self.name, self.url, self.event)
