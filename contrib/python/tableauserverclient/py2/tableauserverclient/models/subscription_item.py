import xml.etree.ElementTree as ET
from .target import Target


class SubscriptionItem(object):

    def __init__(self, subject, schedule_id, user_id, target):
        self.id = None
        self.subject = subject
        self.schedule_id = schedule_id
        self.user_id = user_id
        self.target = target

    def __repr__(self):
        if self.id is not None:
            return "<Subscription#{id} subject({subject}) schedule_id({schedule_id}) user_id({user_id}) \
                target({target})".format(**self.__dict__)
        else:
            return "<Subscription subject({subject}) schedule_id({schedule_id}) user_id({user_id}) \
                target({target})".format(**self.__dict__)

    def _set_id(self, id_):
        self.id = id_

    @classmethod
    def from_response(cls, xml, ns):
        parsed_response = ET.fromstring(xml)
        all_subscriptions_xml = parsed_response.findall(
            './/t:subscription', namespaces=ns)

        all_subscriptions = [SubscriptionItem._parse_element(x, ns) for x in all_subscriptions_xml]
        return all_subscriptions

    @classmethod
    def _parse_element(cls, element, ns):
        schedule_id = None
        target = None

        schedule_element = element.find('.//t:schedule', namespaces=ns)
        content_element = element.find('.//t:content', namespaces=ns)
        user_element = element.find('.//t:user', namespaces=ns)

        if schedule_element is not None:
            schedule_id = schedule_element.get('id', None)

        if content_element is not None:
            target = Target(content_element.get('id', None), content_element.get('type'))

        if user_element is not None:
            user_id = user_element.get('id')

        id_ = element.get('id', None)
        subject = element.get('subject', None)
        sub = cls(subject, schedule_id, user_id, target)
        sub._set_id(id_)
        return sub
