from future.utils import python_2_unicode_compatible
from viberbot.api.messages.typed_message import TypedMessage
from viberbot.api.messages.message_type import MessageType


class URLMessage(TypedMessage):
	def __init__(self, tracking_data=None, keyboard=None, media=None, min_api_version=None):
		super(URLMessage, self).__init__(MessageType.URL, tracking_data, keyboard, min_api_version)
		self._media = media

	def to_dict(self):
		message_data = super(URLMessage, self).to_dict()
		message_data['media'] = self._media
		return message_data

	def from_dict(self, message_data):
		super(URLMessage, self).from_dict(message_data)
		if 'media' in message_data:
			self._media = message_data['media']
		return self

	@property
	def media(self):
		return self._media

	def validate(self):
		return super(URLMessage, self).validate() \
				and self._media is not None

	@python_2_unicode_compatible
	def __str__(self):
		return u"URLMessage [{0}, media={1}]".format(super(URLMessage, self).__str__(), self._media)
