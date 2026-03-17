from future.utils import python_2_unicode_compatible
from viberbot.api.messages.typed_message import TypedMessage
from viberbot.api.messages.message_type import MessageType


class RichMediaMessage(TypedMessage):
	def __init__(self, tracking_data=None, keyboard=None, rich_media=None, min_api_version=None, alt_text=None):
		super(RichMediaMessage, self).__init__(MessageType.RICH_MEDIA, tracking_data, keyboard, min_api_version)
		self._rich_media = rich_media
		self._alt_text = alt_text

	def to_dict(self):
		message_data = super(RichMediaMessage, self).to_dict()
		message_data['rich_media'] = self._rich_media
		message_data['alt_text'] = self._alt_text
		return message_data

	def from_dict(self, message_data):
		super(RichMediaMessage, self).from_dict(message_data)
		if 'rich_media' in message_data:
			self._rich_media = message_data['rich_media']
		if 'alt_text' in message_data:
			self._alt_text = message_data['alt_text']
		return self

	def validate(self):
		return super(RichMediaMessage, self).validate() \
				and self._rich_media is not None

	@property
	def rich_media(self):
		return self._rich_media

	@property
	def alt_text(self):
		return self._alt_text

	@python_2_unicode_compatible
	def __str__(self):
		return u"RichMediaMessage [{0}, rich_media={1}]"\
			.format(
				super(RichMediaMessage, self).__str__(),
				self._rich_media,
				self._alt_text)
