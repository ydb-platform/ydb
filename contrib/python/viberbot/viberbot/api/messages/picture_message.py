from future.utils import python_2_unicode_compatible
from viberbot.api.messages.typed_message import TypedMessage
from viberbot.api.messages.message_type import MessageType


class PictureMessage(TypedMessage):
	def __init__(self, tracking_data=None, keyboard=None, text=None, media=None, thumbnail=None, min_api_version=None):
		super(PictureMessage, self).__init__(MessageType.PICTURE, tracking_data, keyboard, min_api_version)
		self._text = text or ''
		self._media = media
		self._thumbnail = thumbnail

	def to_dict(self):
		message_data = super(PictureMessage, self).to_dict()
		message_data['text'] = self._text
		message_data['media'] = self._media
		message_data['thumbnail'] = self._thumbnail
		return message_data

	def from_dict(self, message_data):
		super(PictureMessage, self).from_dict(message_data)
		if 'text' in message_data:
			self._text = message_data['text'] or ''
		if 'media' in message_data:
			self._media = message_data['media']
		if 'thumbnail' in message_data:
			self._thumbnail = message_data['thumbnail']
		return self

	def validate(self):
		return super(PictureMessage, self).validate() \
				and self._text is not None and self._media is not None

	@property
	def text(self):
		return self._text

	@property
	def media(self):
		return self._media

	@property
	def thumbnail(self):
		return self._thumbnail

	@python_2_unicode_compatible
	def __str__(self):
		return u"PictureMessage [{0}, text={1}, media={2}, thumbnail={3}]"\
			.format(
				super(PictureMessage, self).__str__(),
				self._text,
				self._media,
				self._thumbnail)
