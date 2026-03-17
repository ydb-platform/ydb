from future.utils import python_2_unicode_compatible
from viberbot.api.messages.typed_message import TypedMessage
from viberbot.api.messages.message_type import MessageType


class VideoMessage(TypedMessage):
	def __init__(self, tracking_data=None, keyboard=None, media=None, thumbnail=None, size=None, text=None, duration=None, min_api_version=None):
		super(VideoMessage, self).__init__(MessageType.VIDEO, tracking_data, keyboard, min_api_version)
		self._media = media
		self._thumbnail = thumbnail
		self._size = size
		self._duration = duration
		self._text = text

	def to_dict(self):
		message_data = super(VideoMessage, self).to_dict()
		message_data['media'] = self._media
		message_data['thumbnail'] = self._thumbnail
		message_data['size'] = self._size
		message_data['duration'] = self._duration
		message_data['text'] = self._text
		return message_data

	def from_dict(self, message_data):
		super(VideoMessage, self).from_dict(message_data)
		if 'media' in message_data:
			self._media = message_data['media']
		if 'thumbnail' in message_data:
			self._thumbnail = message_data['thumbnail']
		if 'size' in message_data:
			self._size = message_data['size']
		if 'duration' in message_data:
			self._duration = message_data['duration']
		if 'text' in message_data:
			self._text = message_data['text']
		return self

	def validate(self):
		return super(VideoMessage, self).validate() \
				and self._media is not None \
				and self._size is not None

	@property
	def media(self):
		return self._media

	@property
	def thumbnail(self):
		return self._thumbnail

	@property
	def size(self):
		return self._size

	@property
	def duration(self):
		return self._duration

	@property
	def text(self):
		return self._text

	@python_2_unicode_compatible
	def __str__(self):
		return u"VideoMessage [{0}, media={1}, thumbnail={2}, size={3}, duration={4} text={5}]".\
			format(
				super(VideoMessage, self).__str__(),
				self._media,
				self._thumbnail,
				self._size,
				self._duration,
				self._text)
