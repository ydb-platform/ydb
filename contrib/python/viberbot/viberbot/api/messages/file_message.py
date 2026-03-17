from future.utils import python_2_unicode_compatible
from viberbot.api.messages.typed_message import TypedMessage
from viberbot.api.messages.message_type import MessageType


class FileMessage(TypedMessage):
	def __init__(self, tracking_data=None, keyboard=None, media=None, size=None, file_name=None, min_api_version=None):
		super(FileMessage, self).__init__(MessageType.FILE, tracking_data, keyboard, min_api_version)
		self._media = media
		self._size = size
		self._file_name = file_name

	def to_dict(self):
		message_data = super(FileMessage, self).to_dict()
		message_data['media'] = self._media
		message_data['size'] = self._size
		message_data['file_name'] = self._file_name
		return message_data

	def from_dict(self, message_data):
		super(FileMessage, self).from_dict(message_data)
		if 'media' in message_data:
			self._media = message_data['media']
		if 'size' in message_data:
			self._size = message_data['size']
		if 'file_name' in message_data:
			self._file_name = message_data['file_name']
		return self

	@property
	def media(self):
		return self._media

	@property
	def size(self):
		return self._size

	@property
	def file_name(self):
		return self._file_name

	def validate(self):
		return super(FileMessage, self).validate() \
				and self._media is not None \
				and self._size is not None \
				and self._file_name is not None

	@python_2_unicode_compatible
	def __str__(self):
		return u"FileMessage [{0}, media={1}, size={2}, file_name={3}]". \
			format(
				super(FileMessage, self).__str__(),
				self._media,
				self._size,
				self._file_name)
