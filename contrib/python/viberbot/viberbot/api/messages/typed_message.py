from future.utils import python_2_unicode_compatible
from abc import abstractmethod
from viberbot.api.messages.message import Message


class TypedMessage(Message):
	def __init__(self, message_type, tracking_data=None, keyboard=None, min_api_version=None, alt_text=None):
		super(TypedMessage, self).__init__(tracking_data, keyboard, min_api_version, alt_text)
		self._message_type = message_type

	@abstractmethod
	def to_dict(self):
		message_data = super(TypedMessage, self).to_dict()
		message_data['type'] = self._message_type
		return message_data

	@abstractmethod
	def from_dict(self, message_data):
		super(TypedMessage, self).from_dict(message_data)
		return self

	@abstractmethod
	def validate(self):
		"""
		validates message has all the required fields before send
		"""
		return self._message_type is not None

	@python_2_unicode_compatible
	def __str__(self):
		return super(TypedMessage, self).__str__()
