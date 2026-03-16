from future.utils import python_2_unicode_compatible
from abc import abstractmethod


class Message(object):
	def __init__(self, tracking_data=None, keyboard=None, min_api_version=None, alt_text=None):
		self._tracking_data = tracking_data
		self._keyboard = keyboard
		self._min_api_version = min_api_version
		self._alt_text = alt_text

	@abstractmethod
	def to_dict(self):
		message_data = {}
		if self._tracking_data:
			message_data['tracking_data'] = self._tracking_data
		if self._keyboard:
			message_data['keyboard'] = self._keyboard
		if self._min_api_version:
			message_data['min_api_version'] = self._min_api_version
		if self._alt_text:
			message_data['alt_text'] = self._alt_text
		return message_data

	@abstractmethod
	def from_dict(self, message_data):
		if 'tracking_data' in message_data:
			self._tracking_data = message_data['tracking_data']
		if 'keyboard' in message_data:
			self._keyboard = message_data['keyboard']
		if 'min_api_version' in message_data:
			self._min_api_version = message_data['min_api_version']
		return self

	@property
	def keyboard(self):
		return self._keyboard

	@property
	def tracking_data(self):
		return self._tracking_data

	@property
	def min_api_version(self):
		return self._min_api_version

	@abstractmethod
	def validate(self):
		"""
		validates message has all the required fields before send
		"""
		pass

	@python_2_unicode_compatible
	def __str__(self):
		return u"tracking_data={0}, keyboard={1}, min_api_version={2}"\
			.format(
				self._tracking_data,
				self._keyboard,
				self._min_api_version)
