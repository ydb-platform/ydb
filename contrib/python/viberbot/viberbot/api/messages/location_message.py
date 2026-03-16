from future.utils import python_2_unicode_compatible
from viberbot.api.messages.data_types.location import Location
from viberbot.api.messages.typed_message import TypedMessage
from viberbot.api.messages.message_type import MessageType


class LocationMessage(TypedMessage):
	def __init__(self, tracking_data=None, keyboard=None, location=None, min_api_version=None):
		super(LocationMessage, self).__init__(MessageType.LOCATION, tracking_data, keyboard, min_api_version)
		self._location = location

	def to_dict(self):
		message_data = super(LocationMessage, self).to_dict()
		if self._location is not None:
			message_data['location'] = self._location.to_dict()
		return message_data

	def from_dict(self, message_data):
		super(LocationMessage, self).from_dict(message_data)
		if 'location' in message_data:
			self._location = Location().from_dict(message_data['location'])
		return self

	@property
	def location(self):
		return self._location

	def validate(self):
		return super(LocationMessage, self).validate() \
				and self._location and self._location.validate()

	@python_2_unicode_compatible
	def __str__(self):
		return u"LocationMessage [{0}, contact={1}]"\
			.format(
				super(LocationMessage, self).__str__(),
				self._location)
