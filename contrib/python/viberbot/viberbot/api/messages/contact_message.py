from future.utils import python_2_unicode_compatible
from viberbot.api.messages.data_types.contact import Contact
from viberbot.api.messages.typed_message import TypedMessage
from viberbot.api.messages.message_type import MessageType


class ContactMessage(TypedMessage):
	def __init__(self, tracking_data=None, keyboard=None, contact=None, min_api_version=None):
		super(ContactMessage, self).__init__(MessageType.CONTACT, tracking_data, keyboard, min_api_version)
		self._contact = contact

	def to_dict(self):
		message_data = super(ContactMessage, self).to_dict()
		if self._contact is not None:
			message_data['contact'] = self._contact.to_dict()
		return message_data

	def from_dict(self, message_data):
		super(ContactMessage, self).from_dict(message_data)
		if 'contact' in message_data:
			self._contact = Contact().from_dict(message_data['contact'])
		return self

	@property
	def contact(self):
		return self._contact

	def validate(self):
		return super(ContactMessage, self).validate() \
				and self._contact is not None \
				and self._contact.name is not None \
				and self._contact.phone_number is not None

	@python_2_unicode_compatible
	def __str__(self):
		return u"ContactMessage [{0}, contact={1}]". \
			format(
				super(ContactMessage, self).__str__(),
				self._contact)
