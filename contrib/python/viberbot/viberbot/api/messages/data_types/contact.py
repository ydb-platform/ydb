from future.utils import python_2_unicode_compatible


class Contact(object):
	def __init__(self, name=None, phone_number=None, avatar=None):
		self._name = name
		self._phone_number = phone_number
		self._avatar = avatar

	def from_dict(self, contact):
		if 'name' in contact:
			self._name = contact['name']
		if 'phone_number' in contact:
			self._phone_number = contact['phone_number']
		if 'avatar' in contact:
			self._avatar = contact['avatar']
		return self

	def to_dict(self):
		return {
			'name': self._name,
			'phone_number': self._phone_number,
			'avatar': self._avatar
		}

	@property
	def name(self):
		return self._name

	@property
	def phone_number(self):
		return self._phone_number

	def __eq__(self, other):
		return self._name == other.name and self._phone_number == other.phone_number

	@python_2_unicode_compatible
	def __str__(self):
		return u"Contact[name={0}, phone_number={1}, avatar={2}]".format(self._name, self._phone_number, self._avatar)
