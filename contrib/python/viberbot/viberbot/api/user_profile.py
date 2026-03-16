from future.utils import python_2_unicode_compatible


class UserProfile(object):
	def __init__(self, name=None, avatar=None, user_id=None, country=None, language=None, api_version=None):
		self._name = name
		self._avatar = avatar
		self._id = user_id
		self._country = country
		self._language = language
		self._api_version = api_version

	@property
	def name(self):
		return self._name

	@property
	def avatar(self):
		return self._avatar

	@property
	def id(self):
		return self._id

	@property
	def country(self):
		return self._country

	@property
	def language(self):
		return self._language

	@property
	def api_version(self):
		return self._api_version

	def from_dict(self, user_dict):
		if 'name' in user_dict:
			self._name = user_dict['name']
		if 'avatar' in user_dict:
			self._avatar = user_dict['avatar']
		if 'id' in user_dict:
			self._id = user_dict['id']
		if 'country' in user_dict:
			self._country = user_dict['country']
		if 'language' in user_dict:
			self._language = user_dict['language']
		if 'api_version' in user_dict:
			self._api_version = user_dict['api_version']
		return self

	@python_2_unicode_compatible
	def __str__(self):
		return u"UserProfile[name={0}, avatar={1}, id={2}, country={3}, language={4}, api_version={5}"\
			.format(
			self._name,
			self._avatar,
			self._id,
			self._country,
			self._language,
			self._api_version)
