class BotConfiguration(object):
	def __init__(self, auth_token, name, avatar):
		self._auth_token = auth_token
		self._name = name
		self._avatar = avatar

	@property
	def name(self):
		return self._name

	@property
	def avatar(self):
		return self._avatar

	@property
	def auth_token(self):
		return self._auth_token
