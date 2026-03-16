from future.utils import python_2_unicode_compatible
from viberbot.api import messages
from viberbot.api.event_type import EventType
from viberbot.api.user_profile import UserProfile
from viberbot.api.viber_requests.viber_request import ViberRequest


class ViberMessageRequest(ViberRequest):
	def __init__(self):
		super(ViberMessageRequest, self).__init__(EventType.MESSAGE)
		self._message = None
		self._sender = None
		self._message_token = None
		self._chat_id = None
		self._reply_type = None
		self._silent = None

	def from_dict(self, request_dict):
		super(ViberMessageRequest, self).from_dict(request_dict)
		self._message = messages.get_message(request_dict['message'])
		self._sender = UserProfile().from_dict(request_dict['sender'])
		self._message_token = request_dict['message_token']
		self._silent = request_dict.get('silent', None)
		self._reply_type = request_dict.get('reply_type', None)
		self._chat_id = request_dict.get('chat_id', None)
		return self

	@property
	def message(self):
		return self._message

	@property
	def sender(self):
		return self._sender

	@property
	def message_token(self):
		return self._message_token

	@property
	def chat_id(self):
		return self._chat_id

	@property
	def reply_type(self):
		return self._reply_type

	@property
	def silent(self):
		return self._silent

	@python_2_unicode_compatible
	def __str__(self):
		return u"ViberMessageRequest [{0}, message_token={1}, sender={2}, " \
			u"message={3}, chat_id={4}, reply_type={5}, silent={6}]" \
			.format(
				super(ViberMessageRequest, self).__str__(),
				self._message_token,
				self._sender,
				self._message,
				self._chat_id,
				self._reply_type,
				self._silent)
