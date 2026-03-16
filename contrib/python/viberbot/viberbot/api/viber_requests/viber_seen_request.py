import warnings
from future.utils import python_2_unicode_compatible
from ..event_type import EventType
from viberbot.api.viber_requests.viber_request import ViberRequest


class ViberSeenRequest(ViberRequest):
	def __init__(self):
		super(ViberSeenRequest, self).__init__(EventType.SEEN)
		self._message_token = None
		self._user_id = None

	def from_dict(self, request_dict):
		super(ViberSeenRequest, self).from_dict(request_dict)
		self._message_token = request_dict['message_token']
		self._user_id = request_dict['user_id']
		return self

	@property
	def meesage_token(self):
		warnings.warn('Property `meesage_token` had typo and now is deprecated, please use `message_token` instead')
		return self._message_token

	@property
	def message_token(self):
		return self._message_token

	@property
	def user_id(self):
		return self._user_id

	@python_2_unicode_compatible
	def __str__(self):
		return u"ViberSeenRequest [{0}, message_token={1}, user_id={2}]" \
			.format(super(ViberSeenRequest, self).__str__(), self._message_token, self._user_id)
