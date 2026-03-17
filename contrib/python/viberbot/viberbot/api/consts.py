from ..version import __version__

VIBER_BOT_API_URL = "https://chatapi.viber.com/pa"
VIBER_BOT_USER_AGENT = "ViberBot-Python/" + __version__


class BOT_API_ENDPOINT(object):
	SET_WEBHOOK = 'set_webhook'
	GET_ACCOUNT_INFO = 'get_account_info'
	SEND_MESSAGE = 'send_message'
	GET_ONLINE = 'get_online'
	GET_USER_DETAILS = 'get_user_details'
	POST = 'post'
