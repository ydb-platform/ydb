import requests
from requests import RequestException
import traceback
from viberbot.api.consts import BOT_API_ENDPOINT
import json


class ApiRequestSender(object):
	def __init__(self, logger, viber_bot_api_url, bot_configuration, viber_bot_user_agent):
		self._logger = logger
		self._viber_bot_api_url = viber_bot_api_url
		self._bot_configuration = bot_configuration
		self._user_agent = viber_bot_user_agent

	def set_webhook(self, url, webhook_events=None, is_inline=False):
		payload = {
			'auth_token': self._bot_configuration.auth_token,
			'url': url,
			'is_inline': is_inline
		}

		if webhook_events is not None:
			if not isinstance(webhook_events, list):
				webhook_events = [webhook_events]
			payload['event_types'] = webhook_events

		result = self.post_request(
			endpoint=BOT_API_ENDPOINT.SET_WEBHOOK,
			payload=json.dumps(payload))

		if not result['status'] == 0:
			raise Exception(u"failed with status: {0}, message: {1}".format(result['status'], result['status_message']))

		return result['event_types']

	def get_account_info(self):
		payload = {
			'auth_token': self._bot_configuration.auth_token
		}
		return self.post_request(
			endpoint=BOT_API_ENDPOINT.GET_ACCOUNT_INFO,
			payload=json.dumps(payload))

	def post_request(self, endpoint, payload):
		try:
			headers = requests.utils.default_headers()
			headers.update({
				'User-Agent': self._user_agent
			})
			response = requests.post(self._viber_bot_api_url + '/' + endpoint, data=payload, headers=headers)
			response.raise_for_status()
			return json.loads(response.text)
		except RequestException as e:
			self._logger.error(
				u"failed to post request to endpoint={0}, with payload={1}. error is: {2}"
				.format(endpoint, payload, traceback.format_exc()))
			raise e
		except Exception as ex:
			self._logger.error(
				u"unexpected Exception while trying to post request. error is: {0}"
				.format(traceback.format_exc()))
			raise ex

	def get_online_status(self, ids=[]):
		if ids is None or not isinstance(ids, list) or len(ids) == 0:
			raise Exception(u"missing parameter ids, should be a list of viber memberIds")

		payload = {
			'auth_token': self._bot_configuration.auth_token,
			'ids': ids
		}
		result = self.post_request(
			endpoint=BOT_API_ENDPOINT.GET_ONLINE,
			payload=json.dumps(payload))

		if not result['status'] == 0:
			raise Exception(u"failed with status: {0}, message: {1}".format(result['status'], result['status_message']))

		return result['users']

	def get_user_details(self, user_id):
		if user_id is None:
			raise Exception(u"missing parameter id")

		payload = {
			'auth_token': self._bot_configuration.auth_token,
			'id': user_id
		}
		result = self.post_request(
			endpoint=BOT_API_ENDPOINT.GET_USER_DETAILS,
			payload=json.dumps(payload))

		if not result['status'] == 0:
			raise Exception(u"failed with status: {0}, message: {1}".format(result['status'], result['status_message']))

		return result['user']


