import json
import platform
import requests
import six
import sys

from .version import __version__


class SlackRequest(object):
    def __init__(
            self,
            proxies=None
    ):
        # HTTP configs
        self.custom_user_agent = None
        self.proxies = proxies

        # Construct the user-agent header with the package info, Python version and OS version.
        self.default_user_agent = {
            # __name__ returns all classes, we only want the client
            "client": "{0}/{1}".format(__name__.split('.')[0], __version__),
            "python": "Python/{v.major}.{v.minor}.{v.micro}".format(v=sys.version_info),
            "system": "{0}/{1}".format(platform.system(), platform.release())
        }

    def get_user_agent(self):
        # Check for custom user-agent and append if found
        if self.custom_user_agent:
            custom_ua_list = ["/".join(client_info) for client_info in self.custom_user_agent]
            custom_ua_string = " ".join(custom_ua_list)
            self.default_user_agent['custom'] = custom_ua_string

        # Concatenate and format the user-agent string to be passed into request headers
        ua_string = []
        for key, val in self.default_user_agent.items():
            ua_string.append(val)

        user_agent_string = " ".join(ua_string)
        return user_agent_string

    def append_user_agent(self, name, version):
        if self.custom_user_agent:
            self.custom_user_agent.append([name.replace("/", ":"), version.replace("/", ":")])
        else:
            self.custom_user_agent = [[name, version]]

    def do(self, token=None, request="?", post_data=None, domain="slack.com", timeout=None):
        """
        Perform a POST request to the Slack Web API
        Args:
            token (str): your authentication token
            request (str): the method to call from the Slack API. For example: 'channels.list'
            post_data (dict): key/value arguments to pass for the request. For example:
                {'channel': 'CABC12345'}
            domain (str): if for some reason you want to send your request to something other
                than slack.com
            timeout (float): stop waiting for a response after a given number of seconds
        """
        # Pull `file` out so it isn't JSON encoded like normal fields.
        # Only do this for requests that are UPLOADING files; downloading files
        # use the 'file' argument to point to a File ID.
        post_data = post_data or {}

        # Move singular file objects into `files`
        upload_requests = ['files.upload']

        # Move file content into requests' `files` param
        files = None
        if request in upload_requests:
            files = {'file': post_data.pop('file')} if 'file' in post_data else None

        # Check for plural fields and convert them to comma-separated strings if needed
        for field in {'channels', 'users', 'types'} & set(post_data.keys()):
            if isinstance(post_data[field], list):
                post_data[field] = ",".join(post_data[field])

        # Convert any params which are list-like to JSON strings
        # Example: `attachments` is a dict, and needs to be passed as JSON
        for k, v in six.iteritems(post_data):
            if isinstance(v, (list, dict)):
                post_data[k] = json.dumps(v)

        return self.post_http_request(token, request, post_data, files, timeout, domain)

    def post_http_request(self, token, api_method, post_data,
                          files=None, timeout=None, domain="slack.com"):
        """
        This method build and submits the Web API HTTP request

        :param token: You app's Slack access token
        :param api_method: The API method endpoint to submit the request to
        :param post_data: The request payload
        :param domain: The URL to submit the API request to
        :param files: Any files to be submitted during upload calls
        :param timeout: Stop waiting for a response after a given number of seconds
        :return:
        """
        # Override token header if `token` is passed in post_data
        if post_data is not None and "token" in post_data:
            token = post_data['token']

        # Set user-agent and auth headers
        headers = {
            'user-agent': self.get_user_agent(),
            'Authorization': 'Bearer {}'.format(token)
        }

        # Submit the request
        res = requests.post(
            'https://{0}/api/{1}'.format(domain, api_method),
            headers=headers,
            data=post_data,
            files=files,
            timeout=timeout,
            proxies=self.proxies
        )
        return res
