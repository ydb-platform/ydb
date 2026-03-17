from .reservoir import Reservoir
from ...exceptions.exceptions import InvalidSamplingManifestError
from aws_xray_sdk.core.utils.search_pattern import wildcard_match


class SamplingRule:
    """
    One SamplingRule represents one rule defined from local rule json file
    or from a dictionary. It can be either a custom rule or default rule.
    """
    FIXED_TARGET = 'fixed_target'
    RATE = 'rate'

    HOST = 'host'
    METHOD = 'http_method'
    PATH = 'url_path'
    SERVICE_NAME = 'service_name'

    def __init__(self, rule_dict, version=2, default=False):
        """
        :param dict rule_dict: The dictionary that defines a single rule.
        :param bool default: Indicates if this is the default rule. A default
            rule cannot have `host`, `http_method` or `url_path`.
        """
        if version == 2:
            self._host_key = self.HOST
        elif version == 1:
            self._host_key = self.SERVICE_NAME

        self._fixed_target = rule_dict.get(self.FIXED_TARGET, None)
        self._rate = rule_dict.get(self.RATE, None)

        self._host = rule_dict.get(self._host_key, None)
        self._method = rule_dict.get(self.METHOD, None)
        self._path = rule_dict.get(self.PATH, None)

        self._default = default

        self._validate()

        self._reservoir = Reservoir(self.fixed_target)

    def applies(self, host, method, path):
        """
        Determines whether or not this sampling rule applies to
        the incoming request based on some of the request's parameters.
        Any None parameters provided will be considered an implicit match.
        """
        return (not host or wildcard_match(self.host, host)) \
            and (not method or wildcard_match(self.method, method)) \
            and (not path or wildcard_match(self.path, path))

    @property
    def fixed_target(self):
        """
        Defines fixed number of sampled segments per second.
        This doesn't count for sampling rate.
        """
        return self._fixed_target

    @property
    def rate(self):
        """
        A float number less than 1.0 defines the sampling rate.
        """
        return self._rate

    @property
    def host(self):
        """
        The host name of the reqest to sample.
        """
        return self._host

    @property
    def method(self):
        """
        HTTP method of the request to sample.
        """
        return self._method

    @property
    def path(self):
        """
        The url path of the request to sample.
        """
        return self._path

    @property
    def reservoir(self):
        """
        Keeps track of used sampled targets within the second.
        """
        return self._reservoir

    @property
    def version(self):
        """
        Keeps track of used sampled targets within the second.
        """
        return self._version

    def _validate(self):
        if self.fixed_target < 0 or self.rate < 0:
            raise InvalidSamplingManifestError('All rules must have non-negative values for '
                                               'fixed_target and rate')

        if self._default:
            if self.host or self.method or self.path:
                raise InvalidSamplingManifestError('The default rule must not specify values for '
                                                   'url_path, %s, or http_method', self._host_key)
        else:
            if not self.host or not self.method or not self.path:
                raise InvalidSamplingManifestError('All non-default rules must have values for '
                                                   'url_path, %s, and http_method', self._host_key)
