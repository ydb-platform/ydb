# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Module for Google Compute Engine Driver.
"""


import sys
import time
import datetime
import itertools

from libcloud.pricing import get_pricing
from libcloud.common.base import LazyObject
from libcloud.common.types import LibcloudError
from libcloud.compute.base import (
    Node,
    NodeSize,
    NodeImage,
    UuidMixin,
    NodeDriver,
    NodeLocation,
    StorageVolume,
    VolumeSnapshot,
)
from libcloud.common.google import (
    GoogleResponse,
    GoogleBaseError,
    ResourceExistsError,
    GoogleBaseConnection,
    ResourceNotFoundError,
    GoogleOAuth2Credential,
)
from libcloud.compute.types import NodeState
from libcloud.utils.iso8601 import parse_date
from libcloud.compute.providers import Provider

API_VERSION = "v1"
DEFAULT_TASK_COMPLETION_TIMEOUT = 180


def timestamp_to_datetime(timestamp):
    """
    Return a datetime object that corresponds to the time in an RFC3339
    timestamp.

    :param  timestamp: RFC3339 timestamp string
    :type   timestamp: ``str``

    :return:  Datetime object corresponding to timestamp
    :rtype:   :class:`datetime.datetime`
    """
    # We remove timezone offset and microseconds (Python 2.5 strptime doesn't
    # support %f)
    ts = datetime.datetime.strptime(timestamp[:-10], "%Y-%m-%dT%H:%M:%S")
    tz_hours = int(timestamp[-5:-3])
    tz_mins = int(timestamp[-2:]) * int(timestamp[-6:-5] + "1")
    tz_delta = datetime.timedelta(hours=tz_hours, minutes=tz_mins)

    return ts + tz_delta


class GCEResponse(GoogleResponse):
    pass


class GCEConnection(GoogleBaseConnection):
    """
    Connection class for the GCE driver.

    GCEConnection extends :class:`google.GoogleBaseConnection` for 3 reasons:
      1. modify request_path for GCE URI.
      2. Implement gce_params functionality described below.
      3. Add request_aggregated_items method for making aggregated API calls.

    If the parameter gce_params is set to a dict prior to calling request(),
    the URL parameters will be updated to include those key/values FOR A
    SINGLE REQUEST. If the response contains a nextPageToken,
    gce_params['pageToken'] will be set to its value. This can be used to
    implement paging in list:

    >>> params, more_results = {'maxResults': 2}, True
    >>> while more_results:
    ...     driver.connection.gce_params=params
    ...     driver.ex_list_urlmaps()
    ...     more_results = 'pageToken' in params
    ...
    [<GCEUrlMap id="..." name="cli-map">, <GCEUrlMap id="..." name="lc-map">]
    [<GCEUrlMap id="..." name="web-map">]
    """

    host = "www.googleapis.com"
    responseCls = GCEResponse

    def __init__(
        self,
        user_id,
        key,
        secure,
        auth_type=None,
        credential_file=None,
        project=None,
        **kwargs,
    ):
        super().__init__(
            user_id,
            key,
            secure=secure,
            auth_type=auth_type,
            credential_file=credential_file,
            **kwargs,
        )
        self.request_path = "/compute/{}/projects/{}".format(API_VERSION, project)
        self.gce_params = None

    def pre_connect_hook(self, params, headers):
        """
        Update URL parameters with values from self.gce_params.

        @inherits: :class:`GoogleBaseConnection.pre_connect_hook`
        """
        params, headers = super().pre_connect_hook(params, headers)

        if self.gce_params:
            params.update(self.gce_params)

        return params, headers

    def paginated_request(self, *args, **kwargs):
        """
        Generic function to create a paginated request to any API call
        not only aggregated or zone ones as request_aggregated_items.

        @inherits: :class:`GoogleBaseConnection.request`
        """
        more_results = True
        items = []
        max_results = kwargs["max_results"] if "max_results" in kwargs else 500
        params = {"maxResults": max_results}

        while more_results:
            self.gce_params = params
            response = self.request(*args, **kwargs)
            items.extend(response.object.get("items", []))
            more_results = "pageToken" in params

        return {"items": items}

    def request(self, *args, **kwargs):
        """
        Perform request then do GCE-specific processing of URL params.

        @inherits: :class:`GoogleBaseConnection.request`
        """
        response = super().request(*args, **kwargs)

        # If gce_params has been set, then update the pageToken with the
        # nextPageToken so it can be used in the next request.

        if self.gce_params:
            if "nextPageToken" in response.object:
                self.gce_params["pageToken"] = response.object["nextPageToken"]
            elif "pageToken" in self.gce_params:
                del self.gce_params["pageToken"]
            self.gce_params = None

        return response

    def request_aggregated_items(self, api_name, zone=None):
        """
        Perform request(s) to obtain all results from 'api_name'.

        This method will make requests to the aggregated 'api_name' until
        all results are received.  It will then, through a helper function,
        combine all results and return a single 'items' dictionary.

        :param    api_name: Name of API to call. Consult API docs
                  for valid names.
        :type     api_name: ``str``

        :param   zone: Optional zone to use.
        :type zone: :class:`GCEZone`

        :return:  dict in the format of the API response.
                  format: { 'items': {'key': {api_name: []}} }
                  ex: { 'items': {'zones/us-central1-a': {disks: []}} }
        :rtype:   ``dict``
        """

        if zone:
            request_path = "/zones/{}/{}".format(zone.name, api_name)
        else:
            request_path = "/aggregated/%s" % (api_name)

        api_responses = []

        params = {"maxResults": 500}
        more_results = True

        while more_results:
            self.gce_params = params
            response = self.request(request_path, method="GET").object

            if "items" in response:
                if zone:
                    # Special case when we are handling pagination for a
                    # specific zone
                    items = response["items"]
                    response["items"] = {"zones/%s" % (zone): {api_name: items}}
                api_responses.append(response)
            more_results = "pageToken" in params

        return self._merge_response_items(api_name, api_responses)

    def _merge_response_items(self, list_name, response_list):
        """
        Take a list of API responses ("item"-portion only) and combine them.

        Helper function to combine multiple aggregated responses into a single
        dictionary that resembles an API response.

        Note: keys that don't have a 'list_name" key (including warnings)
        are omitted.

        :param   list_name: Name of list in dict.  Practically, this is
                          the name of the API called (e.g. 'disks').
        :type    list_name: ``str``

        :param   response_list: list of API responses (e.g. resp['items']).
                                Each entry in the list is the result of a
                                single API call.  Expected format is:
                                [ { items: {
                                             key1: { api_name:[]},
                                             key2: { api_name:[]}
                                           }}, ... ]
        :type    response_list: ``dict``

        :return: dict in the format of:
                 { items: {key: {api_name:[]}, key2: {api_name:[]}} }
                 ex: { items: {
                         'us-east1-a': {'disks': []},
                         'us-east1-b': {'disks': []}
                         }}
        :rtype:  ``dict``
        """
        merged_items = {}

        for resp in response_list:
            # example k would be a zone or region name
            # example v would be { "disks" : [], "otherkey" : "..." }

            for k, v in resp.get("items", {}).items():
                if list_name in v:
                    merged_items.setdefault(k, {}).setdefault(list_name, [])
                    # Combine the list with the existing list.
                    merged_items[k][list_name] += v[list_name]

        return {"items": merged_items}


class GCEList:
    """
    An Iterator that wraps list functions to provide additional features.

    GCE enforces a limit on the number of objects returned by a list operation,
    so users with more than 500 objects of a particular type will need to use
    filter(), page() or both.

    >>> l=GCEList(driver, driver.ex_list_urlmaps)
    >>> for sublist in l.filter('name eq ...-map').page(1):
    ...   sublist
    ...
    [<GCEUrlMap id="..." name="cli-map">]
    [<GCEUrlMap id="..." name="web-map">]

    One can create a GCEList manually, but it's slightly easier to use the
    ex_list() method of :class:`GCENodeDriver`.
    """

    def __init__(self, driver, list_fn, **kwargs):
        """
        :param  driver: An initialized :class:``GCENodeDriver``
        :type   driver: :class:``GCENodeDriver``

        :param  list_fn: A bound list method from :class:`GCENodeDriver`.
        :type   list_fn: ``instancemethod``
        """
        self.driver = driver
        self.list_fn = list_fn
        self.kwargs = kwargs
        self.params = {}

    def __iter__(self):
        list_fn = self.list_fn
        more_results = True

        while more_results:
            self.driver.connection.gce_params = self.params
            yield list_fn(**self.kwargs)
            more_results = "pageToken" in self.params

    def __repr__(self):
        return '<GCEList list="{}" params="{}">'.format(
            self.list_fn.__name__,
            repr(self.params),
        )

    def filter(self, expression):
        """
        Filter results of a list operation.

        GCE supports server-side filtering of resources returned by a list
        operation. Syntax of the filter expression is fully described in the
        GCE API reference doc, but in brief it is::

            FIELD_NAME COMPARISON_STRING LITERAL_STRING

        where FIELD_NAME is the resource's property name, COMPARISON_STRING is
        'eq' or 'ne', and LITERAL_STRING is a regular expression in RE2 syntax.

        >>> for sublist in l.filter('name eq ...-map'):
        ...   sublist
        ...
        [<GCEUrlMap id="..." name="cli-map">, \
                <GCEUrlMap id="..." name="web-map">]

        API reference: https://cloud.google.com/compute/docs/reference/latest/
        RE2 syntax: https://github.com/google/re2/blob/master/doc/syntax.txt

        :param  expression: Filter expression described above.
        :type   expression: ``str``

        :return: This :class:`GCEList` instance
        :rtype:  :class:`GCEList`
        """
        self.params["filter"] = expression

        return self

    def page(self, max_results=500):
        """
        Limit the number of results by each iteration.

        This implements the paging functionality of the GCE list methods and
        returns this GCEList instance so that results can be chained:

        >>> for sublist in GCEList(driver, driver.ex_list_urlmaps).page(2):
        ...   sublist
        ...
        [<GCEUrlMap id="..." name="cli-map">, \
                <GCEUrlMap id="..." name="lc-map">]
        [<GCEUrlMap id="..." name="web-map">]

        :keyword  max_results: Maximum number of results to return per
                               iteration. Defaults to the GCE default of 500.
        :type     max_results: ``int``

        :return: This :class:`GCEList` instance
        :rtype:  :class:`GCEList`
        """
        self.params["maxResults"] = max_results

        return self


class GCELicense(UuidMixin, LazyObject):
    """A GCE License used to track software usage in GCE nodes."""

    def __init__(self, name, project, driver):
        UuidMixin.__init__(self)
        self.id = name
        self.name = name
        self.project = project
        self.driver = driver
        self.charges_use_fee = None  # init in _request
        self.extra = None  # init in _request

        self._request()

    def _request(self):
        # TODO(crunkleton@google.com): create new connection? or make
        # connection thread-safe? Saving, modifying, and restoring
        # driver.connection.request_path is really hacky and thread-unsafe.
        saved_request_path = self.driver.connection.request_path
        try:
            new_request_path = saved_request_path.replace(self.driver.project, self.project)
            self.driver.connection.request_path = new_request_path

            request = "/global/licenses/%s" % self.name
            response = self.driver.connection.request(request, method="GET").object
        except Exception:
            raise
        finally:
            # Restore the connection request_path
            self.driver.connection.request_path = saved_request_path

        self.extra = {
            "selfLink": response.get("selfLink"),
            "kind": response.get("kind"),
        }
        self.charges_use_fee = response["chargesUseFee"]

    def destroy(self):
        raise LibcloudError("Can not destroy a License resource.")

    def __repr__(self):
        return '<GCELicense id="{}" name="{}" charges_use_fee="{}">'.format(
            self.id,
            self.name,
            self.charges_use_fee,
        )


class GCEDiskType(UuidMixin):
    """A GCE DiskType resource."""

    def __init__(self, id, name, zone, driver, extra=None):
        self.id = str(id)
        self.name = name
        self.zone = zone
        self.driver = driver
        self.extra = extra
        UuidMixin.__init__(self)

    def destroy(self):
        raise LibcloudError("Can not destroy a DiskType resource.")

    def __repr__(self):
        return '<GCEDiskType id="{}" name="{}" zone="{}">'.format(
            self.id,
            self.name,
            self.zone,
        )


class GCEAcceleratorType(UuidMixin):
    """A GCE AcceleratorType resource."""

    def __init__(self, id, name, zone, driver, extra=None):
        self.id = str(id)
        self.name = name
        self.zone = zone
        self.driver = driver
        self.extra = extra
        UuidMixin.__init__(self)

    def destroy(self):
        raise LibcloudError("Can not destroy an AcceleratorType resource.")

    def __repr__(self):
        return '<GCEAcceleratorType id="{}" name="{}" zone="{}">'.format(
            self.id,
            self.name,
            self.zone,
        )


class GCEAddress(UuidMixin):
    """A GCE Static address."""

    def __init__(self, id, name, address, region, driver, extra=None):
        self.id = str(id)
        self.name = name
        self.address = address
        self.region = region
        self.driver = driver
        self.extra = extra
        UuidMixin.__init__(self)

    def destroy(self):
        """
        Destroy this address.

        :return: True if successful
        :rtype:  ``bool``
        """

        return self.driver.ex_destroy_address(address=self)

    def __repr__(self):
        return '<GCEAddress id="{}" name="{}" address="{}" region="{}">'.format(
            self.id,
            self.name,
            self.address,
            (hasattr(self.region, "name") and self.region.name or self.region),
        )


class GCEBackend(UuidMixin):
    """A GCE Backend.  Only used for creating Backend Services."""

    def __init__(
        self,
        instance_group,
        balancing_mode="UTILIZATION",
        max_utilization=None,
        max_rate=None,
        max_rate_per_instance=None,
        capacity_scaler=1,
        description=None,
    ):
        if isinstance(instance_group, GCEInstanceGroup):
            self.instance_group = instance_group
        elif isinstance(instance_group, GCEInstanceGroupManager):
            self.instance_group = instance_group.instance_group
        else:
            raise ValueError(
                "instance_group must be of type GCEInstanceGroup"
                "or of type GCEInstanceGroupManager"
            )

        self.instance_group = instance_group
        self.balancing_mode = balancing_mode
        self.max_utilization = max_utilization
        self.max_rate = max_rate
        self.max_rate_per_instance = max_rate_per_instance
        self.capacity_scaler = capacity_scaler

        # 'id' and 'name' aren't actually used or provided by the GCE API.
        # We create them for convenience.
        self.id = self._gen_id()
        self.name = self.id

        self.description = description or self.name
        UuidMixin.__init__(self)

    def _gen_id(self):
        """
        Use the Instance Group information to fill in name and id fields.

        :return: id in the format of:
                 ZONE/instanceGroups/INSTANCEGROUPNAME
                 Ex: us-east1-c/instanceGroups/my-instance-group
        :rtype:  ``str``
        """
        zone_name = self.instance_group.zone.name

        return "{}/instanceGroups/{}".format(zone_name, self.instance_group.name)

    def to_backend_dict(self):
        """
        Returns dict formatted for inclusion in Backend Service Request.

        :return: dict formatted as a list entry for Backend Service 'backend'.
        :rtype: ``dict``
        """
        d = {}
        d["group"] = self.instance_group.extra["selfLink"]

        if self.balancing_mode:
            d["balancingMode"] = self.balancing_mode

        if self.max_utilization:
            d["maxUtilization"] = self.max_utilization

        if self.max_rate:
            d["maxRate"] = self.max_rate

        if self.max_rate_per_instance:
            d["maxRatePerInstance"] = self.max_rate_per_instance

        if self.capacity_scaler:
            d["capacityScaler"] = self.capacity_scaler

        return d

    def __repr__(self):
        return '<GCEBackend instancegroup="{}" balancing_mode="{}">'.format(
            self.id,
            self.balancing_mode,
        )


class GCEBackendService(UuidMixin):
    """A GCE Backend Service."""

    def __init__(
        self,
        id,
        name,
        backends,
        healthchecks,
        port,
        port_name,
        protocol,
        timeout,
        driver,
        extra=None,
    ):
        self.id = str(id)
        self.name = name
        self.backends = backends or []
        self.healthchecks = healthchecks or []
        self.port = port
        self.port_name = port_name
        self.protocol = protocol
        self.timeout = timeout
        self.driver = driver
        self.extra = extra or {}
        UuidMixin.__init__(self)

    def __repr__(self):
        return '<GCEBackendService id="{}" name="{}">'.format(self.id, self.name)

    def destroy(self):
        """
        Destroy this Backend Service.

        :return: True if successful
        :rtype:  ``bool``
        """

        return self.driver.ex_destroy_backendservice(backendservice=self)


class GCEFailedDisk:
    """Dummy Node object for disks that are not created."""

    def __init__(self, name, error, code):
        self.name = name
        self.error = error
        self.code = code

    def __repr__(self):
        return '<GCEFailedDisk name="{}" error_code="{}">'.format(self.name, self.code)


class GCEFailedNode:
    """Dummy Node object for nodes that are not created."""

    def __init__(self, name, error, code):
        self.name = name
        self.error = error
        self.code = code

    def __repr__(self):
        return '<GCEFailedNode name="{}" error_code="{}">'.format(self.name, self.code)


class GCEHealthCheck(UuidMixin):
    """A GCE Http Health Check class."""

    def __init__(
        self,
        id,
        name,
        path,
        port,
        interval,
        timeout,
        unhealthy_threshold,
        healthy_threshold,
        driver,
        extra=None,
    ):
        self.id = str(id)
        self.name = name
        self.path = path
        self.port = port
        self.interval = interval
        self.timeout = timeout
        self.unhealthy_threshold = unhealthy_threshold
        self.healthy_threshold = healthy_threshold
        self.driver = driver
        self.extra = extra or {}
        UuidMixin.__init__(self)

    def destroy(self):
        """
        Destroy this Health Check.

        :return:  True if successful
        :rtype:   ``bool``
        """

        return self.driver.ex_destroy_healthcheck(healthcheck=self)

    def update(self):
        """
        Commit updated healthcheck values.

        :return:  Updated Healthcheck object
        :rtype:   :class:`GCEHealthcheck`
        """

        return self.driver.ex_update_healthcheck(healthcheck=self)

    def __repr__(self):
        return '<GCEHealthCheck id="{}" name="{}" path="{}" port="{}">'.format(
            self.id,
            self.name,
            self.path,
            self.port,
        )


class GCEFirewall(UuidMixin):
    """A GCE Firewall rule class."""

    def __init__(
        self,
        id,
        name,
        allowed,
        denied,
        direction,
        network,
        source_ranges,
        source_tags,
        priority,
        source_service_accounts,
        target_service_accounts,
        target_tags,
        target_ranges,
        driver,
        extra=None,
    ):
        self.id = str(id)
        self.name = name
        self.network = network
        self.allowed = allowed
        self.denied = denied
        self.direction = direction
        self.priority = priority
        self.source_ranges = source_ranges
        self.source_tags = source_tags
        self.source_service_accounts = source_service_accounts
        self.target_tags = target_tags
        self.target_service_accounts = target_service_accounts
        self.target_ranges = target_ranges
        self.driver = driver
        self.extra = extra
        UuidMixin.__init__(self)

    def destroy(self):
        """
        Destroy this firewall.

        :return: True if successful
        :rtype:  ``bool``
        """

        return self.driver.ex_destroy_firewall(firewall=self)

    def update(self):
        """
        Commit updated firewall values.

        :return:  Updated Firewall object
        :rtype:   :class:`GCEFirewall`
        """

        return self.driver.ex_update_firewall(firewall=self)

    def __repr__(self):
        return '<GCEFirewall id="{}" name="{}" network="{}">'.format(
            self.id,
            self.name,
            self.network.name,
        )


class GCEForwardingRule(UuidMixin):
    def __init__(self, id, name, region, address, protocol, targetpool, driver, extra=None):
        self.id = str(id)
        self.name = name
        self.region = region
        self.address = address
        self.protocol = protocol
        # TODO: 'targetpool' should more correctly be 'target' since a
        # forwarding rule's target can be something besides a targetpool
        self.targetpool = targetpool
        self.driver = driver
        self.extra = extra
        UuidMixin.__init__(self)

    def destroy(self):
        """
        Destroy this Forwarding Rule

        :return:  True if successful
        :rtype:   ``bool``
        """

        return self.driver.ex_destroy_forwarding_rule(forwarding_rule=self)

    def __repr__(self):
        return '<GCEForwardingRule id="{}" name="{}" address="{}">'.format(
            self.id,
            self.name,
            self.address,
        )


class GCENodeImage(NodeImage):
    """A GCE Node Image class."""

    def __init__(self, id, name, driver, extra=None):
        super().__init__(id, name, driver, extra=extra)

    def delete(self):
        """
        Delete this image

        :return: True if successful
        :rtype:  ``bool``
        """

        return self.driver.ex_delete_image(image=self)

    def deprecate(self, replacement, state, deprecated=None, obsolete=None, deleted=None):
        """
        Deprecate this image

        :param  replacement: Image to use as a replacement
        :type   replacement: ``str`` or :class: `GCENodeImage`

        :param  state: Deprecation state of this image. Possible values include
                       \'ACTIVE\', \'DELETED\', \'DEPRECATED\' or \'OBSOLETE\'.
        :type   state: ``str``

        :param  deprecated: RFC3339 timestamp to mark DEPRECATED
        :type   deprecated: ``str`` or ``None``

        :param  obsolete: RFC3339 timestamp to mark OBSOLETE
        :type   obsolete: ``str`` or ``None``

        :param  deleted: RFC3339 timestamp to mark DELETED
        :type   deleted: ``str`` or ``None``

        :return: True if successful
        :rtype:  ``bool``
        """

        return self.driver.ex_deprecate_image(
            self, replacement, state, deprecated, obsolete, deleted
        )


class GCESslCertificate(UuidMixin):
    """GCESslCertificate represents the SslCertificate resource."""

    def __init__(self, id, name, certificate, driver, extra, private_key=None, description=None):
        """
        :param  name:  Name of the resource. Provided by the client when the
                       resource is created. The name must be 1-63 characters
                       long, and comply with RFC1035. Specifically, the name
                       must be 1-63 characters long and match the regular
                       expression [a-z]([-a-z0-9]*[a-z0-9])? which means the
                       first character must be a lowercase letter, and all
                       following characters must be a dash, lowercase letter,
                       or digit, except the last character, which cannot be a
                       dash.
        :type   name: ``str``

        :param  certificate:  A local certificate file. The certificate must
                              be in PEM format. The certificate chain must be
                              no greater than 5 certs long. The chain must
                              include at least one intermediate cert.
        :type   certificate: ``str``

        :param  private_key:  A write-only private key in PEM format. Only
                              insert RPCs will include this field.
        :type   private_key: ``str``

        :keyword  description:  An optional description of this resource.
                              Provide this property when you create the
                              resource.
        :type   description: ``str``

        :keyword  driver:  An initialized :class: `GCENodeDriver`
        :type   driver: :class:`:class: `GCENodeDriver``

        :keyword  extra:  A dictionary of extra information.
        :type   extra: ``:class: ``dict````

        """

        self.name = name
        self.certificate = certificate
        self.private_key = private_key
        self.description = description
        self.driver = driver
        self.extra = extra
        UuidMixin.__init__(self)

    def __repr__(self):
        return '<GCESslCertificate name="%s">' % (self.name)

    def destroy(self):
        """
        Destroy this SslCertificate.

        :return:  Return True if successful.
        :rtype: ``bool``
        """

        return self.driver.ex_destroy_sslcertificate(sslcertificate=self)


class GCESubnetwork(UuidMixin):
    """A GCE Subnetwork object class."""

    def __init__(self, id, name, cidr, network, region, driver, extra=None):
        self.id = str(id)
        self.name = name
        self.cidr = cidr
        self.network = network
        self.region = region
        self.driver = driver
        self.extra = extra
        UuidMixin.__init__(self)

    def destroy(self):
        """
        Destroy this subnetwork

        :return: True if successful
        :rtype:  ``bool``
        """

        return self.driver.ex_destroy_subnetwork(self)

    def __repr__(self):
        return '<GCESubnetwork id="%s" name="%s" region="%s" network="%s" ' 'cidr="%s">' % (
            self.id,
            self.name,
            self.region.name,
            self.network.name,
            self.cidr,
        )


class GCENetwork(UuidMixin):
    """A GCE Network object class."""

    def __init__(self, id, name, cidr, driver, extra=None):
        self.id = str(id)
        self.name = name
        self.cidr = cidr
        self.driver = driver
        self.extra = extra
        self.mode = "legacy"
        self.subnetworks = []

        if "mode" in extra and extra["mode"] != "legacy":
            self.mode = extra["mode"]
            self.subnetworks = extra["subnetworks"]
        UuidMixin.__init__(self)

    def destroy(self):
        """
        Destroy this network

        :return: True if successful
        :rtype:  ``bool``
        """

        return self.driver.ex_destroy_network(network=self)

    def __repr__(self):
        return '<GCENetwork id="{}" name="{}" cidr="{}" mode="{}">'.format(
            self.id,
            self.name,
            self.cidr,
            self.mode,
        )


class GCERoute(UuidMixin):
    """A GCE Route object class."""

    def __init__(
        self,
        id,
        name,
        dest_range,
        priority,
        network="default",
        tags=None,
        driver=None,
        extra=None,
    ):
        self.id = str(id)
        self.name = name
        self.dest_range = dest_range
        self.priority = priority
        self.network = network
        self.tags = tags
        self.driver = driver
        self.extra = extra
        UuidMixin.__init__(self)

    def destroy(self):
        """
        Destroy this route

        :return: True if successful
        :rtype:  ``bool``
        """

        return self.driver.ex_destroy_route(route=self)

    def __repr__(self):
        network_name = getattr(self.network, "name", self.network)

        return '<GCERoute id="{}" name="{}" dest_range="{}" network="{}">'.format(
            self.id,
            self.name,
            self.dest_range,
            network_name,
        )


class GCENodeSize(NodeSize):
    """A GCE Node Size (MachineType) class."""

    def __init__(self, id, name, ram, disk, bandwidth, price, driver, extra=None):
        self.extra = extra
        super().__init__(id, name, ram, disk, bandwidth, price, driver, extra=extra)


class GCEProject(UuidMixin):
    """GCE Project information."""

    def __init__(self, id, name, metadata, quotas, driver, extra=None):
        self.id = str(id)
        self.name = name
        self.metadata = metadata
        self.quotas = quotas
        self.driver = driver
        self.extra = extra
        UuidMixin.__init__(self)

    def set_common_instance_metadata(self, metadata=None, force=False):
        """
        Set common instance metadata for the project. Common uses
        are for setting 'sshKeys', or setting a project-wide
        'startup-script' for all nodes (instances).  Passing in
        ``None`` for the 'metadata' parameter will clear out all common
        instance metadata *except* for 'sshKeys'. If you also want to
        update 'sshKeys', set the 'force' parameter to ``True``.

        :param  metadata: Dictionary of metadata. Can be either a standard
                          python dictionary, or the format expected by
                          GCE (e.g. {'items': [{'key': k1, 'value': v1}, ...}]
        :type   metadata: ``dict`` or ``None``

        :param  force: Force update of 'sshKeys'. If force is ``False`` (the
                       default), existing sshKeys will be retained. Setting
                       force to ``True`` will either replace sshKeys if a new
                       a new value is supplied, or deleted if no new value
                       is supplied.
        :type   force: ``bool``

        :return: True if successful
        :rtype:  ``bool``
        """

        return self.driver.ex_set_common_instance_metadata(metadata=metadata, force=force)

    def set_usage_export_bucket(self, bucket, prefix=None):
        """
        Used to retain Compute Engine resource usage, storing the CSV data in
        a Google Cloud Storage bucket. See the
        `docs <https://cloud.google.com/compute/docs/usage-export>`_ for more
        information. Please ensure you have followed the necessary setup steps
        prior to enabling this feature (e.g. bucket exists, ACLs are in place,
        etc.)

        :param  bucket: Name of the Google Cloud Storage bucket. Specify the
                        name in either 'gs://<bucket_name>' or the full URL
                        'https://storage.googleapis.com/<bucket_name>'.
        :type   bucket: ``str``

        :param  prefix: Optional prefix string for all reports.
        :type   prefix: ``str`` or ``None``

        :return: True if successful
        :rtype:  ``bool``
        """

        return self.driver.ex_set_usage_export_bucket(bucket=bucket, prefix=prefix)

    def __repr__(self):
        return '<GCEProject id="{}" name="{}">'.format(self.id, self.name)


class GCERegion(UuidMixin):
    def __init__(self, id, name, status, zones, quotas, deprecated, driver, extra=None):
        self.id = str(id)
        self.name = name
        self.status = status
        self.zones = zones
        self.quotas = quotas
        self.deprecated = deprecated
        self.driver = driver
        self.extra = extra
        UuidMixin.__init__(self)

    def __repr__(self):
        return '<GCERegion id="{}" name="{}", status="{}">'.format(
            self.id,
            self.name,
            self.status,
        )


class GCESnapshot(VolumeSnapshot):
    def __init__(self, id, name, size, status, driver, extra=None, created=None):
        self.status = status
        super().__init__(id, driver, size, extra, created, name=name)


class GCETargetHttpProxy(UuidMixin):
    def __init__(self, id, name, urlmap, driver, extra=None):
        self.id = str(id)
        self.name = name
        self.urlmap = urlmap
        self.driver = driver
        self.extra = extra or {}
        UuidMixin.__init__(self)

    def __repr__(self):
        return '<GCETargetHttpProxy id="{}" name="{}">'.format(self.id, self.name)

    def destroy(self):
        """
        Destroy this Target HTTP Proxy.

        :return:  True if successful
        :rtype:   ``bool``
        """

        return self.driver.ex_destroy_targethttpproxy(targethttpproxy=self)


class GCETargetHttpsProxy(UuidMixin):
    """GCETargetHttpsProxy represents the TargetHttpsProxy resource."""

    def __init__(
        self,
        id,
        name,
        description=None,
        sslcertificates=None,
        urlmap=None,
        driver=None,
        extra=None,
    ):
        """
        :param  name:  Name of the resource. Provided by the client when the
                       resource is created. The name must be 1-63 characters
                       long, and comply with RFC1035. Specifically, the name
                       must be 1-63 characters long and match the regular
                       expression [a-z]([-a-z0-9]*[a-z0-9])? which means the
                       first character must be a lowercase letter, and all
                       following characters must be a dash, lowercase letter,
                       or digit, except the last character, which cannot be a
                       dash.
        :type   name: ``str``

        :param  description:  An optional description of this resource.
                              Provide this property when you create the
                              resource.
        :type   description: ``str``

        :param  sslcertificates:  URLs to SslCertificate resources that are
                                   used to authenticate connections between
                                   users and the load balancer. Currently,
                                   exactly one SSL certificate must be
                                   specified.
        :type   sslcertificates: ``list`` of :class:`GCESslcertificates`

        :param  urlmap:  A fully-qualified or valid partial URL to the
                          UrlMap resource that defines the mapping from URL
                          to the BackendService. For example, the following
                          are all valid URLs for specifying a URL map:   - ht
                          tps://www.googleapis.compute/v1/projects/project/gl
                          obal/urlMaps/url-map  -
                          projects/project/global/urlMaps/url-map  -
                          global/urlMaps/url-map
        :type   urlmap: :class:`GCEUrlMap`

        :keyword  driver:  An initialized :class: `GCENodeDriver`
        :type   driver: :class:`:class: `GCENodeDriver``

        :keyword  extra:  A dictionary of extra information.
        :type   extra: ``:class: ``dict````

        """

        self.name = name
        self.description = description
        self.sslcertificates = sslcertificates
        self.urlmap = urlmap
        self.driver = driver
        self.extra = extra
        UuidMixin.__init__(self)

    def __repr__(self):
        return '<GCETargetHttpsProxy name="%s">' % (self.name)

    def set_sslcertificates(self, sslcertificates):
        """
        Set the SSL Certificates for this TargetHTTPSProxy

        :param  sslcertificates: SSL Certificates to set.
        :type   sslcertificates: ``list`` of :class:`GCESslCertificate`

        :return:  True if successful
        :rtype:   ``bool``
        """

        return self.driver.ex_targethttpsproxy_set_sslcertificates(
            targethttpsproxy=self, sslcertificates=sslcertificates
        )

    def set_urlmap(self, urlmap):
        """
        Changes the URL map for TargetHttpsProxy.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute

        :param  targethttpsproxy:  Name of the TargetHttpsProxy resource
                                   whose URL map is to be set.
        :type   targethttpsproxy: ``str``

        :param  urlmap:  UrlMap to set.
        :type   urlmap: :class:`GCEUrlMap`

        :return:  True
        :rtype: ``bool``
        """

        return self.driver.ex_targethttpsproxy_set_urlmap(targethttpsproxy=self, urlmap=urlmap)

    def destroy(self):
        """
        Destroy this TargetHttpsProxy.

        :return:  Return True if successful.
        :rtype: ``bool``
        """

        return self.driver.ex_destroy_targethttpsproxy(targethttpsproxy=self)


class GCETargetInstance(UuidMixin):
    def __init__(self, id, name, zone, node, driver, extra=None):
        self.id = str(id)
        self.name = name
        self.zone = zone
        self.node = node
        self.driver = driver
        self.extra = extra
        UuidMixin.__init__(self)

    def destroy(self):
        """
        Destroy this Target Instance

        :return:  True if successful
        :rtype:   ``bool``
        """

        return self.driver.ex_destroy_targetinstance(targetinstance=self)

    def __repr__(self):
        return '<GCETargetInstance id="{}" name="{}" zone="{}" node="{}">'.format(
            self.id,
            self.name,
            self.zone.name,
            (hasattr(self.node, "name") and self.node.name or self.node),
        )


class GCEAutoscaler(UuidMixin):
    """Represents a autoscaling policy object used to scale Instance Groups."""

    def __init__(self, id, name, zone, target, policy, driver, extra=None):
        self.id = str(id)
        self.name = name
        self.zone = zone
        self.target = target
        self.policy = policy
        self.driver = driver
        self.extra = extra
        UuidMixin.__init__(self)

    def destroy(self):
        """
        Destroy this Autoscaler.

        :return:  True if successful
        :rtype:   ``bool``
        """

        return self.driver.ex_destroy_autoscaler(autoscaler=self)

    def __repr__(self):
        return '<GCEAutoScaler id="{}" name="{}" zone="{}" target="{}">'.format(
            self.id,
            self.name,
            self.zone.name,
            self.target.name,
        )


class GCEInstanceTemplate(UuidMixin):
    """Represents a machine configuration used in creating Instance Groups."""

    def __init__(self, id, name, driver, extra=None):
        self.id = str(id)
        self.name = name
        self.driver = driver
        self.extra = extra
        UuidMixin.__init__(self)

    def __repr__(self):
        return '<GCEInstanceTemplate id="{}" name="{}" machineType="{}">'.format(
            self.id,
            self.name,
            self.extra["properties"].get("machineType", "UNKNOWN"),
        )

    def destroy(self):
        """
        Destroy this InstanceTemplate.

        :return:  Return True if successful.
        :rtype: ``bool``
        """

        return self.driver.ex_destroy_instancetemplate(instancetemplate=self)


class GCEInstanceGroup(UuidMixin):
    """GCEInstanceGroup represents the InstanceGroup resource."""

    def __init__(
        self,
        id,
        name,
        zone,
        driver,
        extra=None,
        network=None,
        subnetwork=None,
        named_ports=None,
    ):
        """
        :param  name:  Required. The name of the instance group. The name
                       must be 1-63 characters long, and comply with RFC1035.
        :type   name: ``str``

        :param  zone:  The URL of the zone where the instance group is
                       located.
        :type   zone: :class:`GCEZone`

        :param  network:  The URL of the network to which all instances in
                          the instance group belong.
        :type   network: :class:`GCENetwork`

        :param  subnetwork:  The URL of the subnetwork to which all instances
                             in the instance group belong.
        :type   subnetwork: :class:`GCESubnetwork`

        :param  named_ports:  Assigns a name to a port number. For example:
                              {name: "http", port: 80}  This allows the
                              system to reference ports by the assigned name
                              instead of a port number. Named ports can also
                              contain multiple ports. For example: [{name:
                              "http", port: 80},{name: "http", port: 8080}]
                              Named ports apply to all instances in this
                              instance group.
        :type   named_ports: ``"<type 'list'>"``

        """

        self.name = name
        self.zone = zone
        self.network = network
        self.subnetwork = subnetwork
        self.named_ports = named_ports
        self.driver = driver
        self.extra = extra
        UuidMixin.__init__(self)

    def __repr__(self):
        return '<GCEInstanceGroup name="{}" zone="{}">'.format(self.name, self.zone.name)

    def destroy(self):
        """
        Destroy this InstanceGroup.

        :return:  Return True if successful.
        :rtype: ``bool``
        """

        return self.driver.ex_destroy_instancegroup(instancegroup=self)

    def add_instances(self, node_list):
        """
        Adds a list of instances to the specified instance group. All of the
        instances in the instance group must be in the same
        network/subnetwork. Read  Adding instances for more information.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute

        :param  instancegroup:  The Instance Group where you are
                                adding instances.
        :type   instancegroup: :class:``GCEInstanceGroup``

        :param  node_list: List of nodes to add.
        :type   node_list: ``list`` of :class:`Node` or ``list`` of
                           :class:`GCENode`

        :return:  Return True if successful.
        :rtype: ``bool``
        """

        return self.driver.ex_instancegroup_add_instances(instancegroup=self, node_list=node_list)

    def list_instances(self):
        """
        Lists the instances in the specified instance group.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute
        * https://www.googleapis.com/auth/compute.readonly

        :return:  List of :class:`GCENode` objects.
        :rtype: ``list`` of :class:`GCENode` objects.
        """

        return self.driver.ex_instancegroup_list_instances(instancegroup=self)

    def remove_instances(self, node_list):
        """
        Removes one or more instances from the specified instance group,
        but does not delete those instances.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute

        :param  instancegroup:  The Instance Group where you are
                                removng instances.
        :type   instancegroup: :class:``GCEInstanceGroup``

        :param  node_list: List of nodes to add.
        :type   node_list: ``list`` of :class:`Node` or ``list`` of
                           :class:`GCENode`

        :return:  Return True if successful.
        :rtype: ``bool``
        """

        return self.driver.ex_instancegroup_remove_instances(
            instancegroup=self, node_list=node_list
        )

    def set_named_ports(self, named_ports):
        """
        Sets the named ports for the specified instance group.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute

        :param  named_ports:  Assigns a name to a port number. For example:
                              {name: "http", port: 80}  This allows the
                              system to reference ports by the assigned name
                              instead of a port number. Named ports can also
                              contain multiple ports. For example: [{name:
                              "http", port: 80},{name: "http", port: 8080}]
                              Named ports apply to all instances in this
                              instance group.
        :type   named_ports: ``list`` of {'name': ``str``, 'port`: ``int``}

        :return:  Return True if successful.
        :rtype: ``bool``
        """

        return self.driver.ex_instancegroup_set_named_ports(
            instancegroup=self, named_ports=named_ports
        )


class GCEInstanceGroupManager(UuidMixin):
    """
    GCE Instance Groups Manager class.

    Handles 'managed' Instance Groups.
    For more information on Instance Groups, see:
    https://cloud.google.com/compute/docs/instance-groups
    """

    def __init__(self, id, name, zone, size, template, instance_group, driver, extra=None):
        """
        :param  id: Internal identifier of Instance Group.  Display only.
        :type   id: ``str``

        :param  name: The name of this Instance Group.
        :type   name: ``str``

        :param  zone: Zone in witch the Instance Group belongs
        :type   zone: :class: ``GCEZone``

        :param  size: Number of instances in this Instance Group.
        :type   size: ``int``

        :param  template: An initialized :class:``GCEInstanceTemplate``
        :type   driver: :class:``GCEInstanceTemplate``

        :param  instance_group: An initialized :class:``GCEInstanceGroup``
        :type   driver: :class:``GCEInstanceGroup``

        :param  driver: An initialized :class:``GCENodeDriver``
        :type   driver: :class:``GCENodeDriver``

        :param  extra: A dictionary of extra information.
        :type   extra: ``dict``
        """
        self.id = str(id)
        self.name = name
        self.zone = zone
        self.size = size or 0
        self.template = template
        self.instance_group = instance_group
        self.driver = driver
        self.extra = extra
        UuidMixin.__init__(self)

    def destroy(self):
        """
        Destroy this Instance Group.  Destroys all instances managed by the
        Instance Group.

        :return:  True if successful
        :rtype:   ``bool``
        """

        return self.driver.ex_destroy_instancegroupmanager(manager=self)

    def list_managed_instances(self):
        """
        Lists all of the instances in this managed instance group.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute
        * https://www.googleapis.com/auth/compute.readonly

        :return:  ``list`` of ``dict`` containing instance URI and
                  currentAction. See
                  ex_instancegroupmanager_list_managed_instances for
                  more details.
        :rtype: ``list``
        """

        return self.driver.ex_instancegroupmanager_list_managed_instances(manager=self)

    def set_instancetemplate(self, instancetemplate):
        """
        Set the Instance Template for this Instance Group.

        :param  instancetemplate: Instance Template to set.
        :type   instancetemplate: :class:`GCEInstanceTemplate`

        :return:  True if successful
        :rtype:   ``bool``
        """

        return self.driver.ex_instancegroupmanager_set_instancetemplate(
            manager=self, instancetemplate=instancetemplate
        )

    def recreate_instances(self):
        """
        Recreate instances in a Managed Instance Group.

        :return:  ``list`` of ``dict`` containing instance URI and
                  currentAction. See
                  ex_instancegroupmanager_list_managed_instances for
                  more details.
        :rtype: ``list``
        """

        return self.driver.ex_instancegroupmanager_recreate_instances(manager=self)

    def delete_instances(self, node_list):
        """
        Removes one or more instances from the specified instance group,
        and delete those instances.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute

        :param  node_list: List of nodes to delete.
        :type   node_list: ``list`` of :class:`Node` or ``list`` of
                           :class:`GCENode`

        :return:  Return True if successful.
        :rtype: ``bool``
        """

        return self.driver.ex_instancegroupmanager_delete_instances(
            manager=self, node_list=node_list
        )

    def resize(self, size):
        """
        Set the number of instances for this Instance Group.  An increase in
        num_instances will result in VMs being created.  A decrease will result
        in VMs being destroyed.

        :param  size: Number to instances to resize to.
        :type   size: ``int``

        :return:  True if successful
        :rtype:   ``bool``
        """

        return self.driver.ex_instancegroupmanager_resize(manager=self, size=size)

    def set_named_ports(self, named_ports):
        """
        Sets the named ports for the instance group controlled by this manager.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute

        :param  named_ports:  Assigns a name to a port number. For example:
                              {name: "http", port: 80}  This allows the
                              system to reference ports by the assigned name
                              instead of a port number. Named ports can also
                              contain multiple ports. For example: [{name:
                              "http", port: 80},{name: "http", port: 8080}]
                              Named ports apply to all instances in this
                              instance group.
        :type   named_ports: ``list`` of {'name': ``str``, 'port`: ``int``}

        :return:  Return True if successful.
        :rtype: ``bool``
        """

        return self.driver.ex_instancegroup_set_named_ports(
            instancegroup=self.instance_group, named_ports=named_ports
        )

    def set_autohealingpolicies(self, healthcheck, initialdelaysec):
        """
        Sets the autohealing policies for the instance for the instance group
        controlled by this manager.

        :param  healthcheck: Healthcheck to add
        :type   healthcheck: :class:`GCEHealthCheck`

        :param  initialdelaysec:  The time to allow an instance to boot and
                                  applications to fully start before the first
                                  health check
        :type   initialdelaysec:  ``int``

        :return:  Return True if successful.
        :rtype: ``bool``
        """

        return self.driver.ex_instancegroupmanager_set_autohealingpolicies(
            manager=self, healthcheck=healthcheck, initialdelaysec=initialdelaysec
        )

    def __repr__(self):
        return '<GCEInstanceGroupManager name="%s" zone="%s" size="%d">' % (
            self.name,
            self.zone.name,
            self.size,
        )


class GCETargetPool(UuidMixin):
    def __init__(self, id, name, region, healthchecks, nodes, driver, extra=None):
        self.id = str(id)
        self.name = name
        self.region = region
        self.healthchecks = healthchecks
        self.nodes = nodes
        self.driver = driver
        self.extra = extra
        UuidMixin.__init__(self)

    def add_node(self, node):
        """
        Add a node to this target pool.

        :param  node: Node to add
        :type   node: ``str`` or :class:`Node`

        :return:  True if successful
        :rtype:   ``bool``
        """

        return self.driver.ex_targetpool_add_node(targetpool=self, node=node)

    def remove_node(self, node):
        """
        Remove a node from this target pool.

        :param  node: Node to remove
        :type   node: ``str`` or :class:`Node`

        :return:  True if successful
        :rtype:   ``bool``
        """

        return self.driver.ex_targetpool_remove_node(targetpool=self, node=node)

    def add_healthcheck(self, healthcheck):
        """
        Add a healthcheck to this target pool.

        :param  healthcheck: Healthcheck to add
        :type   healthcheck: ``str`` or :class:`GCEHealthCheck`

        :return:  True if successful
        :rtype:   ``bool``
        """

        return self.driver.ex_targetpool_add_healthcheck(targetpool=self, healthcheck=healthcheck)

    def remove_healthcheck(self, healthcheck):
        """
        Remove a healthcheck from this target pool.

        :param  healthcheck: Healthcheck to remove
        :type   healthcheck: ``str`` or :class:`GCEHealthCheck`

        :return:  True if successful
        :rtype:   ``bool``
        """

        return self.driver.ex_targetpool_remove_healthcheck(
            targetpool=self, healthcheck=healthcheck
        )

    def set_backup_targetpool(self, backup_targetpool, failover_ratio=0.1):
        """
        Set a backup targetpool.

        :param  backup_targetpool: The existing targetpool to use for
                                   failover traffic.
        :type   backup_targetpool: :class:`GCETargetPool`

        :param  failover_ratio: The percentage of healthy VMs must fall at or
                                below this value before traffic will be sent
                                to the backup targetpool (default 0.10)
        :type   failover_ratio: ``float``

        :return:  True if successful
        :rtype:   ``bool``
        """

        return self.driver.ex_targetpool_set_backup_targetpool(
            targetpool=self,
            backup_targetpool=backup_targetpool,
            failover_ratio=failover_ratio,
        )

    def get_health(self, node=None):
        """
        Return a hash of target pool instances and their health.

        :param  node: Optional node to specify if only a specific node's
                      health status should be returned
        :type   node: ``str``, ``Node``, or ``None``

        :return: List of hashes of nodes and their respective health
        :rtype:  ``list`` of ``dict``
        """

        return self.driver.ex_targetpool_get_health(targetpool=self, node=node)

    def destroy(self):
        """
        Destroy this Target Pool

        :return:  True if successful
        :rtype:   ``bool``
        """

        return self.driver.ex_destroy_targetpool(targetpool=self)

    def __repr__(self):
        return '<GCETargetPool id="{}" name="{}" region="{}">'.format(
            self.id,
            self.name,
            self.region.name,
        )


class GCEUrlMap(UuidMixin):
    """A GCE URL Map."""

    def __init__(
        self,
        id,
        name,
        default_service,
        host_rules,
        path_matchers,
        tests,
        driver,
        extra=None,
    ):
        self.id = str(id)
        self.name = name
        self.default_service = default_service
        self.host_rules = host_rules or []
        self.path_matchers = path_matchers or []
        self.tests = tests or []
        self.driver = driver
        self.extra = extra or {}
        UuidMixin.__init__(self)

    def __repr__(self):
        return '<GCEUrlMap id="{}" name="{}">'.format(self.id, self.name)

    def destroy(self):
        """
        Destroy this URL Map

        :return:  True if successful
        :rtype:   ``bool``
        """

        return self.driver.ex_destroy_urlmap(urlmap=self)


class GCEZone(NodeLocation):
    """Subclass of NodeLocation to provide additional information."""

    def __init__(self, id, name, status, maintenance_windows, deprecated, driver, extra=None):
        self.status = status
        self.maintenance_windows = maintenance_windows
        self.deprecated = deprecated
        self.extra = extra
        country = name.split("-")[0]
        super().__init__(id=str(id), name=name, country=country, driver=driver, extra=extra)

    @property
    def time_until_mw(self):
        """
        Returns the time until the next Maintenance Window as a
        datetime.timedelta object.
        """

        return self._get_time_until_mw()

    @property
    def next_mw_duration(self):
        """
        Returns the duration of the next Maintenance Window as a
        datetime.timedelta object.
        """

        return self._get_next_mw_duration()

    def _now(self):
        """
        Returns current UTC time.

        Can be overridden in unittests.
        """

        return datetime.datetime.utcnow()

    def _get_next_maint(self):
        """
        Returns the next Maintenance Window.

        :return:  A dictionary containing maintenance window info (or None if
                  no maintenance windows are scheduled)
                  The dictionary contains 4 keys with values of type ``str``
                      - name: The name of the maintenance window
                      - description: Description of the maintenance window
                      - beginTime: RFC3339 Timestamp
                      - endTime: RFC3339 Timestamp
        :rtype:   ``dict`` or ``None``
        """
        begin = None
        next_window = None

        if not self.maintenance_windows:
            return None

        if len(self.maintenance_windows) == 1:
            return self.maintenance_windows[0]

        for mw in self.maintenance_windows:
            begin_next = timestamp_to_datetime(mw["beginTime"])

            if (not begin) or (begin_next < begin):
                begin = begin_next
                next_window = mw

        return next_window

    def _get_time_until_mw(self):
        """
        Returns time until next maintenance window.

        :return:  Time until next maintenance window (or None if no
                  maintenance windows are scheduled)
        :rtype:   :class:`datetime.timedelta` or ``None``
        """
        next_window = self._get_next_maint()

        if not next_window:
            return None
        now = self._now()
        next_begin = timestamp_to_datetime(next_window["beginTime"])

        return next_begin - now

    def _get_next_mw_duration(self):
        """
        Returns the duration of the next maintenance window.

        :return:  Duration of next maintenance window (or None if no
                  maintenance windows are scheduled)
        :rtype:   :class:`datetime.timedelta` or ``None``
        """
        next_window = self._get_next_maint()

        if not next_window:
            return None
        next_begin = timestamp_to_datetime(next_window["beginTime"])
        next_end = timestamp_to_datetime(next_window["endTime"])

        return next_end - next_begin

    def __repr__(self):
        return '<GCEZone id="{}" name="{}" status="{}">'.format(
            self.id,
            self.name,
            self.status,
        )


class GCENodeDriver(NodeDriver):
    """
    GCE Node Driver class.

    This is the primary driver for interacting with Google Compute Engine.  It
    contains all of the standard libcloud methods, plus additional ex_* methods
    for more features.

    Note that many methods allow either objects or strings (or lists of
    objects/strings).  In most cases, passing strings instead of objects will
    result in additional GCE API calls.
    """

    connectionCls = GCEConnection
    api_name = "google"
    name = "Google Compute Engine"
    type = Provider.GCE
    website = "https://cloud.google.com/"
    features = {"create_node": ["ssh_key"]}

    # Google Compute Engine node states are mapped to Libcloud node states
    # per the following dict. GCE does not have an actual 'stopped' state
    # but instead uses a 'terminated' state to indicate the node exists
    # but is not running. In order to better match libcloud, GCE maps this
    # 'terminated' state to 'STOPPED'.
    # Also, when a node is deleted from GCE, it no longer exists and instead
    # will result in a ResourceNotFound error versus returning a placeholder
    # node in a 'terminated' state.
    # For more details, please see GCE's docs,
    # https://cloud.google.com/compute/docs/instances#checkmachinestatus
    NODE_STATE_MAP = {
        "PROVISIONING": NodeState.PENDING,
        "STAGING": NodeState.PENDING,
        "RUNNING": NodeState.RUNNING,
        "STOPPING": NodeState.PENDING,
        "SUSPENDED": NodeState.SUSPENDED,
        "TERMINATED": NodeState.STOPPED,
        "UNKNOWN": NodeState.UNKNOWN,
    }

    AUTH_URL = "https://www.googleapis.com/auth/"
    SA_SCOPES_MAP = {
        # list derived from 'gcloud compute instances create --help'
        "bigquery": "bigquery",
        "cloud-platform": "cloud-platform",
        "compute-ro": "compute.readonly",
        "compute-rw": "compute",
        "datastore": "datastore",
        "logging-write": "logging.write",
        "monitoring": "monitoring",
        "monitoring-write": "monitoring.write",
        "service-control": "servicecontrol",
        "service-management": "service.management",
        "sql": "sqlservice",
        "sql-admin": "sqlservice.admin",
        "storage-full": "devstorage.full_control",
        "storage-ro": "devstorage.read_only",
        "storage-rw": "devstorage.read_write",
        "taskqueue": "taskqueue",
        "useraccounts-ro": "cloud.useraccounts.readonly",
        "useraccounts-rw": "cloud.useraccounts",
        "userinfo-email": "userinfo.email",
    }

    # data taken from https://cloud.google.com/compute/docs/images
    IMAGE_PROJECTS = {
        "centos-cloud": ["centos-6", "centos-7", "centos-8"],
        "cos-cloud": ["cos-beta", "cos-dev", "cos-stable"],
        "coreos-cloud": ["coreos-alpha", "coreos-beta", "coreos-stable"],
        "debian-cloud": ["debian-8", "debian-9", "debian-10"],
        "opensuse-cloud": ["opensuse-leap"],
        "rhel-cloud": ["rhel-6", "rhel-7", "rhel-8"],
        "suse-cloud": ["sles-11", "sles-12", "sles-15"],
        "suse-byos-cloud": [
            "sles-11-byos",
            "sles-12-byos",
            "sles-12-sp2-sap-byos",
            "sles-12-sp3-sap-byos",
            "suse-manager-proxy-byos",
            "suse-manager-server-byos",
        ],
        "suse-sap-cloud": [
            "sles-12-sp2-sap",
            "sles-12-sp3-sap",
            "sles-12-sp4-sap",
            "sles-15-sap",
        ],
        "ubuntu-os-cloud": [
            "ubuntu-1404-lts",
            "ubuntu-1604-lts",
            "ubuntu-minimal-1604-lts",
            "ubuntu-1710",
            "ubuntu-1804-lts",
            "ubuntu-minimal-1804-lts",
            "ubuntu-1810",
            "ubuntu-minimal-1810",
            "ubuntu-1904",
            "ubuntu-minimal-1904",
            "ubuntu-1910",
            "ubuntu-minimal-1910",
            "ubuntu-2004-lts",
            "ubuntu-minimal-2004-lts",
        ],
        "windows-cloud": [
            "windows-1709-core-for-containers",
            "windows-1709-core",
            "windows-2008-r2",
            "windows-2012-r2-core",
            "windows-2012-r2",
            "windows-2016-core",
            "windows-2016",
        ],
        "windows-sql-cloud": [
            "sql-ent-2012-win-2012-r2",
            "sql-std-2012-win-2012-r2",
            "sql-web-2012-win-2012-r2",
            "sql-ent-2014-win-2012-r2",
            "sql-ent-2014-win-2016",
            "sql-std-2014-win-2012-r2",
            "sql-web-2014-win-2012-r2",
            "sql-ent-2016-win-2012-r2",
            "sql-ent-2016-win-2016",
            "sql-std-2016-win-2012-r2",
            "sql-std-2016-win-2016",
            "sql-web-2016-win-2012-r2",
            "sql-web-2016-win-2016",
            "sql-ent-2017-win-2016",
            "sql-exp-2017-win-2012-r2",
            "sql-exp-2017-win-2016",
            "sql-std-2017-win-2016",
            "sql-web-2017-win-2016",
        ],
    }

    BACKEND_SERVICE_PROTOCOLS = ["HTTP", "HTTPS", "HTTP2", "TCP", "SSL"]

    def __init__(
        self,
        user_id,
        key=None,
        datacenter=None,
        project=None,
        auth_type=None,
        scopes=None,
        credential_file=None,
        **kwargs,
    ):
        """
        :param  user_id: The email address (for service accounts) or Client ID
                         (for installed apps) to be used for authentication.
        :type   user_id: ``str``

        :param  key: The RSA Key (for service accounts) or file path containing
                     key or Client Secret (for installed apps) to be used for
                     authentication.
        :type   key: ``str``

        :keyword  datacenter: The name of the datacenter (zone) used for
                              operations.
        :type     datacenter: ``str``

        :keyword  project: Your GCE project name. (required)
        :type     project: ``str``

        :keyword  auth_type: Accepted values are "SA" or "IA" or "GCE"
                             ("Service Account" or "Installed Application" or
                             "GCE" if libcloud is being used on a GCE instance
                             with service account enabled).
                             If not supplied, auth_type will be guessed based
                             on value of user_id or if the code is being
                             executed in a GCE instance.
        :type     auth_type: ``str``

        :keyword  scopes: List of authorization URLs. Default is empty and
                          grants read/write to Compute, Storage, DNS.
        :type     scopes: ``list``

        :keyword  credential_file: Path to file for caching authentication
                                   information used by GCEConnection.
        :type     credential_file: ``str``
        """

        if not project:
            raise ValueError("Project name must be specified using " '"project" keyword.')

        self.auth_type = auth_type
        self.project = project
        self.scopes = scopes
        self.credential_file = (
            credential_file or GoogleOAuth2Credential.default_credential_file + "." + self.project
        )

        super().__init__(user_id, key, **kwargs)

        # Cache Zone and Region information to reduce API calls and
        # increase speed
        self.base_path = "/compute/{}/projects/{}".format(API_VERSION, self.project)

        self._zone_dict = None
        self._zone_list = None

        if datacenter:
            self.zone = self.ex_get_zone(datacenter)
        else:
            self.zone = None

        self._region_dict = None
        self._region_list = None

        if self.zone:
            self.region = self._get_region_from_zone(self.zone)
        else:
            self.region = None

        # Volume details are looked up in this name-zone dict.
        # It is populated if the volume name is not found or the dict is empty.
        self._ex_volume_dict = {}

    @property
    def zone_dict(self):
        if self._zone_dict is None:
            zones = self.ex_list_zones()
            self._zone_dict = {zone.name: zone for zone in zones}

        return self._zone_dict

    @property
    def zone_list(self):
        if self._zone_list is None:
            self._zone_list = list(self.zone_dict.values())

        return self._zone_list

    @property
    def region_dict(self):
        if self._region_dict is None:
            regions = self.ex_list_regions()
            self._region_dict = {region.name: region for region in regions}

        return self._region_dict

    @property
    def region_list(self):
        if self._region_list is None:
            self._region_list = list(self.region_dict.values())

        return self._region_list

    def ex_add_access_config(self, node, name, nic, nat_ip=None, config_type=None):
        """
        Add a network interface access configuration to a node.

        :keyword  node: The existing target Node (instance) that will receive
                        the new access config.
        :type     node: ``Node``

        :keyword  name: Name of the new access config.
        :type     node: ``str``

        :keyword  nat_ip: The external existing static IP Address to use for
                          the access config. If not provided, an ephemeral
                          IP address will be allocated.
        :type     nat_ip: ``str`` or ``None``

        :keyword  config_type: The type of access config to create. Currently
                               the only supported type is 'ONE_TO_ONE_NAT'.
        :type     config_type: ``str`` or ``None``

        :return: True if successful
        :rtype:  ``bool``
        """

        if not isinstance(node, Node):
            raise ValueError("Must specify a valid libcloud node object.")
        node_name = node.name
        zone_name = node.extra["zone"].name

        config = {"name": name}

        if config_type is None:
            config_type = "ONE_TO_ONE_NAT"
        config["type"] = config_type

        if nat_ip is not None:
            config["natIP"] = nat_ip
        params = {"networkInterface": nic}
        request = "/zones/{}/instances/{}/addAccessConfig".format(zone_name, node_name)
        self.connection.async_request(request, method="POST", data=config, params=params)

        return True

    def ex_delete_access_config(self, node, name, nic):
        """
        Delete a network interface access configuration from a node.

        :keyword  node: The existing target Node (instance) for the request.
        :type     node: ``Node``

        :keyword  name: Name of the access config.
        :type     name: ``str``

        :keyword  nic: Name of the network interface.
        :type     nic: ``str``

        :return: True if successful
        :rtype:  ``bool``
        """

        if not isinstance(node, Node):
            raise ValueError("Must specify a valid libcloud node object.")
        node_name = node.name
        zone_name = node.extra["zone"].name

        params = {"accessConfig": name, "networkInterface": nic}
        request = "/zones/{}/instances/{}/deleteAccessConfig".format(zone_name, node_name)
        self.connection.async_request(request, method="POST", params=params)

        return True

    def ex_set_node_metadata(self, node, metadata):
        """
        Set metadata for the specified node.

        :keyword  node: The existing target Node (instance) for the request.
        :type     node: ``Node``

        :keyword  metadata: Set (or clear with None) metadata for this
                            particular node.
        :type     metadata: ``dict`` or ``None``

        :return: True if successful
        :rtype:  ``bool``
        """

        if not isinstance(node, Node):
            raise ValueError("Must specify a valid libcloud node object.")
        node_name = node.name
        zone_name = node.extra["zone"].name

        if "metadata" in node.extra and "fingerprint" in node.extra["metadata"]:
            current_fp = node.extra["metadata"]["fingerprint"]
        else:
            current_fp = "absent"
        body = self._format_metadata(current_fp, metadata)
        request = "/zones/{}/instances/{}/setMetadata".format(zone_name, node_name)
        self.connection.async_request(request, method="POST", data=body)

        return True

    def ex_set_node_labels(self, node, labels):
        """
        Set labels for the specified node.

        :keyword  node: The existing target Node (instance) for the request.
        :type     node: ``Node``

        :keyword  labels: Set (or clear with None) labels for this node.
        :type     labels: ``dict`` or ``None``

        :return: True if successful
        :rtype:  ``bool``
        """

        if not isinstance(node, Node):
            raise ValueError("Must specify a valid libcloud node object.")
        node_name = node.name
        zone_name = node.extra["zone"].name
        current_fp = node.extra["labelFingerprint"]
        body = {"labels": labels, "labelFingerprint": current_fp}
        request = "/zones/{}/instances/{}/setLabels".format(zone_name, node_name)
        self.connection.async_request(request, method="POST", data=body)

        return True

    def ex_set_image_labels(self, image, labels):
        """
        Set labels for the specified image.

        :keyword  image: The existing target Image for the request.
        :type     image: ``NodeImage``

        :keyword  labels: Set (or clear with None) labels for this image.
        :type     labels: ``dict`` or ``None``

        :return: True if successful
        :rtype:  ``bool``
        """

        if not isinstance(image, NodeImage):
            raise ValueError("Must specify a valid libcloud image object.")
        current_fp = image.extra["labelFingerprint"]
        body = {"labels": labels, "labelFingerprint": current_fp}
        request = "/global/images/%s/setLabels" % (image.name)
        self.connection.async_request(request, method="POST", data=body)

        return True

    def ex_set_volume_labels(self, volume, labels):
        """
        Set labels for the specified volume (disk).

        :keyword  volume: The existing target StorageVolume for the request.
        :type     volume: ``StorageVolume``

        :keyword  labels: Set (or clear with None) labels for this image.
        :type     labels: ``dict`` or ``None``

        :return: True if successful
        :rtype:  ``bool``
        """

        if not isinstance(volume, StorageVolume):
            raise ValueError("Must specify a valid libcloud volume object.")

        volume_name = volume.name
        zone_name = volume.extra["zone"].name
        current_fp = volume.extra["labelFingerprint"]
        body = {"labels": labels, "labelFingerprint": current_fp}
        request = "/zones/{}/disks/{}/setLabels".format(zone_name, volume_name)
        self.connection.async_request(request, method="POST", data=body)

        return True

    def ex_get_serial_output(self, node):
        """
        Fetch the console/serial port output from the node.

        :keyword  node: The existing target Node (instance) for the request.
        :type     node: ``Node``

        :return: A string containing serial port output of the node.
        :rtype:  ``str``
        """

        if not isinstance(node, Node):
            raise ValueError("Must specify a valid libcloud node object.")
        node_name = node.name
        zone_name = node.extra["zone"].name
        request = "/zones/{}/instances/{}/serialPort".format(zone_name, node_name)
        response = self.connection.request(request, method="GET").object

        return response["contents"]

    def ex_list(self, list_fn, **kwargs):
        """
        Wrap a list method in a :class:`GCEList` iterator.

        >>> for sublist in driver.ex_list(driver.ex_list_urlmaps).page(1):
        ...   sublist
        ...
        [<GCEUrlMap id="..." name="cli-map">]
        [<GCEUrlMap id="..." name="lc-map">]
        [<GCEUrlMap id="..." name="web-map">]

        :param  list_fn: A bound list method from :class:`GCENodeDriver`.
        :type   list_fn: ``instancemethod``

        :return: An iterator that returns sublists from list_fn.
        :rtype: :class:`GCEList`
        """

        return GCEList(driver=self, list_fn=list_fn, **kwargs)

    def ex_list_disktypes(self, zone=None):
        """
        Return a list of DiskTypes for a zone or all.

        :keyword  zone: The zone to return DiskTypes from. For example:
                        'us-central1-a'.  If None, will return DiskTypes from
                        self.zone.  If 'all', will return all DiskTypes.
        :type     zone: ``str`` or ``None``

        :return: A list of static DiskType objects.
        :rtype: ``list`` of :class:`GCEDiskType`
        """
        list_disktypes = []
        zone = self._set_zone(zone)

        if zone is None:
            request = "/aggregated/diskTypes"
        else:
            request = "/zones/%s/diskTypes" % (zone.name)
        response = self.connection.request(request, method="GET").object

        if "items" in response:
            # The aggregated result returns dictionaries for each region

            if zone is None:
                for v in response["items"].values():
                    zone_disktypes = [self._to_disktype(a) for a in v.get("diskTypes", [])]
                    list_disktypes.extend(zone_disktypes)
            else:
                list_disktypes = [self._to_disktype(a) for a in response["items"]]

        return list_disktypes

    def ex_set_usage_export_bucket(self, bucket, prefix=None):
        """
        Used to retain Compute Engine resource usage, storing the CSV data in
        a Google Cloud Storage bucket. See the
        `docs <https://cloud.google.com/compute/docs/usage-export>`_ for more
        information. Please ensure you have followed the necessary setup steps
        prior to enabling this feature (e.g. bucket exists, ACLs are in place,
        etc.)

        :param  bucket: Name of the Google Cloud Storage bucket. Specify the
                        name in either 'gs://<bucket_name>' or the full URL
                        'https://storage.googleapis.com/<bucket_name>'.
        :type   bucket: ``str``

        :param  prefix: Optional prefix string for all reports.
        :type   prefix: ``str`` or ``None``

        :return: True if successful
        :rtype:  ``bool``
        """

        if bucket.startswith("https://www.googleapis.com/") or bucket.startswith("gs://"):
            data = {"bucketName": bucket}
        else:
            raise ValueError("Invalid bucket name: %s" % bucket)

        if prefix:
            data["reportNamePrefix"] = prefix

        request = "/setUsageExportBucket"
        self.connection.async_request(request, method="POST", data=data)

        return True

    def ex_set_common_instance_metadata(self, metadata=None, force=False):
        """
        Set common instance metadata for the project. Common uses
        are for setting 'sshKeys', or setting a project-wide
        'startup-script' for all nodes (instances).  Passing in
        ``None`` for the 'metadata' parameter will clear out all common
        instance metadata *except* for 'sshKeys'. If you also want to
        update 'sshKeys', set the 'force' parameter to ``True``.

        :param  metadata: Dictionary of metadata. Can be either a standard
                          python dictionary, or the format expected by
                          GCE (e.g. {'items': [{'key': k1, 'value': v1}, ...}]
        :type   metadata: ``dict`` or ``None``

        :param  force: Force update of 'sshKeys'. If force is ``False`` (the
                       default), existing sshKeys will be retained. Setting
                       force to ``True`` will either replace sshKeys if a new
                       a new value is supplied, or deleted if no new value
                       is supplied.
        :type   force: ``bool``

        :return: True if successful
        :rtype:  ``bool``
        """

        if metadata:
            metadata = self._format_metadata("na", metadata)

        request = "/setCommonInstanceMetadata"

        project = self.ex_get_project()
        current_metadata = project.extra["commonInstanceMetadata"]
        fingerprint = current_metadata["fingerprint"]
        md_items = []

        if "items" in current_metadata:
            md_items = current_metadata["items"]

        # grab copy of current 'sshKeys' in case we want to retain them
        current_keys = ""

        for md in md_items:
            if md["key"] == "sshKeys":
                current_keys = md["value"]

        new_md = self._set_project_metadata(metadata, force, current_keys)

        md = {"fingerprint": fingerprint, "items": new_md}
        self.connection.async_request(request, method="POST", data=md)

        return True

    def ex_list_addresses(self, region=None):
        """
        Return a list of static addresses for a region, 'global', or all.

        :keyword  region: The region to return addresses from. For example:
                          'us-central1'.  If None, will return addresses from
                          region of self.zone.  If 'all', will return all
                          addresses. If 'global', it will return addresses in
                          the global namespace.
        :type     region: ``str`` or ``None``

        :return: A list of static address objects.
        :rtype: ``list`` of :class:`GCEAddress`
        """
        list_addresses = []

        if region != "global":
            region = self._set_region(region)

        if region is None:
            request = "/aggregated/addresses"
        elif region == "global":
            request = "/global/addresses"
        else:
            request = "/regions/%s/addresses" % (region.name)
        response = self.connection.request(request, method="GET").object

        if "items" in response:
            # The aggregated result returns dictionaries for each region

            if region is None:
                for v in response["items"].values():
                    region_addresses = [self._to_address(a) for a in v.get("addresses", [])]
                    list_addresses.extend(region_addresses)
            else:
                list_addresses = [self._to_address(a) for a in response["items"]]

        return list_addresses

    def ex_list_backendservices(self):
        """
        Return a list of backend services.

        :return: A list of backend service objects.
        :rtype: ``list`` of :class:`GCEBackendService`
        """
        list_backendservices = []
        response = self.connection.request("/global/backendServices", method="GET").object

        list_backendservices = [self._to_backendservice(d) for d in response.get("items", [])]

        return list_backendservices

    def ex_list_healthchecks(self):
        """
        Return the list of health checks.

        :return: A list of health check objects.
        :rtype: ``list`` of :class:`GCEHealthCheck`
        """
        list_healthchecks = []
        request = "/global/httpHealthChecks"
        response = self.connection.request(request, method="GET").object
        list_healthchecks = [self._to_healthcheck(h) for h in response.get("items", [])]

        return list_healthchecks

    def ex_list_firewalls(self):
        """
        Return the list of firewalls.

        :return: A list of firewall objects.
        :rtype: ``list`` of :class:`GCEFirewall`
        """
        list_firewalls = []
        request = "/global/firewalls"
        response = self.connection.request(request, method="GET").object
        list_firewalls = [self._to_firewall(f) for f in response.get("items", [])]

        return list_firewalls

    def ex_list_forwarding_rules(self, region=None, global_rules=False):
        """
        Return the list of forwarding rules for a region or all.

        :keyword  region: The region to return forwarding rules from.  For
                          example: 'us-central1'.  If None, will return
                          forwarding rules from the region of self.region
                          (which is based on self.zone).  If 'all', will
                          return forwarding rules for all regions, which does
                          not include the global forwarding rules.
        :type     region: ``str`` or :class:`GCERegion` or ``None``

        :keyword  global_rules: List global forwarding rules instead of
                                per-region rules.  Setting True will cause
                                'region' parameter to be ignored.
        :type     global_rules: ``bool``

        :return: A list of forwarding rule objects.
        :rtype: ``list`` of :class:`GCEForwardingRule`
        """
        list_forwarding_rules = []

        if global_rules:
            region = None
            request = "/global/forwardingRules"
        else:
            region = self._set_region(region)

            if region is None:
                request = "/aggregated/forwardingRules"
            else:
                request = "/regions/%s/forwardingRules" % (region.name)
        response = self.connection.request(request, method="GET").object

        if "items" in response:
            # The aggregated result returns dictionaries for each region

            if not global_rules and region is None:
                for v in response["items"].values():
                    region_forwarding_rules = [
                        self._to_forwarding_rule(f) for f in v.get("forwardingRules", [])
                    ]
                    list_forwarding_rules.extend(region_forwarding_rules)
            else:
                list_forwarding_rules = [self._to_forwarding_rule(f) for f in response["items"]]

        return list_forwarding_rules

    def list_images(self, ex_project=None, ex_include_deprecated=False):
        """
        Return a list of image objects. If no project is specified, a list of
        all non-deprecated global and vendor images images is returned. By
        default, only non-deprecated images are returned.

        :keyword  ex_project: Optional alternate project name.
        :type     ex_project: ``str``, ``list`` of ``str``, or ``None``

        :keyword  ex_include_deprecated: If True, even DEPRECATED images will
                                         be returned.
        :type     ex_include_deprecated: ``bool``

        :return:  List of GCENodeImage objects
        :rtype:   ``list`` of :class:`GCENodeImage`
        """
        dep = ex_include_deprecated

        if ex_project is not None:
            return self.ex_list_project_images(ex_project=ex_project, ex_include_deprecated=dep)
        image_list = self.ex_list_project_images(ex_project=None, ex_include_deprecated=dep)

        for img_proj in list(self.IMAGE_PROJECTS.keys()):
            try:
                image_list.extend(
                    self.ex_list_project_images(ex_project=img_proj, ex_include_deprecated=dep)
                )
            except Exception:
                # do not break if an OS type is invalid
                pass

        return image_list

    def ex_list_project_images(self, ex_project=None, ex_include_deprecated=False):
        """
        Return a list of image objects for a project. If no project is
        specified, only a list of 'global' images is returned.

        :keyword  ex_project: Optional alternate project name.
        :type     ex_project: ``str``, ``list`` of ``str``, or ``None``

        :keyword  ex_include_deprecated: If True, even DEPRECATED images will
                                         be returned.
        :type     ex_include_deprecated: ``bool``

        :return:  List of GCENodeImage objects
        :rtype:   ``list`` of :class:`GCENodeImage`
        """
        list_images = []
        request = "/global/images"

        if ex_project is None:
            response = self.connection.paginated_request(request, method="GET")

            for img in response.get("items", []):
                if "deprecated" not in img:
                    list_images.append(self._to_node_image(img))
                else:
                    if ex_include_deprecated:
                        list_images.append(self._to_node_image(img))
        else:
            list_images = []
            # Save the connection request_path
            save_request_path = self.connection.request_path

            if isinstance(ex_project, str):
                ex_project = [ex_project]

            for proj in ex_project:
                # Override the connection request path
                new_request_path = save_request_path.replace(self.project, proj)
                self.connection.request_path = new_request_path
                try:
                    response = self.connection.paginated_request(request, method="GET")
                except Exception:
                    raise
                finally:
                    # Restore the connection request_path
                    self.connection.request_path = save_request_path

                for img in response.get("items", []):
                    if "deprecated" not in img:
                        list_images.append(self._to_node_image(img))
                    else:
                        if ex_include_deprecated:
                            list_images.append(self._to_node_image(img))

        return list_images

    def list_locations(self):
        """
        Return a list of locations (zones).

        The :class:`ex_list_zones` method returns more comprehensive results,
        but this is here for compatibility.

        :return: List of NodeLocation objects
        :rtype: ``list`` of :class:`NodeLocation`
        """
        list_locations = []
        request = "/zones"
        response = self.connection.request(request, method="GET").object
        list_locations = [self._to_node_location(loc) for loc in response["items"]]

        return list_locations

    def ex_list_routes(self):
        """
        Return the list of routes.

        :return: A list of route objects.
        :rtype: ``list`` of :class:`GCERoute`
        """
        list_routes = []
        request = "/global/routes"
        response = self.connection.request(request, method="GET").object
        list_routes = [self._to_route(n) for n in response.get("items", [])]

        return list_routes

    def ex_list_sslcertificates(self):
        """
        Retrieves the list of SslCertificate resources available to the
        specified project.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute
        * https://www.googleapis.com/auth/compute.readonly

        :return: A list of SSLCertificate objects.
        :rtype: ``list`` of :class:`GCESslCertificate`
        """
        list_data = []
        request = "/global/sslCertificates"
        response = self.connection.request(request, method="GET").object
        list_data = [self._to_sslcertificate(a) for a in response.get("items", [])]

        return list_data

    def ex_list_subnetworks(self, region=None):
        """
        Return the list of subnetworks.

        :keyword  region: Region for the subnetwork. Specify 'all' to return
                          the aggregated list of subnetworks.
        :type     region: ``str`` or :class:`GCERegion`

        :return: A list of subnetwork objects.
        :rtype: ``list`` of :class:`GCESubnetwork`
        """
        region = self._set_region(region)

        if region is None:
            request = "/aggregated/subnetworks"
        else:
            request = "/regions/%s/subnetworks" % (region.name)

        list_subnetworks = []
        response = self.connection.request(request, method="GET").object

        if "items" in response:
            if region is None:
                for v in response["items"].values():
                    for i in v.get("subnetworks", []):
                        try:
                            list_subnetworks.append(self._to_subnetwork(i))
                        except ResourceNotFoundError:
                            pass
            else:
                for i in response["items"]:
                    try:
                        list_subnetworks.append(self._to_subnetwork(i))
                    except ResourceNotFoundError:
                        pass

        return list_subnetworks

    def ex_list_networks(self):
        """
        Return the list of networks.

        :return: A list of network objects.
        :rtype: ``list`` of :class:`GCENetwork`
        """
        list_networks = []
        request = "/global/networks"
        response = self.connection.request(request, method="GET").object
        list_networks = [self._to_network(n) for n in response.get("items", [])]

        return list_networks

    def list_nodes(self, ex_zone=None, ex_use_disk_cache=True):
        """
        Return a list of nodes in the current zone or all zones.

        :keyword  ex_zone:  Optional zone name or 'all'
        :type     ex_zone:  ``str`` or :class:`GCEZone` or
                            :class:`NodeLocation` or ``None``

        :keyword  ex_use_disk_cache:  Disk information for each node will
                                   retrieved from a dictionary rather
                                   than making a distinct API call for it.
        :type     ex_use_disk_cache: ``bool``

        :return:  List of Node objects
        :rtype:   ``list`` of :class:`Node`
        """
        zone = self._set_zone(ex_zone)
        response = self.connection.request_aggregated_items("instances", zone=zone)

        if not response.get("items", []):
            return []

        list_nodes = []

        # The aggregated response returns a dict for each zone
        # Create volume cache now for fast lookups of disk info.
        self._ex_populate_volume_dict()

        items = response["items"].values()
        instances = [item.get("instances", []) for item in items]
        instances = itertools.chain(*instances)

        for instance in instances:
            try:
                node = self._to_node(instance, use_disk_cache=ex_use_disk_cache)
            except ResourceNotFoundError:
                # If a GCE node has been deleted between
                #   - is was listed by `request('.../instances', 'GET')
                #   - it is converted by `self._to_node(i)`
                # `_to_node()` will raise a ResourceNotFoundError.
                #
                # Just ignore that node and return the list of the
                # other nodes.

                continue

            list_nodes.append(node)

        # Clear the volume cache as lookups are complete.
        self._ex_volume_dict = {}

        return list_nodes

    def ex_list_regions(self):
        """
        Return the list of regions.

        :return: A list of region objects.
        :rtype: ``list`` of :class:`GCERegion`
        """
        list_regions = []
        request = "/regions"
        response = self.connection.request(request, method="GET").object
        list_regions = [self._to_region(r) for r in response["items"]]

        return list_regions

    def list_sizes(self, location=None):
        """
        Return a list of sizes (machineTypes) in a zone.

        :keyword  location: Location or Zone for sizes
        :type     location: ``str`` or :class:`GCEZone` or
                            :class:`NodeLocation` or ``None``

        :return:  List of GCENodeSize objects
        :rtype:   ``list`` of :class:`GCENodeSize`
        """
        list_sizes = []
        zone = self._set_zone(location)

        if zone is None:
            request = "/aggregated/machineTypes"
        else:
            request = "/zones/%s/machineTypes" % (zone.name)

        response = self.connection.request(request, method="GET").object
        # getting pricing data here so it is done only once
        instance_prices = get_pricing(driver_type="compute", driver_name="gce_instances")

        if "items" in response:
            # The aggregated response returns a dict for each zone

            if zone is None:
                for v in response["items"].values():
                    zone_sizes = [
                        self._to_node_size(s, instance_prices) for s in v.get("machineTypes", [])
                    ]
                    list_sizes.extend(zone_sizes)
            else:
                list_sizes = [self._to_node_size(s, instance_prices) for s in response["items"]]

        return list_sizes

    def ex_list_snapshots(self):
        """
        Return the list of disk snapshots in the project.

        :return:  A list of snapshot objects
        :rtype:   ``list`` of :class:`GCESnapshot`
        """
        list_snapshots = []
        request = "/global/snapshots"
        response = self.connection.request(request, method="GET").object
        list_snapshots = [self._to_snapshot(s) for s in response.get("items", [])]

        return list_snapshots

    def ex_list_targethttpproxies(self):
        """
        Return the list of target HTTP proxies.

        :return:  A list of target http proxy objects
        :rtype:   ``list`` of :class:`GCETargetHttpProxy`
        """
        request = "/global/targetHttpProxies"
        response = self.connection.request(request, method="GET").object

        return [self._to_targethttpproxy(u) for u in response.get("items", [])]

    def ex_list_targethttpsproxies(self):
        """
        Return the list of target HTTPs proxies.

        :return:  A list of target https proxy objects
        :rtype:   ``list`` of :class:`GCETargetHttpsProxy`
        """
        request = "/global/targetHttpsProxies"
        response = self.connection.request(request, method="GET").object

        return [self._to_targethttpsproxy(x) for x in response.get("items", [])]

    def ex_list_targetinstances(self, zone=None):
        """
        Return the list of target instances.

        :return:  A list of target instance objects
        :rtype:   ``list`` of :class:`GCETargetInstance`
        """
        list_targetinstances = []
        zone = self._set_zone(zone)

        if zone is None:
            request = "/aggregated/targetInstances"
        else:
            request = "/zones/%s/targetInstances" % (zone.name)
        response = self.connection.request(request, method="GET").object

        if "items" in response:
            # The aggregated result returns dictionaries for each region

            if zone is None:
                for v in response["items"].values():
                    zone_targetinstances = [
                        self._to_targetinstance(t) for t in v.get("targetInstances", [])
                    ]
                    list_targetinstances.extend(zone_targetinstances)
            else:
                list_targetinstances = [self._to_targetinstance(t) for t in response["items"]]

        return list_targetinstances

    def ex_list_targetpools(self, region=None):
        """
        Return the list of target pools.

        :return:  A list of target pool objects
        :rtype:   ``list`` of :class:`GCETargetPool`
        """
        list_targetpools = []
        region = self._set_region(region)

        if region is None:
            request = "/aggregated/targetPools"
        else:
            request = "/regions/%s/targetPools" % (region.name)
        response = self.connection.request(request, method="GET").object

        if "items" in response:
            # The aggregated result returns dictionaries for each region

            if region is None:
                for v in response["items"].values():
                    region_targetpools = [self._to_targetpool(t) for t in v.get("targetPools", [])]
                    list_targetpools.extend(region_targetpools)
            else:
                list_targetpools = [self._to_targetpool(t) for t in response["items"]]

        return list_targetpools

    def ex_list_urlmaps(self):
        """
        Return the list of URL Maps in the project.

        :return:  A list of url map objects
        :rtype:   ``list`` of :class:`GCEUrlMap`
        """
        request = "/global/urlMaps"
        response = self.connection.request(request, method="GET").object

        return [self._to_urlmap(u) for u in response.get("items", [])]

    def ex_list_instancegroups(self, zone):
        """
        Retrieves the list of instance groups that are located in the specified
        project and zone.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute
        * https://www.googleapis.com/auth/compute.readonly

        :param  zone:  The name of the zone where the instance group is
                       located.
        :type   zone: ``str``

        :return: A list of instance group mgr  objects.
        :rtype: ``list`` of :class:`GCEInstanceGroupManagers`
        """

        list_data = []
        zone = self._set_zone(zone)

        if zone is None:
            request = "/aggregated/instanceGroups"
        else:
            request = "/zones/%s/instanceGroups" % (zone.name)
        response = self.connection.request(request, method="GET").object

        if "items" in response:
            # The aggregated result returns dictionaries for each region

            if zone is None:
                for v in response["items"].values():
                    zone_data = [self._to_instancegroup(a) for a in v.get("instanceGroups", [])]
                    list_data.extend(zone_data)
            else:
                list_data = [self._to_instancegroup(a) for a in response["items"]]

        return list_data

    def ex_list_instancegroupmanagers(self, zone=None):
        """
        Return a list of Instance Group Managers.

        :keyword  zone: The zone to return InstanceGroupManagers from.
                        For example: 'us-central1-a'.  If None, will return
                        InstanceGroupManagers from self.zone.  If 'all', will
                        return all InstanceGroupManagers.
        :type     zone: ``str`` or ``None``

        :return: A list of instance group mgr  objects.
        :rtype: ``list`` of :class:`GCEInstanceGroupManagers`
        """
        list_managers = []
        zone = self._set_zone(zone)

        if zone is None:
            request = "/aggregated/instanceGroupManagers"
        else:
            request = "/zones/%s/instanceGroupManagers" % (zone.name)
        response = self.connection.request(request, method="GET").object

        if "items" in response:
            # The aggregated result returns dictionaries for each region

            if zone is None:
                for v in response["items"].values():
                    zone_managers = [
                        self._to_instancegroupmanager(a) for a in v.get("instanceGroupManagers", [])
                    ]
                    list_managers.extend(zone_managers)
            else:
                list_managers = [self._to_instancegroupmanager(a) for a in response["items"]]

        return list_managers

    def ex_list_instancetemplates(self):
        """
        Return the list of Instance Templates.

        :return:  A list of Instance Template Objects
        :rtype:   ``list`` of :class:`GCEInstanceTemplate`
        """
        request = "/global/instanceTemplates"
        response = self.connection.request(request, method="GET").object

        return [self._to_instancetemplate(u) for u in response.get("items", [])]

    def ex_list_autoscalers(self, zone=None):
        """
        Return the list of AutoScalers.

        :keyword  zone: The zone to return InstanceGroupManagers from.
                        For example: 'us-central1-a'.  If None, will return
                        InstanceGroupManagers from self.zone.  If 'all', will
                        return all InstanceGroupManagers.
        :type     zone: ``str`` or ``None``

        :return:  A list of AutoScaler Objects
        :rtype:   ``list`` of :class:`GCEAutoScaler`
        """
        list_autoscalers = []
        zone = self._set_zone(zone)

        if zone is None:
            request = "/aggregated/autoscalers"
        else:
            request = "/zones/%s/autoscalers" % (zone.name)

        response = self.connection.request(request, method="GET").object

        if "items" in response:
            # The aggregated result returns dictionaries for each zone.

            if zone is None:
                for v in response["items"].values():
                    zone_as = [self._to_autoscaler(a) for a in v.get("autoscalers", [])]
                    list_autoscalers.extend(zone_as)
            else:
                list_autoscalers = [self._to_autoscaler(a) for a in response["items"]]

        return list_autoscalers

    def list_volumes(self, ex_zone=None):
        """
        Return a list of volumes for a zone or all.

        Will return list from provided zone, or from the default zone unless
        given the value of 'all'.

        :keyword  ex_zone: The zone to return volumes from.
        :type     ex_zone: ``str`` or :class:`GCEZone` or
                            :class:`NodeLocation` or ``None``

        :return: A list of volume objects.
        :rtype: ``list`` of :class:`StorageVolume`
        """
        list_volumes = []
        zone = self._set_zone(ex_zone)

        if zone is None:
            request = "/aggregated/disks"
        else:
            request = "/zones/%s/disks" % (zone.name)

        response = self.connection.request(request, method="GET").object

        if "items" in response:
            # The aggregated response returns a dict for each zone

            if zone is None:
                for v in response["items"].values():
                    zone_volumes = [self._to_storage_volume(d) for d in v.get("disks", [])]
                    list_volumes.extend(zone_volumes)
            else:
                list_volumes = [self._to_storage_volume(d) for d in response["items"]]

        return list_volumes

    def ex_list_zones(self):
        """
        Return the list of zones.

        :return: A list of zone objects.
        :rtype: ``list`` of :class:`GCEZone`
        """
        list_zones = []
        request = "/zones"
        response = self.connection.request(request, method="GET").object
        list_zones = [self._to_zone(z) for z in response["items"]]

        return list_zones

    def ex_create_address(
        self,
        name,
        region=None,
        address=None,
        description=None,
        address_type="EXTERNAL",
        subnetwork=None,
    ):
        """
        Create a static address in a region, or a global address.

        :param  name: Name of static address
        :type   name: ``str``

        :keyword  region: Name of region for the address (e.g. 'us-central1')
                          Use 'global' to create a global address.
        :type     region: ``str`` or :class:`GCERegion`

        :keyword  address: Ephemeral IP address to promote to a static one
                           (e.g. 'xxx.xxx.xxx.xxx')
        :type     address: ``str`` or ``None``

        :keyword  description: Optional descriptive comment.
        :type     description: ``str`` or ``None``

        :keyword  address_type: Optional The type of address to reserve,
                                either INTERNAL or EXTERNAL. If unspecified,
                                defaults to EXTERNAL.
        :type     description: ``str``

        :keyword  subnetwork: Optional The URL of the subnetwork in which to
                              reserve the address. If an IP address is
                              specified, it must be within the subnetwork's
                              IP range. This field can only be used with
                              INTERNAL type with GCE_ENDPOINT/DNS_RESOLVER
                              purposes.
        :type     description: ``str``

        :return:  Static Address object
        :rtype:   :class:`GCEAddress`
        """
        region = region or self.region

        if region is None:
            raise ValueError("REGION_NOT_SPECIFIED", "Region must be provided for an address")

        if region != "global" and not hasattr(region, "name"):
            region = self.ex_get_region(region)
        address_data = {"name": name}

        if address:
            address_data["address"] = address

        if description:
            address_data["description"] = description

        if address_type:
            if address_type not in ["EXTERNAL", "INTERNAL"]:
                raise ValueError(
                    "ADDRESS_TYPE_WRONG",
                    "Address type must be either EXTERNAL or \
                                 INTERNAL",
                )
            else:
                address_data["addressType"] = address_type

        if subnetwork and address_type != "INTERNAL":
            raise ValueError(
                "INVALID_ARGUMENT_COMBINATION",
                "Address type must be internal if subnetwork \
                             provided",
            )

        if subnetwork and not hasattr(subnetwork, "name"):
            subnetwork = self.ex_get_subnetwork(subnetwork, region)

        if region == "global":
            request = "/global/addresses"
        else:
            request = "/regions/%s/addresses" % (region.name)
        self.connection.async_request(request, method="POST", data=address_data)

        return self.ex_get_address(name, region=region)

    def ex_create_autoscaler(self, name, zone, instance_group, policy, description=None):
        """
        Create an Autoscaler for an Instance Group.

        :param  name: The name of the Autoscaler
        :type   name: ``str``

        :param  zone: The zone to which the Instance Group belongs
        :type   zone: ``str`` or :class:`GCEZone`

        :param  instance_group:  An Instance Group Manager object.
        :type:  :class:`GCEInstanceGroupManager`

        :param  policy:  A dict containing policy configuration.  See the
                         API documentation for Autoscalers for more details.
        :type:  ``dict``

        :return:  An Autoscaler object.
        :rtype:   :class:`GCEAutoscaler`
        """
        zone = zone or self.zone
        autoscaler_data = {}
        autoscaler_data = {"name": name}

        if not hasattr(zone, "name"):
            zone = self.ex_get_zone(zone)
        autoscaler_data["zone"] = zone.extra["selfLink"]

        # TODO(supertom): we should validate the policy
        autoscaler_data["autoscalingPolicy"] = policy
        request = "/zones/%s/autoscalers" % zone.name
        autoscaler_data["target"] = instance_group.extra["selfLink"]
        self.connection.async_request(request, method="POST", data=autoscaler_data)

        return self.ex_get_autoscaler(name, zone)

    def ex_create_backend(
        self,
        instance_group,
        balancing_mode="UTILIZATION",
        max_utilization=None,
        max_rate=None,
        max_rate_per_instance=None,
        capacity_scaler=1,
        description=None,
    ):
        """
        Helper Object to create a backend.

        :param  instance_group: The Instance Group for this Backend.
        :type   instance_group: :class: `GCEInstanceGroup`

        :param  balancing_mode: Specifies the balancing mode for this backend.
                                For global HTTP(S) load balancing, the valid
                                values are UTILIZATION (default) and RATE.
                                For global SSL load balancing, the valid
                                values are UTILIZATION (default) and
                                CONNECTION.
        :type   balancing_mode: ``str``

        :param  max_utilization: Used when balancingMode is UTILIZATION.
                                 This ratio defines the CPU utilization
                                 target for the group. The default is 0.8.
                                 Valid range is [0.0, 1.0].
        :type   max_utilization: ``float``

        :param  max_rate: The max requests per second (RPS) of the group.
                          Can be used with either RATE or UTILIZATION balancing
                          modes, but required if RATE mode. For RATE mode,
                          either maxRate or maxRatePerInstance must be set.
        :type   max_rate: ``int``

        :param  max_rate_per_instance: The max requests per second (RPS) that
                                       a single backend instance can handle.
                                       This is used to calculate the capacity
                                       of the group. Can be used in either
                                       balancing mode. For RATE mode, either
                                       maxRate or maxRatePerInstance must be
                                       set.
        :type   max_rate_per_instance: ``float``

        :param  capacity_scaler: A multiplier applied to the group's maximum
                                 servicing capacity (based on UTILIZATION,
                                 RATE, or CONNECTION). Default value is 1,
                                 which means the group will serve up to 100%
                                 of its configured capacity (depending on
                                 balancingMode). A setting of 0 means the
                                 group is completely drained, offering 0%
                                 of its available capacity. Valid range is
                                 [0.0,1.0].
        :type   capacity_scaler: ``float``

        :param  description: An optional description of this resource.
                             Provide this property when you create the
                             resource.
        :type   description: ``str``

        :return: A GCEBackend object.
        :rtype: :class: `GCEBackend`
        """

        return GCEBackend(
            instance_group=instance_group,
            balancing_mode=balancing_mode,
            max_utilization=max_utilization,
            max_rate=max_rate,
            max_rate_per_instance=max_rate_per_instance,
            capacity_scaler=capacity_scaler,
            description=description,
        )

    def ex_create_backendservice(
        self,
        name,
        healthchecks,
        backends=[],
        protocol=None,
        description=None,
        timeout_sec=None,
        enable_cdn=False,
        port=None,
        port_name=None,
    ):
        """
        Create a global Backend Service.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute

        :param  name:  Name of the resource. Provided by the client when the
                       resource is created. The name must be 1-63 characters
                       long, and comply with RFC1035. Specifically, the name
                       must be 1-63 characters long and match the regular
                       expression [a-z]([-a-z0-9]*[a-z0-9])? which means the
                       first character must be a lowercase letter, and all
                       following characters must be a dash, lowercase letter,
                       or digit, except the last character, which cannot be a
                       dash.
        :type   name: ``str``

        :param    healthchecks: A list of HTTP Health Checks to use for this
                                service.  There must be at least one.
        :type     healthchecks: ``list`` of (``str`` or
                                :class:`GCEHealthCheck`)

        :keyword  backends:  The list of backends that serve this
                             BackendService.
        :type   backends: ``list`` of :class `GCEBackend` or list of ``dict``

        :keyword  timeout_sec:  How many seconds to wait for the backend
                                before considering it a failed request.
                                Default is 30 seconds.
        :type   timeout_sec: ``integer``

        :keyword  enable_cdn:  If true, enable Cloud CDN for this
                                 BackendService.  When the load balancing
                                 scheme is INTERNAL, this field is not used.
        :type   enable_cdn: ``bool``

        :keyword  port:  Deprecated in favor of port_name. The TCP port to
                         connect on the backend. The default value is 80.
                         This cannot be used for internal load balancing.
        :type   port: ``integer``

        :keyword  port_name: Name of backend port. The same name should appear
                             in the instance groups referenced by this service.
        :type     port_name: ``str``

        :keyword  protocol: The protocol this Backend Service uses to
                            communicate with backends.
                            Possible values are HTTP, HTTPS, HTTP2, TCP
                            and SSL.
        :type     protocol: ``str``

        :return:  A Backend Service object.
        :rtype:   :class:`GCEBackendService`
        """
        backendservice_data = {
            "name": name,
            "healthChecks": [],
            "backends": [],
            "enableCDN": enable_cdn,
        }

        for hc in healthchecks:
            if not hasattr(hc, "extra"):
                hc = self.ex_get_healthcheck(name=hc)
            backendservice_data["healthChecks"].append(hc.extra["selfLink"])

        for be in backends:
            if isinstance(be, GCEBackend):
                backendservice_data["backends"].append(be.to_backend_dict())
            else:
                backendservice_data["backends"].append(be)

        if port:
            backendservice_data["port"] = port

        if port_name:
            backendservice_data["portName"] = port_name

        if timeout_sec:
            backendservice_data["timeoutSec"] = timeout_sec

        if protocol:
            if protocol in self.BACKEND_SERVICE_PROTOCOLS:
                backendservice_data["protocol"] = protocol
            else:
                raise ValueError(
                    "Protocol must be one of %s" % ",".join(self.BACKEND_SERVICE_PROTOCOLS)
                )

        if description:
            backendservice_data["description"] = description

        request = "/global/backendServices"
        self.connection.async_request(request, method="POST", data=backendservice_data)

        return self.ex_get_backendservice(name)

    def ex_create_healthcheck(
        self,
        name,
        host=None,
        path=None,
        port=None,
        interval=None,
        timeout=None,
        unhealthy_threshold=None,
        healthy_threshold=None,
        description=None,
    ):
        """
        Create an Http Health Check.

        :param  name: Name of health check
        :type   name: ``str``

        :keyword  host: Hostname of health check request.  Defaults to empty
                        and public IP is used instead.
        :type     host: ``str``

        :keyword  path: The request path for the check.  Defaults to /.
        :type     path: ``str``

        :keyword  port: The TCP port number for the check.  Defaults to 80.
        :type     port: ``int``

        :keyword  interval: How often (in seconds) to check.  Defaults to 5.
        :type     interval: ``int``

        :keyword  timeout: How long to wait before failing. Defaults to 5.
        :type     timeout: ``int``

        :keyword  unhealthy_threshold: How many failures before marking
                                       unhealthy.  Defaults to 2.
        :type     unhealthy_threshold: ``int``

        :keyword  healthy_threshold: How many successes before marking as
                                     healthy.  Defaults to 2.
        :type     healthy_threshold: ``int``

        :keyword  description: The description of the check.  Defaults to None.
        :type     description: ``str`` or ``None``

        :return:  Health Check object
        :rtype:   :class:`GCEHealthCheck`
        """
        hc_data = {}
        hc_data["name"] = name

        if host:
            hc_data["host"] = host

        if description:
            hc_data["description"] = description
        # As of right now, the 'default' values aren't getting set when called
        # through the API, so set them explicitly
        hc_data["requestPath"] = path or "/"
        hc_data["port"] = port or 80
        hc_data["checkIntervalSec"] = interval or 5
        hc_data["timeoutSec"] = timeout or 5
        hc_data["unhealthyThreshold"] = unhealthy_threshold or 2
        hc_data["healthyThreshold"] = healthy_threshold or 2

        request = "/global/httpHealthChecks"

        self.connection.async_request(request, method="POST", data=hc_data)

        return self.ex_get_healthcheck(name)

    def ex_create_firewall(
        self,
        name,
        allowed=None,
        denied=None,
        network="default",
        target_ranges=None,
        direction="INGRESS",
        priority=1000,
        source_service_accounts=None,
        target_service_accounts=None,
        source_ranges=None,
        source_tags=None,
        target_tags=None,
        description=None,
    ):
        """
        Create a firewall rule on a network.
        Rules can be for Ingress or Egress, and they may Allow or
        Deny traffic. They are also applied in order based on action
        (Deny, Allow) and Priority. Rules can be applied using various Source
        and Target filters.

        Firewall rules should be supplied in the "allowed" or "denied" field.
        This is a list of dictionaries formatted like so ("ports" is optional):

            [{"IPProtocol": "<protocol string or number>",
              "ports": "<port_numbers or ranges>"}]

        For example, to allow tcp on port 8080 and udp on all ports, 'allowed'
        would be::

            [{"IPProtocol": "tcp",
              "ports": ["8080"]},
             {"IPProtocol": "udp"}]

        Note that valid inputs vary by direction (INGRESS vs EGRESS), action
        (allow/deny), and source/target filters (tag vs range etc).

        See `Firewall Reference <https://developers.google.com/compute/docs/
        reference/latest/firewalls/insert>`_ for more information.

        :param  name: Name of the firewall to be created
        :type   name: ``str``

        :param  description: Optional description of the rule.
        :type   description: ``str``

        :param  direction: Direction of the FW rule - "INGRESS" or "EGRESS"
                           Defaults to 'INGRESS'.
        :type   direction: ``str``

        :param  priority: Priority integer of the rule -
                          lower is applied first. Defaults to 1000
        :type   priority: ``int``

        :param  allowed: List of dictionaries with rules for type INGRESS
        :type   allowed: ``list`` of ``dict``

        :param  denied: List of dictionaries with rules for type EGRESS
        :type   denied: ``list`` of ``dict``

        :keyword  network: The network that the firewall applies to.
        :type     network: ``str`` or :class:`GCENetwork`

        :keyword  source_ranges: A list of IP ranges in CIDR format that the
                                 firewall should apply to. Defaults to
                                 ['0.0.0.0/0']
        :type     source_ranges: ``list`` of ``str``

        :keyword  source_service_accounts: A list of source service accounts
                                        the rules apply to.
        :type     source_service_accounts: ``list`` of ``str``

        :keyword  source_tags: A list of source instance tags the rules apply
                               to.
        :type     source_tags: ``list`` of ``str``

        :keyword  target_tags: A list of target instance tags the rules apply
                               to.
        :type     target_tags: ``list`` of ``str``

        :keyword  target_service_accounts: A list of target service accounts
                                        the rules apply to.
        :type     target_service_accounts: ``list`` of ``str``

        :keyword  target_ranges: A list of IP ranges in CIDR format that the
                                EGRESS type rule should apply to. Defaults
                                to ['0.0.0.0/0']
        :type     target_ranges: ``list`` of ``str``

        :return:  Firewall object
        :rtype:   :class:`GCEFirewall`
        """
        firewall_data = {}

        if not hasattr(network, "name"):
            nw = self.ex_get_network(network)
        else:
            nw = network

        firewall_data["name"] = name
        firewall_data["direction"] = direction
        firewall_data["priority"] = priority
        firewall_data["description"] = description

        if direction == "INGRESS":
            firewall_data["allowed"] = allowed
        elif direction == "EGRESS":
            firewall_data["denied"] = denied
        firewall_data["network"] = nw.extra["selfLink"]

        if source_ranges is None and source_tags is None and source_service_accounts is None:
            source_ranges = ["0.0.0.0/0"]

        if source_ranges is not None:
            firewall_data["sourceRanges"] = source_ranges

        if source_tags is not None:
            firewall_data["sourceTags"] = source_tags

        if source_service_accounts is not None:
            firewall_data["sourceServiceAccounts"] = source_service_accounts

        if target_tags is not None:
            firewall_data["targetTags"] = target_tags

        if target_service_accounts is not None:
            firewall_data["targetServiceAccounts"] = target_service_accounts

        if target_ranges is not None:
            firewall_data["destinationRanges"] = target_ranges

        request = "/global/firewalls"

        self.connection.async_request(request, method="POST", data=firewall_data)

        return self.ex_get_firewall(name)

    def ex_create_forwarding_rule(
        self,
        name,
        target=None,
        region=None,
        protocol="tcp",
        port_range=None,
        address=None,
        description=None,
        global_rule=False,
        targetpool=None,
        lb_scheme=None,
    ):
        """
        Create a forwarding rule.

        :param  name: Name of forwarding rule to be created
        :type   name: ``str``

        :keyword  target: The target of this forwarding rule.  For global
                          forwarding rules this must be a global
                          TargetHttpProxy. For regional rules this may be
                          either a TargetPool or TargetInstance. If passed
                          a string instead of the object, it will be the name
                          of a TargetHttpProxy for global rules or a
                          TargetPool for regional rules.  A TargetInstance
                          must be passed by object. (required)
        :type     target: ``str`` or :class:`GCETargetHttpProxy` or
                          :class:`GCETargetInstance` or :class:`GCETargetPool`

        :keyword  region: Region to create the forwarding rule in.  Defaults to
                          self.region.  Ignored if global_rule is True.
        :type     region: ``str`` or :class:`GCERegion`

        :keyword  protocol: Should be 'tcp' or 'udp'
        :type     protocol: ``str``

        :keyword  port_range: Single port number or range separated by a dash.
                              Examples: '80', '5000-5999'.  Required for global
                              forwarding rules, optional for regional rules.
        :type     port_range: ``str``

        :keyword  address: Optional static address for forwarding rule. Must be
                           in same region.
        :type     address: ``str`` or :class:`GCEAddress`

        :keyword  description: The description of the forwarding rule.
                               Defaults to None.
        :type     description: ``str`` or ``None``

        :keyword  targetpool: Deprecated parameter for backwards compatibility.
                              Use target instead.
        :type     targetpool: ``str`` or :class:`GCETargetPool`

        :keyword  lb_scheme: Load balancing scheme, can be 'EXTERNAL' or
                             'INTERNAL'. Defaults to 'EXTERNAL'.
        :type     lb_scheme: ``str`` or ``None``

        :return:  Forwarding Rule object
        :rtype:   :class:`GCEForwardingRule`
        """
        forwarding_rule_data = {"name": name}

        if global_rule:
            if not hasattr(target, "name"):
                target = self.ex_get_targethttpproxy(target)
        else:
            region = region or self.region

            if not hasattr(region, "name"):
                region = self.ex_get_region(region)
            forwarding_rule_data["region"] = region.extra["selfLink"]

            if not target:
                target = targetpool  # Backwards compatibility

            if not hasattr(target, "name"):
                target = self.ex_get_targetpool(target, region)

        forwarding_rule_data["target"] = target.extra["selfLink"]
        forwarding_rule_data["IPProtocol"] = protocol.upper()

        if address:
            if not hasattr(address, "name"):
                address = self.ex_get_address(address, "global" if global_rule else region)
            forwarding_rule_data["IPAddress"] = address.address

        if port_range:
            forwarding_rule_data["portRange"] = port_range

        if description:
            forwarding_rule_data["description"] = description

        if lb_scheme:
            forwarding_rule_data["loadBalancingScheme"] = lb_scheme

        if global_rule:
            request = "/global/forwardingRules"
        else:
            request = "/regions/%s/forwardingRules" % (region.name)

        self.connection.async_request(request, method="POST", data=forwarding_rule_data)

        return self.ex_get_forwarding_rule(name, global_rule=global_rule)

    def ex_create_image(
        self,
        name,
        volume,
        description=None,
        family=None,
        guest_os_features=None,
        use_existing=True,
        wait_for_completion=True,
        ex_licenses=None,
        ex_labels=None,
    ):
        """
        Create an image from the provided volume.

        :param  name: The name of the image to create.
        :type   name: ``str``

        :param  volume: The volume to use to create the image, or the
                        Google Cloud Storage URI
        :type   volume: ``str`` or :class:`StorageVolume`

        :keyword  description: Description of the new Image
        :type     description: ``str``

        :keyword  family: The name of the image family to which this image
                          belongs. If you create resources by specifying an
                          image family instead of a specific image name, the
                          resource uses the latest non-deprecated image that
                          is set with that family name.
        :type     family: ``str``

        :keyword  guest_os_features: Features of the guest operating system,
                                     valid for bootable images only.
        :type     guest_os_features: ``list`` of ``str`` or ``None``

        :keyword  ex_licenses: List of strings representing licenses
                               to be associated with the image.
        :type     ex_licenses: ``list`` of ``str``

        :keyword  ex_labels: Labels dictionary for image.
        :type     ex_labels: ``dict`` or ``None``

        :keyword  use_existing: If True and an image with the given name
                                already exists, return an object for that
                                image instead of attempting to create
                                a new image.
        :type     use_existing: ``bool``

        :keyword  wait_for_completion: If True, wait until the new image is
                                       created before returning a new NodeImage
                                       Otherwise, return a new NodeImage
                                       instance, and let the user track the
                                       creation progress
        :type     wait_for_completion: ``bool``

        :return:  A GCENodeImage object for the new image
        :rtype:   :class:`GCENodeImage`

        """
        image_data = {}
        image_data["name"] = name
        image_data["description"] = description
        image_data["family"] = family

        if isinstance(volume, StorageVolume):
            image_data["sourceDisk"] = volume.extra["selfLink"]
            image_data["zone"] = volume.extra["zone"].name
        elif (
            isinstance(volume, str) and volume.startswith("https://") and volume.endswith("tar.gz")
        ):
            image_data["rawDisk"] = {"source": volume, "containerType": "TAR"}
        else:
            raise ValueError("Source must be instance of StorageVolume or URI")

        if ex_licenses:
            if isinstance(ex_licenses, str):
                ex_licenses = [ex_licenses]
            image_data["licenses"] = ex_licenses

        if ex_labels:
            image_data["labels"] = ex_labels

        if guest_os_features:
            image_data["guestOsFeatures"] = []

            if isinstance(guest_os_features, str):
                guest_os_features = [guest_os_features]

            for feature in guest_os_features:
                image_data["guestOsFeatures"].append({"type": feature})
        request = "/global/images"

        try:
            if wait_for_completion:
                self.connection.async_request(request, method="POST", data=image_data)
            else:
                self.connection.request(request, method="POST", data=image_data)

        except ResourceExistsError as e:
            if not use_existing:
                raise e

        return self.ex_get_image(name)

    def ex_copy_image(self, name, url, description=None, family=None, guest_os_features=None):
        """
        Copy an image to your image collection.

        :param  name: The name of the image
        :type   name: ``str``

        :param  url: The URL to the image. The URL can start with `gs://`
        :type url: ``str``

        :param  description: The description of the image
        :type   description: ``str``

        :param  family: The family of the image
        :type   family: ``str``

        :param  guest_os_features: The features of the guest operating system.
        :type   guest_os_features: ``list`` of ``str`` or ``None``

        :return:  NodeImage object based on provided information or None if an
                  image with that name is not found.
        :rtype:   :class:`NodeImage` or ``None``
        """

        # The URL for an image can start with gs://

        if url.startswith("gs://"):
            url = url.replace("gs://", "https://storage.googleapis.com/", 1)

        image_data = {
            "name": name,
            "description": description,
            "family": family,
            "sourceType": "RAW",
            "rawDisk": {"source": url},
        }

        if guest_os_features:
            image_data["guestOsFeatures"] = []

            if isinstance(guest_os_features, str):
                guest_os_features = [guest_os_features]

            for feature in guest_os_features:
                image_data["guestOsFeatures"].append({"type": feature})

        request = "/global/images"
        self.connection.async_request(request, method="POST", data=image_data)

        return self.ex_get_image(name)

    def ex_create_instancegroup(
        self,
        name,
        zone,
        description=None,
        network=None,
        subnetwork=None,
        named_ports=None,
    ):
        """
        Creates an instance group in the specified project using the
        parameters that are included in the request.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute

        :param  name:  Required. The name of the instance group. The name
                       must be 1-63 characters long, and comply with RFC1035.
        :type   name: ``str``

        :param  zone:  The URL of the zone where the instance group is
                       located.
        :type   zone: :class:`GCEZone`

        :keyword  description:  An optional description of this resource.
                                Provide this property when you create the
                                resource.
        :type   description: ``str``

        :keyword  network:  The URL of the network to which all instances in
                            the instance group belong.
        :type   network: :class:`GCENetwork`

        :keyword  subnetwork:  The URL of the subnetwork to which all
                               instances in the instance group belong.
        :type   subnetwork: :class:`GCESubnetwork`

        :keyword  named_ports:  Assigns a name to a port number. For example:
                                {name: "http", port: 80}  This allows the
                                system to reference ports by the assigned
                                name instead of a port number. Named ports
                                can also contain multiple ports. For example:
                                [{name: "http", port: 80},{name: "http",
                                port: 8080}]   Named ports apply to all
                                instances in this instance group.
        :type   named_ports: ``list`` of {'name': ``str``, 'port`: ``int``}

        :return:  `GCEInstanceGroup` object.
        :rtype: :class:`GCEInstanceGroup`
        """
        zone = zone or self.zone

        if not hasattr(zone, "name"):
            zone = self.ex_get_zone(zone)
        request = "/zones/%s/instanceGroups" % (zone.name)
        request_data = {}
        request_data["name"] = name
        request_data["zone"] = zone.extra["selfLink"]

        if description:
            request_data["description"] = description

        if network:
            request_data["network"] = network.extra["selfLink"]

        if subnetwork:
            request_data["subnetwork"] = subnetwork.extra["selfLink"]

        if named_ports:
            request_data["namedPorts"] = named_ports

        self.connection.async_request(request, method="POST", data=request_data)

        return self.ex_get_instancegroup(name, zone)

    def ex_create_instancegroupmanager(
        self, name, zone, template, size, base_instance_name=None, description=None
    ):
        """
        Create a Managed Instance Group.

        :param  name: Name of the Instance Group.
        :type   name: ``str``

        :param  zone: The zone to which the Instance Group belongs
        :type   zone: ``str`` or :class:`GCEZone` or ``None``

        :param  template: The Instance Template.  Should be an instance
                                of GCEInstanceTemplate or a string.
        :type   template: ``str`` or :class:`GCEInstanceTemplate`

        :param  base_instance_name: The prefix for each instance created.
                                    If None, Instance Group name will be used.
        :type   base_instance_name: ``str``

        :param  description: User-supplied text about the Instance Group.
        :type   description: ``str``

        :return:  An Instance Group Manager object.
        :rtype:   :class:`GCEInstanceGroupManager`
        """
        zone = zone or self.zone

        if not hasattr(zone, "name"):
            zone = self.ex_get_zone(zone)

        request = "/zones/%s/instanceGroupManagers" % (zone.name)

        manager_data = {}

        # If the user gave us a name, we fetch the GCEInstanceTemplate for it.

        if not hasattr(template, "name"):
            template = self.ex_get_instancetemplate(template)
        manager_data["instanceTemplate"] = template.extra["selfLink"]

        # If base_instance_name is not set, we use name.
        manager_data["baseInstanceName"] = name

        if base_instance_name is not None:
            manager_data["baseInstanceName"] = base_instance_name

        manager_data["name"] = name
        manager_data["targetSize"] = size
        manager_data["description"] = description

        self.connection.async_request(request, method="POST", data=manager_data)

        return self.ex_get_instancegroupmanager(name, zone)

    def ex_create_route(
        self,
        name,
        dest_range,
        priority=500,
        network="default",
        tags=None,
        next_hop=None,
        description=None,
    ):
        """
        Create a route.

        :param  name: Name of route to be created
        :type   name: ``str``

        :param  dest_range: Address range of route in CIDR format.
        :type   dest_range: ``str``

        :param  priority: Priority value, lower values take precedence
        :type   priority: ``int``

        :param  network: The network the route belongs to. Can be either the
                         full URL of the network, the name of the network  or
                         a libcloud object.
        :type   network: ``str`` or ``GCENetwork``

        :param  tags: List of instance-tags for routing, empty for all nodes
        :type   tags: ``list`` of ``str`` or ``None``

        :param  next_hop: Next traffic hop. Use ``None`` for the default
                          Internet gateway, or specify an instance or IP
                          address.
        :type   next_hop: ``str``, ``Node``, or ``None``

        :param  description: Custom description for the route.
        :type   description: ``str`` or ``None``

        :return:  Route object
        :rtype:   :class:`GCERoute`
        """
        route_data = {}
        route_data["name"] = name
        route_data["destRange"] = dest_range
        route_data["priority"] = priority
        route_data["description"] = description

        if isinstance(network, str) and network.startswith("https://"):
            network_uri = network
        elif isinstance(network, str):
            network = self.ex_get_network(network)
            network_uri = network.extra["selfLink"]
        else:
            network_uri = network.extra["selfLink"]
        route_data["network"] = network_uri
        route_data["tags"] = tags

        if next_hop is None:
            url = "https://www.googleapis.com/compute/{}/projects/{}/{}".format(
                API_VERSION,
                self.project,
                "global/gateways/default-internet-gateway",
            )
            route_data["nextHopGateway"] = url
        elif isinstance(next_hop, str):
            route_data["nextHopIp"] = next_hop
        else:
            route_data["nextHopInstance"] = next_hop.extra["selfLink"]

        request = "/global/routes"
        self.connection.async_request(request, method="POST", data=route_data)

        return self.ex_get_route(name)

    def ex_create_sslcertificate(self, name, certificate=None, private_key=None, description=None):
        """
        Creates a SslCertificate resource in the specified project using the
        data included in the request.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute

        :param  name:  Name of the resource. Provided by the client when the
                       resource is created. The name must be 1-63 characters
                       long, and comply with RFC1035. Specifically, the name
                       must be 1-63 characters long and match the regular
                       expression [a-z]([-a-z0-9]*[a-z0-9])? which means the
                       first character must be a lowercase letter, and all
                       following characters must be a dash, lowercase letter,
                       or digit, except the last character, which cannot be a
                       dash.
        :type   name: ``str``

        :param  certificate:  A string containing local certificate file in
                              PEM format. The certificate chain
                              must be no greater than 5 certs long. The
                              chain must include at least one intermediate
                              cert.
        :type   certificate: ``str``

        :param  private_key:  A string containing a write-only private key
                              in PEM format. Only insert RPCs will include
                              this field.
        :type   private_key: ``str``

        :keyword  description:  An optional description of this resource.
                                Provide this property when you create the
                                resource.
        :type   description: ``str``

        :return:  `GCESslCertificate` object.
        :rtype: :class:`GCESslCertificate`
        """

        request = "/global/sslCertificates"
        request_data = {}
        request_data["name"] = name
        request_data["certificate"] = certificate
        request_data["privateKey"] = private_key
        request_data["description"] = description

        self.connection.async_request(request, method="POST", data=request_data)

        return self.ex_get_sslcertificate(name)

    def ex_create_subnetwork(
        self,
        name,
        cidr=None,
        network=None,
        region=None,
        description=None,
        privateipgoogleaccess=None,
        secondaryipranges=None,
    ):
        """
        Create a subnetwork.

        :param  name: Name of subnetwork to be created
        :type   name: ``str``

        :param  cidr: Address range of network in CIDR format.
        :type   cidr: ``str``

        :param  network: The network name or object this subnet belongs to.
        :type   network: ``str`` or :class:`GCENetwork`

        :param  region: The region the subnetwork belongs to.
        :type   region: ``str`` or :class:`GCERegion`

        :param  description: Custom description for the network.
        :type   description: ``str`` or ``None``

        :param  privateipgoogleaccess: Allow access to Google services without
                                       assigned external IP addresses.
        :type   privateipgoogleaccess: ``bool` or ``None``

        :param  secondaryipranges: List of dicts of secondary or "alias" IP
                                   ranges for this subnetwork in
                                   [{"rangeName": "second1",
                                   "ipCidrRange": "192.168.168.0/24"},
                                   {k:v, k:v}] format.
        :type   secondaryipranges: ``list`` of ``dict`` or ``None``

        :return:  Subnetwork object
        :rtype:   :class:`GCESubnetwork`
        """

        if not cidr:
            raise ValueError("Must provide an IP network in CIDR notation.")

        if not network:
            raise ValueError("Must provide a network for the subnetwork.")
        else:
            if isinstance(network, GCENetwork):
                network_url = network.extra["selfLink"]
            else:
                if network.startswith("https://"):
                    network_url = network
                else:
                    network_obj = self.ex_get_network(network)
                    network_url = network_obj.extra["selfLink"]

        if not region:
            raise ValueError("Must provide a region for the subnetwork.")
        else:
            if isinstance(region, GCERegion):
                region_url = region.extra["selfLink"]
            else:
                if region.startswith("https://"):
                    region_url = region
                else:
                    region_obj = self.ex_get_region(region)
                    region_url = region_obj.extra["selfLink"]

        subnet_data = {}
        subnet_data["name"] = name
        subnet_data["description"] = description
        subnet_data["ipCidrRange"] = cidr
        subnet_data["network"] = network_url
        subnet_data["region"] = region_url
        subnet_data["privateIpGoogleAccess"] = privateipgoogleaccess
        subnet_data["secondaryIpRanges"] = secondaryipranges
        region_name = region_url.split("/")[-1]

        request = "/regions/%s/subnetworks" % (region_name)
        self.connection.async_request(request, method="POST", data=subnet_data)

        return self.ex_get_subnetwork(name, region_name)

    def ex_create_network(self, name, cidr, description=None, mode="legacy", routing_mode=None):
        """
        Create a network. In November 2015, Google introduced Subnetworks and
        suggests using networks with 'auto' generated subnetworks. See, the
        `subnet docs <https://cloud.google.com/compute/docs/subnetworks>`_ for
        more details. Note that libcloud follows the usability pattern from
        the Cloud SDK (e.g. 'gcloud compute' command-line utility) and uses
        'mode' to specify 'auto', 'custom', or 'legacy'.

        :param  name: Name of network to be created
        :type   name: ``str``

        :param  cidr: Address range of network in CIDR format.
        :type   cidr: ``str`` or ``None``

        :param  description: Custom description for the network.
        :type   description: ``str`` or ``None``

        :param  mode: Create a 'auto', 'custom', or 'legacy' network.
        :type   mode: ``str``

        :param  routing_mode: Create network with 'Global' or 'Regional'
                              routing mode for BGP advertisements.
                              Defaults to 'Regional'
        :type   routing_mode: ``str`` or ``None``

        :return:  Network object
        :rtype:   :class:`GCENetwork`
        """
        network_data = {}
        network_data["name"] = name
        network_data["description"] = description

        if mode.lower() not in ["auto", "custom", "legacy"]:
            raise ValueError(
                "Invalid network mode: '%s'. Must be 'auto', " "'custom', or 'legacy'." % mode
            )

        if cidr and mode in ["auto", "custom"]:
            raise ValueError("Can only specify IPv4Range with 'legacy' mode.")

        if mode == "legacy":
            if not cidr:
                raise ValueError("Must specify IPv4Range with 'legacy' mode.")
            network_data["IPv4Range"] = cidr
        else:
            network_data["autoCreateSubnetworks"] = mode.lower() == "auto"

        if routing_mode:
            if routing_mode.lower() not in ["regional", "global"]:
                raise ValueError(
                    "Invalid Routing Mode: '%s'. Must be " "'REGIONAL', or 'GLOBAL'." % routing_mode
                )
            else:
                network_data["routingConfig"] = {"routingMode": routing_mode.upper()}

        request = "/global/networks"

        self.connection.async_request(request, method="POST", data=network_data)

        return self.ex_get_network(name)

    def create_node(
        self,
        name,
        size,
        image,
        location=None,
        ex_network="default",
        ex_subnetwork=None,
        ex_tags=None,
        ex_metadata=None,
        ex_boot_disk=None,
        use_existing_disk=True,
        external_ip="ephemeral",
        internal_ip=None,
        ex_disk_type="pd-standard",
        ex_disk_auto_delete=True,
        ex_service_accounts=None,
        description=None,
        ex_can_ip_forward=None,
        ex_disks_gce_struct=None,
        ex_nic_gce_struct=None,
        ex_on_host_maintenance=None,
        ex_automatic_restart=None,
        ex_preemptible=None,
        ex_image_family=None,
        ex_labels=None,
        ex_accelerator_type=None,
        ex_accelerator_count=None,
        ex_disk_size=None,
    ):
        """
        Create a new node and return a node object for the node.

        :param  name: The name of the node to create.
        :type   name: ``str``

        :param  size: The machine type to use.
        :type   size: ``str`` or :class:`GCENodeSize`

        :param  image: The image to use to create the node (or, if attaching
                       a persistent disk, the image used to create the disk)
        :type   image: ``str`` or :class:`GCENodeImage` or ``None``

        :keyword  location: The location (zone) to create the node in.
        :type     location: ``str`` or :class:`NodeLocation` or
                            :class:`GCEZone` or ``None``

        :keyword  ex_network: The network to associate with the node.
        :type     ex_network: ``str`` or :class:`GCENetwork`

        :keyword  ex_subnetwork: The subnetwork to associate with the node.
        :type     ex_subnetwork: ``str`` or :class:`GCESubnetwork`

        :keyword  ex_tags: A list of tags to associate with the node.
        :type     ex_tags: ``list`` of ``str`` or ``None``

        :keyword  ex_metadata: Metadata dictionary for instance.
        :type     ex_metadata: ``dict`` or ``None``

        :keyword  ex_boot_disk: The boot disk to attach to the instance.
        :type     ex_boot_disk: :class:`StorageVolume` or ``str`` or ``None``

        :keyword  use_existing_disk: If True and if an existing disk with the
                                     same name/location is found, use that
                                     disk instead of creating a new one.
        :type     use_existing_disk: ``bool``

        :keyword  external_ip: The external IP address to use.  If 'ephemeral'
                               (default), a new non-static address will be
                               used.  If 'None', then no external address will
                               be used.  To use an existing static IP address,
                               a GCEAddress object should be passed in.
        :type     external_ip: :class:`GCEAddress` or ``str`` or ``None``

        :keyword  internal_ip: The private IP address to use.
        :type     internal_ip: :class:`GCEAddress` or ``str`` or ``None``

        :keyword  ex_disk_type: Specify a pd-standard (default) disk or pd-ssd
                                for an SSD disk.
        :type     ex_disk_type: ``str`` or :class:`GCEDiskType`

        :keyword  ex_disk_auto_delete: Indicate that the boot disk should be
                                       deleted when the Node is deleted. Set to
                                       True by default.
        :type     ex_disk_auto_delete: ``bool``

        :keyword  ex_service_accounts: Specify a list of serviceAccounts when
                                       creating the instance. The format is a
                                       list of dictionaries containing email
                                       and list of scopes, e.g.
                                       [{'email':'default',
                                       'scopes':['compute', ...]}, ...]
                                       Scopes can either be full URLs or short
                                       names. If not provided, use the
                                       'default' service account email and a
                                       scope of 'devstorage.read_only'. Also
                                       accepts the aliases defined in
                                       'gcloud compute'.
        :type     ex_service_accounts: ``list``

        :keyword  description: The description of the node (instance).
        :type     description: ``str`` or ``None``

        :keyword  ex_can_ip_forward: Set to ``True`` to allow this node to
                                  send/receive non-matching src/dst packets.
        :type     ex_can_ip_forward: ``bool`` or ``None``

        :keyword  ex_disks_gce_struct: Support for passing in the GCE-specific
                                       formatted disks[] structure. No attempt
                                       is made to ensure proper formatting of
                                       the disks[] structure. Using this
                                       structure obviates the need of using
                                       other disk params like 'ex_boot_disk',
                                       etc. See the GCE docs for specific
                                       details.
        :type     ex_disks_gce_struct: ``list`` or ``None``

        :keyword  ex_nic_gce_struct: Support passing in the GCE-specific
                                     formatted networkInterfaces[] structure.
                                     No attempt is made to ensure proper
                                     formatting of the networkInterfaces[]
                                     data. Using this structure obviates the
                                     need of using 'external_ip' and
                                     'ex_network'.  See the GCE docs for
                                     details.
        :type     ex_nic_gce_struct: ``list`` or ``None``

        :keyword  ex_on_host_maintenance: Defines whether node should be
                                          terminated or migrated when host
                                          machine goes down. Acceptable values
                                          are: 'MIGRATE' or 'TERMINATE' (If
                                          not supplied, value will be reset to
                                          GCE default value for the instance
                                          type.)
        :type     ex_on_host_maintenance: ``str`` or ``None``

        :keyword  ex_automatic_restart: Defines whether the instance should be
                                        automatically restarted when it is
                                        terminated by Compute Engine. (If not
                                        supplied, value will be set to the GCE
                                        default value for the instance type.)
        :type     ex_automatic_restart: ``bool`` or ``None``

        :keyword  ex_preemptible: Defines whether the instance is preemptible.
                                  (If not supplied, the instance will not be
                                  preemptible)
        :type     ex_preemptible: ``bool`` or ``None``

        :keyword  ex_image_family: Determine image from an 'Image Family'
                                   instead of by name. 'image' should be None
                                   to use this keyword.
        :type     ex_image_family: ``str`` or ``None``

        :keyword  ex_labels: Labels dictionary for instance.
        :type     ex_labels: ``dict`` or ``None``

        :keyword  ex_accelerator_type: Defines the accelerator to use with this
                                       node. Must set 'ex_on_host_maintenance'
                                       to 'TERMINATE'. Must include a count of
                                       accelerators to use in
                                       'ex_accelerator_count'.
        :type     ex_accelerator_type: ``str`` or ``None``

        :keyword  ex_accelerator_count: The number of 'ex_accelerator_type'
                                        accelerators to attach to the node.
        :type     ex_accelerator_count: ``int`` or ``None``

        :keyword  ex_disk_size: Defines size of the boot disk.
                                Integer in gigabytes.
        :type     ex_disk_size: ``int`` or ``None``

        :return:  A Node object for the new node.
        :rtype:   :class:`Node`
        """

        if ex_boot_disk and ex_disks_gce_struct:
            raise ValueError("Cannot specify both 'ex_boot_disk' and " "'ex_disks_gce_struct'")

        if image and ex_image_family:
            raise ValueError("Cannot specify both 'image' and " "'ex_image_family'")

        if not (image or ex_image_family or ex_boot_disk or ex_disks_gce_struct):
            raise ValueError(
                "Missing root device or image. Must specify an "
                "'image', 'ex_image_family', existing "
                "'ex_boot_disk', or use the "
                "'ex_disks_gce_struct'."
            )

        location = location or self.zone

        if location and not hasattr(location, "name"):
            location = self.ex_get_zone(location)

        if not hasattr(size, "name"):
            size = self.ex_get_size(size, location)

        if not hasattr(ex_network, "name"):
            ex_network = self.ex_get_network(ex_network)

        if ex_subnetwork and not hasattr(ex_subnetwork, "name"):
            ex_subnetwork = self.ex_get_subnetwork(
                ex_subnetwork, region=self._get_region_from_zone(location)
            )

        if ex_image_family:
            image = self.ex_get_image_from_family(ex_image_family)

        if image and not hasattr(image, "name"):
            image = self.ex_get_image(image)

        if ex_disk_type and not hasattr(ex_disk_type, "name"):
            ex_disk_type = self.ex_get_disktype(ex_disk_type, zone=location)

        if ex_boot_disk and not hasattr(ex_boot_disk, "name"):
            ex_boot_disk = self.ex_get_volume(ex_boot_disk, zone=location)

        if ex_accelerator_type and not hasattr(ex_accelerator_type, "name"):
            if ex_accelerator_count is None:
                raise ValueError(
                    "Missing accelerator count. Must specify an "
                    "'ex_accelerator_count' when using "
                    "'ex_accelerator_type'."
                )
            ex_accelerator_type = self.ex_get_accelerator_type(ex_accelerator_type, zone=location)

        # Use disks[].initializeParams to auto-create the boot disk

        if not ex_disks_gce_struct and not ex_boot_disk:
            ex_disks_gce_struct = [
                {
                    "autoDelete": ex_disk_auto_delete,
                    "boot": True,
                    "type": "PERSISTENT",
                    "mode": "READ_WRITE",
                    "deviceName": name,
                    "initializeParams": {
                        "diskName": name,
                        "diskSizeGb": ex_disk_size,
                        "diskType": ex_disk_type.extra["selfLink"],
                        "sourceImage": image.extra["selfLink"],
                    },
                }
            ]

        if not location and size.extra.get("zone", None):
            # If location is not provided (either via datacenter driver
            # constructor argument or via location argument in this method)
            # we simply default to size location. Not ideal, but it works
            # for the default experience.
            location = size.extra["zone"]

        # Verify that the location is provided if we can't infer one
        self._verify_zone_is_set(zone=location)

        request, node_data = self._create_node_req(
            name,
            size,
            image,
            location,
            network=ex_network,
            tags=ex_tags,
            metadata=ex_metadata,
            boot_disk=ex_boot_disk,
            external_ip=external_ip,
            internal_ip=internal_ip,
            ex_disk_type=ex_disk_type,
            ex_disk_auto_delete=ex_disk_auto_delete,
            ex_service_accounts=ex_service_accounts,
            description=description,
            ex_can_ip_forward=ex_can_ip_forward,
            ex_disks_gce_struct=ex_disks_gce_struct,
            ex_nic_gce_struct=ex_nic_gce_struct,
            ex_on_host_maintenance=ex_on_host_maintenance,
            ex_automatic_restart=ex_automatic_restart,
            ex_preemptible=ex_preemptible,
            ex_subnetwork=ex_subnetwork,
            ex_labels=ex_labels,
            ex_accelerator_type=ex_accelerator_type,
            ex_accelerator_count=ex_accelerator_count,
        )
        self.connection.async_request(request, method="POST", data=node_data)

        return self.ex_get_node(name, location.name)

    def ex_create_instancetemplate(
        self,
        name,
        size,
        source=None,
        image=None,
        disk_type="pd-standard",
        disk_auto_delete=True,
        network="default",
        subnetwork=None,
        can_ip_forward=None,
        external_ip="ephemeral",
        internal_ip=None,
        service_accounts=None,
        on_host_maintenance=None,
        automatic_restart=None,
        preemptible=None,
        tags=None,
        metadata=None,
        description=None,
        disks_gce_struct=None,
        nic_gce_struct=None,
    ):
        """
        Creates an instance template in the specified project using the data
        that is included in the request. If you are creating a new template to
        update an existing instance group, your new instance template must
        use the same network or, if applicable, the same subnetwork as the
        original template.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute

        :param  name: The name of the node to create.
        :type   name: ``str``

        :param  size: The machine type to use.
        :type   size: ``str`` or :class:`GCENodeSize`

        :param  image: The image to use to create the node (or, if attaching
                       a persistent disk, the image used to create the disk)
        :type   image: ``str`` or :class:`GCENodeImage` or ``None``

        :keyword  network: The network to associate with the template.
        :type     network: ``str`` or :class:`GCENetwork`

        :keyword  subnetwork: The subnetwork to associate with the node.
        :type     subnetwork: ``str`` or :class:`GCESubnetwork`

        :keyword  tags: A list of tags to associate with the node.
        :type     tags: ``list`` of ``str`` or ``None``

        :keyword  metadata: Metadata dictionary for instance.
        :type     metadata: ``dict`` or ``None``

        :keyword  external_ip: The external IP address to use.  If 'ephemeral'
                               (default), a new non-static address will be
                               used.  If 'None', then no external address will
                               be used.  To use an existing static IP address,
                               a GCEAddress object should be passed in.
        :type     external_ip: :class:`GCEAddress` or ``str`` or ``None``

        :keyword  internal_ip: The private IP address to use.
        :type     internal_ip: :class:`GCEAddress` or ``str`` or ``None``

        :keyword  disk_type: Specify a pd-standard (default) disk or pd-ssd
                                for an SSD disk.
        :type     disk_type: ``str`` or :class:`GCEDiskType`

        :keyword  disk_auto_delete: Indicate that the boot disk should be
                                       deleted when the Node is deleted. Set to
                                       True by default.
        :type     disk_auto_delete: ``bool``

        :keyword  service_accounts: Specify a list of serviceAccounts when
                                       creating the instance. The format is a
                                       list of dictionaries containing email
                                       and list of scopes, e.g.
                                       [{'email':'default',
                                       'scopes':['compute', ...]}, ...]
                                       Scopes can either be full URLs or short
                                       names. If not provided, use the
                                       'default' service account email and a
                                       scope of 'devstorage.read_only'. Also
                                       accepts the aliases defined in
                                       'gcloud compute'.
        :type     service_accounts: ``list``

        :keyword  description: The description of the node (instance).
        :type     description: ``str`` or ``None``

        :keyword  can_ip_forward: Set to ``True`` to allow this node to
                                  send/receive non-matching src/dst packets.
        :type     can_ip_forward: ``bool`` or ``None``

        :keyword  disks_gce_struct: Support for passing in the GCE-specific
                                       formatted disks[] structure. No attempt
                                       is made to ensure proper formatting of
                                       the disks[] structure. Using this
                                       structure obviates the need of using
                                       other disk params like 'ex_boot_disk',
                                       etc. See the GCE docs for specific
                                       details.
        :type     disks_gce_struct: ``list`` or ``None``

        :keyword  nic_gce_struct: Support passing in the GCE-specific
                                     formatted networkInterfaces[] structure.
                                     No attempt is made to ensure proper
                                     formatting of the networkInterfaces[]
                                     data. Using this structure obviates the
                                     need of using 'external_ip' and
                                     'ex_network'.  See the GCE docs for
                                     details.
        :type     nic_gce_struct: ``list`` or ``None``

        :keyword  on_host_maintenance: Defines whether node should be
                                          terminated or migrated when host
                                          machine goes down. Acceptable values
                                          are: 'MIGRATE' or 'TERMINATE' (If
                                          not supplied, value will be reset to
                                          GCE default value for the instance
                                          type.)
        :type     ex_on_host_maintenance: ``str`` or ``None``

        :keyword  automatic_restart: Defines whether the instance should be
                                        automatically restarted when it is
                                        terminated by Compute Engine. (If not
                                        supplied, value will be set to the GCE
                                        default value for the instance type.)
        :type     automatic_restart: ``bool`` or ``None``

        :keyword  preemptible: Defines whether the instance is preemptible.
                                  (If not supplied, the instance will not be
                                  preemptible)
        :type     preemptible: ``bool`` or ``None``

        :return:  An Instance Template object.
        :rtype:   :class:`GCEInstanceTemplate`
        """
        request = "/global/instanceTemplates"

        properties = self._create_instance_properties(
            name,
            node_size=size,
            source=source,
            image=image,
            disk_type=disk_type,
            disk_auto_delete=True,
            external_ip=external_ip,
            network=network,
            subnetwork=subnetwork,
            can_ip_forward=can_ip_forward,
            service_accounts=service_accounts,
            on_host_maintenance=on_host_maintenance,
            internal_ip=internal_ip,
            automatic_restart=automatic_restart,
            preemptible=preemptible,
            tags=tags,
            metadata=metadata,
            description=description,
            disks_gce_struct=disks_gce_struct,
            nic_gce_struct=nic_gce_struct,
            use_selflinks=False,
        )

        request_data = {
            "name": name,
            "description": description,
            "properties": properties,
        }

        self.connection.async_request(request, method="POST", data=request_data)

        return self.ex_get_instancetemplate(name)

    def _create_instance_properties(
        self,
        name,
        node_size,
        source=None,
        image=None,
        disk_type="pd-standard",
        disk_auto_delete=True,
        network="default",
        subnetwork=None,
        external_ip="ephemeral",
        internal_ip=None,
        can_ip_forward=None,
        service_accounts=None,
        on_host_maintenance=None,
        automatic_restart=None,
        preemptible=None,
        tags=None,
        metadata=None,
        description=None,
        disks_gce_struct=None,
        nic_gce_struct=None,
        use_selflinks=True,
        labels=None,
        accelerator_type=None,
        accelerator_count=None,
        disk_size=None,
    ):
        """
        Create the GCE instance properties needed for instance templates.

        :param    node_size: The machine type to use.
        :type     node_size: ``str`` or :class:`GCENodeSize`

        :keyword  source: A source disk to attach to the instance. Cannot
                          specify both 'image' and 'source'.
        :type     source: :class:`StorageVolume` or ``str`` or ``None``

        :param    image: The image to use to create the node. Cannot specify
                         both 'image' and 'source'.
        :type     image: ``str`` or :class:`GCENodeImage` or ``None``

        :keyword  disk_type: Specify a pd-standard (default) disk or pd-ssd
                             for an SSD disk.
        :type     disk_type: ``str`` or :class:`GCEDiskType`

        :keyword  disk_auto_delete: Indicate that the boot disk should be
                                    deleted when the Node is deleted. Set to
                                    True by default.
        :type     disk_auto_delete: ``bool``

        :keyword  network: The network to associate with the node.
        :type     network: ``str`` or :class:`GCENetwork`

        :keyword  subnetwork: The Subnetwork resource for this instance. If
                              the network resource is in legacy mode, do not
                              provide this property. If the network is in auto
                              subnet mode, providing the subnetwork is
                              optional. If the network is in custom subnet
                              mode, then this field should be specified.
        :type     subnetwork: :class: `GCESubnetwork` or None

        :keyword  external_ip: The external IP address to use.  If 'ephemeral'
                               (default), a new non-static address will be
                               used.  If 'None', then no external address will
                               be used.  To use an existing static IP address,
                               a GCEAddress object should be passed in.
        :type     external_ip: :class:`GCEAddress` or ``str`` or ``None``

        :keyword  internal_ip: The private IP address to use.
        :type     internal_ip: :class:`GCEAddress` or ``str`` or ``None``

        :keyword  can_ip_forward: Set to ``True`` to allow this node to
                                  send/receive non-matching src/dst packets.
        :type     can_ip_forward: ``bool`` or ``None``

        :keyword  service_accounts: Specify a list of serviceAccounts when
                                    creating the instance. The format is a
                                    list of dictionaries containing email
                                    and list of scopes, e.g.
                                    [{'email':'default',
                                    'scopes':['compute', ...]}, ...]
                                    Scopes can either be full URLs or short
                                    names. If not provided, use the
                                    'default' service account email and a
                                    scope of 'devstorage.read_only'. Also
                                    accepts the aliases defined in
                                    'gcloud compute'.
        :type     service_accounts: ``list``

        :keyword  on_host_maintenance: Defines whether node should be
                                       terminated or migrated when host
                                       machine goes down. Acceptable values
                                       are: 'MIGRATE' or 'TERMINATE' (If
                                       not supplied, value will be reset to
                                       GCE default value for the instance
                                       type.)
        :type     on_host_maintenance: ``str`` or ``None``

        :keyword  automatic_restart: Defines whether the instance should be
                                     automatically restarted when it is
                                     terminated by Compute Engine. (If not
                                     supplied, value will be set to the GCE
                                     default value for the instance type.)
        :type     automatic_restart: ``bool`` or ``None``

        :keyword  preemptible: Defines whether the instance is preemptible.
                               (If not supplied, the instance will not be
                               preemptible)
        :type     preemptible: ``bool`` or ``None``

        :keyword  tags: A list of tags to associate with the node.
        :type     tags: ``list`` of ``str`` or ``None``

        :keyword  metadata: Metadata dictionary for instance.
        :type     metadata: ``dict`` or ``None``

        :keyword  description: The description of the node (instance).
        :type     description: ``str`` or ``None``

        :keyword  disks_gce_struct: Support for passing in the GCE-specific
                                    formatted disks[] structure. No attempt
                                    is made to ensure proper formatting of
                                    the disks[] structure. Using this
                                    structure obviates the need of using
                                    other disk params like 'boot_disk',
                                    etc. See the GCE docs for specific
                                    details.
        :type     disks_gce_struct: ``list`` or ``None``

        :keyword  nic_gce_struct: Support passing in the GCE-specific
                                  formatted networkInterfaces[] structure.
                                  No attempt is made to ensure proper
                                  formatting of the networkInterfaces[]
                                  data. Using this structure obviates the
                                  need of using 'external_ip' and
                                  'network'.  See the GCE docs for
                                  details.
        :type     nic_gce_struct: ``list`` or ``None``

        :type     labels: Labels dict for instance
        :type     labels: ``dict`` or ``None``

        :keyword  accelerator_type: Support for passing in the GCE-specifc
                                    accelerator type to request for the VM.
        :type     accelerator_type: :class:`GCEAcceleratorType` or ``None``

        :keyword  accelerator_count: Support for passing in the number of
                                     requested 'accelerator_type' accelerators
                                     attached to the VM. Will only pay attention
                                     to this field if 'accelerator_type' is not
                                     None.
        :type     accelerator_count: ``int`` or ``None``

        :keyword  disk_size: Specify size of the boot disk.
                             Integer in gigabytes.
        :type     disk_size: ``int`` or ``None``

        :return:  A dictionary formatted for use with the GCE API.
        :rtype:   ``dict``
        """
        instance_properties = {}

        # build disks

        if not image and not source and not disks_gce_struct:
            raise ValueError(
                "Missing root device or image. Must specify an "
                "'image', source, or use the "
                "'disks_gce_struct'."
            )

        if source and disks_gce_struct:
            raise ValueError(
                "Cannot specify both 'source' and " "'disks_gce_struct'. Use one or the other."
            )

        if disks_gce_struct:
            instance_properties["disks"] = disks_gce_struct
        else:
            disk_name = None
            device_name = None

            if source:
                disk_name = source.name
                # TODO(supertom): what about device name?
                device_name = source.name
                image = None

            instance_properties["disks"] = [
                self._build_disk_gce_struct(
                    device_name,
                    source=source,
                    disk_type=disk_type,
                    image=image,
                    disk_name=disk_name,
                    usage_type="PERSISTENT",
                    mount_mode="READ_WRITE",
                    auto_delete=disk_auto_delete,
                    is_boot=True,
                    use_selflinks=use_selflinks,
                    disk_size=disk_size,
                )
            ]

        # build network interfaces

        if nic_gce_struct is not None:
            if hasattr(external_ip, "address"):
                raise ValueError(
                    "Cannot specify both a static IP address "
                    "and 'nic_gce_struct'. Use one or the "
                    "other."
                )

            if hasattr(network, "name"):
                if network.name == "default":  # pylint: disable=no-member
                    # assume this is just the default value from create_node()
                    # and since the user specified ex_nic_gce_struct, the
                    # struct should take precedence
                    network = None
                else:
                    raise ValueError(
                        "Cannot specify both 'network' and "
                        "'nic_gce_struct'. Use one or the "
                        "other."
                    )
            instance_properties["networkInterfaces"] = nic_gce_struct
        else:
            instance_properties["networkInterfaces"] = [
                self._build_network_gce_struct(
                    network=network,
                    subnetwork=subnetwork,
                    external_ip=external_ip,
                    use_selflinks=True,
                    internal_ip=internal_ip,
                )
            ]

        # build scheduling
        scheduling = self._build_scheduling_gce_struct(
            on_host_maintenance, automatic_restart, preemptible
        )

        if scheduling:
            instance_properties["scheduling"] = scheduling

        # build service accounts/scopes
        instance_properties["serviceAccounts"] = self._build_service_accounts_gce_list(
            service_accounts
        )

        # build accelerators

        if accelerator_type is not None:
            instance_properties["guestAccelerators"] = self._format_guest_accelerators(
                accelerator_type, accelerator_count
            )

        # include general properties

        if description:
            instance_properties["description"] = str(description)

        if tags:
            instance_properties["tags"] = {"items": tags}

        if metadata:
            instance_properties["metadata"] = self._format_metadata(
                fingerprint="na", metadata=metadata
            )

        if labels:
            instance_properties["labels"] = labels

        if can_ip_forward:
            instance_properties["canIpForward"] = True

        instance_properties["machineType"] = self._get_selflink_or_name(
            obj=node_size, get_selflinks=use_selflinks, objname="size"
        )

        return instance_properties

    def _build_disk_gce_struct(
        self,
        device_name,
        source=None,
        disk_type=None,
        disk_size=None,
        image=None,
        disk_name=None,
        is_boot=True,
        mount_mode="READ_WRITE",
        usage_type="PERSISTENT",
        auto_delete=True,
        use_selflinks=True,
    ):
        """
        Generates the GCP dict for a disk.

        :param    device_name: Specifies a unique device name of your
                               choice that is reflected into the
                               /dev/disk/by-id/google-* tree
                               of a Linux operating system running within the
                               instance. This name can be used to reference the
                               device for mounting, resizing, and so on, from
                               within the instance.  Defaults to disk_name.
        :type      device_name: ``str``

        :keyword   source: The disk to attach to the instance.
        :type      source: ``str`` of selfLink, :class:`StorageVolume` or None

        :keyword   disk_type: Specify a URL or DiskType object.
        :type      disk_type: ``str`` or :class:`GCEDiskType` or ``None``

        :keyword   image: The image to use to create the disk.
        :type      image: :class:`GCENodeImage` or ``None``

        :keyword   disk_size: Integer in gigabytes.
        :type      disk_size: ``int``

        :param     disk_name: Specifies the disk name. If not specified, the
                              default is to use the device_name.
        :type      disk_name: ``str``

        :keyword   mount_mode: The mode in which to attach this disk, either
                               READ_WRITE or READ_ONLY. If not specified,
                               the default is to attach the disk in READ_WRITE
                               mode.
        :type      mount_mode: ``str``

        :keyword   usage_type: Specifies the type of the disk, either SCRATCH
                               or PERSISTENT. If not specified, the default
                               is PERSISTENT.
        :type      usage_type: ``str``

        :keyword   auto_delete: Indicate that the boot disk should be
                                deleted when the Node is deleted. Set to
                                True by default.
        :type      auto_delete: ``bool``

        :return:   Dictionary to be used in disk-portion of
                   instance API call.
        :rtype:    ``dict``
        """
        # validation

        if source is None and image is None:
            raise ValueError("Either the 'source' or 'image' argument must be specified.")

        if not isinstance(auto_delete, bool):
            raise ValueError("auto_delete field is not a bool.")

        if disk_size is not None and not (isinstance(disk_size, int) or disk_size.isdigit()):
            raise ValueError("disk_size must be a digit, '%s' provided." % str(disk_size))

        mount_modes = ["READ_WRITE", "READ_ONLY"]

        if mount_mode not in mount_modes:
            raise ValueError("mount mode must be one of: %s." % (",".join(mount_modes)))
        usage_types = ["PERSISTENT", "SCRATCH"]

        if usage_type not in usage_types:
            raise ValueError("usage type must be one of: %s." % (",".join(usage_types)))

        disk = {}

        if not disk_name:
            disk_name = device_name

        if source is not None:
            disk["source"] = self._get_selflink_or_name(
                obj=source, get_selflinks=use_selflinks, objname="volume"
            )

        else:
            # create new disk
            # we need the URL of the image, always.
            image = self._get_selflink_or_name(obj=image, get_selflinks=True, objname="image")
            disk_type = self._get_selflink_or_name(
                obj=disk_type, get_selflinks=use_selflinks, objname="disktype"
            )

            disk["initializeParams"] = {
                "diskName": disk_name,
                "diskType": disk_type,
                "sourceImage": image,
            }

            if disk_size is not None:
                disk["initializeParams"]["diskSizeGb"] = disk_size

        # add in basic attributes
        disk.update(
            {
                "boot": is_boot,
                "type": usage_type,
                "mode": mount_mode,
                "deviceName": device_name,
                "autoDelete": auto_delete,
            }
        )

        return disk

    def _get_selflink_or_name(self, obj, get_selflinks=True, objname=None):
        """
        Return the selflink or name, given a name or object.

        Will try to fetch the appropriate object if necessary (assumes
        we only need one parameter to fetch the object, no introspection
        is performed).

        :param    obj: object to test.
        :type     obj: ``str`` or ``object``

        :param    get_selflinks: Inform if we should return selfLinks or just
                              the name.  Default is True.
        :param    get_selflinks: ``bool``

        :param    objname: string to use in constructing method call
        :type     objname: ``str`` or None

        :return:  URL from extra['selfLink'] or name
        :rtype:   ``str``
        """

        if get_selflinks:
            if not hasattr(obj, "name"):
                if objname:
                    getobj = getattr(self, "ex_get_%s" % (objname))
                    obj = getobj(obj)
                else:
                    raise ValueError("objname must be set if selflinks is True.")

            return obj.extra["selfLink"]
        else:
            if not hasattr(obj, "name"):
                return obj
            else:
                return obj.name

    def _build_network_gce_struct(
        self,
        network,
        subnetwork=None,
        external_ip=None,
        use_selflinks=True,
        internal_ip=None,
    ):
        """
        Build network interface dict for use in the GCE API.

        Note: Must be wrapped in a list before passing to the GCE API.

        :param    network: The network to associate with the node.
        :type     network: :class:`GCENetwork`

        :keyword  subnetwork: The subnetwork to include.
        :type     subnetwork: :class:`GCESubNetwork`

        :keyword  external_ip: The external IP address to use.  If 'ephemeral'
                               (default), a new non-static address will be
                               used.  If 'None', then no external address will
                               be used.  To use an existing static IP address,
                               a GCEAddress object should be passed in.
        :type     external_ip: :class:`GCEAddress`

        :keyword  internal_ip: The private IP address to use.
        :type     internal_ip: :class:`GCEAddress` or ``str``

        :return:  network interface dict
        :rtype:   ``dict``
        """
        ni = {}
        ni = {"kind": "compute#instanceNetworkInterface"}

        if network is None:
            network = "default"

        ni["network"] = self._get_selflink_or_name(
            obj=network, get_selflinks=use_selflinks, objname="network"
        )

        if subnetwork:
            ni["subnetwork"] = self._get_selflink_or_name(
                obj=subnetwork, get_selflinks=use_selflinks, objname="subnetwork"
            )

        if external_ip:
            access_configs = [{"name": "External NAT", "type": "ONE_TO_ONE_NAT"}]

            if hasattr(external_ip, "address"):
                access_configs[0]["natIP"] = external_ip.address
            ni["accessConfigs"] = access_configs

        if internal_ip:
            ni["networkIP"] = internal_ip

        return ni

    def _build_service_account_gce_struct(
        self,
        service_account,
        default_email="default",
        default_scope="devstorage.read_only",
    ):
        """
         Helper to create Service Account dict.  Use
         _build_service_accounts_gce_list to create a list ready for the
         GCE API.

         :param: service_account: dictionarie containing email
                                  and list of scopes, e.g.
                                  [{'email':'default',
                                  'scopes':['compute', ...]}, ...]
                                  Scopes can either be full URLs or short
                                  names. If not provided, use the
                                  'default' service account email and a
                                  scope of 'devstorage.read_only'. Also
                                  accepts the aliases defined in
                                  'gcloud compute'.
        :type    service_account: ``dict`` or None

        :return: dict usable in GCE API call.
        :rtype:  ``dict``
        """

        if not isinstance(service_account, dict):
            raise ValueError(
                "service_account not in the correct format,"
                "'%s - %s'" % (str(type(service_account)), str(service_account))
            )
        sa = {}

        if "email" not in service_account:
            sa["email"] = default_email
        else:
            sa["email"] = service_account["email"]

        if "scopes" not in service_account:
            sa["scopes"] = [self.AUTH_URL + default_scope]
        else:
            ps = []

            for scope in service_account["scopes"]:
                if scope.startswith(self.AUTH_URL):
                    ps.append(scope)
                elif scope in self.SA_SCOPES_MAP:
                    ps.append(self.AUTH_URL + self.SA_SCOPES_MAP[scope])
                else:
                    ps.append(self.AUTH_URL + scope)
            sa["scopes"] = ps

        return sa

    def _build_service_accounts_gce_list(
        self,
        service_accounts=None,
        default_email="default",
        default_scope="devstorage.read_only",
    ):
        """
        Helper to create service account list for GCE API.

        :keyword  service_accounts: Specify a list of serviceAccounts when
                                       creating the instance. The format is a
                                       list of dictionaries containing email
                                       and list of scopes, e.g.
                                       [{'email':'default',
                                       'scopes':['compute', ...]}, ...]
                                       Scopes can either be full URLs or short
                                       names. If not provided, use the
                                       'default' service account email and a
                                       scope of 'devstorage.read_only'. Also
                                       accepts the aliases defined in
                                       'gcloud compute'.

        :type     service_accounts: ``list`` of ``dict``, ``None`` or an empty
                                    list. ``None` means use a default service
                                    account and an empty list indicates no
                                    service account.

        :return:  list of dictionaries usable in the GCE API.
        :rtype:   ``list`` of ``dict``
        """
        gce_service_accounts = []

        if service_accounts is None:
            gce_service_accounts = [
                {"email": default_email, "scopes": [self.AUTH_URL + default_scope]}
            ]
        elif not isinstance(service_accounts, list):
            raise ValueError("service_accounts field is not a list.")
        else:
            for sa in service_accounts:
                gce_service_accounts.append(
                    self._build_service_account_gce_struct(service_account=sa)
                )

        return gce_service_accounts

    def _build_scheduling_gce_struct(
        self, on_host_maintenance=None, automatic_restart=None, preemptible=None
    ):
        """
        Build the scheduling dict suitable for use with the GCE API.

        :param    on_host_maintenance: Defines whether node should be
                                          terminated or migrated when host
                                          machine goes down. Acceptable values
                                          are: 'MIGRATE' or 'TERMINATE' (If
                                          not supplied, value will be reset to
                                          GCE default value for the instance
                                          type.)
        :type     on_host_maintenance: ``str`` or ``None``

        :param    automatic_restart: Defines whether the instance should be
                                        automatically restarted when it is
                                        terminated by Compute Engine. (If not
                                        supplied, value will be set to the GCE
                                        default value for the instance type.)
        :type     automatic_restart: ``bool`` or ``None``

        :param    preemptible: Defines whether the instance is preemptible.
                                        (If not supplied, the instance will
                                         not be preemptible)
        :type     preemptible: ``bool`` or ``None``

        :return:  A dictionary of scheduling options for the GCE API.
        :rtype:   ``dict``
        """
        scheduling = {}

        if preemptible is not None:
            if isinstance(preemptible, bool):
                scheduling["preemptible"] = preemptible
            else:
                raise ValueError("boolean expected for preemptible")

        if on_host_maintenance is not None:
            maint_opts = ["MIGRATE", "TERMINATE"]

            if isinstance(on_host_maintenance, str) and on_host_maintenance in maint_opts:
                if preemptible is True and on_host_maintenance == "MIGRATE":
                    raise ValueError(
                        "host maintenance cannot be 'MIGRATE' " "if instance is preemptible."
                    )
                scheduling["onHostMaintenance"] = on_host_maintenance
            else:
                raise ValueError("host maintenance must be one of %s" % (",".join(maint_opts)))

        if automatic_restart is not None:
            if isinstance(automatic_restart, bool):
                if automatic_restart is True and preemptible is True:
                    raise ValueError("instance cannot be restarted if it is preemptible.")
                scheduling["automaticRestart"] = automatic_restart

            else:
                raise ValueError("boolean expected for automatic")

        return scheduling

    def ex_create_multiple_nodes(
        self,
        base_name,
        size,
        image,
        number,
        location=None,
        ex_network="default",
        ex_subnetwork=None,
        ex_tags=None,
        ex_metadata=None,
        ignore_errors=True,
        use_existing_disk=True,
        poll_interval=2,
        external_ip="ephemeral",
        internal_ip=None,
        ex_disk_type="pd-standard",
        ex_disk_auto_delete=True,
        ex_service_accounts=None,
        timeout=DEFAULT_TASK_COMPLETION_TIMEOUT,
        description=None,
        ex_can_ip_forward=None,
        ex_disks_gce_struct=None,
        ex_nic_gce_struct=None,
        ex_on_host_maintenance=None,
        ex_automatic_restart=None,
        ex_image_family=None,
        ex_preemptible=None,
        ex_labels=None,
        ex_disk_size=None,
    ):
        """
        Create multiple nodes and return a list of Node objects.

        Nodes will be named with the base name and a number.  For example, if
        the base name is 'libcloud' and you create 3 nodes, they will be
        named::

            libcloud-000
            libcloud-001
            libcloud-002

        :param  base_name: The base name of the nodes to create.
        :type   base_name: ``str``

        :param  size: The machine type to use.
        :type   size: ``str`` or :class:`GCENodeSize`

        :param  image: The image to use to create the nodes.
        :type   image: ``str`` or :class:`GCENodeImage`

        :param  number: The number of nodes to create.
        :type   number: ``int``

        :keyword  location: The location (zone) to create the nodes in.
        :type     location: ``str`` or :class:`NodeLocation` or
                            :class:`GCEZone` or ``None``

        :keyword  ex_network: The network to associate with the nodes.
        :type     ex_network: ``str`` or :class:`GCENetwork`

        :keyword  ex_tags: A list of tags to associate with the nodes.
        :type     ex_tags: ``list`` of ``str`` or ``None``

        :keyword  ex_metadata: Metadata dictionary for instances.
        :type     ex_metadata: ``dict`` or ``None``

        :keyword  ignore_errors: If True, don't raise Exceptions if one or
                                 more nodes fails.
        :type     ignore_errors: ``bool``

        :keyword  use_existing_disk: If True and if an existing disk with the
                                     same name/location is found, use that
                                     disk instead of creating a new one.
        :type     use_existing_disk: ``bool``

        :keyword  poll_interval: Number of seconds between status checks.
        :type     poll_interval: ``int``

        :keyword  external_ip: The external IP address to use.  If 'ephemeral'
                               (default), a new non-static address will be
                               used. If 'None', then no external address will
                               be used. (Static addresses are not supported for
                               multiple node creation.)
        :type     external_ip: ``str`` or None


        :keyword  internal_ip: The private IP address to use.
        :type     internal_ip: :class:`GCEAddress` or ``str`` or ``None``

        :keyword  ex_disk_type: Specify a pd-standard (default) disk or pd-ssd
                                for an SSD disk.
        :type     ex_disk_type: ``str`` or :class:`GCEDiskType`

        :keyword  ex_disk_auto_delete: Indicate that the boot disk should be
                                       deleted when the Node is deleted. Set to
                                       True by default.
        :type     ex_disk_auto_delete: ``bool``

        :keyword  ex_service_accounts: Specify a list of serviceAccounts when
                                       creating the instance. The format is a
                                       list of dictionaries containing email
                                       and list of scopes, e.g.
                                       [{'email':'default',
                                       'scopes':['compute', ...]}, ...]
                                       Scopes can either be full URLs or short
                                       names. If not provided, use the
                                       'default' service account email and a
                                       scope of 'devstorage.read_only'. Also
                                       accepts the aliases defined in
                                       'gcloud compute'.
        :type     ex_service_accounts: ``list``

        :keyword  timeout: The number of seconds to wait for all nodes to be
                           created before timing out.
        :type     timeout: ``int``

        :keyword  description: The description of the node (instance).
        :type     description: ``str`` or ``None``

        :keyword  ex_can_ip_forward: Set to ``True`` to allow this node to
                                     send/receive non-matching src/dst packets.
        :type     ex_can_ip_forward: ``bool`` or ``None``

        :keyword  ex_preemptible: Defines whether the instance is preemptible.
                                  (If not supplied, the instance will
                                  not be preemptible)
        :type     ex_preemptible: ``bool`` or ``None``

        :keyword  ex_disks_gce_struct: Support for passing in the GCE-specific
                                       formatted disks[] structure. No attempt
                                       is made to ensure proper formatting of
                                       the disks[] structure. Using this
                                       structure obviates the need of using
                                       other disk params like 'ex_boot_disk',
                                       etc. See the GCE docs for specific
                                       details.
        :type     ex_disks_gce_struct: ``list`` or ``None``

        :keyword  ex_nic_gce_struct: Support passing in the GCE-specific
                                     formatted networkInterfaces[] structure.
                                     No attempt is made to ensure proper
                                     formatting of the networkInterfaces[]
                                     data. Using this structure obviates the
                                     need of using 'external_ip' and
                                     'ex_network'.  See the GCE docs for
                                     details.
        :type     ex_nic_gce_struct: ``list`` or ``None``

        :keyword  ex_on_host_maintenance: Defines whether node should be
                                          terminated or migrated when host
                                          machine goes down. Acceptable values
                                          are: 'MIGRATE' or 'TERMINATE' (If
                                          not supplied, value will be reset to
                                          GCE default value for the instance
                                          type.)
        :type     ex_on_host_maintenance: ``str`` or ``None``

        :keyword  ex_automatic_restart: Defines whether the instance should be
                                        automatically restarted when it is
                                        terminated by Compute Engine. (If not
                                        supplied, value will be set to the GCE
                                        default value for the instance type.)
        :type     ex_automatic_restart: ``bool`` or ``None``

        :keyword  ex_image_family: Determine image from an 'Image Family'
                                   instead of by name. 'image' should be None
                                   to use this keyword.
        :type     ex_image_family: ``str`` or ``None``

        :param    ex_labels: Label dict for node.
        :type     ex_labels: ``dict``

        :keyword  ex_disk_size: Defines size of the boot disk.
                                Integer in gigabytes.
        :type     ex_disk_size: ``int`` or ``None``

        :return:  A list of Node objects for the new nodes.
        :rtype:   ``list`` of :class:`Node`

        """

        if image and ex_disks_gce_struct:
            raise ValueError("Cannot specify both 'image' and " "'ex_disks_gce_struct'.")

        if image and ex_image_family:
            raise ValueError("Cannot specify both 'image' and " "'ex_image_family'")

        location = location or self.zone

        if not hasattr(location, "name"):
            location = self.ex_get_zone(location)

        if not hasattr(size, "name"):
            size = self.ex_get_size(size, location)

        if not hasattr(ex_network, "name"):
            ex_network = self.ex_get_network(ex_network)

        if ex_subnetwork and not hasattr(ex_subnetwork, "name"):
            ex_subnetwork = self.ex_get_subnetwork(
                ex_subnetwork, region=self._get_region_from_zone(location)
            )

        if ex_image_family:
            image = self.ex_get_image_from_family(ex_image_family)

        if image and not hasattr(image, "name"):
            image = self.ex_get_image(image)

        if not hasattr(ex_disk_type, "name"):
            ex_disk_type = self.ex_get_disktype(ex_disk_type, zone=location)

        node_attrs = {
            "size": size,
            "image": image,
            "location": location,
            "network": ex_network,
            "subnetwork": ex_subnetwork,
            "tags": ex_tags,
            "metadata": ex_metadata,
            "ignore_errors": ignore_errors,
            "use_existing_disk": use_existing_disk,
            "external_ip": external_ip,
            "internal_ip": internal_ip,
            "ex_disk_type": ex_disk_type,
            "ex_disk_auto_delete": ex_disk_auto_delete,
            "ex_service_accounts": ex_service_accounts,
            "description": description,
            "ex_can_ip_forward": ex_can_ip_forward,
            "ex_disks_gce_struct": ex_disks_gce_struct,
            "ex_nic_gce_struct": ex_nic_gce_struct,
            "ex_on_host_maintenance": ex_on_host_maintenance,
            "ex_automatic_restart": ex_automatic_restart,
            "ex_preemptible": ex_preemptible,
            "ex_labels": ex_labels,
            "ex_disk_size": ex_disk_size,
        }
        # List for holding the status information for disk/node creation.
        status_list = []

        for i in range(number):
            name = "%s-%03d" % (base_name, i)
            status = {"name": name, "node_response": None, "node": None}
            status_list.append(status)

        start_time = time.time()
        complete = False

        while not complete:
            if time.time() - start_time >= timeout:
                raise Exception("Timeout (%s sec) while waiting for multiple " "instances")
            complete = True
            time.sleep(poll_interval)

            for status in status_list:
                # Create the node or check status if already in progress.

                if not status["node"]:
                    if not status["node_response"]:
                        self._multi_create_node(status, node_attrs)
                    else:
                        self._multi_check_node(status, node_attrs)
                # If any of the nodes have not been created (or failed) we are
                # not done yet.

                if not status["node"]:
                    complete = False

        # Return list of nodes
        node_list = []

        for status in status_list:
            node_list.append(status["node"])

        return node_list

    def ex_create_targethttpproxy(self, name, urlmap):
        """
        Create a target HTTP proxy.

        :param  name: Name of target HTTP proxy
        :type   name: ``str``

        :keyword  urlmap: URL map defining the mapping from URl to the
                           backendservice.
        :type     healthchecks: ``str`` or :class:`GCEUrlMap`

        :return:  Target Pool object
        :rtype:   :class:`GCETargetPool`
        """
        targetproxy_data = {"name": name}

        if not hasattr(urlmap, "name"):
            urlmap = self.ex_get_urlmap(urlmap)
        targetproxy_data["urlMap"] = urlmap.extra["selfLink"]

        request = "/global/targetHttpProxies"
        self.connection.async_request(request, method="POST", data=targetproxy_data)

        return self.ex_get_targethttpproxy(name)

    def ex_create_targethttpsproxy(self, name, urlmap, sslcertificates, description=None):
        """
        Creates a TargetHttpsProxy resource in the specified project
        using the data included in the request.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute

        :param  name:  Name of the resource. Provided by the client when the
                       resource is created. The name must be 1-63 characters
                       long, and comply with RFC1035. Specifically, the name
                       must be 1-63 characters long and match the regular
                       expression [a-z]([-a-z0-9]*[a-z0-9])? which means the
                       first character must be a lowercase letter, and all
                       following characters must be a dash, lowercase letter,
                       or digit, except the last character, which cannot be a
                       dash.
        :type   name: ``str``

        :param  sslcertificates:  URLs to SslCertificate resources that
                                     are used to authenticate connections
                                     between users and the load balancer.
                                     Currently, exactly one SSL certificate
                                     must be specified.
        :type   sslcertificates: ``list`` of :class:`GCESslcertificates`

        :param  urlmap:  A fully-qualified or valid partial URL to the
                            UrlMap resource that defines the mapping from URL
                            to the BackendService.
        :type   urlmap: :class:`GCEUrlMap`

        :keyword  description:  An optional description of this resource.
                                Provide this property when you create the
                                resource.
        :type   description: ``str``

        :return:  `GCETargetHttpsProxy` object.
        :rtype: :class:`GCETargetHttpsProxy`
        """
        request = "/global/targetHttpsProxies"
        request_data = {}
        request_data["name"] = name
        request_data["description"] = description
        request_data["sslCertificates"] = [x.extra["selfLink"] for x in sslcertificates]
        request_data["urlMap"] = urlmap.extra["selfLink"]

        self.connection.async_request(request, method="POST", data=request_data)

        return self.ex_get_targethttpsproxy(name)

    def ex_create_targetinstance(
        self, name, zone=None, node=None, description=None, nat_policy="NO_NAT"
    ):
        """
        Create a target instance.

        :param  name: Name of target instance
        :type   name: ``str``

        :keyword  region: Zone to create the target pool in. Defaults to
                          self.zone
        :type     region: ``str`` or :class:`GCEZone` or ``None``

        :keyword  node: The actual instance to be used as the traffic target.
        :type     node: ``str`` or :class:`Node`

        :keyword  description: A text description for the target instance
        :type     description: ``str`` or ``None``

        :keyword  nat_policy: The NAT option for how IPs are NAT'd to the node.
        :type     nat_policy: ``str``

        :return:  Target Instance object
        :rtype:   :class:`GCETargetInstance`
        """
        zone = zone or self.zone
        targetinstance_data = {}
        targetinstance_data["name"] = name

        if not hasattr(zone, "name"):
            zone = self.ex_get_zone(zone)
        targetinstance_data["zone"] = zone.extra["selfLink"]

        if node is not None:
            if not hasattr(node, "name"):
                node = self.ex_get_node(node, zone)
            targetinstance_data["instance"] = node.extra["selfLink"]
        targetinstance_data["natPolicy"] = nat_policy

        if description:
            targetinstance_data["description"] = description

        request = "/zones/%s/targetInstances" % (zone.name)
        self.connection.async_request(request, method="POST", data=targetinstance_data)

        return self.ex_get_targetinstance(name, zone)

    def ex_create_targetpool(
        self,
        name,
        region=None,
        healthchecks=None,
        nodes=None,
        session_affinity=None,
        backup_pool=None,
        failover_ratio=None,
    ):
        """
        Create a target pool.

        :param  name: Name of target pool
        :type   name: ``str``

        :keyword  region: Region to create the target pool in. Defaults to
                          self.region
        :type     region: ``str`` or :class:`GCERegion` or ``None``

        :keyword  healthchecks: Optional list of health checks to attach
        :type     healthchecks: ``list`` of ``str`` or :class:`GCEHealthCheck`

        :keyword  nodes:  Optional list of nodes to attach to the pool
        :type     nodes:  ``list`` of ``str`` or :class:`Node`

        :keyword  session_affinity:  Optional algorithm to use for session
                                     affinity.
        :type     session_affinity:  ``str``

        :keyword  backup_pool: Optional backup targetpool to take over traffic
                               if the failover_ratio is exceeded.
        :type     backup_pool: ``GCETargetPool`` or ``None``

        :keyword  failover_ratio: The percentage of healthy VMs must fall at
                                  or below this value before traffic will be
                                  sent to the backup_pool.
        :type     failover_ratio: :class:`GCETargetPool` or ``None``

        :return:  Target Pool object
        :rtype:   :class:`GCETargetPool`
        """
        targetpool_data = {}
        region = region or self.region

        if backup_pool and not failover_ratio:
            failover_ratio = 0.1
            targetpool_data["failoverRatio"] = failover_ratio
            targetpool_data["backupPool"] = backup_pool.extra["selfLink"]

        if failover_ratio and not backup_pool:
            e = "Must supply a backup targetPool when setting failover_ratio"
            raise ValueError(e)
        targetpool_data["name"] = name

        if not hasattr(region, "name"):
            region = self.ex_get_region(region)
        targetpool_data["region"] = region.extra["selfLink"]

        if healthchecks:
            if not hasattr(healthchecks[0], "name"):
                hc_list = [self.ex_get_healthcheck(h).extra["selfLink"] for h in healthchecks]
            else:
                hc_list = [h.extra["selfLink"] for h in healthchecks]
            targetpool_data["healthChecks"] = hc_list

        if nodes:
            if not hasattr(nodes[0], "name"):
                node_list = [self.ex_get_node(n, "all").extra["selfLink"] for n in nodes]
            else:
                node_list = [n.extra["selfLink"] for n in nodes]
            targetpool_data["instances"] = node_list

        if session_affinity:
            targetpool_data["sessionAffinity"] = session_affinity

        request = "/regions/%s/targetPools" % (region.name)

        self.connection.async_request(request, method="POST", data=targetpool_data)

        return self.ex_get_targetpool(name, region)

    def ex_create_urlmap(self, name, default_service):
        """
        Create a URL Map.

        :param  name: Name of the URL Map.
        :type   name: ``str``

        :keyword  default_service: Default backend service for the map.
        :type     default_service: ``str`` or :class:`GCEBackendService`

        :return:  URL Map object
        :rtype:   :class:`GCEUrlMap`
        """
        urlmap_data = {"name": name}

        # TODO: support hostRules, pathMatchers, tests

        if not hasattr(default_service, "name"):
            default_service = self.ex_get_backendservice(default_service)
        urlmap_data["defaultService"] = default_service.extra["selfLink"]

        request = "/global/urlMaps"
        self.connection.async_request(request, method="POST", data=urlmap_data)

        return self.ex_get_urlmap(name)

    def create_volume(
        self,
        size,
        name,
        location=None,
        snapshot=None,
        image=None,
        use_existing=True,
        ex_disk_type="pd-standard",
        ex_image_family=None,
    ):
        """
        Create a volume (disk).

        :param  size: Size of volume to create (in GB). Can be None if image
                      or snapshot is supplied.
        :type   size: ``int`` or ``str`` or ``None``

        :param  name: Name of volume to create
        :type   name: ``str``

        :keyword  location: Location (zone) to create the volume in
        :type     location: ``str`` or :class:`GCEZone` or
                            :class:`NodeLocation` or ``None``

        :keyword  snapshot: Snapshot to create image from
        :type     snapshot: :class:`GCESnapshot` or ``str`` or ``None``

        :keyword  image: Image to create disk from.
        :type     image: :class:`GCENodeImage` or ``str`` or ``None``

        :keyword  use_existing: If True and a disk with the given name already
                                exists, return an object for that disk instead
                                of attempting to create a new disk.
        :type     use_existing: ``bool``

        :keyword  ex_disk_type: Specify a pd-standard (default) disk or pd-ssd
                                for an SSD disk.
        :type     ex_disk_type: ``str`` or :class:`GCEDiskType`

        :keyword  ex_image_family: Determine image from an 'Image Family'
                                   instead of by name. 'image' should be None
                                   to use this keyword.
        :type     ex_image_family: ``str`` or ``None``

        :return:  Storage Volume object
        :rtype:   :class:`StorageVolume`
        """

        if image and ex_image_family:
            raise ValueError("Cannot specify both 'image' and " "'ex_image_family'")

        if ex_image_family:
            image = self.ex_get_image_from_family(ex_image_family)

        request, volume_data, params = self._create_vol_req(
            size, name, location, snapshot, image, ex_disk_type
        )
        try:
            self.connection.async_request(request, method="POST", data=volume_data, params=params)
        except ResourceExistsError as e:
            if not use_existing:
                raise e

        return self.ex_get_volume(name, location)

    def create_volume_snapshot(self, volume, name):
        """
        Create a snapshot of the provided Volume.

        :param  volume: A StorageVolume object
        :type   volume: :class:`StorageVolume`

        :return:  A GCE Snapshot object
        :rtype:   :class:`GCESnapshot`
        """
        snapshot_data = {}
        snapshot_data["name"] = name
        request = "/zones/{}/disks/{}/createSnapshot".format(
            volume.extra["zone"].name,
            volume.name,
        )
        self.connection.async_request(request, method="POST", data=snapshot_data)

        return self.ex_get_snapshot(name)

    def list_volume_snapshots(self, volume):
        """
        List snapshots created from the provided volume.

        For GCE, snapshots are global, but while the volume they were
        created from still exists, the source disk for the snapshot is
        tracked.

        :param  volume: A StorageVolume object
        :type   volume: :class:`StorageVolume`

        :return:  A list of Snapshot objects
        :rtype:   ``list`` of :class:`GCESnapshot`
        """
        volume_snapshots = []
        volume_link = volume.extra["selfLink"]
        all_snapshots = self.ex_list_snapshots()

        for snapshot in all_snapshots:
            if snapshot.extra["sourceDisk"] == volume_link:
                volume_snapshots.append(snapshot)

        return volume_snapshots

    def ex_update_autoscaler(self, autoscaler):
        """
        Update an autoscaler with new values.

        To update, change the attributes of the autoscaler object and pass
        the updated object to the method.

        :param  autoscaler: An Autoscaler object with updated values.
        :type   autoscaler: :class:`GCEAutoscaler`

        :return:  An Autoscaler object representing the new state.
        :rtype:   :class:`GCEAutoscaler``
        """
        request = "/zones/%s/autoscalers" % (autoscaler.zone.name)
        as_data = {}
        as_data["name"] = autoscaler.name
        as_data["autoscalingPolicy"] = autoscaler.policy
        as_data["target"] = autoscaler.target.extra["selfLink"]

        self.connection.async_request(request, method="PUT", data=as_data)

        return self.ex_get_autoscaler(autoscaler.name, autoscaler.zone)

    def ex_update_healthcheck(self, healthcheck):
        """
        Update a health check with new values.

        To update, change the attributes of the health check object and pass
        the updated object to the method.

        :param  healthcheck: A healthcheck object with updated values.
        :type   healthcheck: :class:`GCEHealthCheck`

        :return:  An object representing the new state of the health check.
        :rtype:   :class:`GCEHealthCheck`
        """
        hc_data = {}
        hc_data["name"] = healthcheck.name
        hc_data["requestPath"] = healthcheck.path
        hc_data["port"] = healthcheck.port
        hc_data["checkIntervalSec"] = healthcheck.interval
        hc_data["timeoutSec"] = healthcheck.timeout
        hc_data["unhealthyThreshold"] = healthcheck.unhealthy_threshold
        hc_data["healthyThreshold"] = healthcheck.healthy_threshold

        if healthcheck.extra["host"]:
            hc_data["host"] = healthcheck.extra["host"]

        if healthcheck.extra["description"]:
            hc_data["description"] = healthcheck.extra["description"]

        request = "/global/httpHealthChecks/%s" % (healthcheck.name)

        self.connection.async_request(request, method="PUT", data=hc_data)

        return self.ex_get_healthcheck(healthcheck.name)

    def ex_update_firewall(self, firewall):
        """
        Update a firewall with new values.

        To update, change the attributes of the firewall object and pass the
        updated object to the method.

        :param  firewall: A firewall object with updated values.
        :type   firewall: :class:`GCEFirewall`

        :return:  An object representing the new state of the firewall.
        :rtype:   :class:`GCEFirewall`
        """
        firewall_data = {}
        firewall_data["name"] = firewall.name
        firewall_data["allowed"] = firewall.allowed
        firewall_data["denied"] = firewall.denied
        # Priority updates not yet exposed via API
        firewall_data["network"] = firewall.network.extra["selfLink"]

        if firewall.source_ranges:
            firewall_data["sourceRanges"] = firewall.source_ranges

        if firewall.source_tags:
            firewall_data["sourceTags"] = firewall.source_tags

        if firewall.source_service_accounts:
            firewall_data["sourceServiceAccounts"] = firewall.source_service_accounts

        if firewall.target_tags:
            firewall_data["targetTags"] = firewall.target_tags

        if firewall.target_service_accounts:
            firewall_data["targetServiceAccounts"] = firewall.target_service_accounts

        if firewall.target_ranges:
            firewall_data["destinationRanges"] = firewall.target_ranges

        if firewall.extra["description"]:
            firewall_data["description"] = firewall.extra["description"]

        request = "/global/firewalls/%s" % (firewall.name)

        self.connection.async_request(request, method="PUT", data=firewall_data)

        return self.ex_get_firewall(firewall.name)

    def ex_targethttpsproxy_set_sslcertificates(self, targethttpsproxy, sslcertificates):
        """
        Replaces SslCertificates for TargetHttpsProxy.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute

        :param  targethttpsproxy:  Name of the TargetHttpsProxy resource to
                                   set an SslCertificates resource for.
        :type   targethttpsproxy: ``str``

        :param  sslcertificates:  sslcertificates to set.
        :type   sslcertificates: ``list`` of :class:`GCESslCertificates`

        :return:  True
        :rtype: ``bool``
        """

        request = "/targetHttpsProxies/%s/setSslCertificates" % (targethttpsproxy.name)
        request_data = {"sslCertificates": [x.extra["selfLink"] for x in sslcertificates]}
        self.connection.async_request(request, method="POST", data=request_data)

        return True

    def ex_targethttpsproxy_set_urlmap(self, targethttpsproxy, urlmap):
        """
        Changes the URL map for TargetHttpsProxy.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute

        :param  targethttpsproxy:  Name of the TargetHttpsProxy resource
                                   whose URL map is to be set.
        :type   targethttpsproxy: ``str``

        :param  urlmap:  urlmap to set.
        :type   urlmap: :class:`GCEUrlMap`

        :return:  True
        :rtype: ``bool``
        """

        request = "/targetHttpsProxies/%s/setUrlMap" % (targethttpsproxy.name)
        request_data = {"urlMap": urlmap.extra["selfLink"]}
        self.connection.async_request(request, method="POST", data=request_data)

        return True

    def ex_targetpool_get_health(self, targetpool, node=None):
        """
        Return a hash of target pool instances and their health.

        :param  targetpool: Targetpool containing healthchecked instances.
        :type   targetpool: :class:`GCETargetPool`

        :param  node: Optional node to specify if only a specific node's
                      health status should be returned
        :type   node: ``str``, ``Node``, or ``None``

        :return: List of hashes of instances and their respective health,
                 e.g. [{'node': ``Node``, 'health': 'UNHEALTHY'}, ...]
        :rtype:  ``list`` of ``dict``
        """
        health = []
        region_name = targetpool.region.name
        request = "/regions/{}/targetPools/{}/getHealth".format(
            region_name,
            targetpool.name,
        )

        if node is not None:
            if hasattr(node, "name"):
                node_name = node.name
            else:
                node_name = node
        else:
            node_name = None

        nodes = targetpool.nodes

        for node_object in nodes:
            if node:
                if node_name == node_object.name:
                    body = {"instance": node_object.extra["selfLink"]}
                    resp = self.connection.request(request, method="POST", data=body).object
                    status = resp["healthStatus"][0]["healthState"]
                    health.append({"node": node_object, "health": status})
            else:
                body = {"instance": node_object.extra["selfLink"]}
                resp = self.connection.request(request, method="POST", data=body).object
                status = resp["healthStatus"][0]["healthState"]
                health.append({"node": node_object, "health": status})

        return health

    def ex_targetpool_set_backup_targetpool(
        self, targetpool, backup_targetpool, failover_ratio=0.1
    ):
        """
        Set a backup targetpool.

        :param  targetpool: The existing primary targetpool
        :type   targetpool: :class:`GCETargetPool`

        :param  backup_targetpool: The existing targetpool to use for
                                   failover traffic.
        :type   backup_targetpool: :class:`GCETargetPool`

        :param  failover_ratio: The percentage of healthy VMs must fall at or
                                below this value before traffic will be sent
                                to the backup targetpool (default 0.10)
        :type   failover_ratio: ``float``

        :return:  True if successful
        :rtype:   ``bool``
        """
        region = targetpool.region.name
        name = targetpool.name
        req_data = {"target": backup_targetpool.extra["selfLink"]}
        params = {"failoverRatio": failover_ratio}

        request = "/regions/{}/targetPools/{}/setBackup".format(region, name)
        self.connection.async_request(request, method="POST", data=req_data, params=params)

        return True

    def ex_targetpool_add_node(self, targetpool, node):
        """
        Add a node to a target pool.

        :param  targetpool: The targetpool to add node to
        :type   targetpool: ``str`` or :class:`GCETargetPool`

        :param  node: The node to add
        :type   node: ``str`` or :class:`Node`

        :return: True if successful
        :rtype:  ``bool``
        """

        if not hasattr(targetpool, "name"):
            targetpool = self.ex_get_targetpool(targetpool)

        if hasattr(node, "name"):
            node_uri = node.extra["selfLink"]
        else:
            if node.startswith("https://"):
                node_uri = node
            else:
                node = self.ex_get_node(node, "all")
                node_uri = node.extra["selfLink"]

        targetpool_data = {"instances": [{"instance": node_uri}]}

        request = "/regions/{}/targetPools/{}/addInstance".format(
            targetpool.region.name,
            targetpool.name,
        )
        self.connection.async_request(request, method="POST", data=targetpool_data)

        if all(
            (node_uri != n) and (not hasattr(n, "extra") or n.extra["selfLink"] != node_uri)
            for n in targetpool.nodes
        ):
            targetpool.nodes.append(node)

        return True

    def ex_targetpool_add_healthcheck(self, targetpool, healthcheck):
        """
        Add a health check to a target pool.

        :param  targetpool: The targetpool to add health check to
        :type   targetpool: ``str`` or :class:`GCETargetPool`

        :param  healthcheck: The healthcheck to add
        :type   healthcheck: ``str`` or :class:`GCEHealthCheck`

        :return: True if successful
        :rtype:  ``bool``
        """

        if not hasattr(targetpool, "name"):
            targetpool = self.ex_get_targetpool(targetpool)

        if not hasattr(healthcheck, "name"):
            healthcheck = self.ex_get_healthcheck(healthcheck)

        targetpool_data = {"healthChecks": [{"healthCheck": healthcheck.extra["selfLink"]}]}

        request = "/regions/{}/targetPools/{}/addHealthCheck".format(
            targetpool.region.name,
            targetpool.name,
        )
        self.connection.async_request(request, method="POST", data=targetpool_data)
        targetpool.healthchecks.append(healthcheck)

        return True

    def ex_targetpool_remove_node(self, targetpool, node):
        """
        Remove a node from a target pool.

        :param  targetpool: The targetpool to remove node from
        :type   targetpool: ``str`` or :class:`GCETargetPool`

        :param  node: The node to remove
        :type   node: ``str`` or :class:`Node`

        :return: True if successful
        :rtype:  ``bool``
        """

        if not hasattr(targetpool, "name"):
            targetpool = self.ex_get_targetpool(targetpool)

        if hasattr(node, "name"):
            node_uri = node.extra["selfLink"]
        else:
            if node.startswith("https://"):
                node_uri = node
            else:
                node = self.ex_get_node(node, "all")
                node_uri = node.extra["selfLink"]

        targetpool_data = {"instances": [{"instance": node_uri}]}

        request = "/regions/{}/targetPools/{}/removeInstance".format(
            targetpool.region.name,
            targetpool.name,
        )
        self.connection.async_request(request, method="POST", data=targetpool_data)
        # Remove node object from node list
        index = None

        for i, nd in enumerate(targetpool.nodes):
            if nd == node_uri or (hasattr(nd, "extra") and nd.extra["selfLink"] == node_uri):
                index = i

                break

        if index is not None:
            targetpool.nodes.pop(index)

        return True

    def ex_targetpool_remove_healthcheck(self, targetpool, healthcheck):
        """
        Remove a health check from a target pool.

        :param  targetpool: The targetpool to remove health check from
        :type   targetpool: ``str`` or :class:`GCETargetPool`

        :param  healthcheck: The healthcheck to remove
        :type   healthcheck: ``str`` or :class:`GCEHealthCheck`

        :return: True if successful
        :rtype:  ``bool``
        """

        if not hasattr(targetpool, "name"):
            targetpool = self.ex_get_targetpool(targetpool)

        if not hasattr(healthcheck, "name"):
            healthcheck = self.ex_get_healthcheck(healthcheck)

        targetpool_data = {"healthChecks": [{"healthCheck": healthcheck.extra["selfLink"]}]}

        request = "/regions/{}/targetPools/{}/removeHealthCheck".format(
            targetpool.region.name,
            targetpool.name,
        )
        self.connection.async_request(request, method="POST", data=targetpool_data)
        # Remove healthcheck object from healthchecks list
        index = None

        for i, hc in enumerate(targetpool.healthchecks):
            if hc.name == healthcheck.name:
                index = i

        if index is not None:
            targetpool.healthchecks.pop(index)

        return True

    def ex_instancegroup_add_instances(self, instancegroup, node_list):
        """
        Adds a list of instances to the specified instance group. All of the
        instances in the instance group must be in the same
        network/subnetwork. Read  Adding instances for more information.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute

        :param  instancegroup:  The Instance Group where you are
                                adding instances.
        :type   instancegroup: :class:``GCEInstanceGroup``

        :param  node_list: List of nodes to add.
        :type   node_list: ``list`` of :class:`Node` or ``list`` of
                           :class:`GCENode`

        :return:  Return True if successful.
        :rtype: ``bool``
        """
        request = "/zones/{}/instanceGroups/{}/addInstances".format(
            instancegroup.zone.name,
            instancegroup.name,
        )
        request_data = {"instances": [{"instance": x.extra["selfLink"]} for x in node_list]}
        self.connection.async_request(request, method="POST", data=request_data)

        return True

    def ex_instancegroup_remove_instances(self, instancegroup, node_list):
        """
        Removes one or more instances from the specified instance group,
        but does not delete those instances.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute

        :param  instancegroup:  The Instance Group where the
                                specified instances will be removed.
        :type   instancegroup: :class:``GCEInstanceGroup``

        :param  node_list: List of nodes to add.
        :type   node_list: ``list`` of :class:`Node` or ``list`` of
                           :class:`GCENode`

        :return:  True if successful.
        :rtype: ``bool``
        """
        request = "/zones/{}/instanceGroups/{}/removeInstances".format(
            instancegroup.zone.name,
            instancegroup.name,
        )
        request_data = {"instances": [{"instance": x.extra["selfLink"]} for x in node_list]}
        self.connection.async_request(request, method="POST", data=request_data)

        return True

    def ex_instancegroup_list_instances(self, instancegroup):
        """
        Lists the instances in the specified instance group.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute
        * https://www.googleapis.com/auth/compute.readonly

        :param  instancegroup:  The Instance Group where from which you
                                want to generate a list of included
                                instances.
        :type   instancegroup: :class:`GCEInstanceGroup`

        :return:  List of :class:`GCENode` objects.
        :rtype: ``list`` of :class:`GCENode` objects.
        """
        request = "/zones/{}/instanceGroups/{}/listInstances".format(
            instancegroup.zone.name,
            instancegroup.name,
        )

        # Note: This API requires a 'POST'.
        response = self.connection.request(request, method="POST").object

        list_data = []

        if "items" in response:
            for v in response["items"]:
                instance_info = self._get_components_from_path(v["instance"])
                list_data.append(self.ex_get_node(instance_info["name"], instance_info["zone"]))

        return list_data

    def ex_instancegroup_set_named_ports(self, instancegroup, named_ports=[]):
        """
        Sets the named ports for the specified instance group.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute

        :param  instancegroup:  The Instance Group where where the
                                named ports are updated.
        :type   instancegroup: :class:`GCEInstanceGroup`

        :param  named_ports:  Assigns a name to a port number. For example:
                              {name: "http", port: 80}  This allows the
                              system to reference ports by the assigned name
                              instead of a port number. Named ports can also
                              contain multiple ports. For example: [{name:
                              "http", port: 80},{name: "http", port: 8080}]
                              Named ports apply to all instances in this
                              instance group.
        :type   named_ports: ``list`` of {'name': ``str``, 'port`: ``int``}

        :return:  Return True if successful.
        :rtype: ``bool``
        """

        if not isinstance(named_ports, list):
            raise ValueError("'named_ports' must be a list of name/port" " dictionaries.")

        request = "/zones/{}/instanceGroups/{}/setNamedPorts".format(
            instancegroup.zone.name,
            instancegroup.name,
        )
        request_data = {
            "namedPorts": named_ports,
            "fingerprint": instancegroup.extra["fingerprint"],
        }
        self.connection.async_request(request, method="POST", data=request_data)

        return True

    def ex_destroy_instancegroup(self, instancegroup):
        """
        Deletes the specified instance group. The instances in the group
        are not deleted. Note that instance group must not belong to a backend
        service. Read  Deleting an instance group for more information.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute

        :param  instancegroup:  The name of the instance group to delete.
        :type   instancegroup: :class:`GCEInstanceGroup`

        :return:  Return True if successful.
        :rtype: ``bool``
        """

        request = "/zones/{}/instanceGroups/{}".format(
            instancegroup.zone.name,
            instancegroup.name,
        )
        request_data = {}
        self.connection.async_request(request, method="DELETE", data=request_data)

        return True

    def ex_instancegroupmanager_list_managed_instances(self, manager):
        """
        Lists all of the instances in the Managed Instance Group.

        Each instance in the list has a currentAction, which indicates
        the action that the managed instance group is performing on the
        instance. For example, if the group is still creating an instance,
        the currentAction is 'CREATING'.  Note that 'instanceStatus' might not
        be available, for example, if currentAction is 'CREATING' or
        'RECREATING'. If a previous action failed, the list displays the errors
        for that failed action.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute
        * https://www.googleapis.com/auth/compute.readonly

        'currentAction' values are one of:
           'ABANDONING', 'CREATING', 'DELETING', 'NONE',
           'RECREATING', 'REFRESHING', 'RESTARTING'

        :param  manager: Instance Group Manager to operate on.
        :type   manager: :class:`GCEInstanceGroupManager`

        :return: ``list`` of ``dict`` containing 'name', 'zone', 'lastAttempt',
                 'currentAction', 'instance' and 'instanceStatus'.
        :rtype: ``list``
        """
        request = "/zones/{}/instanceGroupManagers/{}/listManagedInstances".format(
            manager.zone.name,
            manager.name,
        )

        # Note: This API requires a 'POST'.
        response = self.connection.request(request, method="POST").object

        instance_data = []

        if "managedInstances" in response:
            for i in response["managedInstances"]:
                i["name"] = self._get_components_from_path(i["instance"])["name"]
                i["zone"] = manager.zone.name
                instance_data.append(i)

        return instance_data

    def ex_instancegroupmanager_set_autohealingpolicies(
        self, manager, healthcheck, initialdelaysec
    ):
        """
        Set the Autohealing Policies for this Instance Group.

        :param  healthcheck: Healthcheck to add
        :type   healthcheck: :class:`GCEHealthCheck`

        :param  initialdelaysec:  The time to allow an instance to boot and
                                  applications to fully start before the first
                                  health check
        :type   initialdelaysec:  ``int``

        :return:  True if successful
        :rtype:   ``bool``
        """
        request_data = {}
        request_data["autoHealingPolicies"] = [
            {"healthCheck": healthcheck.path, "initialDelaySec": initialdelaysec}
        ]

        request = "/zones/{}/instanceGroupManagers/{}/".format(
            manager.zone.name,
            manager.name,
        )
        self.connection.async_request(request, method="PATCH", data=request_data)

        return True

    def ex_instancegroupmanager_set_instancetemplate(self, manager, instancetemplate):
        """
        Set the Instance Template for this Instance Group.  Existing VMs are
        not recreated by setting a new InstanceTemplate.

        :param  manager: Instance Group Manager to operate on.
        :type   manager: :class:`GCEInstanceGroupManager`

        :param  instancetemplate: Instance Template to set.
        :type   instancetemplate: :class:`GCEInstanceTemplate`

        :return:  True if successful
        :rtype:   ``bool``
        """
        req_data = {"instanceTemplate": instancetemplate.extra["selfLink"]}

        request = "/zones/%s/instanceGroupManagers/" "%s/setInstanceTemplate" % (
            manager.zone.name,
            manager.name,
        )
        self.connection.async_request(request, method="POST", data=req_data)

        return True

    def ex_instancegroupmanager_recreate_instances(self, manager, instances=None):
        """
        Schedules a group action to recreate the specified instances in the
        managed instance group. The instances are deleted and recreated using
        the current instance template for the managed instance group. This
        operation is marked as DONE when the action is scheduled even if the
        instances have not yet been recreated. You must separately verify
        the status of the recreating action with the listmanagedinstances
        method or querying the managed instance group directly.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute

        :param  manager:  Required. The name of the managed instance group. The
                       name must be 1-63 characters long, and comply with
                       RFC1035.
        :type   manager: ``str`` or :class: `GCEInstanceGroupManager`

        :keyword  instances:  list of Node objects to be recreated. If equal
                              to None, all instances in the managed instance
                              group are recreated.
        :type   instances: ``list`` of :class: `Node`, ``list`` of instance
                            names (only), ``list`` of instance URIs, or None.

        :return:  Dictionary containing instance URI and currentAction.
                  See ex_instancegroupmanager_list_managed_instances for
                  more details.
        :rtype: ``dict``
        """
        instance_uris = []

        if not isinstance(manager, GCEInstanceGroupManager) and not isinstance(manager, str):
            raise ValueError(
                "InstanceGroupManager must be of type str or "
                "GCEInstanceGroupManager. Type '%s' provided" % (type(manager))
            )

        if isinstance(manager, str):
            manager = self.ex_get_instancegroupmanager(manager)

        if instances is None:
            il = self.ex_instancegroupmanager_list_managed_instances(manager)
            instance_uris = [x["instance"] for x in il]
        elif isinstance(instances, list):
            for i in instances:
                if i.startswith("https://"):
                    instance_uris.append(i)
                else:
                    instance_uris.append(self.ex_get_node(i, manager.zone).extra["selfLink"])
        else:
            raise ValueError(
                "instances must be 'None or "
                "a list of instance URIs, instance names, or"
                "Node objects"
            )

        request = "/zones/{}/instanceGroupManagers/{}/recreateInstances".format(
            manager.zone.name,
            manager.name,
        )
        request_data = {"instances": instance_uris}
        self.connection.request(request, method="POST", data=request_data).object

        return self.ex_instancegroupmanager_list_managed_instances(manager)

    def ex_instancegroupmanager_delete_instances(self, manager, node_list):
        """
        Remove instances from GCEInstanceGroupManager and destroy
        the instance

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute

        :param  manager:  Required. The name of the managed instance group. The
                       name must be 1-63 characters long, and comply with
                       RFC1035.
        :type   manager: ``str`` or :class: `GCEInstanceGroupManager`

        :param  node_list:  list of Node objects to delete.
        :type   node_list: ``list`` of :class:`Node`

        :return:  True if successful
        :rtype: ``bool``
        """

        request = "/zones/{}/instanceGroupManagers/{}/deleteInstances".format(
            manager.zone.name,
            manager.name,
        )
        request_data = {"instances": [x.extra["selfLink"] for x in node_list]}
        self.connection.request(request, method="POST", data=request_data).object

        return True

    def ex_instancegroupmanager_resize(self, manager, size):
        """
        Set the Instance Template for this Instance Group.

        :param  manager: Instance Group Manager to operate on.
        :type   manager: :class:`GCEInstanceGroupManager`

        :param  size: New size of Managed Instance Group.
        :type   size: ``int``

        :return:  True if successful
        :rtype:   ``bool``
        """
        req_params = {"size": size}

        request = "/zones/{}/instanceGroupManagers/{}/resize".format(
            manager.zone.name,
            manager.name,
        )
        self.connection.async_request(request, method="POST", params=req_params)

        return True

    def reboot_node(self, node):
        """
        Reboot a node.

        :param  node: Node to be rebooted
        :type   node: :class:`Node`

        :return:  True if successful, False if not
        :rtype:   ``bool``
        """
        request = "/zones/{}/instances/{}/reset".format(node.extra["zone"].name, node.name)
        self.connection.async_request(request, method="POST", data="ignored")

        return True

    def ex_set_node_tags(self, node, tags):
        """
        Set the tags on a Node instance.

        Note that this updates the node object directly.

        :param  node: Node object
        :type   node: :class:`Node`

        :param  tags: List of tags to apply to the object
        :type   tags: ``list`` of ``str``

        :return:  True if successful
        :rtype:   ``bool``
        """
        request = "/zones/{}/instances/{}/setTags".format(
            node.extra["zone"].name,
            node.name,
        )

        tags_data = {}
        tags_data["items"] = tags
        tags_data["fingerprint"] = node.extra["tags_fingerprint"]

        self.connection.async_request(request, method="POST", data=tags_data)
        new_node = self.ex_get_node(node.name, node.extra["zone"])
        node.extra["tags"] = new_node.extra["tags"]
        node.extra["tags_fingerprint"] = new_node.extra["tags_fingerprint"]

        return True

    def ex_set_node_scheduling(self, node, on_host_maintenance=None, automatic_restart=None):
        """Set the maintenance behavior for the node.

        See `Scheduling <https://developers.google.com/compute/
        docs/instances#onhostmaintenance>`_ documentation for more info.

        :param  node: Node object
        :type   node: :class:`Node`

        :keyword  on_host_maintenance: Defines whether node should be
                                       terminated or migrated when host machine
                                       goes down. Acceptable values are:
                                       'MIGRATE' or 'TERMINATE' (If not
                                       supplied, value will be reset to GCE
                                       default value for the instance type.)
        :type     on_host_maintenance: ``str``

        :keyword  automatic_restart: Defines whether the instance should be
                                     automatically restarted when it is
                                     terminated by Compute Engine. (If not
                                     supplied, value will be set to the GCE
                                     default value for the instance type.)
        :type     automatic_restart: ``bool``

        :return:  True if successful.
        :rtype:   ``bool``
        """

        if not hasattr(node, "name"):
            node = self.ex_get_node(node, "all")

        if on_host_maintenance is not None:
            on_host_maintenance = on_host_maintenance.upper()
            ohm_values = ["MIGRATE", "TERMINATE"]

            if on_host_maintenance not in ohm_values:
                raise ValueError("on_host_maintenance must be one of %s" % ",".join(ohm_values))

        request = "/zones/{}/instances/{}/setScheduling".format(
            node.extra["zone"].name,
            node.name,
        )

        scheduling_data = {}

        if on_host_maintenance is not None:
            scheduling_data["onHostMaintenance"] = on_host_maintenance

        if automatic_restart is not None:
            scheduling_data["automaticRestart"] = automatic_restart

        self.connection.async_request(request, method="POST", data=scheduling_data)

        new_node = self.ex_get_node(node.name, node.extra["zone"])
        node.extra["scheduling"] = new_node.extra["scheduling"]

        ohm = node.extra["scheduling"].get("onHostMaintenance")
        ar = node.extra["scheduling"].get("automaticRestart")

        success = True

        if on_host_maintenance not in [None, ohm]:
            success = False

        if automatic_restart not in [None, ar]:
            success = False

        return success

    def attach_volume(
        self,
        node,
        volume,
        device=None,
        ex_mode=None,
        ex_boot=False,
        ex_type=None,
        ex_source=None,
        ex_auto_delete=None,
        ex_initialize_params=None,
        ex_licenses=None,
        ex_interface=None,
    ):
        """
        Attach a volume to a node.

        If volume is None, an ex_source URL must be provided.

        :param  node: The node to attach the volume to
        :type   node: :class:`Node` or ``None``

        :param  volume: The volume to attach.
        :type   volume: :class:`StorageVolume` or ``None``

        :keyword  device: The device name to attach the volume as. Defaults to
                          volume name.
        :type     device: ``str``

        :keyword  ex_mode: Either 'READ_WRITE' or 'READ_ONLY'
        :type     ex_mode: ``str``

        :keyword  ex_boot: If true, disk will be attached as a boot disk
        :type     ex_boot: ``bool``

        :keyword  ex_type: Specify either 'PERSISTENT' (default) or 'SCRATCH'.
        :type     ex_type: ``str``

        :keyword  ex_source: URL (full or partial) of disk source. Must be
                             present if not using an existing StorageVolume.
        :type     ex_source: ``str`` or ``None``

        :keyword  ex_auto_delete: If set, the disk will be auto-deleted
                                  if the parent node/instance is deleted.
        :type     ex_auto_delete: ``bool`` or ``None``

        :keyword  ex_initialize_params: Allow user to pass in full JSON
                                        struct of `initializeParams` as
                                        documented in GCE's API.
        :type     ex_initialize_params: ``dict`` or ``None``

        :keyword  ex_licenses: List of strings representing licenses
                               associated with the volume/disk.
        :type     ex_licenses: ``list`` of ``str``

        :keyword  ex_interface: User can specify either 'SCSI' (default) or
                                'NVME'.
        :type     ex_interface: ``str`` or ``None``

        :return:  True if successful
        :rtype:   ``bool``
        """

        if volume is None and ex_source is None:
            raise ValueError(
                "Must supply either a StorageVolume or " "set `ex_source` URL for an existing disk."
            )

        if volume is None and device is None:
            raise ValueError("Must supply either a StorageVolume or " "set `device` name.")

        volume_data = {}

        if ex_source:
            volume_data["source"] = ex_source

        if ex_initialize_params:
            volume_data["initialzeParams"] = ex_initialize_params

        if ex_licenses:
            volume_data["licenses"] = ex_licenses

        if ex_interface:
            volume_data["interface"] = ex_interface

        if ex_type:
            volume_data["type"] = ex_type

        if ex_auto_delete:
            volume_data["autoDelete"] = ex_auto_delete

        volume_data["source"] = ex_source or volume.extra["selfLink"]
        volume_data["mode"] = ex_mode or "READ_WRITE"

        if device:
            volume_data["deviceName"] = device
        else:
            volume_data["deviceName"] = volume.name

        volume_data["boot"] = ex_boot

        request = "/zones/{}/instances/{}/attachDisk".format(
            node.extra["zone"].name,
            node.name,
        )
        self.connection.async_request(request, method="POST", data=volume_data)

        return True

    def ex_resize_volume(self, volume, size):
        """
        Resize a volume to the specified size.

        :param volume: Volume object to resize
        :type  volume: :class:`StorageVolume`

        :param size: The size in GB of the volume to resize to.
        :type size: ``int``

        :return:  True if successful
        :rtype:   ``bool``
        """
        request = "/zones/{}/disks/{}/resize".format(volume.extra["zone"].name, volume.name)
        request_data = {"sizeGb": int(size)}

        self.connection.async_request(request, method="POST", data=request_data)

        return True

    def detach_volume(self, volume, ex_node=None):
        """
        Detach a volume from a node.

        :param  volume: Volume object to detach
        :type   volume: :class:`StorageVolume`

        :keyword  ex_node: Node object to detach volume from (required)
        :type     ex_node: :class:`Node`

        :return:  True if successful
        :rtype:   ``bool``
        """

        if not ex_node:
            return False
        request = "/zones/{}/instances/{}/detachDisk?deviceName={}".format(
            ex_node.extra["zone"].name,
            ex_node.name,
            volume.name,
        )

        self.connection.async_request(request, method="POST", data="ignored")

        return True

    def ex_set_volume_auto_delete(self, volume, node, auto_delete=True):
        """
        Sets the auto-delete flag for a volume attached to a node.

        :param  volume: Volume object to auto-delete
        :type   volume: :class:`StorageVolume`

        :param   ex_node: Node object to auto-delete volume from
        :type    ex_node: :class:`Node`

        :keyword auto_delete: Flag to set for the auto-delete value
        :type    auto_delete: ``bool`` (default True)

        :return:  True if successful
        :rtype:   ``bool``
        """
        request = "/zones/{}/instances/{}/setDiskAutoDelete".format(
            node.extra["zone"].name,
            node.name,
        )
        delete_params = {
            "deviceName": volume.name,
            "autoDelete": auto_delete,
        }
        self.connection.async_request(request, method="POST", params=delete_params)

        return True

    def ex_destroy_address(self, address):
        """
        Destroy a static address.

        :param  address: Address object to destroy
        :type   address: ``str`` or :class:`GCEAddress`

        :return:  True if successful
        :rtype:   ``bool``
        """

        if not hasattr(address, "name"):
            address = self.ex_get_address(address)

        if hasattr(address.region, "name"):
            request = "/regions/{}/addresses/{}".format(address.region.name, address.name)
        else:
            request = "/global/addresses/%s" % (address.name)

        self.connection.async_request(request, method="DELETE")

        return True

    def ex_destroy_backendservice(self, backendservice):
        """
        Destroy a Backend Service.

        :param  backendservice: BackendService object to destroy
        :type   backendservice: :class:`GCEBackendService`

        :return:  True if successful
        :rtype:   ``bool``
        """
        request = "/global/backendServices/%s" % backendservice.name

        self.connection.async_request(request, method="DELETE")

        return True

    def ex_delete_image(self, image):
        """
        Delete a specific image resource.

        :param  image: Image object to delete
        :type   image: ``str`` or :class:`GCENodeImage`

        :return: True if successful
        :rtype:  ``bool``
        """

        if not hasattr(image, "name"):
            image = self.ex_get_image(image)

        request = "/global/images/%s" % (image.name)
        self.connection.async_request(request, method="DELETE")

        return True

    def ex_deprecate_image(
        self,
        image,
        replacement,
        state=None,
        deprecated=None,
        obsolete=None,
        deleted=None,
    ):
        """
        Deprecate a specific image resource.

        :param  image: Image object to deprecate
        :type   image: ``str`` or :class: `GCENodeImage`

        :param  replacement: Image object to use as a replacement
        :type   replacement: ``str`` or :class: `GCENodeImage`

        :param  state: State of the image
        :type   state: ``str``

        :param  deprecated: RFC3339 timestamp to mark DEPRECATED
        :type   deprecated: ``str`` or ``None``

        :param  obsolete: RFC3339 timestamp to mark OBSOLETE
        :type   obsolete: ``str`` or ``None``

        :param  deleted: RFC3339 timestamp to mark DELETED
        :type   deleted: ``str`` or ``None``

        :return: True if successful
        :rtype:  ``bool``
        """

        if not hasattr(image, "name"):
            image = self.ex_get_image(image)

        if not hasattr(replacement, "name"):
            replacement = self.ex_get_image(replacement)

        if state is None:
            state = "DEPRECATED"

        possible_states = ["ACTIVE", "DELETED", "DEPRECATED", "OBSOLETE"]

        if state not in possible_states:
            raise ValueError("state must be one of %s" % ",".join(possible_states))

        if state == "ACTIVE":
            image_data = {}
        else:
            image_data = {
                "state": state,
                "replacement": replacement.extra["selfLink"],
            }

            for attribute, value in [
                ("deprecated", deprecated),
                ("obsolete", obsolete),
                ("deleted", deleted),
            ]:
                if value is None:
                    continue

                try:
                    timestamp_to_datetime(value)
                except Exception:
                    raise ValueError("%s must be an RFC3339 timestamp" % attribute)
                image_data[attribute] = value

        request = "/global/images/%s/deprecate" % (image.name)

        self.connection.request(request, method="POST", data=image_data).object

        return True

    def ex_destroy_healthcheck(self, healthcheck):
        """
        Destroy a healthcheck.

        :param  healthcheck: Health check object to destroy
        :type   healthcheck: :class:`GCEHealthCheck`

        :return:  True if successful
        :rtype:   ``bool``
        """
        request = "/global/httpHealthChecks/%s" % (healthcheck.name)
        self.connection.async_request(request, method="DELETE")

        return True

    def ex_destroy_firewall(self, firewall):
        """
        Destroy a firewall.

        :param  firewall: Firewall object to destroy
        :type   firewall: :class:`GCEFirewall`

        :return:  True if successful
        :rtype:   ``bool``
        """
        request = "/global/firewalls/%s" % (firewall.name)
        self.connection.async_request(request, method="DELETE")

        return True

    def ex_destroy_forwarding_rule(self, forwarding_rule):
        """
        Destroy a forwarding rule.

        :param  forwarding_rule: Forwarding Rule object to destroy
        :type   forwarding_rule: :class:`GCEForwardingRule`

        :return:  True if successful
        :rtype:   ``bool``
        """

        if forwarding_rule.region:
            request = "/regions/{}/forwardingRules/{}".format(
                forwarding_rule.region.name,
                forwarding_rule.name,
            )
        else:
            request = "/global/forwardingRules/%s" % forwarding_rule.name
        self.connection.async_request(request, method="DELETE")

        return True

    def ex_destroy_route(self, route):
        """
        Destroy a route.

        :param  route: Route object to destroy
        :type   route: :class:`GCERoute`

        :return:  True if successful
        :rtype:   ``bool``
        """
        request = "/global/routes/%s" % (route.name)
        self.connection.async_request(request, method="DELETE")

        return True

    def ex_destroy_network(self, network):
        """
        Destroy a network.

        :param  network: Network object to destroy
        :type   network: :class:`GCENetwork`

        :return:  True if successful
        :rtype:   ``bool``
        """
        request = "/global/networks/%s" % (network.name)
        self.connection.async_request(request, method="DELETE")

        return True

    def ex_set_machine_type(self, node, machine_type="n1-standard-1"):
        """
        Set the machine type of the stopped instance. Can be the short-name,
        a full, or partial URL.

        :param  node: Target node object to change
        :type   node: :class:`Node`

        :param  machine_type: Desired machine type
        :type   machine_type: ``str``

        :return:  True if successful
        :rtype:   ``bool``
        """
        request = mt_url = "/zones/%s" % node.extra["zone"].name

        mt = machine_type.split("/")[-1]
        mt_url = "{}/machineTypes/{}".format(mt_url, mt)

        request = "{}/instances/{}/setMachineType".format(request, node.name)
        body = {"machineType": mt_url}
        self.connection.async_request(request, method="POST", data=body)

        return True

    def start_node(self, node, ex_sync=True):
        """
        Start a node that is stopped and in TERMINATED state.

        :param  node: Node object to start
        :type   node: :class:`Node`

        :keyword  sync: If true, do not return until destroyed or timeout
        :type     sync: ``bool``

        :return:  True if successful
        :rtype:   ``bool``
        """
        request = "/zones/{}/instances/{}/start".format(node.extra["zone"].name, node.name)

        if ex_sync:
            self.connection.async_request(request, method="POST")
        else:
            self.connection.request(request, method="POST")

        return True

    def stop_node(self, node, ex_sync=True):
        """
        Stop a running node.

        :param  node: Node object to stop
        :type   node: :class:`Node`

        :keyword  sync: If true, do not return until destroyed or timeout
        :type     sync: ``bool``

        :return:  True if successful
        :rtype:   ``bool``
        """
        request = "/zones/{}/instances/{}/stop".format(node.extra["zone"].name, node.name)

        if ex_sync:
            self.connection.async_request(request, method="POST")
        else:
            self.connection.request(request, method="POST")

        return True

    def ex_start_node(self, node, sync=True):
        # NOTE: This method is here for backward compatibility reasons after
        # this method was promoted to be part of the standard compute API in
        # Libcloud v2.7.0

        return self.start_node(node=node, ex_sync=sync)

    def ex_stop_node(self, node, sync=True):
        # NOTE: This method is here for backward compatibility reasons after
        # this method was promoted to be part of the standard compute API in
        # Libcloud v2.7.0

        return self.stop_node(node=node, ex_sync=sync)

    def ex_destroy_instancegroupmanager(self, manager):
        """
        Destroy a managed instance group.  This will destroy all instances
        that belong to the instance group.

        :param  manager: InstanceGroup object to destroy.
        :type   manager: :class:`GCEInstanceGroup`

        :return:  True if successful
        :rtype:   ``bool``
        """
        request = "/zones/{}/instanceGroupManagers/{}".format(
            manager.zone.name,
            manager.name,
        )

        self.connection.async_request(request, method="DELETE")

        return True

    def ex_destroy_instancetemplate(self, instancetemplate):
        """
        Deletes the specified instance template. If you delete an instance
        template that is being referenced from another instance group, the
        instance group will not be able to create or recreate virtual machine
        instances. Deleting an instance template is permanent and cannot be
        undone.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute

        :param  instancetemplate:  The name of the instance template to
                                   delete.
        :type   instancetemplate: ``str``

        :return  instanceTemplate:  Return True if successful.
        :rtype   instanceTemplate: ````bool````
        """

        request = "/global/instanceTemplates/%s" % (instancetemplate.name)
        request_data = {}
        self.connection.async_request(request, method="DELETE", data=request_data)

        return True

    def ex_destroy_autoscaler(self, autoscaler):
        """
        Destroy an Autoscaler.

        :param  autoscaler: Autoscaler object to destroy.
        :type   autoscaler: :class:`GCEAutoscaler`

        :return:  True if successful
        :rtype:   ``bool``
        """
        request = "/zones/{}/autoscalers/{}".format(autoscaler.zone.name, autoscaler.name)

        self.connection.async_request(request, method="DELETE")

        return True

    def destroy_node(self, node, destroy_boot_disk=False, ex_sync=True):
        """
        Destroy a node.

        :param  node: Node object to destroy
        :type   node: :class:`Node`

        :keyword  destroy_boot_disk: If true, also destroy the node's
                                     boot disk. (Note that this keyword is not
                                     accessible from the node's .destroy()
                                     method.)
        :type     destroy_boot_disk: ``bool``

        :keyword  ex_sync: If true, do not return until destroyed or timeout
        :type     ex_sync: ``bool``

        :return:  True if successful
        :rtype:   ``bool``
        """
        request = "/zones/{}/instances/{}".format(node.extra["zone"].name, node.name)

        if ex_sync:
            self.connection.async_request(request, method="DELETE")
        else:
            self.connection.request(request, method="DELETE")

        if destroy_boot_disk and node.extra["boot_disk"]:
            node.extra["boot_disk"].destroy()

        return True

    def ex_destroy_multiple_nodes(
        self,
        node_list,
        ignore_errors=True,
        destroy_boot_disk=False,
        poll_interval=2,
        timeout=DEFAULT_TASK_COMPLETION_TIMEOUT,
    ):
        """
        Destroy multiple nodes at once.

        :param  node_list: List of nodes to destroy
        :type   node_list: ``list`` of :class:`Node`

        :keyword  ignore_errors: If true, don't raise an exception if one or
                                 more nodes fails to be destroyed.
        :type     ignore_errors: ``bool``

        :keyword  destroy_boot_disk: If true, also destroy the nodes' boot
                                     disks.
        :type     destroy_boot_disk: ``bool``

        :keyword  poll_interval: Number of seconds between status checks.
        :type     poll_interval: ``int``

        :keyword  timeout: Number of seconds to wait for all nodes to be
                           destroyed.
        :type     timeout: ``int``

        :return:  A list of boolean values.  One for each node.  True means
                  that the node was successfully destroyed.
        :rtype:   ``list`` of ``bool``
        """
        status_list = []
        complete = False
        start_time = time.time()

        for node in node_list:
            request = "/zones/{}/instances/{}".format(node.extra["zone"].name, node.name)
            try:
                response = self.connection.request(request, method="DELETE").object
            except GoogleBaseError:
                self._catch_error(ignore_errors=ignore_errors)
                response = None

            status = {
                "node": node,
                "node_success": False,
                "node_response": response,
                "disk_success": not destroy_boot_disk,
                "disk_response": None,
            }

            status_list.append(status)

        while not complete:
            if time.time() - start_time >= timeout:
                raise Exception("Timeout (%s sec) while waiting to delete " "multiple instances")
            complete = True

            for status in status_list:
                # If one of the operations is running, check the status
                operation = status["node_response"] or status["disk_response"]
                delete_disk = False

                if operation:
                    no_errors = True
                    try:
                        response = self.connection.request(operation["selfLink"]).object
                    except GoogleBaseError:
                        self._catch_error(ignore_errors=ignore_errors)
                        no_errors = False
                        response = {"status": "DONE"}

                    if response["status"] == "DONE":
                        # If a node was deleted, update status and indicate
                        # that the disk is ready to be deleted.

                        if status["node_response"]:
                            status["node_response"] = None
                            status["node_success"] = no_errors
                            delete_disk = True
                        else:
                            status["disk_response"] = None
                            status["disk_success"] = no_errors
                # If we are destroying disks, and the node has been deleted,
                # destroy the disk.

                if delete_disk and destroy_boot_disk:
                    boot_disk = status["node"].extra["boot_disk"]

                    if boot_disk:
                        request = "/zones/{}/disks/{}".format(
                            boot_disk.extra["zone"].name,
                            boot_disk.name,
                        )
                        try:
                            response = self.connection.request(request, method="DELETE").object
                        except GoogleBaseError:
                            self._catch_error(ignore_errors=ignore_errors)
                            no_errors = False
                            response = None
                        status["disk_response"] = response
                    else:  # If there is no boot disk, ignore
                        status["disk_success"] = True
                operation = status["node_response"] or status["disk_response"]

                if operation:
                    time.sleep(poll_interval)
                    complete = False

        success = []

        for status in status_list:
            s = status["node_success"] and status["disk_success"]
            success.append(s)

        return success

    def ex_destroy_targethttpproxy(self, targethttpproxy):
        """
        Destroy a target HTTP proxy.

        :param  targethttpproxy: TargetHttpProxy object to destroy
        :type   targethttpproxy: :class:`GCETargetHttpProxy`

        :return:  True if successful
        :rtype:   ``bool``
        """
        request = "/global/targetHttpProxies/%s" % targethttpproxy.name
        self.connection.async_request(request, method="DELETE")

        return True

    def ex_destroy_targethttpsproxy(self, targethttpsproxy):
        """
        Deletes the specified TargetHttpsProxy resource.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute

        :param  targethttpsproxy:  Name of the TargetHttpsProxy resource to
                                   delete.
        :type   targethttpsproxy: ``str``

        :return  targetHttpsProxy:  Return True if successful.
        :rtype   targetHttpsProxy: ````bool````
        """

        request = "/global/targetHttpsProxies/%s" % (targethttpsproxy.name)
        request_data = {}
        self.connection.async_request(request, method="DELETE", data=request_data)

        return True

    def ex_destroy_targetinstance(self, targetinstance):
        """
        Destroy a target instance.

        :param  targetinstance: TargetInstance object to destroy
        :type   targetinstance: :class:`GCETargetInstance`

        :return:  True if successful
        :rtype:   ``bool``
        """
        request = "/zones/{}/targetInstances/{}".format(
            targetinstance.zone.name,
            targetinstance.name,
        )
        self.connection.async_request(request, method="DELETE")

        return True

    def ex_destroy_targetpool(self, targetpool):
        """
        Destroy a target pool.

        :param  targetpool: TargetPool object to destroy
        :type   targetpool: :class:`GCETargetPool`

        :return:  True if successful
        :rtype:   ``bool``
        """
        request = "/regions/{}/targetPools/{}".format(
            targetpool.region.name,
            targetpool.name,
        )

        self.connection.async_request(request, method="DELETE")

        return True

    def ex_destroy_urlmap(self, urlmap):
        """
        Destroy a URL map.

        :param  urlmap: UrlMap object to destroy
        :type   urlmap: :class:`GCEUrlMap`

        :return:  True if successful
        :rtype:   ``bool``
        """
        request = "/global/urlMaps/%s" % urlmap.name

        self.connection.async_request(request, method="DELETE")

        return True

    def destroy_volume(self, volume):
        """
        Destroy a volume.

        :param  volume: Volume object to destroy
        :type   volume: :class:`StorageVolume`

        :return:  True if successful
        :rtype:   ``bool``
        """
        request = "/zones/{}/disks/{}".format(volume.extra["zone"].name, volume.name)
        self.connection.async_request(request, method="DELETE")

        return True

    def destroy_volume_snapshot(self, snapshot):
        """
        Destroy a snapshot.

        :param  snapshot: Snapshot object to destroy
        :type   snapshot: :class:`GCESnapshot`

        :return:  True if successful
        :rtype:   ``bool``
        """
        request = "/global/snapshots/%s" % (snapshot.name)
        self.connection.async_request(request, method="DELETE")

        return True

    def ex_get_license(self, project, name):
        """
        Return a License object for specified project and name.

        :param  project: The project to reference when looking up the license.
        :type   project: ``str``

        :param  name: The name of the License
        :type   name: ``str``

        :return:  A License object for the name
        :rtype:   :class:`GCELicense`
        """

        return GCELicense.lazy(name, project, self)

    def ex_get_disktype(self, name, zone=None):
        """
        Return a DiskType object based on a name and optional zone.

        :param  name: The name of the DiskType
        :type   name: ``str``

        :keyword  zone: The zone to search for the DiskType in (set to
                          'all' to search all zones)
        :type     zone: ``str`` :class:`GCEZone` or ``None``

        :return:  A DiskType object for the name
        :rtype:   :class:`GCEDiskType`
        """
        zone = self._set_zone(zone)

        if zone:
            request_path = "/zones/{}/diskTypes/{}".format(zone.name, name)
        else:
            request_path = "/aggregated/diskTypes"

        response = self.connection.request(request_path, method="GET")

        if "items" in response.object:
            # Aggregated response, zone not provided
            data = None

            for zone_item in response.object["items"].values():
                for item in zone_item["diskTypes"]:
                    if item["name"] == name:
                        data = item

                        break
        else:
            data = response.object

        if not data:
            raise ValueError('Disk type with name "%s" not found' % (name))

        return self._to_disktype(data)

    def ex_get_accelerator_type(self, name, zone=None):
        """
        Return an AcceleratorType object based on a name and zone.

        :param  name: The name of the AcceleratorType
        :type   name: ``str``

        :param  zone: The zone to search for the AcceleratorType in.
        :type   zone: :class:`GCEZone`

        :return:  An AcceleratorType object for the name
        :rtype:   :class:`GCEAcceleratorType`
        """
        zone = self._set_zone(zone)
        request = "/zones/{}/acceleratorTypes/{}".format(zone.name, name)
        response = self.connection.request(request, method="GET").object

        return self._to_accelerator_type(response)

    def ex_get_address(self, name, region=None):
        """
        Return an Address object based on an address name and optional region.

        :param  name: The name of the address
        :type   name: ``str``

        :keyword  region: The region to search for the address in (set to
                          'all' to search all regions)
        :type     region: ``str`` :class:`GCERegion` or ``None``

        :return:  An Address object for the address
        :rtype:   :class:`GCEAddress`
        """

        if region == "global":
            request = "/global/addresses/%s" % (name)
        else:
            region = self._set_region(region) or self._find_zone_or_region(
                name, "addresses", region=True, res_name="Address"
            )
            request = "/regions/{}/addresses/{}".format(region.name, name)
        response = self.connection.request(request, method="GET").object

        return self._to_address(response)

    def ex_get_backendservice(self, name):
        """
        Return a Backend Service object based on name

        :param  name: The name of the backend service
        :type   name: ``str``

        :return:  A BackendService object for the backend service
        :rtype:   :class:`GCEBackendService`
        """
        request = "/global/backendServices/%s" % name
        response = self.connection.request(request, method="GET").object

        return self._to_backendservice(response)

    def ex_get_healthcheck(self, name):
        """
        Return a HealthCheck object based on the healthcheck name.

        :param  name: The name of the healthcheck
        :type   name: ``str``

        :return:  A GCEHealthCheck object
        :rtype:   :class:`GCEHealthCheck`
        """
        request = "/global/httpHealthChecks/%s" % (name)
        response = self.connection.request(request, method="GET").object

        return self._to_healthcheck(response)

    def ex_get_firewall(self, name):
        """
        Return a Firewall object based on the firewall name.

        :param  name: The name of the firewall
        :type   name: ``str``

        :return:  A GCEFirewall object
        :rtype:   :class:`GCEFirewall`
        """
        request = "/global/firewalls/%s" % (name)
        response = self.connection.request(request, method="GET").object

        return self._to_firewall(response)

    def ex_get_forwarding_rule(self, name, region=None, global_rule=False):
        """
        Return a Forwarding Rule object based on the forwarding rule name.

        :param  name: The name of the forwarding rule
        :type   name: ``str``

        :keyword  region: The region to search for the rule in (set to 'all'
                          to search all regions).
        :type     region: ``str`` or ``None``

        :keyword  global_rule: Set to True to get a global forwarding rule.
                                Region will be ignored if True.
        :type     global_rule: ``bool``

        :return:  A GCEForwardingRule object
        :rtype:   :class:`GCEForwardingRule`
        """

        if global_rule:
            request = "/global/forwardingRules/%s" % name
        else:
            region = self._set_region(region) or self._find_zone_or_region(
                name, "forwardingRules", region=True, res_name="ForwardingRule"
            )
            request = "/regions/{}/forwardingRules/{}".format(region.name, name)

        response = self.connection.request(request, method="GET").object

        return self._to_forwarding_rule(response)

    def ex_get_image(self, partial_name, ex_project_list=None, ex_standard_projects=True):
        """
        Return an GCENodeImage object based on the name or link provided.

        :param  partial_name: The name, partial name, or full path of a GCE
                              image.
        :type   partial_name: ``str``

        :param  ex_project_list: The name of the project to list for images.
                                 Examples include: 'debian-cloud'.
        :type   ex_project_list: ``str`` or ``list`` of ``str`` or ``None``

        :param  ex_standard_projects: If true, check in standard projects if
                                      the image is not found.
        :type   ex_standard_projects: ``bool``

        :return:  GCENodeImage object based on provided information or None if
                  an image with that name is not found.
        :rtype:   :class:`GCENodeImage` or raise ``ResourceNotFoundError``
        """

        if partial_name.startswith("https://"):
            response = self.connection.request(partial_name, method="GET")

            return self._to_node_image(response.object)
        image = self._match_images(ex_project_list, partial_name)

        if not image and ex_standard_projects:
            for img_proj, short_list in self.IMAGE_PROJECTS.items():
                for short_name in short_list:
                    if partial_name.startswith(short_name):
                        image = self._match_images(img_proj, partial_name)

        if not image:
            raise ResourceNotFoundError("Could not find image '%s'" % (partial_name), None, None)

        return image

    def ex_get_image_from_family(
        self, image_family, ex_project_list=None, ex_standard_projects=True
    ):
        """
        Return an GCENodeImage object based on an image family name.

        :param  image_family: The name of the 'Image Family' to return the
                              latest image from.
        :type   image_family: ``str``

        :param  ex_project_list: The name of the project to list for images.
                                 Examples include: 'debian-cloud'.
        :type   ex_project_list: ``list`` of ``str``, or ``None``

        :param  ex_standard_projects: If true, check in standard projects if
                                      the image is not found.
        :type   ex_standard_projects: ``bool``

        :return:  GCENodeImage object based on provided information or
                  ResourceNotFoundError if the image family is not found.
        :rtype:   :class:`GCENodeImage` or raise ``ResourceNotFoundError``
        """

        def _try_image_family(image_family, project=None):
            request = "/global/images/family/%s" % (image_family)
            save_request_path = self.connection.request_path

            if project:
                new_request_path = save_request_path.replace(self.project, project)
                self.connection.request_path = new_request_path
            try:
                response = self.connection.request(request, method="GET")
                image = self._to_node_image(response.object)
            except ResourceNotFoundError:
                image = None
            finally:
                self.connection.request_path = save_request_path

            return image

        image = None

        if image_family.startswith("https://"):
            response = self.connection.request(image_family, method="GET")

            return self._to_node_image(response.object)

        if not ex_project_list:
            image = _try_image_family(image_family)
        else:
            for img_proj in ex_project_list:
                image = _try_image_family(image_family, project=img_proj)

                if image:
                    break

        if not image and ex_standard_projects:
            for img_proj, short_list in self.IMAGE_PROJECTS.items():
                for short_name in short_list:
                    if image_family.startswith(short_name):
                        image = _try_image_family(image_family, project=img_proj)

                        if image:
                            break

        if not image:
            raise ResourceNotFoundError(
                "Could not find image for family " "'%s'" % (image_family), None, None
            )

        return image

    def ex_get_route(self, name):
        """
        Return a Route object based on a route name.

        :param  name: The name of the route
        :type   name: ``str``

        :return:  A Route object for the named route
        :rtype:   :class:`GCERoute`
        """
        request = "/global/routes/%s" % (name)
        response = self.connection.request(request, method="GET").object

        return self._to_route(response)

    def ex_destroy_sslcertificate(self, sslcertificate):
        """
        Deletes the specified SslCertificate resource.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute

        :param  sslcertificate:  Name of the SslCertificate resource to
                                 delete.
        :type   sslcertificate: ``str``

        :return  sslCertificate:  Return True if successful.
        :rtype   sslCertificate: ````bool````
        """

        request = "/global/sslCertificates/%s" % (sslcertificate.name)
        request_data = {}
        self.connection.async_request(request, method="DELETE", data=request_data)

        return True

    def ex_destroy_subnetwork(self, name, region=None):
        """
        Delete a Subnetwork object based on name and region.

        :param  name: The name, URL or object of the subnetwork
        :type   name: ``str`` or :class:`GCESubnetwork`

        :keyword region: The region object, name, or URL of the subnetwork
        :type   region: ``str`` or :class:`GCERegion` or ``None``

        :return:  True if successful
        :rtype:   ``bool``
        """
        region_name = None
        subnet_name = None

        if region:
            if isinstance(region, GCERegion):
                region_name = region.name
            else:
                if region.startswith("https://"):
                    region_name = region.split("/")[-1]
                else:
                    region_name = region

        if isinstance(name, GCESubnetwork):
            subnet_name = name.name

            if not region_name:
                region_name = name.region.name
        else:
            if name.startswith("https://"):
                url_parts = self._get_components_from_path(name)
                subnet_name = url_parts["name"]

                if not region_name:
                    region_name = url_parts["region"]
            else:
                subnet_name = name

        if not region_name:
            region = self._set_region(region)

            if not region:
                raise ValueError("Could not determine region for subnetwork.")
            else:
                region_name = region.name

        request = "/regions/{}/subnetworks/{}".format(region_name, subnet_name)
        self.connection.async_request(request, method="DELETE").object

        return True

    def ex_get_subnetwork(self, name, region=None):
        """
        Return a Subnetwork object based on name and region.

        :param  name: The name or URL of the subnetwork
        :type   name: ``str``

        :keyword region: The region of the subnetwork
        :type   region: ``str`` or :class:`GCERegion` or ``None``

        :return:  A Subnetwork object
        :rtype:   :class:`GCESubnetwork`
        """
        region_name = None

        if name.startswith("https://"):
            request = name
        else:
            if isinstance(region, GCERegion):
                region_name = region.name
            elif isinstance(region, str):
                if region.startswith("https://"):
                    region_name = region.split("/")[-1]
                else:
                    region_name = region

            if not region_name:
                region = self._set_region(region)

                if not region:
                    raise ValueError("Could not determine region for subnetwork.")
                else:
                    region_name = region.name

            request = "/regions/{}/subnetworks/{}".format(region_name, name)

        response = self.connection.request(request, method="GET").object

        return self._to_subnetwork(response)

    def ex_get_network(self, name):
        """
        Return a Network object based on a network name.

        :param  name: The name or URL of the network
        :type   name: ``str``

        :return:  A Network object for the network
        :rtype:   :class:`GCENetwork`
        """

        if name.startswith("https://"):
            request = name
        else:
            request = "/global/networks/%s" % (name)
        response = self.connection.request(request, method="GET").object

        return self._to_network(response)

    def ex_get_node(self, name, zone=None):
        """
        Return a Node object based on a node name and optional zone.

        :param  name: The name of the node
        :type   name: ``str``

        :keyword  zone: The zone to search for the node in.  If set to 'all',
                        search all zones for the instance.
        :type     zone: ``str`` or :class:`GCEZone` or
                        :class:`NodeLocation` or ``None``

        :return:  A Node object for the node
        :rtype:   :class:`Node`
        """
        zone = self._set_zone(zone) or self._find_zone_or_region(name, "instances", res_name="Node")
        request = "/zones/{}/instances/{}".format(zone.name, name)
        response = self.connection.request(request, method="GET").object

        return self._to_node(response)

    def ex_get_project(self):
        """
        Return a Project object with project-wide information.

        :return:  A GCEProject object
        :rtype:   :class:`GCEProject`
        """
        response = self.connection.request("", method="GET").object

        return self._to_project(response)

    def ex_get_size(self, name, zone=None):
        """
        Return a size object based on a machine type name and zone.

        :param  name: The name of the node
        :type   name: ``str``

        :keyword  zone: The zone to search for the machine type in
        :type     zone: ``str`` or :class:`GCEZone` or
                        :class:`NodeLocation` or ``None``

        :return:  A GCENodeSize object for the machine type
        :rtype:   :class:`GCENodeSize`
        """
        zone = zone or self.zone

        if not hasattr(zone, "name"):
            zone = self.ex_get_zone(zone)
        request = "/zones/{}/machineTypes/{}".format(zone.name, name)
        response = self.connection.request(request, method="GET").object
        instance_prices = get_pricing(driver_type="compute", driver_name="gce_instances")

        return self._to_node_size(response, instance_prices)

    def ex_get_snapshot(self, name):
        """
        Return a Snapshot object based on snapshot name.

        :param  name: The name of the snapshot
        :type   name: ``str``

        :return:  A GCESnapshot object for the snapshot
        :rtype:   :class:`GCESnapshot`
        """
        request = "/global/snapshots/%s" % (name)
        response = self.connection.request(request, method="GET").object

        return self._to_snapshot(response)

    def ex_get_volume(self, name, zone=None, use_cache=False):
        """
        Return a Volume object based on a volume name and optional zone.

        To improve performance, we request all disks and allow the user
        to consult the cache dictionary rather than making an API call.

        :param    name: The name of the volume
        :type     name: ``str``

        :keyword  zone: The zone to search for the volume in (set to 'all' to
                        search all zones)
        :type     zone: ``str`` or :class:`GCEZone` or :class:`NodeLocation`
                        or ``None``

        :keyword  use_cache: Search for the volume in the existing cache of
                             volumes.  If True, we omit the API call and search
                             self.volumes_dict.  If False, a call to
                             disks/aggregatedList is made prior to searching
                             self._ex_volume_dict.
        :type     use_cache: ``bool``

        :return:  A StorageVolume object for the volume
        :rtype:   :class:`StorageVolume`
        """

        if not self._ex_volume_dict or use_cache is False:
            # Make the API call and build volume dictionary
            self._ex_populate_volume_dict()

        try:
            # if zone is of class GCEZone or NodeLocation, get name instead
            zone = zone.name
        except AttributeError:
            pass

        return self._ex_lookup_volume(name, zone)

    def ex_get_region(self, name):
        """
        Return a Region object based on the region name.

        :param  name: The name of the region.
        :type   name: ``str``

        :return:  A GCERegion object for the region
        :rtype:   :class:`GCERegion`
        """

        if name.startswith("https://"):
            short_name = self._get_components_from_path(name)["name"]
            request = name
        else:
            short_name = name
            request = "/regions/%s" % (name)
        # Check region cache first

        if short_name in self.region_dict:
            return self.region_dict[short_name]
        # Otherwise, look up region information
        response = self.connection.request(request, method="GET").object

        return self._to_region(response)

    def ex_get_sslcertificate(self, name):
        """
        Returns the specified SslCertificate resource. Get a list of available
        SSL certificates by making a list() request.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute
        * https://www.googleapis.com/auth/compute.readonly

        :param  name:  Name of the SslCertificate resource to
                                 return.
        :type   name: ``str``

        :return:  `GCESslCertificate` object.
        :rtype: :class:`GCESslCertificate`
        """

        request = "/global/sslCertificates/%s" % (name)
        response = self.connection.request(request, method="GET").object

        return self._to_sslcertificate(response)

    def ex_get_targethttpproxy(self, name):
        """
        Return a Target HTTP Proxy object based on its name.

        :param  name: The name of the target HTTP proxy.
        :type   name: ``str``

        :return:  A Target HTTP Proxy object for the pool
        :rtype:   :class:`GCETargetHttpProxy`
        """
        request = "/global/targetHttpProxies/%s" % name
        response = self.connection.request(request, method="GET").object

        return self._to_targethttpproxy(response)

    def ex_get_targethttpsproxy(self, name):
        """
        Returns the specified TargetHttpsProxy resource. Get a list of
        available target HTTPS proxies by making a list() request.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute
        * https://www.googleapis.com/auth/compute.readonly

        :param  name:  Name of the TargetHttpsProxy resource to
                                   return.
        :type   name: ``str``

        :return:  `GCETargetHttpsProxy` object.
        :rtype: :class:`GCETargetHttpsProxy`
        """

        request = "/global/targetHttpsProxies/%s" % (name)
        response = self.connection.request(request, method="GET").object

        return self._to_targethttpsproxy(response)

    def ex_get_targetinstance(self, name, zone=None):
        """
        Return a TargetInstance object based on a name and optional zone.

        :param  name: The name of the target instance
        :type   name: ``str``

        :keyword  zone: The zone to search for the target instance in (set to
                          'all' to search all zones).
        :type     zone: ``str`` or :class:`GCEZone` or ``None``

        :return:  A TargetInstance object for the instance
        :rtype:   :class:`GCETargetInstance`
        """
        zone = self._set_zone(zone) or self._find_zone_or_region(
            name, "targetInstances", res_name="TargetInstance"
        )
        request = "/zones/{}/targetInstances/{}".format(zone.name, name)
        response = self.connection.request(request, method="GET").object

        return self._to_targetinstance(response)

    def ex_get_targetpool(self, name, region=None):
        """
        Return a TargetPool object based on a name and optional region.

        :param  name: The name of the target pool
        :type   name: ``str``

        :keyword  region: The region to search for the target pool in (set to
                          'all' to search all regions).
        :type     region: ``str`` or :class:`GCERegion` or ``None``

        :return:  A TargetPool object for the pool
        :rtype:   :class:`GCETargetPool`
        """
        region = self._set_region(region) or self._find_zone_or_region(
            name, "targetPools", region=True, res_name="TargetPool"
        )
        request = "/regions/{}/targetPools/{}".format(region.name, name)
        response = self.connection.request(request, method="GET").object

        return self._to_targetpool(response)

    def ex_get_urlmap(self, name):
        """
        Return a URL Map object based on name

        :param  name: The name of the url map
        :type   name: ``str``

        :return:  A URL Map object for the backend service
        :rtype:   :class:`GCEUrlMap`
        """
        request = "/global/urlMaps/%s" % name
        response = self.connection.request(request, method="GET").object

        return self._to_urlmap(response)

    def ex_get_instancegroup(self, name, zone=None):
        """
        Returns the specified Instance Group. Get a list of available instance
        groups by making a list() request.

        Scopes needed - one of the following:
        * https://www.googleapis.com/auth/cloud-platform
        * https://www.googleapis.com/auth/compute
        * https://www.googleapis.com/auth/compute.readonly

        :param  name:  The name of the instance group.
        :type   name: ``str``

        :param  zone:  The name of the zone where the instance group is
                       located.
        :type   zone: ``str``

        :return:  `GCEInstanceGroup` object.
        :rtype:   :class:`GCEInstanceGroup`
        """
        zone = self._set_zone(zone) or self._find_zone_or_region(
            name, "instanceGroups", region=False, res_name="Instancegroup"
        )
        request = "/zones/{}/instanceGroups/{}".format(zone.name, name)
        response = self.connection.request(request, method="GET").object

        return self._to_instancegroup(response)

    def ex_get_instancegroupmanager(self, name, zone=None):
        """
        Return a InstanceGroupManager object based on a name and optional zone.

        :param  name: The name of the Instance Group Manager.
        :type   name: ``str``

        :keyword  zone: The zone to search for the Instance Group Manager.
                        Set to 'all' to search all zones.
        :type     zone: ``str`` or :class:`GCEZone` or ``None``

        :return:  An Instance Group Manager object.
        :rtype:   :class:`GCEInstanceGroupManager`
        """
        zone = self._set_zone(zone) or self._find_zone_or_region(
            name, "instanceGroupManagers", region=False, res_name="Instancegroupmanager"
        )
        request = "/zones/{}/instanceGroupManagers/{}".format(zone.name, name)
        response = self.connection.request(request, method="GET").object

        return self._to_instancegroupmanager(response)

    def ex_get_instancetemplate(self, name):
        """
        Return an InstanceTemplate object based on a name and optional zone.

        :param  name: The name of the Instance Template.
        :type   name: ``str``

        :return:  An Instance Template object.
        :rtype:   :class:`GCEInstanceTemplate`
        """
        request = "/global/instanceTemplates/%s" % (name)
        response = self.connection.request(request, method="GET").object

        return self._to_instancetemplate(response)

    def ex_get_autoscaler(self, name, zone=None):
        """
        Return an Autoscaler object based on a name and optional zone.

        :param  name: The name of the Autoscaler.
        :type   name: ``str``

        :keyword  zone: The zone to search for the Autoscaler.  Set to
                          'all' to search all zones.
        :type     zone: ``str`` or :class:`GCEZone` or ``None``

        :return:  An Autoscaler object.
        :rtype:   :class:`GCEAutoscaler`
        """
        zone = self._set_zone(zone) or self._find_zone_or_region(
            name, "Autoscalers", region=False, res_name="Autoscalers"
        )
        request = "/zones/{}/autoscalers/{}".format(zone.name, name)
        response = self.connection.request(request, method="GET").object

        return self._to_autoscaler(response)

    def ex_get_zone(self, name):
        """
        Return a Zone object based on the zone name.

        :param  name: The name of the zone.
        :type   name: ``str``

        :return:  A GCEZone object for the zone or None if not found
        :rtype:   :class:`GCEZone` or ``None``
        """

        if name.startswith("https://"):
            short_name = self._get_components_from_path(name)["name"]
            request = name
        else:
            short_name = name
            request = "/zones/%s" % (name)
        # Check zone cache first

        if short_name in self.zone_dict:
            return self.zone_dict[short_name]
        # Otherwise, look up zone information
        try:
            response = self.connection.request(request, method="GET").object
        except ResourceNotFoundError:
            return None

        return self._to_zone(response)

    def _ex_connection_class_kwargs(self):
        return {
            "auth_type": self.auth_type,
            "project": self.project,
            "scopes": self.scopes,
            "credential_file": self.credential_file,
        }

    def _build_volume_dict(self, zone_dict):
        """
        Build a dictionary in [name][zone]=disk format.

        :param  zone_dict: dict in the format of:
                 { items: {key: {api_name:[], key2: api_name:[]}} }
        :type   zone_dict: ``dict``

        :return:  dict of volumes, organized by name, then zone  Format:
                  { 'disk_name':
                   {'zone_name1': disk_info, 'zone_name2': disk_info} }
        :rtype: ``dict``
        """
        name_zone_dict = {}

        for k, v in zone_dict.items():
            zone_name = k.replace("zones/", "")
            disks = v.get("disks", [])

            for disk in disks:
                n = disk["name"]
                name_zone_dict.setdefault(n, {})
                name_zone_dict[n].update({zone_name: disk})

        return name_zone_dict

    def _ex_lookup_volume(self, volume_name, zone=None):
        """
        Look up volume by name and zone in volume dict.

        If zone isn't specified or equals 'all', we return the volume
        for the first zone, as determined alphabetically.

        :param    volume_name: The name of the volume.
        :type     volume_name: ``str``

        :keyword  zone: The zone to search for the volume in (set to 'all' to
                        search all zones)
        :type     zone: ``str`` or ``None``

        :return:  A StorageVolume object for the volume.
        :rtype:   :class:`StorageVolume` or raise ``ResourceNotFoundError``.
        """

        if volume_name not in self._ex_volume_dict:
            # Possibly added through another thread/process, so re-populate
            # _volume_dict and try again.  If still not found, raise exception.
            self._ex_populate_volume_dict()

            if volume_name not in self._ex_volume_dict:
                raise ResourceNotFoundError(
                    "Volume name: '{}' not found. Zone: {}".format(volume_name, zone),
                    None,
                    None,
                )
        # Disk names are not unique across zones, so if zone is None or
        # 'all', we return the first one we find for that disk name.  For
        # consistency, we sort by keys and set the zone to the first key.

        if zone is None or zone == "all":
            zone = sorted(self._ex_volume_dict[volume_name])[0]

        volume = self._ex_volume_dict[volume_name].get(zone, None)

        if not volume:
            raise ResourceNotFoundError(
                "Volume '{}' not found for zone {}.".format(volume_name, zone), None, None
            )

        return self._to_storage_volume(volume)

    def _ex_populate_volume_dict(self):
        """
        Fetch the volume information using disks/aggregatedList
        and store it in _ex_volume_dict.

        return:  ``None``
        """
        # fill the volume dict by making an aggegatedList call to disks.
        aggregated_items = self.connection.request_aggregated_items("disks")

        # _ex_volume_dict is in the format of:
        # { 'disk_name' : { 'zone1': disk, 'zone2': disk, ... }}
        self._ex_volume_dict = self._build_volume_dict(aggregated_items["items"])

        return None

    def _catch_error(self, ignore_errors=False):
        """
        Catch an exception and raise it unless asked to ignore it.

        :keyword  ignore_errors: If true, just return the error.  Otherwise,
                                 raise the error.
        :type     ignore_errors: ``bool``

        :return:  The exception that was raised.
        :rtype:   :class:`Exception`
        """
        e = sys.exc_info()[1]

        if ignore_errors:
            return e
        else:
            raise e

    def _get_components_from_path(self, path):
        """
        Return a dictionary containing name & zone/region from a request path.

        :param  path: HTTP request path (e.g.
                      '/project/pjt-name/zones/us-central1-a/instances/mynode')
        :type   path: ``str``

        :return:  Dictionary containing name and zone/region of resource
        :rtype:   ``dict``
        """
        region = None
        zone = None
        glob = False
        components = path.split("/")
        name = components[-1]

        if components[-4] == "regions":
            region = components[-3]
        elif components[-4] == "zones":
            zone = components[-3]
        elif components[-3] == "global":
            glob = True

        return {"name": name, "region": region, "zone": zone, "global": glob}

    def _get_object_by_kind(self, url):
        """
        Fetch a resource and return its object representation by mapping its
        'kind' parameter to the appropriate class.  Returns ``None`` if url is
        ``None``

        :param  url: fully qualified URL of the resource to request from GCE
        :type   url: ``str``

        :return:  Object representation of the requested resource.
        "rtype:   :class:`object` or ``None``
        """

        if not url:
            return None

        # Relies on GoogleBaseConnection.morph_action_hook to rewrite
        # the URL to a request
        response = self.connection.request(url, method="GET").object

        return GCENodeDriver.KIND_METHOD_MAP[response["kind"]](self, response)

    def _get_region_from_zone(self, zone):
        """
        Return the Region object that contains the given Zone object.

        :param  zone: Zone object
        :type   zone: :class:`GCEZone`

        :return:  Region object that contains the zone
        :rtype:   :class:`GCERegion`
        """

        for region in self.region_list:
            zones = [z.name for z in region.zones]

            if zone.name in zones:
                return region

    def _find_zone_or_region(self, name, res_type, region=False, res_name=None):
        """
        Find the zone or region for a named resource.

        :param  name: Name of resource to find
        :type   name: ``str``

        :param  res_type: Type of resource to find.
                          Examples include: 'disks', 'instances' or 'addresses'
        :type   res_type: ``str``

        :keyword  region: If True, search regions instead of zones
        :type     region: ``bool``

        :keyword  res_name: The name of the resource type for error messages.
                            Examples: 'Volume', 'Node', 'Address'
        :keyword  res_name: ``str``

        :return:  Zone/Region object for the zone/region for the resource.
        :rtype:   :class:`GCEZone` or :class:`GCERegion`
        """

        if region:
            rz = "region"
        else:
            rz = "zone"
        rz_name = None
        res_name = res_name or res_type
        res_list = self.connection.request_aggregated_items(res_type)

        for k, v in res_list["items"].items():
            for res in v.get(res_type, []):
                if res["name"] == name:
                    rz_name = k.replace("%ss/" % (rz), "")

                    break

        if not rz_name:
            raise ResourceNotFoundError(
                "{} '{}' not found in any {}.".format(res_name, name, rz), None, None
            )
        else:
            getrz = getattr(self, "ex_get_%s" % (rz))

            return getrz(rz_name)

    def _match_images(self, project, partial_name):
        """
        Find the latest image, given a partial name.

        For example, providing 'debian-7' will return the image object for the
        most recent image with a name that starts with 'debian-7' in the
        supplied project.  If no project is given, it will search your own
        project.

        :param  project: The name of the project to search for images.
                         Examples include: 'debian-cloud' and 'centos-cloud'.
        :type   project: ``str``, ``list`` of ``str``, or ``None``

        :param  partial_name: The full name or beginning of a name for an
                              image.
        :type   partial_name: ``str``

        :return:  The latest image object that matches the partial name or None
                  if no matching image is found.
        :rtype:   :class:`GCENodeImage` or ``None``
        """
        project_images_list = self.ex_list(
            self.list_images, ex_project=project, ex_include_deprecated=True
        )
        partial_match = []

        for page in project_images_list.page():
            for image in page:
                if image.name == partial_name:
                    return image

                if image.name.startswith(partial_name):
                    ts = timestamp_to_datetime(image.extra["creationTimestamp"])

                    if not partial_match or partial_match[0] < ts:
                        partial_match = [ts, image]

        if partial_match:
            return partial_match[1]

    def _set_region(self, region):
        """
        Return the region to use for listing resources.

        :param  region: A name, region object, None, or 'all'
        :type   region: ``str`` or :class:`GCERegion` or ``None``

        :return:  A region object or None if all regions should be considered
        :rtype:   :class:`GCERegion` or ``None``
        """
        region = region or self.region

        if region == "all" or region is None:
            return None

        if not hasattr(region, "name"):
            region = self.ex_get_region(region)

        return region

    def _set_zone(self, zone):
        """
        Return the zone to use for listing resources.

        :param  zone: A name, zone object, None, or 'all'
        :type   zone: ``str`` or :class:`GCEZone` or ``None``

        :return:  A zone object or None if all zones should be considered
        :rtype:   :class:`GCEZone` or ``None``
        """
        zone = zone or self.zone

        if zone == "all" or zone is None:
            return None

        if not hasattr(zone, "name"):
            zone = self.ex_get_zone(zone)

        return zone

    def _create_node_req(
        self,
        name,
        size,
        image,
        location,
        network=None,
        tags=None,
        metadata=None,
        boot_disk=None,
        external_ip="ephemeral",
        internal_ip=None,
        ex_disk_type="pd-standard",
        ex_disk_auto_delete=True,
        ex_service_accounts=None,
        description=None,
        ex_can_ip_forward=None,
        ex_disks_gce_struct=None,
        ex_nic_gce_struct=None,
        ex_on_host_maintenance=None,
        ex_automatic_restart=None,
        ex_preemptible=None,
        ex_subnetwork=None,
        ex_labels=None,
        ex_accelerator_type=None,
        ex_accelerator_count=None,
        ex_disk_size=None,
    ):
        """
        Returns a request and body to create a new node.

        This is a helper method to support both :class:`create_node` and
        :class:`ex_create_multiple_nodes`.

        :param  name: The name of the node to create.
        :type   name: ``str``

        :param  size: The machine type to use.
        :type   size: :class:`GCENodeSize`

        :param  image: The image to use to create the node (or, if using a
                       persistent disk, the image the disk was created from).
        :type   image: :class:`GCENodeImage` or ``None``

        :param  location: The location (zone) to create the node in.
        :type   location: :class:`NodeLocation` or :class:`GCEZone`

        :param  network: The network to associate with the node.
        :type   network: :class:`GCENetwork`

        :keyword  tags: A list of tags to associate with the node.
        :type     tags: ``list`` of ``str``

        :keyword  metadata: Metadata dictionary for instance.
        :type     metadata: ``dict``

        :keyword  boot_disk: Persistent boot disk to attach.
        :type     :class:`StorageVolume` or ``None``

        :keyword  external_ip: The external IP address to use.  If 'ephemeral'
                               (default), a new non-static address will be
                               used.  If 'None', then no external address will
                               be used.  To use an existing static IP address,
                               a GCEAddress object should be passed in. This
                               param will be ignored if also using the
                               ex_nic_gce_struct param.
        :type     external_ip: :class:`GCEAddress` or ``str`` or None

        :keyword  internal_ip: The private IP address to use.
        :type     internal_ip: :class:`GCEAddress` or ``str`` or ``None``

        :keyword  ex_disk_type: Specify a pd-standard (default) disk or pd-ssd
                                for an SSD disk.
        :type     ex_disk_type: ``str`` or :class:`GCEDiskType` or ``None``

        :keyword  ex_disk_auto_delete: Indicate that the boot disk should be
                                       deleted when the Node is deleted. Set to
                                       True by default.
        :type     ex_disk_auto_delete: ``bool``

        :keyword  ex_service_accounts: Specify a list of serviceAccounts when
                                       creating the instance. The format is a
                                       list of dictionaries containing email
                                       and list of scopes, e.g.
                                       [{'email':'default',
                                       'scopes':['compute', ...]}, ...]
                                       Scopes can either be full URLs or short
                                       names. If not provided, use the
                                       'default' service account email and a
                                       scope of 'devstorage.read_only'. Also
                                       accepts the aliases defined in
                                       'gcloud compute'.
        :type     ex_service_accounts: ``list``

        :keyword  description: The description of the node (instance).
        :type     description: ``str`` or ``None``

        :keyword  ex_can_ip_forward: Set to ``True`` to allow this node to
                                  send/receive non-matching src/dst packets.
        :type     ex_can_ip_forward: ``bool`` or ``None``

        :keyword  ex_disks_gce_struct: Support for passing in the GCE-specific
                                       formatted disks[] structure. No attempt
                                       is made to ensure proper formatting of
                                       the disks[] structure. Using this
                                       structure obviates the need of using
                                       other disk params like 'boot_disk',
                                       etc. See the GCE docs for specific
                                       details.
        :type     ex_disks_gce_struct: ``list`` or ``None``

        :keyword  ex_nic_gce_struct: Support passing in the GCE-specific
                                     formatted networkInterfaces[] structure.
                                     No attempt is made to ensure proper
                                     formatting of the networkInterfaces[]
                                     data. Using this structure obviates the
                                     need of using 'external_ip' and
                                     'ex_network'.  See the GCE docs for
                                     details.
        :type     ex_nic_gce_struct: ``list`` or ``None``

        :keyword  ex_on_host_maintenance: Defines whether node should be
                                          terminated or migrated when host
                                          machine goes down. Acceptable values
                                          are: 'MIGRATE' or 'TERMINATE' (If
                                          not supplied, value will be reset to
                                          GCE default value for the instance
                                          type.)
        :type     ex_on_host_maintenance: ``str`` or ``None``

        :keyword  ex_automatic_restart: Defines whether the instance should be
                                        automatically restarted when it is
                                        terminated by Compute Engine. (If not
                                        supplied, value will be set to the GCE
                                        default value for the instance type.)
        :type     ex_automatic_restart: ``bool`` or ``None``

        :keyword  ex_preemptible: Defines whether the instance is preemptible.
                                        (If not supplied, the instance will
                                         not be preemptible)
        :type     ex_preemptible: ``bool`` or ``None``

        :param  ex_subnetwork: The network to associate with the node.
        :type   ex_subnetwork: :class:`GCESubnetwork`

        :keyword  ex_disk_size: Specify the size of boot disk.
                                Integer in gigabytes.
        :type     ex_disk_size: ``int`` or ``None``

        :param  ex_labels: Label dict for node.
        :type   ex_labels: ``dict`` or ``None``

        :param  ex_accelerator_type: The accelerator to associate with the
                                     node.
        :type   ex_accelerator_type: :class:`GCEAcceleratorType` or ``None``

        :param  ex_accelerator_count: The number of accelerators to associate
                                      with the node.
        :type   ex_accelerator_count: ``int`` or ``None``

        :keyword  ex_disk_size: Specify size of the boot disk.
                                Integer in gigabytes.
        :type     ex_disk_size: ``int`` or ``None``

        :return:  A tuple containing a request string and a node_data dict.
        :rtype:   ``tuple`` of ``str`` and ``dict``
        """

        # build disks

        if not image and not boot_disk and not ex_disks_gce_struct:
            raise ValueError(
                "Missing root device or image. Must specify an "
                "'image', existing 'boot_disk', or use the "
                "'ex_disks_gce_struct'."
            )

        if boot_disk and ex_disks_gce_struct:
            raise ValueError(
                "Cannot specify both 'boot_disk' and "
                "'ex_disks_gce_struct'. Use one or the other."
            )

        use_selflinks = True
        source = None

        if boot_disk:
            source = boot_disk

        node_data = self._create_instance_properties(
            name,
            node_size=size,
            image=image,
            source=source,
            disk_type=ex_disk_type,
            disk_auto_delete=ex_disk_auto_delete,
            external_ip=external_ip,
            network=network,
            subnetwork=ex_subnetwork,
            can_ip_forward=ex_can_ip_forward,
            internal_ip=internal_ip,
            service_accounts=ex_service_accounts,
            on_host_maintenance=ex_on_host_maintenance,
            automatic_restart=ex_automatic_restart,
            preemptible=ex_preemptible,
            tags=tags,
            metadata=metadata,
            labels=ex_labels,
            description=description,
            disks_gce_struct=ex_disks_gce_struct,
            nic_gce_struct=ex_nic_gce_struct,
            accelerator_type=ex_accelerator_type,
            accelerator_count=ex_accelerator_count,
            use_selflinks=use_selflinks,
            disk_size=ex_disk_size,
        )
        node_data["name"] = name

        request = "/zones/%s/instances" % (location.name)

        return request, node_data

    def _multi_create_disk(self, status, node_attrs):
        """Create disk for ex_create_multiple_nodes.

        :param  status: Dictionary for holding node/disk creation status.
                        (This dictionary is modified by this method)
        :type   status: ``dict``

        :param  node_attrs: Dictionary for holding node attribute information.
                            (size, image, location, ex_disk_type, etc.)
        :type   node_attrs: ``dict``
        """
        disk = None
        # Check for existing disk

        if node_attrs["use_existing_disk"]:
            try:
                disk = self.ex_get_volume(status["name"], node_attrs["location"])
            except ResourceNotFoundError:
                pass

        if disk:
            status["disk"] = disk
        else:
            # Create disk and return response object back in the status dict.
            # Or, if there is an error, mark as failed.
            disk_req, disk_data, disk_params = self._create_vol_req(
                None,
                status["name"],
                location=node_attrs["location"],
                image=node_attrs["image"],
                ex_disk_type=node_attrs["ex_disk_type"],
            )
            try:
                disk_res = self.connection.request(
                    disk_req, method="POST", data=disk_data, params=disk_params
                ).object
            except GoogleBaseError:
                e = self._catch_error(ignore_errors=node_attrs["ignore_errors"])
                error = e.value
                code = e.code
                disk_res = None
                status["disk"] = GCEFailedDisk(status["name"], error, code)
            status["disk_response"] = disk_res

    def _multi_check_disk(self, status, node_attrs):
        """Check disk status for ex_create_multiple_nodes.

        :param  status: Dictionary for holding node/disk creation status.
                        (This dictionary is modified by this method)
        :type   status: ``dict``

        :param  node_attrs: Dictionary for holding node attribute information.
                            (size, image, location, etc.)
        :type   node_attrs: ``dict``
        """
        error = None
        code = None
        try:
            response = self.connection.request(status["disk_response"]["selfLink"]).object
        except GoogleBaseError:
            e = self._catch_error(ignore_errors=node_attrs["ignore_errors"])
            error = e.value
            code = e.code
            response = {"status": "DONE"}

        if response["status"] == "DONE":
            status["disk_response"] = None

            if error:
                status["disk"] = GCEFailedDisk(status["name"], error, code)
            else:
                status["disk"] = self.ex_get_volume(status["name"], node_attrs["location"])

    def _multi_create_node(self, status, node_attrs):
        """Create node for ex_create_multiple_nodes.

        :param  status: Dictionary for holding node creation status.
                        (This dictionary is modified by this method)
        :type   status: ``dict``

        :param  node_attrs: Dictionary for holding node attribute information.
                            (size, image, location, etc.)
        :type   node_attrs: ``dict``
        """

        # Create node and return response object in status dictionary.
        # Or, if there is an error, mark as failed.
        request, node_data = self._create_node_req(
            status["name"],
            node_attrs["size"],
            node_attrs["image"],
            node_attrs["location"],
            node_attrs["network"],
            node_attrs["tags"],
            node_attrs["metadata"],
            external_ip=node_attrs["external_ip"],
            internal_ip=node_attrs["internal_ip"],
            ex_service_accounts=node_attrs["ex_service_accounts"],
            description=node_attrs["description"],
            ex_can_ip_forward=node_attrs["ex_can_ip_forward"],
            ex_disk_auto_delete=node_attrs["ex_disk_auto_delete"],
            ex_disks_gce_struct=node_attrs["ex_disks_gce_struct"],
            ex_nic_gce_struct=node_attrs["ex_nic_gce_struct"],
            ex_on_host_maintenance=node_attrs["ex_on_host_maintenance"],
            ex_automatic_restart=node_attrs["ex_automatic_restart"],
            ex_subnetwork=node_attrs["subnetwork"],
            ex_preemptible=node_attrs["ex_preemptible"],
            ex_labels=node_attrs["ex_labels"],
            ex_disk_size=node_attrs["ex_disk_size"],
        )

        try:
            node_res = self.connection.request(request, method="POST", data=node_data).object
        except GoogleBaseError:
            e = self._catch_error(ignore_errors=node_attrs["ignore_errors"])
            error = e.value
            code = e.code
            node_res = None
            status["node"] = GCEFailedNode(status["name"], error, code)
        status["node_response"] = node_res

    def _multi_check_node(self, status, node_attrs):
        """Check node status for ex_create_multiple_nodes.

        :param  status: Dictionary for holding node/disk creation status.
                        (This dictionary is modified by this method)
        :type   status: ``dict``

        :param  node_attrs: Dictionary for holding node attribute information.
                            (size, image, location, etc.)
        :type   node_attrs: ``dict``
        """
        error = None
        code = None
        try:
            response = self.connection.request(status["node_response"]["selfLink"]).object
        except GoogleBaseError:
            e = self._catch_error(ignore_errors=node_attrs["ignore_errors"])
            error = e.value
            code = e.code
            response = {"status": "DONE"}

        if response["status"] == "DONE":
            status["node_response"] = None

            if error:
                status["node"] = GCEFailedNode(status["name"], error, code)
            else:
                status["node"] = self.ex_get_node(status["name"], node_attrs["location"])

    def _create_vol_req(
        self,
        size,
        name,
        location=None,
        snapshot=None,
        image=None,
        ex_disk_type="pd-standard",
    ):
        """
        Assemble the request/data for creating a volume.

        Used by create_volume and ex_create_multiple_nodes

        :param  size: Size of volume to create (in GB). Can be None if image
                      or snapshot is supplied.
        :type   size: ``int`` or ``str`` or ``None``

        :param  name: Name of volume to create
        :type   name: ``str``

        :keyword  location: Location (zone) to create the volume in
        :type     location: ``str`` or :class:`GCEZone` or
                            :class:`NodeLocation` or ``None``

        :keyword  snapshot: Snapshot to create image from
        :type     snapshot: :class:`GCESnapshot` or ``str`` or ``None``

        :keyword  image: Image to create disk from.
        :type     image: :class:`GCENodeImage` or ``str`` or ``None``

        :keyword  ex_disk_type: Specify pd-standard (default) or pd-ssd
        :type     ex_disk_type: ``str`` or :class:`GCEDiskType`

        :return:  Tuple containing the request string, the data dictionary and
                  the URL parameters
        :rtype:   ``tuple``
        """
        volume_data = {}
        params = None
        volume_data["name"] = name

        if size:
            volume_data["sizeGb"] = str(size)

        if image:
            if not hasattr(image, "name"):
                image = self.ex_get_image(image)
            params = {"sourceImage": image.extra["selfLink"]}
            volume_data["description"] = "Image: %s" % (image.extra["selfLink"])

        if snapshot:
            if not hasattr(snapshot, "name"):
                # Check for full URI to not break backward-compatibility

                if snapshot.startswith("https"):
                    snapshot = self._get_components_from_path(snapshot)["name"]
                snapshot = self.ex_get_snapshot(snapshot)
            snapshot_link = snapshot.extra["selfLink"]
            volume_data["sourceSnapshot"] = snapshot_link
            volume_data["description"] = "Snapshot: %s" % (snapshot_link)
        location = location or self.zone

        if not hasattr(location, "name"):
            location = self.ex_get_zone(location)

        if hasattr(ex_disk_type, "name"):
            # pylint: disable=no-member
            volume_data["type"] = ex_disk_type.extra["selfLink"]
        elif ex_disk_type.startswith("https"):
            volume_data["type"] = ex_disk_type
        else:
            volume_data["type"] = "https://www.googleapis.com/compute/"
            volume_data["type"] += "{}/projects/{}/zones/{}/diskTypes/{}".format(
                API_VERSION,
                self.project,
                location.name,
                ex_disk_type,
            )
        request = "/zones/%s/disks" % (location.name)

        return request, volume_data, params

    def _to_disktype(self, disktype):
        """
        Return a DiskType object from the JSON-response dictionary.

        :param  disktype: The dictionary describing the disktype.
        :type   disktype: ``dict``

        :return: DiskType object
        :rtype: :class:`GCEDiskType`
        """
        extra = {}

        zone = self.ex_get_zone(disktype["zone"])

        extra["selfLink"] = disktype.get("selfLink")
        extra["creationTimestamp"] = disktype.get("creationTimestamp")
        extra["description"] = disktype.get("description")
        extra["valid_disk_size"] = disktype.get("validDiskSize")
        extra["default_disk_size_gb"] = disktype.get("defaultDiskSizeGb")
        type_id = "{}:{}".format(zone.name, disktype["name"])

        return GCEDiskType(id=type_id, name=disktype["name"], zone=zone, driver=self, extra=extra)

    def _to_accelerator_type(self, accelerator_type):
        """
        Return an AcceleratorType object from the JSON-response dictionary.

        :param  accelerator_type: The dictionary describing the
                                  accelerator_type.
        :type   accelerator_type: ``dict``

        :return: AcceleratorType object
        :rtype:  :class:`GCEAcceleratorType`
        """
        extra = {}

        zone = self.ex_get_zone(accelerator_type["zone"])

        extra["selfLink"] = accelerator_type.get("selfLink")
        extra["creationTimestamp"] = accelerator_type.get("creationTimestamp")
        extra["description"] = accelerator_type.get("description")
        extra["maximumCardsPerInstance"] = accelerator_type.get("maximumCardsPerInstance")
        extra["default_disk_size_gb"] = accelerator_type.get("defaultDiskSizeGb")
        type_id = "{}:{}".format(zone.name, accelerator_type["name"])

        return GCEAcceleratorType(
            id=type_id,
            name=accelerator_type["name"],
            zone=zone,
            driver=self,
            extra=extra,
        )

    def _to_address(self, address):
        """
        Return an Address object from the JSON-response dictionary.

        :param  address: The dictionary describing the address.
        :type   address: ``dict``

        :return: Address object
        :rtype: :class:`GCEAddress`
        """
        extra = {}

        if "region" in address:
            region = self.ex_get_region(address["region"])
        else:
            region = "global"

        extra["selfLink"] = address.get("selfLink")
        extra["status"] = address.get("status")
        extra["description"] = address.get("description", None)

        if address.get("users", None) is not None:
            extra["users"] = address.get("users")
        extra["creationTimestamp"] = address.get("creationTimestamp")

        return GCEAddress(
            id=address["id"],
            name=address["name"],
            address=address["address"],
            region=region,
            driver=self,
            extra=extra,
        )

    def _to_backendservice(self, backendservice):
        """
        Return a Backend Service object from the JSON-response dictionary.

        :param  backendservice: The dictionary describing the backend service.
        :type   backendservice: ``dict``

        :return: BackendService object
        :rtype: :class:`GCEBackendService`
        """
        extra = {}

        for extra_key in (
            "selfLink",
            "creationTimestamp",
            "fingerprint",
            "description",
        ):
            extra[extra_key] = backendservice.get(extra_key)

        backends = backendservice.get("backends", [])
        healthchecks = [self._get_object_by_kind(h) for h in backendservice.get("healthChecks", [])]

        return GCEBackendService(
            id=backendservice["id"],
            name=backendservice["name"],
            backends=backends,
            healthchecks=healthchecks,
            port=backendservice["port"],
            port_name=backendservice["portName"],
            protocol=backendservice["protocol"],
            timeout=backendservice["timeoutSec"],
            driver=self,
            extra=extra,
        )

    def _to_healthcheck(self, healthcheck):
        """
        Return a HealthCheck object from the JSON-response dictionary.

        :param  healthcheck: The dictionary describing the healthcheck.
        :type   healthcheck: ``dict``

        :return: HealthCheck object
        :rtype: :class:`GCEHealthCheck`
        """
        extra = {}
        extra["selfLink"] = healthcheck.get("selfLink")
        extra["creationTimestamp"] = healthcheck.get("creationTimestamp")
        extra["description"] = healthcheck.get("description")
        extra["host"] = healthcheck.get("host")

        return GCEHealthCheck(
            id=healthcheck["id"],
            name=healthcheck["name"],
            path=healthcheck.get("requestPath"),
            port=healthcheck.get("port"),
            interval=healthcheck.get("checkIntervalSec"),
            timeout=healthcheck.get("timeoutSec"),
            unhealthy_threshold=healthcheck.get("unhealthyThreshold"),
            healthy_threshold=healthcheck.get("healthyThreshold"),
            driver=self,
            extra=extra,
        )

    def _to_firewall(self, firewall):
        """
        Return a Firewall object from the JSON-response dictionary.

        :param  firewall: The dictionary describing the firewall.
        :type   firewall: ``dict``

        :return: Firewall object
        :rtype: :class:`GCEFirewall`
        """
        extra = {}
        extra["selfLink"] = firewall.get("selfLink")
        extra["creationTimestamp"] = firewall.get("creationTimestamp")
        extra["description"] = firewall.get("description")
        extra["network_name"] = self._get_components_from_path(firewall["network"])["name"]

        network = self.ex_get_network(extra["network_name"])

        allowed = firewall.get("allowed")
        denied = firewall.get("denied")
        priority = firewall.get("priority")
        direction = firewall.get("direction")
        source_ranges = firewall.get("sourceRanges")
        source_tags = firewall.get("sourceTags")
        source_service_accounts = firewall.get("sourceServiceAccounts")
        target_tags = firewall.get("targetTags")
        target_service_accounts = firewall.get("targetServiceAccounts")
        target_ranges = firewall.get("targetRanges")

        return GCEFirewall(
            id=firewall["id"],
            name=firewall["name"],
            allowed=allowed,
            denied=denied,
            network=network,
            target_ranges=target_ranges,
            source_ranges=source_ranges,
            priority=priority,
            source_tags=source_tags,
            target_tags=target_tags,
            source_service_accounts=source_service_accounts,
            target_service_accounts=target_service_accounts,
            direction=direction,
            driver=self,
            extra=extra,
        )

    def _to_forwarding_rule(self, forwarding_rule):
        """
        Return a Forwarding Rule object from the JSON-response dictionary.

        :param  forwarding_rule: The dictionary describing the rule.
        :type   forwarding_rule: ``dict``

        :return: ForwardingRule object
        :rtype: :class:`GCEForwardingRule`
        """
        extra = {}
        extra["selfLink"] = forwarding_rule.get("selfLink")
        extra["portRange"] = forwarding_rule.get("portRange")
        extra["creationTimestamp"] = forwarding_rule.get("creationTimestamp")
        extra["description"] = forwarding_rule.get("description")

        region = forwarding_rule.get("region")

        if region:
            region = self.ex_get_region(region)
        target = self._get_object_by_kind(forwarding_rule["target"])

        return GCEForwardingRule(
            id=forwarding_rule["id"],
            name=forwarding_rule["name"],
            region=region,
            address=forwarding_rule.get("IPAddress"),
            protocol=forwarding_rule.get("IPProtocol"),
            targetpool=target,
            driver=self,
            extra=extra,
        )

    def _to_sslcertificate(self, sslcertificate):
        """
        Return the SslCertificate object from the JSON-response.

        :param  sslcertificate:  Dictionary describing SslCertificate
        :type   sslcertificate: ``dict``

        :return:  Return SslCertificate object.
        :rtype: :class:`GCESslCertificate`
        """
        extra = {}

        if "description" in sslcertificate:
            extra["description"] = sslcertificate["description"]
        extra["selfLink"] = sslcertificate["selfLink"]

        return GCESslCertificate(
            id=sslcertificate["id"],
            name=sslcertificate["name"],
            certificate=sslcertificate["certificate"],
            driver=self,
            extra=extra,
        )

    def _to_subnetwork(self, subnetwork):
        """
        Return a Subnetwork object from the JSON-response dictionary.

        :param  subnetwork: The dictionary describing the subnetwork.
        :type   subnetwork: ``dict``

        :return: Subnetwork object
        :rtype: :class:`GCESubnetwork`
        """
        extra = {}

        extra["creationTimestamp"] = subnetwork.get("creationTimestamp")
        extra["description"] = subnetwork.get("description")
        extra["gatewayAddress"] = subnetwork.get("gatewayAddress")
        extra["ipCidrRange"] = subnetwork.get("ipCidrRange")
        extra["network"] = subnetwork.get("network")
        extra["region"] = subnetwork.get("region")
        extra["selfLink"] = subnetwork.get("selfLink")
        extra["privateIpGoogleAccess"] = subnetwork.get("privateIpGoogleAccess")
        extra["secondaryIpRanges"] = subnetwork.get("secondaryIpRanges")
        network = self._get_object_by_kind(subnetwork.get("network"))
        region = self._get_object_by_kind(subnetwork.get("region"))

        return GCESubnetwork(
            id=subnetwork["id"],
            name=subnetwork["name"],
            cidr=subnetwork.get("ipCidrRange"),
            network=network,
            region=region,
            driver=self,
            extra=extra,
        )

    def _to_network(self, network):
        """
        Return a Network object from the JSON-response dictionary.

        :param  network: The dictionary describing the network.
        :type   network: ``dict``

        :return: Network object
        :rtype: :class:`GCENetwork`
        """
        extra = {}

        extra["selfLink"] = network.get("selfLink")
        extra["description"] = network.get("description")
        extra["creationTimestamp"] = network.get("creationTimestamp")
        # 'legacy'
        extra["gatewayIPv4"] = network.get("gatewayIPv4")
        extra["IPv4Range"] = network.get("IPv4Range")
        # 'auto' or 'custom'
        extra["autoCreateSubnetworks"] = network.get("autoCreateSubnetworks")
        extra["subnetworks"] = network.get("subnetworks")
        extra["routingConfig"] = network.get("routingConfig")

        # match Cloud SDK 'gcloud'

        if "autoCreateSubnetworks" in network:
            if network["autoCreateSubnetworks"]:
                extra["mode"] = "auto"
            else:
                extra["mode"] = "custom"
        else:
            extra["mode"] = "legacy"

        return GCENetwork(
            id=network["id"],
            name=network["name"],
            cidr=network.get("IPv4Range"),
            driver=self,
            extra=extra,
        )

    def _to_route(self, route):
        """
        Return a Route object from the JSON-response dictionary.

        :param  route: The dictionary describing the route.
        :type   route: ``dict``

        :return: Route object
        :rtype: :class:`GCERoute`
        """
        extra = {}

        extra["selfLink"] = route.get("selfLink")
        extra["description"] = route.get("description")
        extra["creationTimestamp"] = route.get("creationTimestamp")
        network = route.get("network")
        priority = route.get("priority")

        if "nextHopInstance" in route:
            extra["nextHopInstance"] = route["nextHopInstance"]

        if "nextHopIp" in route:
            extra["nextHopIp"] = route["nextHopIp"]

        if "nextHopNetwork" in route:
            extra["nextHopNetwork"] = route["nextHopNetwork"]

        if "nextHopGateway" in route:
            extra["nextHopGateway"] = route["nextHopGateway"]

        if "warnings" in route:
            extra["warnings"] = route["warnings"]

        return GCERoute(
            id=route["id"],
            name=route["name"],
            dest_range=route.get("destRange"),
            priority=priority,
            network=network,
            tags=route.get("tags"),
            driver=self,
            extra=extra,
        )

    def _to_node_image(self, image):
        """
        Return an Image object from the JSON-response dictionary.

        :param  image: The dictionary describing the image.
        :type   image: ``dict``

        :return: Image object
        :rtype: :class:`GCENodeImage`
        """
        extra = {}

        if "preferredKernel" in image:
            extra["preferredKernel"] = image.get("preferredKernel", None)
        extra["description"] = image.get("description", None)
        extra["family"] = image.get("family", None)
        extra["creationTimestamp"] = image.get("creationTimestamp")
        extra["selfLink"] = image.get("selfLink")

        if "deprecated" in image:
            extra["deprecated"] = image.get("deprecated", None)
        extra["sourceType"] = image.get("sourceType", None)
        extra["rawDisk"] = image.get("rawDisk", None)
        extra["status"] = image.get("status", None)
        extra["archiveSizeBytes"] = image.get("archiveSizeBytes", None)
        extra["diskSizeGb"] = image.get("diskSizeGb", None)

        if "guestOsFeatures" in image:
            extra["guestOsFeatures"] = image.get("guestOsFeatures", [])

        if "sourceDisk" in image:
            extra["sourceDisk"] = image.get("sourceDisk", None)

        if "sourceDiskId" in image:
            extra["sourceDiskId"] = image.get("sourceDiskId", None)

        if "licenses" in image:
            lic_objs = self._licenses_from_urls(licenses=image["licenses"])
            extra["licenses"] = lic_objs
        extra["labels"] = image.get("labels", None)
        extra["labelFingerprint"] = image.get("labelFingerprint", None)

        return GCENodeImage(id=image["id"], name=image["name"], driver=self, extra=extra)

    def _to_node_location(self, location):
        """
        Return a Location object from the JSON-response dictionary.

        :param  location: The dictionary describing the location.
        :type   location: ``dict``

        :return: Location object
        :rtype: :class:`NodeLocation`
        """

        return NodeLocation(
            id=location["id"],
            name=location["name"],
            country=location["name"].split("-")[0],
            driver=self,
        )

    def _to_node(self, node, use_disk_cache=False):
        """
        Return a Node object from the JSON-response dictionary.

        :param    node: The dictionary describing the node.
        :type     node: ``dict``

        :keyword  use_disk_cache: If true, ex_get_volume call will use cache.
        :type     use_disk_cache: ``bool``

        :return:  Node object
        :rtype:   :class:`Node`
        """
        public_ips = []
        private_ips = []
        extra = {}

        extra["status"] = node.get("status", "UNKNOWN")
        extra["statusMessage"] = node.get("statusMessage")
        extra["description"] = node.get("description")
        extra["zone"] = self.ex_get_zone(node["zone"])
        extra["image"] = node.get("image")
        extra["machineType"] = node.get("machineType")
        extra["cpuPlatform"] = node.get("cpuPlatform")
        extra["minCpuPlatform"] = node.get("minCpuPlatform")
        extra["disks"] = node.get("disks", [])
        extra["networkInterfaces"] = node.get("networkInterfaces")
        extra["id"] = node["id"]
        extra["selfLink"] = node.get("selfLink")
        extra["kind"] = node.get("kind")
        extra["creationTimestamp"] = node.get("creationTimestamp")
        extra["name"] = node["name"]
        extra["metadata"] = node.get("metadata", {})
        extra["tags_fingerprint"] = node["tags"]["fingerprint"]
        extra["scheduling"] = node.get("scheduling", {})
        extra["deprecated"] = True if node.get("deprecated", None) else False
        extra["canIpForward"] = node.get("canIpForward")
        extra["serviceAccounts"] = node.get("serviceAccounts", [])
        extra["scheduling"] = node.get("scheduling", {})
        extra["boot_disk"] = None
        extra["labels"] = node.get("labels")
        extra["labelFingerprint"] = node.get("labelFingerprint")

        for disk in extra["disks"]:
            if disk.get("boot") and disk.get("type") == "PERSISTENT":
                bd = self._get_components_from_path(disk["source"])
                extra["boot_disk"] = self.ex_get_volume(
                    bd["name"], bd["zone"], use_cache=use_disk_cache
                )

        if "items" in node["tags"]:
            tags = node["tags"]["items"]
        else:
            tags = []
        extra["tags"] = tags

        for network_interface in node.get("networkInterfaces", []):
            private_ips.append(network_interface.get("networkIP"))

            for access_config in network_interface.get("accessConfigs", []):
                public_ips.append(access_config.get("natIP"))

        # For the node attributes, use just machine and image names, not full
        # paths.  Full paths are available in the "extra" dict.
        image = None

        if extra["image"]:
            image = self._get_components_from_path(extra["image"])["name"]
        else:
            if (
                extra["boot_disk"]
                and hasattr(extra["boot_disk"], "extra")
                and "sourceImage" in extra["boot_disk"].extra
                and extra["boot_disk"].extra["sourceImage"] is not None
            ):
                src_image = extra["boot_disk"].extra["sourceImage"]
                image = self._get_components_from_path(src_image)["name"]
            extra["image"] = image
        size = self._get_components_from_path(node["machineType"])["name"]

        state = self.NODE_STATE_MAP.get(node["status"], NodeState.UNKNOWN)

        return Node(
            id=node["id"],
            name=node["name"],
            state=state,
            public_ips=public_ips,
            private_ips=private_ips,
            driver=self,
            size=size,
            image=image,
            extra=extra,
        )

    def _to_node_size(self, machine_type, instance_prices):
        """
        Return a Size object from the JSON-response dictionary.

        :param  machine_type: The dictionary describing the machine.
        :type   machine_type: ``dict``

        :return: Size object
        :rtype: :class:`GCENodeSize`
        """
        extra = {}
        extra["selfLink"] = machine_type.get("selfLink")
        extra["zone"] = self.ex_get_zone(machine_type["zone"])
        extra["description"] = machine_type.get("description")
        extra["guestCpus"] = machine_type.get("guestCpus")
        extra["creationTimestamp"] = machine_type.get("creationTimestamp")
        extra["accelerators"] = machine_type.get("accelerators", [])
        try:
            size_name = machine_type["name"][:2]
            location = extra["zone"].name
            location = "-".join(location.split("-")[:2])
            machine_ram = float(machine_type.get("memoryMb", 0)) / 1024
            machine_cpus = float(extra["guestCpus"])
            cpu_price = instance_prices[size_name]["cpu"]["on_demand"][location]["price"]
            ram_price = instance_prices[size_name]["ram"]["on_demand"][location]["price"]
            price = machine_cpus * cpu_price + machine_ram * ram_price
        except KeyError:
            price = None
        except AttributeError:  # no zone
            price = None

        return GCENodeSize(
            id=machine_type["id"],
            name=machine_type["name"],
            ram=machine_type.get("memoryMb"),
            disk=machine_type.get("imageSpaceGb"),
            bandwidth=0,
            price=price,
            driver=self,
            extra=extra,
        )

    def _to_project(self, project):
        """
        Return a Project object from the JSON-response dictionary.

        :param  project: The dictionary describing the project.
        :type   project: ``dict``

        :return: Project object
        :rtype: :class:`GCEProject`
        """
        extra = {}
        extra["selfLink"] = project.get("selfLink")
        extra["creationTimestamp"] = project.get("creationTimestamp")
        extra["description"] = project.get("description")
        metadata = project["commonInstanceMetadata"].get("items")

        if "commonInstanceMetadata" in project:
            # add this struct to get 'fingerprint' too
            extra["commonInstanceMetadata"] = project["commonInstanceMetadata"]

        if "usageExportLocation" in project:
            extra["usageExportLocation"] = project["usageExportLocation"]

        return GCEProject(
            id=project["id"],
            name=project["name"],
            metadata=metadata,
            quotas=project.get("quotas"),
            driver=self,
            extra=extra,
        )

    def _to_region(self, region):
        """
        Return a Region object from the JSON-response dictionary.

        :param  region: The dictionary describing the region.
        :type   region: ``dict``

        :return: Region object
        :rtype: :class:`GCERegion`
        """
        extra = {}
        extra["selfLink"] = region.get("selfLink")
        extra["creationTimestamp"] = region.get("creationTimestamp")
        extra["description"] = region.get("description")

        quotas = region.get("quotas")
        zones = [self.ex_get_zone(z) for z in region.get("zones", [])]
        # Work around a bug that will occasionally list missing zones in the
        # region output
        zones = [z for z in zones if z is not None]
        deprecated = region.get("deprecated")

        return GCERegion(
            id=region["id"],
            name=region["name"],
            status=region.get("status"),
            zones=zones,
            quotas=quotas,
            deprecated=deprecated,
            driver=self,
            extra=extra,
        )

    def _to_snapshot(self, snapshot):
        """
        Return a Snapshot object from the JSON-response dictionary.

        :param  snapshot: The dictionary describing the snapshot
        :type   snapshot: ``dict``

        :return:  Snapshot object
        :rtype:   :class:`VolumeSnapshot`
        """
        extra = {}
        extra["selfLink"] = snapshot.get("selfLink")
        extra["creationTimestamp"] = snapshot.get("creationTimestamp")
        extra["sourceDisk"] = snapshot.get("sourceDisk")

        if "description" in snapshot:
            extra["description"] = snapshot["description"]

        if "sourceDiskId" in snapshot:
            extra["sourceDiskId"] = snapshot["sourceDiskId"]

        if "storageBytes" in snapshot:
            extra["storageBytes"] = snapshot["storageBytes"]

        if "storageBytesStatus" in snapshot:
            extra["storageBytesStatus"] = snapshot["storageBytesStatus"]

        if "licenses" in snapshot:
            lic_objs = self._licenses_from_urls(licenses=snapshot["licenses"])
            extra["licenses"] = lic_objs

        try:
            created = parse_date(snapshot.get("creationTimestamp"))
        except ValueError:
            created = None

        return GCESnapshot(
            id=snapshot["id"],
            name=snapshot["name"],
            size=snapshot["diskSizeGb"],
            status=snapshot.get("status"),
            driver=self,
            extra=extra,
            created=created,
        )

    def _to_storage_volume(self, volume):
        """
        Return a Volume object from the JSON-response dictionary.

        :param  volume: The dictionary describing the volume.
        :type   volume: ``dict``

        :return: Volume object
        :rtype: :class:`StorageVolume`
        """
        extra = {}
        extra["selfLink"] = volume.get("selfLink")
        extra["zone"] = self.ex_get_zone(volume["zone"])
        extra["status"] = volume.get("status")
        extra["creationTimestamp"] = volume.get("creationTimestamp")
        extra["description"] = volume.get("description")
        extra["sourceImage"] = volume.get("sourceImage")
        extra["sourceImageId"] = volume.get("sourceImageId")
        extra["sourceSnapshot"] = volume.get("sourceSnapshot")
        extra["sourceSnapshotId"] = volume.get("sourceSnapshotId")
        extra["options"] = volume.get("options")
        extra["labels"] = volume.get("labels", {})
        extra["labelFingerprint"] = volume.get("labelFingerprint")
        extra["users"] = volume.get("users", [])

        if "licenses" in volume:
            lic_objs = self._licenses_from_urls(licenses=volume["licenses"])
            extra["licenses"] = lic_objs

        extra["type"] = volume.get("type", "pd-standard").split("/")[-1]

        return StorageVolume(
            id=volume["id"],
            name=volume["name"],
            size=volume["sizeGb"],
            driver=self,
            extra=extra,
        )

    def _to_targethttpproxy(self, targethttpproxy):
        """
        Return a Target HTTP Proxy object from the JSON-response dictionary.

        :param  targethttpproxy: The dictionary describing the proxy.
        :type   targethttpproxy: ``dict``

        :return: Target HTTP Proxy object
        :rtype:  :class:`GCETargetHttpProxy`
        """
        extra = {
            k: targethttpproxy.get(k) for k in ("creationTimestamp", "description", "selfLink")
        }

        urlmap = self._get_object_by_kind(targethttpproxy.get("urlMap"))

        return GCETargetHttpProxy(
            id=targethttpproxy["id"],
            name=targethttpproxy["name"],
            urlmap=urlmap,
            driver=self,
            extra=extra,
        )

    def _to_targethttpsproxy(self, targethttpsproxy):
        """
        Return the TargetHttpsProxy object from the JSON-response.

        :param  targethttpsproxy:  Dictionary describing TargetHttpsProxy
        :type   targethttpsproxy: ``dict``

        :return:  Return TargetHttpsProxy object.
        :rtype: :class:`GCETargetHttpsProxy`
        """
        extra = {}

        if "description" in targethttpsproxy:
            extra["description"] = targethttpsproxy["description"]
        extra["selfLink"] = targethttpsproxy["selfLink"]

        sslcertificates = [
            self._get_object_by_kind(x) for x in targethttpsproxy.get("sslCertificates", [])
        ]
        obj_name = self._get_components_from_path(targethttpsproxy["urlMap"])["name"]
        urlmap = self.ex_get_urlmap(obj_name)

        return GCETargetHttpsProxy(
            id=targethttpsproxy["id"],
            name=targethttpsproxy["name"],
            sslcertificates=sslcertificates,
            urlmap=urlmap,
            driver=self,
            extra=extra,
        )

    def _to_targetinstance(self, targetinstance):
        """
        Return a Target Instance object from the JSON-response dictionary.

        :param  targetinstance: The dictionary describing the target instance.
        :type   targetinstance: ``dict``

        :return: Target Instance object
        :rtype:  :class:`GCETargetInstance`
        """
        node = None
        extra = {}
        extra["selfLink"] = targetinstance.get("selfLink")
        extra["description"] = targetinstance.get("description")
        extra["natPolicy"] = targetinstance.get("natPolicy")
        zone = self.ex_get_zone(targetinstance["zone"])

        if "instance" in targetinstance:
            node_name = targetinstance["instance"].split("/")[-1]
            try:
                node = self.ex_get_node(node_name, zone)
            except ResourceNotFoundError:
                node = targetinstance["instance"]

        return GCETargetInstance(
            id=targetinstance["id"],
            name=targetinstance["name"],
            zone=zone,
            node=node,
            driver=self,
            extra=extra,
        )

    def _to_targetpool(self, targetpool):
        """
        Return a Target Pool object from the JSON-response dictionary.

        :param  targetpool: The dictionary describing the volume.
        :type   targetpool: ``dict``

        :return: Target Pool object
        :rtype:  :class:`GCETargetPool`
        """
        extra = {}
        extra["selfLink"] = targetpool.get("selfLink")
        extra["description"] = targetpool.get("description")
        extra["sessionAffinity"] = targetpool.get("sessionAffinity")
        region = self.ex_get_region(targetpool["region"])
        healthcheck_list = [
            self.ex_get_healthcheck(h.split("/")[-1]) for h in targetpool.get("healthChecks", [])
        ]
        node_list = []

        for n in targetpool.get("instances", []):
            # Nodes that do not exist can be part of a target pool.  If the
            # node does not exist, use the URL of the node instead of the node
            # object.
            comp = self._get_components_from_path(n)
            try:
                node = self.ex_get_node(comp["name"], comp["zone"])
            except ResourceNotFoundError:
                node = n
            node_list.append(node)

        if "failoverRatio" in targetpool:
            extra["failoverRatio"] = targetpool["failoverRatio"]

        if "backupPool" in targetpool:
            tp_split = targetpool["backupPool"].split("/")
            extra["backupPool"] = self.ex_get_targetpool(tp_split[10], tp_split[8])

        return GCETargetPool(
            id=targetpool["id"],
            name=targetpool["name"],
            region=region,
            healthchecks=healthcheck_list,
            nodes=node_list,
            driver=self,
            extra=extra,
        )

    def _to_instancegroup(self, instancegroup):
        """
        Return the InstanceGroup object from the JSON-response.

        :param  instancegroup:  Dictionary describing InstanceGroup
        :type   instancegroup: ``dict``

        :return: InstanceGroup object.
        :rtype: :class:`GCEInstanceGroup`
        """
        extra = {}
        extra["description"] = instancegroup.get("description", None)
        extra["selfLink"] = instancegroup["selfLink"]
        extra["namedPorts"] = instancegroup.get("namedPorts", [])
        extra["fingerprint"] = instancegroup.get("fingerprint", None)

        zone = instancegroup.get("zone", None)

        if zone:
            # Apparently zone attribute is not always present, see
            # https://github.com/apache/libcloud/issues/1346 for details
            zone = self.ex_get_zone(zone)

        # Note: network/subnetwork will not be available if the Instance Group
        # does not contain instances.
        network = instancegroup.get("network", None)

        if network:
            network = self.ex_get_network(network)

        subnetwork = instancegroup.get("subnetwork", None)

        if subnetwork:
            subnetwork = self.ex_get_subnetwork(subnetwork)

        return GCEInstanceGroup(
            id=instancegroup["id"],
            name=instancegroup["name"],
            zone=zone,
            network=network,
            subnetwork=subnetwork,
            named_ports=instancegroup.get("namedPorts", []),
            driver=self,
            extra=extra,
        )

    def _to_instancegroupmanager(self, manager):
        """
        Return a Instance Group Manager object from the JSON-response.

        :param  instancegroupmanager: dictionary describing the Instance
                                  Group Manager.
        :type   instancegroupmanager: ``dict``

        :return: Instance Group Manager object.
        :rtype:  :class:`GCEInstanceGroupManager`
        """
        zone = self.ex_get_zone(manager["zone"])

        extra = {}
        extra["selfLink"] = manager.get("selfLink")
        extra["description"] = manager.get("description")
        extra["currentActions"] = manager.get("currentActions")
        extra["baseInstanceName"] = manager.get("baseInstanceName")
        extra["namedPorts"] = manager.get("namedPorts", [])
        extra["autoHealingPolicies"] = manager.get("autoHealingPolicies", [])
        template_name = self._get_components_from_path(manager["instanceTemplate"])["name"]
        template = self.ex_get_instancetemplate(template_name)
        ig_name = self._get_components_from_path(manager["instanceGroup"])["name"]
        instance_group = self.ex_get_instancegroup(ig_name, zone)

        return GCEInstanceGroupManager(
            id=manager["id"],
            name=manager["name"],
            zone=zone,
            size=manager["targetSize"],
            instance_group=instance_group,
            template=template,
            driver=self,
            extra=extra,
        )

    def _to_instancetemplate(self, instancetemplate):
        """
        Return a Instance Template object from the JSON-response.

        :param  instancetemplate: dictionary describing the Instance
                                  Template.
        :type   instancetemplate: ``dict``

        :return: Instance Template object.
        :rtype:  :class:`GCEInstanceTemplate`
        """
        extra = {}
        extra["selfLink"] = instancetemplate.get("selfLink")
        extra["description"] = instancetemplate.get("description")
        extra["properties"] = instancetemplate.get("properties")

        return GCEInstanceTemplate(
            id=instancetemplate["id"],
            name=instancetemplate["name"],
            driver=self,
            extra=extra,
        )

    def _to_autoscaler(self, autoscaler):
        """
        Return an Autoscaler object from the JSON-response.

        :param  autoscaler: dictionary describing the Autoscaler.
        :type   autoscaler: ``dict``

        :return: Autoscaler object.
        :rtype:  :class:`GCEAutoscaler`
        """
        extra = {}
        extra["selfLink"] = autoscaler.get("selfLink")
        extra["description"] = autoscaler.get("description")
        zone = self.ex_get_zone(autoscaler.get("zone"))
        ig_name = self._get_components_from_path(autoscaler.get("target"))["name"]
        target = self.ex_get_instancegroupmanager(ig_name, zone)

        return GCEAutoscaler(
            id=autoscaler["id"],
            name=autoscaler["name"],
            zone=zone,
            target=target,
            policy=autoscaler["autoscalingPolicy"],
            driver=self,
            extra=extra,
        )

    def _format_guest_accelerators(self, accelerator_type, accelerator_count):
        """
        Formats a GCE-friendly guestAccelerators request. Accepts an
        accelerator_type and accelerator_count that is wrapped up into a list
        of dictionaries for GCE to consume for a node creation request.

        :param  accelerator_type: Accelerator type to request.
        :type   accelerator_type: :class:`GCEAcceleratorType`

        :param  accelerator_count: Number of accelerators to request.
        :type   accelerator_count: ``int``

        :return: GCE-friendly guestAccelerators list of dictionaries.
        :rtype:  ``list``
        """
        accelerator_type = self._get_selflink_or_name(
            obj=accelerator_type, get_selflinks=True, objname="accelerator_type"
        )

        return [{"acceleratorType": accelerator_type, "acceleratorCount": accelerator_count}]

    def _format_metadata(self, fingerprint, metadata=None):
        """
        Convert various data formats into the metadata format expected by
        Google Compute Engine and suitable for passing along to the API. Can
        accept the following formats:

          (a) [{'key': 'k1', 'value': 'v1'}, ...]
          (b) [{'k1': 'v1'}, ...]
          (c) {'key': 'k1', 'value': 'v1'}
          (d) {'k1': 'v1', 'k2': v2', ...}
          (e) {'items': [...]}       # does not check for valid list contents

        The return value is a 'dict' that GCE expects, e.g.

          {'fingerprint': 'xx...',
           'items': [{'key': 'key1', 'value': 'val1'},
                     {'key': 'key2', 'value': 'val2'},
                     ...,
                    ]
          }

        :param  fingerprint: Current metadata fingerprint
        :type   fingerprint: ``str``

        :param  metadata: Variety of input formats.
        :type   metadata: ``list``, ``dict``, or ``None``

        :return: GCE-friendly metadata dict
        :rtype:  ``dict``
        """

        if not metadata:
            return {"fingerprint": fingerprint, "items": []}
        md = {"fingerprint": fingerprint}

        # Check `list` format. Can support / convert the following:
        # (a) [{'key': 'k1', 'value': 'v1'}, ...]
        # (b) [{'k1': 'v1'}, ...]

        if isinstance(metadata, list):
            item_list = []

            for i in metadata:
                if isinstance(i, dict):
                    # check (a)

                    if "key" in i and "value" in i and len(i) == 2:
                        item_list.append(i)
                    # check (b)
                    elif len(i) == 1:
                        item_list.append({"key": list(i.keys())[0], "value": list(i.values())[0]})
                    else:
                        raise ValueError("Unsupported metadata format.")
                else:
                    raise ValueError("Unsupported metadata format.")
            md["items"] = item_list

        # Check `dict` format. Can support / convert the following:
        # (c) {'key': 'k1', 'value': 'v1'}
        # (d) {'k1': 'v1', 'k2': 'v2', ...}
        # (e) {'items': [...]}

        if isinstance(metadata, dict):
            # Check (c)

            if "key" in metadata and "value" in metadata and len(metadata) == 2:
                md["items"] = [metadata]
            # check (d)
            elif len(metadata) == 1:
                if "items" in metadata:
                    # check (e)

                    if isinstance(metadata["items"], list):
                        md["items"] = metadata["items"]
                    else:
                        raise ValueError("Unsupported metadata format.")
                else:
                    md["items"] = [
                        {
                            "key": list(metadata.keys())[0],
                            "value": list(metadata.values())[0],
                        }
                    ]
            else:
                # check (d)
                md["items"] = []

                for k, v in metadata.items():
                    md["items"].append({"key": k, "value": v})

        if "items" not in md:
            raise ValueError("Unsupported metadata format.")

        return md

    def _to_urlmap(self, urlmap):
        """
        Return a UrlMap object from the JSON-response dictionary.

        :param  zone: The dictionary describing the url-map.
        :type   zone: ``dict``

        :return: UrlMap object
        :rtype: :class:`GCEUrlMap`
        """
        extra = {
            k: urlmap.get(k)
            for k in ("creationTimestamp", "description", "fingerprint", "selfLink")
        }

        default_service = self._get_object_by_kind(urlmap.get("defaultService"))

        host_rules = urlmap.get("hostRules", [])
        path_matchers = urlmap.get("pathMatchers", [])
        tests = urlmap.get("tests", [])

        return GCEUrlMap(
            id=urlmap["id"],
            name=urlmap["name"],
            default_service=default_service,
            host_rules=host_rules,
            path_matchers=path_matchers,
            tests=tests,
            driver=self,
            extra=extra,
        )

    def _to_zone(self, zone):
        """
        Return a Zone object from the JSON-response dictionary.

        :param  zone: The dictionary describing the zone.
        :type   zone: ``dict``

        :return: Zone object
        :rtype: :class:`GCEZone`
        """
        extra = {}
        extra["selfLink"] = zone.get("selfLink")
        extra["creationTimestamp"] = zone.get("creationTimestamp")
        extra["description"] = zone.get("description")
        extra["region"] = zone.get("region")

        deprecated = zone.get("deprecated")

        return GCEZone(
            id=zone["id"],
            name=zone["name"],
            status=zone["status"],
            maintenance_windows=zone.get("maintenanceWindows"),
            deprecated=deprecated,
            driver=self,
            extra=extra,
        )

    def _set_project_metadata(self, metadata=None, force=False, current_keys=""):
        """
        Return the GCE-friendly dictionary of metadata with/without an
        entry for 'sshKeys' based on params for 'force' and 'current_keys'.
        This method was added to simplify the set_common_instance_metadata
        method and make it easier to test.

        :param  metadata: The GCE-formatted dict (e.g. 'items' list of dicts)
        :type   metadata: ``dict`` or ``None``

        :param  force: Flag to specify user preference for keeping current_keys
        :type   force: ``bool``

        :param  current_keys: The value, if any, of existing 'sshKeys'
        :type   current_keys: ``str``

        :return: GCE-friendly metadata dict
        :rtype:  ``dict``
        """

        if metadata is None:
            # User wants to delete metadata, but if 'force' is False
            # and we already have sshKeys, we should retain them.
            # Otherwise, delete ALL THE THINGS!

            if not force and current_keys:
                new_md = [{"key": "sshKeys", "value": current_keys}]
            else:
                new_md = []
        else:
            # User is providing new metadata. If 'force' is False, they
            # want to preserve existing sshKeys, otherwise 'force' is True
            # and the user wants to add/replace sshKeys.
            new_md = metadata["items"]

            if not force and current_keys:
                # not sure how duplicate keys would be resolved, so ensure
                # existing 'sshKeys' entry is removed.
                updated_md = []

                for d in new_md:
                    if d["key"] != "sshKeys":
                        updated_md.append({"key": d["key"], "value": d["value"]})
                new_md = updated_md
                new_md.append({"key": "sshKeys", "value": current_keys})

        return new_md

    def _licenses_from_urls(self, licenses):
        """
        Convert a list of license selfLinks into a list of :class:`GCELicense`
        objects.

        :param  licenses: A list of GCE license selfLink URLs.
        :type   licenses: ``list`` of ``str``

        :return: List of :class:`GCELicense` objects.
        :rtype:  ``list``
        """
        return_list = []

        for license in licenses:
            selfLink_parts = license.split("/")
            lic_proj = selfLink_parts[6]
            lic_name = selfLink_parts[-1]
            return_list.append(self.ex_get_license(project=lic_proj, name=lic_name))

        return return_list

    KIND_METHOD_MAP = {
        "compute#address": _to_address,
        "compute#backendService": _to_backendservice,
        "compute#disk": _to_storage_volume,
        "compute#firewall": _to_firewall,
        "compute#forwardingRule": _to_forwarding_rule,
        "compute#httpHealthCheck": _to_healthcheck,
        "compute#image": _to_node_image,
        "compute#instance": _to_node,
        "compute#machineType": _to_node_size,
        "compute#network": _to_network,
        "compute#project": _to_project,
        "compute#region": _to_region,
        "compute#snapshot": _to_snapshot,
        "compute#sslCertificate": _to_sslcertificate,
        "compute#targetHttpProxy": _to_targethttpproxy,
        "compute#targetHttpsProxy": _to_targethttpsproxy,
        "compute#targetInstance": _to_targetinstance,
        "compute#targetPool": _to_targetpool,
        "compute#urlMap": _to_urlmap,
        "compute#zone": _to_zone,
    }

    def _verify_zone_is_set(self, zone=None):
        """
        Verify that the zone / location is set for a particular operation -
        either via "datacenter" driver constructor argument or via
        "location" / "zone" keyword argument passed to the specific method
        call.

        This check is mandatory for methods which rely on the location being
        set - e.g. create_node.
        """

        if self.zone:
            return True

        if not zone:
            msg = (
                "Zone not provided. Zone needs to be specified for this"
                'operation. This can be done by passing "datacenter" '
                "argument to the driver constructor or by passing location "
                "/ zone argument to this method."
            )
            raise ValueError(msg)

        return True
