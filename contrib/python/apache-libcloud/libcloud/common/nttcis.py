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
NTTCIS Common Components
"""

import re
import xml.etree.ElementTree as etree
from io import BytesIO
from copy import deepcopy
from time import sleep
from base64 import b64encode
from typing import Dict
from functools import wraps

# TODO: use distutils.version when Travis CI fixed the pylint issue with version
# from distutils.version import LooseVersion
from libcloud.utils.py3 import b, httplib, basestring
from libcloud.utils.xml import findtext
from libcloud.common.base import RawResponse, XmlResponse, ConnectionUserAndKey
from libcloud.compute.base import Node
from libcloud.compute.types import LibcloudError, InvalidCredsError

try:
    from collections.abc import Mapping, MutableSequence
except ImportError:
    from collections.abc import Mapping, MutableSequence


# Roadmap / TODO:
#
# 1.0 - Copied from OpSource API, named provider details.

# setup a few variables to represent all of the NTTC-CIS cloud namespaces
NAMESPACE_BASE = "http://oec.api.opsource.net/schemas"
ORGANIZATION_NS = NAMESPACE_BASE + "/organization"
SERVER_NS = NAMESPACE_BASE + "/server"
NETWORK_NS = NAMESPACE_BASE + "/network"
DIRECTORY_NS = NAMESPACE_BASE + "/directory"
GENERAL_NS = NAMESPACE_BASE + "/general"
BACKUP_NS = NAMESPACE_BASE + "/backup"

# API 2.0 Namespaces and URNs
TYPES_URN = "urn:didata.com:api:cloud:types"

# API end-points
API_ENDPOINTS = {
    "na": {
        "name": "North America (NA)",
        "host": "api-na.dimensiondata.com",
        "vendor": "NTTC-CIS",
    },
    "eu": {
        "name": "Europe (EU)",
        "host": "api-eu.dimensiondata.com",
        "vendor": "NTTC-CIS",
    },
    "au": {
        "name": "Australia (AU)",
        "host": "api-au.dimensiondata.com",
        "vendor": "NTTC-CIS",
    },
    "au-gov": {
        "name": "Australia Canberra ACT (AU)",
        "host": "api-canberra.dimensiondata.com",
        "vendor": "NTTC-CIS",
    },
    "af": {
        "name": "Africa (AF)",
        "host": "api-mea.dimensiondata.com",
        "vendor": "NTTC-CIS",
    },
    "ap": {
        "name": "Asia Pacific (AP)",
        "host": "api-ap.dimensiondata.com",
        "vendor": "NTTC-CIS",
    },
    "ca": {
        "name": "Canada (CA)",
        "host": "api-canada.dimensiondata.com",
        "vendor": "NTTC-CIS",
    },
    "is-na": {
        "name": "North America (NA)",
        "host": "usapi.cloud.is.co.za",
        "vendor": "InternetSolutions",
    },
    "is-eu": {
        "name": "Europe (EU)",
        "host": "euapi.cloud.is.co.za",
        "vendor": "InternetSolutions",
    },
    "is-au": {
        "name": "Australia (AU)",
        "host": "auapi.cloud.is.co.za",
        "vendor": "InternetSolutions",
    },
    "is-af": {
        "name": "Africa (AF)",
        "host": "meaapi.cloud.is.co.za",
        "vendor": "InternetSolutions",
    },
    "is-ap": {
        "name": "Asia Pacific (AP)",
        "host": "apapi.cloud.is.co.za",
        "vendor": "InternetSolutions",
    },
    "is-latam": {
        "name": "South America (LATAM)",
        "host": "latamapi.cloud.is.co.za",
        "vendor": "InternetSolutions",
    },
    "is-canada": {
        "name": "Canada (CA)",
        "host": "canadaapi.cloud.is.co.za",
        "vendor": "InternetSolutions",
    },
    "ntta-na": {
        "name": "North America (NA)",
        "host": "cloudapi.nttamerica.com",
        "vendor": "NTTNorthAmerica",
    },
    "ntta-eu": {
        "name": "Europe (EU)",
        "host": "eucloudapi.nttamerica.com",
        "vendor": "NTTNorthAmerica",
    },
    "ntta-au": {
        "name": "Australia (AU)",
        "host": "aucloudapi.nttamerica.com",
        "vendor": "NTTNorthAmerica",
    },
    "ntta-af": {
        "name": "Africa (AF)",
        "host": "sacloudapi.nttamerica.com",
        "vendor": "NTTNorthAmerica",
    },
    "ntta-ap": {
        "name": "Asia Pacific (AP)",
        "host": "hkcloudapi.nttamerica.com",
        "vendor": "NTTNorthAmerica",
    },
    "cisco-na": {
        "name": "North America (NA)",
        "host": "iaas-api-na.cisco-ccs.com",
        "vendor": "Cisco",
    },
    "cisco-eu": {
        "name": "Europe (EU)",
        "host": "iaas-api-eu.cisco-ccs.com",
        "vendor": "Cisco",
    },
    "cisco-au": {
        "name": "Australia (AU)",
        "host": "iaas-api-au.cisco-ccs.com",
        "vendor": "Cisco",
    },
    "cisco-af": {
        "name": "Africa (AF)",
        "host": "iaas-api-mea.cisco-ccs.com",
        "vendor": "Cisco",
    },
    "cisco-ap": {
        "name": "Asia Pacific (AP)",
        "host": "iaas-api-ap.cisco-ccs.com",
        "vendor": "Cisco",
    },
    "cisco-latam": {
        "name": "South America (LATAM)",
        "host": "iaas-api-sa.cisco-ccs.com",
        "vendor": "Cisco",
    },
    "cisco-canada": {
        "name": "Canada (CA)",
        "host": "iaas-api-ca.cisco-ccs.com",
        "vendor": "Cisco",
    },
}

# Default API end-point for the base connection class.
DEFAULT_REGION = "na"

BAD_CODE_XML_ELEMENTS = (
    ("responseCode", SERVER_NS),
    ("responseCode", TYPES_URN),
    ("result", GENERAL_NS),
)

BAD_MESSAGE_XML_ELEMENTS = (
    ("message", SERVER_NS),
    ("message", TYPES_URN),
    ("resultDetail", GENERAL_NS),
)


def get_params(func):
    @wraps(func)
    def paramed(*args, **kwargs):
        if kwargs:
            params = {}
            for k, v in kwargs.items():
                matches = re.findall(r"_(\w)", k)
                for match in matches:
                    k = k.replace("_" + match, match.upper())
                params[k] = v
            result = func(args[0], params)
        else:
            result = func(args[0])
        return result

    return paramed


def dd_object_to_id(obj, obj_type, id_value="id"):
    """
    Takes in a DD object or string and prints out it's id
    This is a helper method, as many of our functions can take either an object
    or a string, and we need an easy way of converting them

    :param obj: The object to get the id for
    :type  obj: ``object``

    :param  func: The function to call, e.g. ex_get_vlan. Note: This
                  function needs to return an object which has ``status``
                  attribute.
    :type   func: ``function``

    :rtype: ``str``
    """
    if isinstance(obj, obj_type):
        return getattr(obj, id_value)
    elif isinstance(obj, (basestring)):
        return obj
    else:
        raise TypeError(
            "Invalid type {} looking for basestring or {}".format(
                type(obj).__name__, obj_type.__name__
            )
        )


# TODO: use distutils.version when Travis CI fixed the pylint issue with version
#       This is a temporary workaround.
def LooseVersion(version):
    return float(version)


class NetworkDomainServicePlan:
    ESSENTIALS = "ESSENTIALS"
    ADVANCED = "ADVANCED"


class NttCisRawResponse(RawResponse):
    pass


class NttCisResponse(XmlResponse):
    def parse_error(self):
        if self.status == httplib.UNAUTHORIZED:
            raise InvalidCredsError(self.body)
        elif self.status == httplib.FORBIDDEN:
            raise InvalidCredsError(self.body)

        body = self.parse_body()

        if self.status == httplib.BAD_REQUEST:
            for response_code in BAD_CODE_XML_ELEMENTS:
                code = findtext(body, response_code[0], response_code[1])
                if code is not None:
                    break
            for message in BAD_MESSAGE_XML_ELEMENTS:
                message = findtext(body, message[0], message[1])
                if message is not None:
                    break
            raise NttCisAPIException(code=code, msg=message, driver=self.connection.driver)
        if self.status is not httplib.OK:
            raise NttCisAPIException(code=self.status, msg=body, driver=self.connection.driver)

        return self.body


class NttCisAPIException(LibcloudError):
    def __init__(self, code, msg, driver):
        self.code = code
        self.msg = msg
        self.driver = driver

    def __str__(self):
        return "{}: {}".format(self.code, self.msg)

    def __repr__(self):
        return "<NttCisAPIException: code='{}', msg='{}'>".format(self.code, self.msg)


class NttCisConnection(ConnectionUserAndKey):
    """
    Connection class for the NttCis driver
    """

    api_path_version_1 = "/oec"
    api_path_version_2 = "/caas"
    api_version_1 = 0.9

    # Earliest version supported
    oldest_api_version = "2.2"

    # Latest version supported
    latest_api_version = "2.7"

    # Default api version
    active_api_version = "2.7"

    _orgId = None
    responseCls = NttCisResponse
    rawResponseCls = NttCisRawResponse

    allow_insecure = False

    def __init__(
        self,
        user_id,
        key,
        secure=True,
        host=None,
        port=None,
        url=None,
        timeout=None,
        proxy_url=None,
        api_version=None,
        **conn_kwargs,
    ):
        super().__init__(
            user_id=user_id,
            key=key,
            secure=secure,
            host=host,
            port=port,
            url=url,
            timeout=timeout,
            proxy_url=proxy_url,
        )

        if conn_kwargs["region"]:
            self.host = conn_kwargs["region"]["host"]

        if api_version:
            if LooseVersion(api_version) < LooseVersion(self.oldest_api_version):
                msg = (
                    "API Version specified is too old. No longer "
                    "supported. Please upgrade to the latest version {}".format(
                        self.active_api_version
                    )
                )

                raise NttCisAPIException(code=None, msg=msg, driver=self.driver)
            elif LooseVersion(api_version) > LooseVersion(self.latest_api_version):
                msg = (
                    "Unsupported API Version. The version specified is "
                    "not release yet. Please use the latest supported "
                    "version {}".format(self.active_api_version)
                )

                raise NttCisAPIException(code=None, msg=msg, driver=self.driver)

            else:
                # Overwrite default version using the version user specified
                self.active_api_version = api_version

    def add_default_headers(self, headers):
        headers["Authorization"] = "Basic %s" % b64encode(
            b("{}:{}".format(self.user_id, self.key))
        ).decode("utf-8")
        headers["Content-Type"] = "application/xml"
        return headers

    def request_api_1(self, action, params=None, data="", headers=None, method="GET"):
        action = "{}/{}/{}".format(self.api_path_version_1, self.api_version_1, action)

        return super().request(
            action=action, params=params, data=data, method=method, headers=headers
        )

    def request_api_2(self, path, action, params=None, data="", headers=None, method="GET"):
        action = "{}/{}/{}/{}".format(
            self.api_path_version_2,
            self.active_api_version,
            path,
            action,
        )

        return super().request(
            action=action, params=params, data=data, method=method, headers=headers
        )

    def raw_request_with_orgId_api_1(
        self, action, params=None, data="", headers=None, method="GET"
    ):
        action = "{}/{}".format(self.get_resource_path_api_1(), action)
        return super().request(
            action=action,
            params=params,
            data=data,
            method=method,
            headers=headers,
            raw=True,
        )

    def request_with_orgId_api_1(self, action, params=None, data="", headers=None, method="GET"):
        action = "{}/{}".format(self.get_resource_path_api_1(), action)

        return super().request(
            action=action, params=params, data=data, method=method, headers=headers
        )

    def request_with_orgId_api_2(self, action, params=None, data="", headers=None, method="GET"):
        action = "{}/{}".format(self.get_resource_path_api_2(), action)

        return super().request(
            action=action, params=params, data=data, method=method, headers=headers
        )

    def paginated_request_with_orgId_api_2(
        self, action, params=None, data="", headers=None, method="GET", page_size=250
    ):
        """
        A paginated request to the MCP2.0 API
        This essentially calls out to request_with_orgId_api_2 for each page
        and yields the response to make a generator
        This generator can be looped through to grab all the pages.

        :param action: The resource to access (i.e. 'network/vlan')
        :type  action: ``str``

        :param params: Parameters to give to the action
        :type  params: ``dict`` or ``None``

        :param data: The data payload to be added to the request
        :type  data: ``str``

        :param headers: Additional header to be added to the request
        :type  headers: ``str`` or ``dict`` or ``None``

        :param method: HTTP Method for the request (i.e. 'GET', 'POST')
        :type  method: ``str``

        :param page_size: The size of each page to be returned
                          Note: Max page size in MCP2.0 is currently 250
        :type  page_size: ``int``
        """
        if params is None:
            params = {}
        params["pageSize"] = page_size

        resp = self.request_with_orgId_api_2(action, params, data, headers, method).object
        yield resp
        if len(resp) <= 0:
            return

        pcount = resp.get("pageCount")  # pylint: disable=no-member
        psize = resp.get("pageSize")  # pylint: disable=no-member
        pnumber = resp.get("pageNumber")  # pylint: disable=no-member

        while int(pcount) >= int(psize):
            params["pageNumber"] = int(pnumber) + 1
            resp = self.request_with_orgId_api_2(action, params, data, headers, method).object
            pcount = resp.get("pageCount")  # pylint: disable=no-member
            psize = resp.get("pageSize")  # pylint: disable=no-member
            pnumber = resp.get("pageNumber")  # pylint: disable=no-member
            yield resp

    def get_resource_path_api_1(self):
        """
        This method returns a resource path which is necessary for referencing
        resources that require a full path instead of just an ID, such as
        networks, and customer snapshots.
        """
        return "{}/{}/{}".format(
            self.api_path_version_1,
            self.api_version_1,
            self._get_orgId(),
        )

    def get_resource_path_api_2(self):
        """
        This method returns a resource path which is necessary for referencing
        resources that require a full path instead of just an ID, such as
        networks, and customer snapshots.
        """
        return "{}/{}/{}".format(
            self.api_path_version_2,
            self.active_api_version,
            self._get_orgId(),
        )

    def wait_for_state(self, state, func, poll_interval=2, timeout=60, *args, **kwargs):
        """
        Wait for the function which returns a instance with field status/state
        to match.

        Keep polling func until one of the desired states is matched

        :param state: Either the desired state (`str`) or a `list` of states
        :type  state: ``str`` or ``list``

        :param  func: The function to call, e.g. ex_get_vlan. Note: This
                      function needs to return an object which has ``status``
                      attribute.
        :type   func: ``function``

        :param  poll_interval: The number of seconds to wait between checks
        :type   poll_interval: `int`

        :param  timeout: The total number of seconds to wait to reach a state
        :type   timeout: `int`

        :param  args: The arguments for func
        :type   args: Positional arguments

        :param  kwargs: The arguments for func
        :type   kwargs: Keyword arguments

        :return: Result from the calling function.
        """
        cnt = 0
        result = None
        object_state = None
        state = state.lower()
        while cnt < timeout / poll_interval:
            result = func(*args, **kwargs)
            if isinstance(result, Node):
                object_state = result.state.lower()
            else:
                # BUG: need to use result.status.lower() or
                #  will never match if client uses lower case
                object_state = result.status.lower()
            if object_state is state or object_state in state:
                return result
            sleep(poll_interval)
            cnt += 1

        msg = "Status check for object %s timed out" % (result)
        raise NttCisAPIException(code=object_state, msg=msg, driver=self.driver)

    def _get_orgId(self):
        """
        Send the /myaccount API request to NTTC-CIS cloud and parse the
        'orgId' from the XML response object. We need the orgId to use most
        of the other API functions
        """
        if self._orgId is None:
            body = self.request_api_1("myaccount").object
            self._orgId = findtext(body, "orgId", DIRECTORY_NS)
        return self._orgId

    def get_account_details(self):
        """
        Get the details of this account

        :rtype: :class:`DimensionDataAccountDetails`
        """
        body = self.request_api_1("myaccount").object
        return NttCisAccountDetails(
            user_name=findtext(body, "userName", DIRECTORY_NS),
            full_name=findtext(body, "fullName", DIRECTORY_NS),
            first_name=findtext(body, "firstName", DIRECTORY_NS),
            last_name=findtext(body, "lastName", DIRECTORY_NS),
            email=findtext(body, "emailAddress", DIRECTORY_NS),
        )


class NttCisAccountDetails:
    """
    NTTCIS account class details
    """

    def __init__(self, user_name, full_name, first_name, last_name, email):
        self.user_name = user_name
        self.full_name = full_name
        self.first_name = first_name
        self.last_name = last_name
        self.email = email


class NttCisStatus:
    """
    NTTCIS API pending operation status class
        action, request_time, user_name, number_of_steps, update_time,
        step.name, step.number, step.percent_complete, failure_reason,
    """

    def __init__(
        self,
        action=None,
        request_time=None,
        user_name=None,
        number_of_steps=None,
        update_time=None,
        step_name=None,
        step_number=None,
        step_percent_complete=None,
        failure_reason=None,
    ):
        self.action = action
        self.request_time = request_time
        self.user_name = user_name
        self.number_of_steps = number_of_steps
        self.update_time = update_time
        self.step_name = step_name
        self.step_number = step_number
        self.step_percent_complete = step_percent_complete
        self.failure_reason = failure_reason

    def __repr__(self):
        return (
            "<NttCisStatus: action=%s, request_time=%s, "
            "user_name=%s, number_of_steps=%s, update_time=%s, "
            "step_name=%s, step_number=%s, "
            "step_percent_complete=%s, failure_reason=%s>"
        ) % (
            self.action,
            self.request_time,
            self.user_name,
            self.number_of_steps,
            self.update_time,
            self.step_name,
            self.step_number,
            self.step_percent_complete,
            self.failure_reason,
        )


class NttCisNetwork:
    """
    NTTCIS network with location.
    """

    def __init__(self, id, name, description, location, private_net, multicast, status):
        self.id = str(id)
        self.name = name
        self.description = description
        self.location = location
        self.private_net = private_net
        self.multicast = multicast
        self.status = status

    def __repr__(self):
        return (
            "<NttCisNetwork: id=%s, name=%s, description=%s, "
            "location=%s, private_net=%s, multicast=%s>"
        ) % (
            self.id,
            self.name,
            self.description,
            self.location,
            self.private_net,
            self.multicast,
        )


class NttCisNetworkDomain:
    """
    NttCis network domain with location.
    """

    def __init__(self, id, name, description, location, status, plan):
        self.id = str(id)
        self.name = name
        self.description = description
        self.location = location
        self.status = status
        self.plan = plan

    def __repr__(self):
        return (
            "<NttCisNetworkDomain: id=%s, name=%s, "
            "description=%s, location=%s, status=%s, plan=%s>"
        ) % (
            self.id,
            self.name,
            self.description,
            self.location,
            self.status,
            self.plan,
        )


class NttCisPublicIpBlock:
    """
    NTTCIS Public IP Block with location.
    """

    def __init__(self, id, base_ip, size, location, network_domain, status):
        self.id = str(id)
        self.base_ip = base_ip
        self.size = size
        self.location = location
        self.network_domain = network_domain
        self.status = status

    def __repr__(self):
        return ("<NttCisNetworkDomain: id=%s, base_ip=%s, " "size=%s, location=%s, status=%s>") % (
            self.id,
            self.base_ip,
            self.size,
            self.location,
            self.status,
        )


class NttCisServerCpuSpecification:
    """
    A class that represents the specification of the CPU(s) for a
    node
    """

    def __init__(self, cpu_count, cores_per_socket, performance):
        """
        Instantiate a new :class:`NttCisServerCpuSpecification`

        :param cpu_count: The number of CPUs
        :type  cpu_count: ``int``

        :param cores_per_socket: The number of cores per socket, the
            recommendation is 1
        :type  cores_per_socket: ``int``

        :param performance: The performance type, e.g. HIGHPERFORMANCE
        :type  performance: ``str``
        """
        self.cpu_count = cpu_count
        self.cores_per_socket = cores_per_socket
        self.performance = performance

    def __repr__(self):
        return (
            "<NttCisServerCpuSpecification: "
            "cpu_count=%s, cores_per_socket=%s, "
            "performance=%s>"
        ) % (self.cpu_count, self.cores_per_socket, self.performance)


class NttCisServerDisk:
    """
    A class that represents the disk on a server
    """

    def __init__(self, id=None, scsi_id=None, size_gb=None, speed=None, state=None):
        """
        Instantiate a new :class:`DimensionDataServerDisk`

        :param id: The id of the disk
        :type  id: ``str``

        :param scsi_id: Representation for scsi
        :type  scsi_id: ``int``

        :param size_gb: Size of the disk
        :type  size_gb: ``int``

        :param speed: Speed of the disk (i.e. STANDARD)
        :type  speed: ``str``

        :param state: State of the disk (i.e. PENDING)
        :type  state: ``str``
        """
        self.id = id
        self.scsi_id = scsi_id
        self.size_gb = size_gb
        self.speed = speed
        self.state = state

    def __repr__(self):
        return ("<NttCisServerDisk: " "id=%s, size_gb=%s") % (self.id, self.size_gb)


class NttCisScsiController:
    """
    A class that represents the disk on a server
    """

    def __init__(self, id, adapter_type, bus_number, state):
        """
        Instantiate a new :class:`DimensionDataServerDisk`

        :param id: The id of the controller
        :type  id: ``str``

        :param adapter_type: The 'brand' of adapter
        :type  adapter_type: ``str``

        :param bus_number: The bus number occupied on the virtual hardware
        :type  bus_nubmer: ``str``

        :param state: Current state (i.e. NORMAL)
        :type  speed: ``str``

        :param state: State of the disk (i.e. PENDING)
        :type  state: ``str``
        """
        self.id = id
        self.adapter_type = adapter_type
        self.bus_number = bus_number
        self.state = state

    def __repr__(self):
        return ("<NttCisScsiController: " "id=%s, adapter_type=%s, bus_number=%s, state=%s") % (
            self.id,
            self.adapter_type,
            self.bus_number,
            self.state,
        )


class NttCisServerVMWareTools:
    """
    A class that represents the VMWareTools for a node
    """

    def __init__(self, status, version_status, api_version):
        """
        Instantiate a new :class:`NttCisServerVMWareTools` object

        :param status: The status of VMWare Tools
        :type  status: ``str``

        :param version_status: The status for the version of VMWare Tools
            (i.e NEEDS_UPGRADE)
        :type  version_status: ``str``

        :param api_version: The API version of VMWare Tools
        :type  api_version: ``str``
        """
        self.status = status
        self.version_status = version_status
        self.api_version = api_version

    def __repr__(self):
        return ("<NttCisServerVMWareTools " "status=%s, version_status=%s, " "api_version=%s>") % (
            self.status,
            self.version_status,
            self.api_version,
        )


class NttCisSnapshot:
    """
    NTTCIS Class representing server snapshots
    """

    def __init__(
        self,
        server_id,
        service_plan,
        id=None,
        window_id=None,
        start_time=None,
        state=None,
        end_time=None,
        type=None,
        expiry_time=None,
        action=None,
    ):
        self.server_id = server_id
        self.service_plan = service_plan
        self.id = id
        self.window_id = window_id
        self.start_time = start_time
        self.end_time = end_time
        self.state = state
        self.end_time = end_time
        self.type = type
        self.expiry_time = expiry_time
        self.action = action

    def __repr__(self):
        return (
            "<NttCisSnapshots "
            "id=%s, start_time=%s, "
            "end_time=%s, self.type=%s, "
            "self.expiry_timne=%s, self.state=%s>"
        ) % (
            self.id,
            self.start_time,
            self.end_time,
            self.type,
            self.expiry_time,
            self.state,
        )


class NttCisReservedIpAddress:
    """
    NTTCIS Rerverse IPv4 address
    """

    def __init__(self, datacenter_id, exclusive, vlan_id, ip, description=None):
        self.datacenter_id = datacenter_id
        self.exclusive = exclusive
        self.vlan_id = vlan_id
        self.ip = ip
        self.description = description

    def __repr__(self):
        return (
            "<NttCisReservedIpAddress "
            "datacenterId=%s, exclusiven=%s, vlanId=%s, ipAddress=%s,"
            " description=-%s"
        ) % (
            self.datacenter_id,
            self.exclusive,
            self.vlan_id,
            self.ip,
            self.description,
        )


class NttCisFirewallRule:
    """
    NTTCIS Firewall Rule for a network domain
    """

    def __init__(
        self,
        id,
        name,
        action,
        location,
        network_domain,
        status,
        ip_version,
        protocol,
        source,
        destination,
        enabled,
    ):
        self.id = str(id)
        self.name = name
        self.action = action
        self.location = location
        self.network_domain = network_domain
        self.status = status
        self.ip_version = ip_version
        self.protocol = protocol
        self.source = source
        self.destination = destination
        self.enabled = enabled

    def __repr__(self):
        return (
            "<NttCisFirewallRule: id=%s, name=%s, "
            "action=%s, location=%s, network_domain=%s, "
            "status=%s, ip_version=%s, protocol=%s, source=%s, "
            "destination=%s, enabled=%s>"
        ) % (
            self.id,
            self.name,
            self.action,
            self.location,
            self.network_domain,
            self.status,
            self.ip_version,
            self.protocol,
            self.source,
            self.destination,
            self.enabled,
        )


"""
class NttCisFirewallAddress(object):
    The source or destination model in a firewall rule
    def __init__(self, any_ip, ip_address, ip_prefix_size,
                 port_begin, port_end, address_list_id,
                 port_list_id):
        self.any_ip = any_ip
        self.ip_address = ip_address
        self.ip_prefix_size = ip_prefix_size
        self.port_list_id = port_list_id
        self.port_begin = port_begin
        self.port_end = port_end
        self.address_list_id = address_list_id
        self.port_list_id = port_list_id
    def __repr__(self):
        return (
            '<NttCisFirewallAddress: any_ip=%s, ip_address=%s, '
            'ip_prefix_size=%s, port_begin=%s, port_end=%s, '
            'address_list_id=%s, port_list_id=%s>'
            % (self.any_ip, self.ip_address, self.ip_prefix_size,
               self.port_begin, self.port_end, self.address_list_id,
               self.port_list_id))
"""


class NttCisFirewallAddress:
    """
    The source or destination model in a firewall rule
    9/4/18: Editing Class to use with ex_create_firewall_rtule method.
    Will haved to circle back and test for any other uses.
    """

    def __init__(
        self,
        any_ip=None,
        ip_address=None,
        ip_prefix_size=None,
        port_begin=None,
        port_end=None,
        address_list_id=None,
        port_list_id=None,
    ):
        """
        :param any_ip: used to set ip address to "ANY"
        :param ip_address: Optional, an ip address of either IPv4 decimal
                           notation or an IPv6 address
        :type ``str``

        :param ip_prefix_size: An integer denoting prefix size.
        :type ``int``

        :param port_begin: integer for an individual port or start of a list
                           of ports if not using a port list
        :type ``int``

        :param port_end: integer required if using a list of ports
                         (NOT a port list but a list starting with port begin)
        :type  ``int``

        :param address_list_id: An id identifying an address list
        :type ``str``

        :param port_list_id:  An id identifying a port list
        :type ``str``
        """
        self.any_ip = any_ip
        self.ip_address = ip_address
        self.ip_prefix_size = ip_prefix_size
        self.port_list_id = port_list_id
        self.port_begin = port_begin
        self.port_end = port_end
        self.address_list_id = address_list_id
        self.port_list_id = port_list_id

    def __repr__(self):
        return (
            "<NttCisFirewallAddress: any_ip=%s, ip_address=%s, "
            "ip_prefix_size=%s, port_begin=%s, port_end=%s, "
            "address_list_id=%s, port_list_id=%s>"
            % (
                self.any_ip,
                self.ip_address,
                self.ip_prefix_size,
                self.port_begin,
                self.port_end,
                self.address_list_id,
                self.port_list_id,
            )
        )


class NttCisNatRule:
    """
    An IP NAT rule in a network domain
    """

    def __init__(self, id, network_domain, internal_ip, external_ip, status):
        self.id = id
        self.network_domain = network_domain
        self.internal_ip = internal_ip
        self.external_ip = external_ip
        self.status = status

    def __repr__(self):
        return ("<NttCisNatRule: id=%s, status=%s>") % (self.id, self.status)


class NttCisAntiAffinityRule:
    """
    Anti-Affinity rule for NTTCIS

    An Anti-Affinity rule ensures that servers in the rule will
    not reside on the same VMware ESX host.
    """

    def __init__(self, id, node_list):
        """
        Instantiate a new :class:`NttCisDataAntiAffinityRule`

        :param id: The ID of the Anti-Affinity rule
        :type  id: ``str``

        :param node_list: List of node ids that belong in this rule
        :type  node_list: ``list`` of ``str``
        """
        self.id = id
        self.node_list = node_list

    def __repr__(self):
        return ("<NttCisAntiAffinityRule: id=%s>") % (self.id)


class NttCisVlan:
    """
    NTTCIS VLAN.
    """

    def __init__(
        self,
        id,
        name,
        description,
        location,
        network_domain,
        status,
        private_ipv4_range_address,
        private_ipv4_range_size,
        ipv6_range_address,
        ipv6_range_size,
        ipv4_gateway,
        ipv6_gateway,
    ):
        """
        Initialize an instance of ``DimensionDataVlan``

        :param id: The ID of the VLAN
        :type  id: ``str``

        :param name: The name of the VLAN
        :type  name: ``str``

        :param description: Plan text description of the VLAN
        :type  description: ``str``

        :param location: The location (data center) of the VLAN
        :type  location: ``NodeLocation``

        :param network_domain: The Network Domain that owns this VLAN
        :type  network_domain: :class:`DimensionDataNetworkDomain`

        :param status: The status of the VLAN
        :type  status: :class:`DimensionDataStatus`

        :param private_ipv4_range_address: The host address of the VLAN
                                            IP space
        :type  private_ipv4_range_address: ``str``

        :param private_ipv4_range_size: The size (e.g. '24') of the VLAN
                                            as a CIDR range size
        :type  private_ipv4_range_size: ``int``

        :param ipv6_range_address: The host address of the VLAN
                                            IP space
        :type  ipv6_range_address: ``str``

        :param ipv6_range_size: The size (e.g. '32') of the VLAN
                                            as a CIDR range size
        :type  ipv6_range_size: ``int``

        :param ipv4_gateway: The IPv4 default gateway address
        :type  ipv4_gateway: ``str``

        :param ipv6_gateway: The IPv6 default gateway address
        :type  ipv6_gateway: ``str``
        """
        self.id = str(id)
        self.name = name
        self.location = location
        self.description = description
        self.network_domain = network_domain
        self.status = status
        self.private_ipv4_range_address = private_ipv4_range_address
        self.private_ipv4_range_size = private_ipv4_range_size
        self.ipv6_range_address = ipv6_range_address
        self.ipv6_range_size = ipv6_range_size
        self.ipv4_gateway = ipv4_gateway
        self.ipv6_gateway = ipv6_gateway

    def __repr__(self):
        return ("<NttCisVlan: id=%s, name=%s, " "description=%s, location=%s, status=%s>") % (
            self.id,
            self.name,
            self.description,
            self.location,
            self.status,
        )


class NttCisPool:
    """
    NttCis VIP Pool.
    """

    def __init__(
        self,
        id,
        name,
        description,
        status,
        load_balance_method,
        health_monitor_id,
        service_down_action,
        slow_ramp_time,
    ):
        """
        Initialize an instance of ``NttCisPool``

        :param id: The ID of the pool
        :type  id: ``str``

        :param name: The name of the pool
        :type  name: ``str``

        :param description: Plan text description of the pool
        :type  description: ``str``

        :param status: The status of the pool
        :type  status: :class:NttCisStatus`

        :param load_balance_method: The load balancer method
        :type  load_balance_method: ``str``

        :param health_monitor_id: The ID of the health monitor
        :type  health_monitor_id: ``str``

        :param service_down_action: Action to take when pool is down
        :type  service_down_action: ``str``

        :param slow_ramp_time: The ramp-up time for service recovery
        :type  slow_ramp_time: ``int``
        """
        self.id = str(id)
        self.name = name
        self.description = description
        self.status = status
        self.load_balance_method = load_balance_method
        self.health_monitor_id = health_monitor_id
        self.service_down_action = service_down_action
        self.slow_ramp_time = slow_ramp_time

    def __repr__(self):
        return ("<NttCisPool: id=%s, name=%s, " "description=%s, status=%s>") % (
            self.id,
            self.name,
            self.description,
            self.status,
        )


class NttCisPoolMember:
    """
    NTTCIS VIP Pool Member.
    """

    def __init__(self, id, name, status, ip, port, node_id):
        """
        Initialize an instance of ``NttCisPoolMember``

        :param id: The ID of the pool member
        :type  id: ``str``

        :param name: The name of the pool member
        :type  name: ``str``

        :param status: The status of the pool
        :type  status: :class:`NttCisStatus`

        :param ip: The IP of the pool member
        :type  ip: ``str``

        :param port: The port of the pool member
        :type  port: ``int``

        :param node_id: The ID of the associated node
        :type  node_id: ``str``
        """
        self.id = str(id)
        self.name = name
        self.status = status
        self.ip = ip
        self.port = port
        self.node_id = node_id

    def __repr__(self):
        return ("NttCisPoolMember: id=%s, name=%s, " "ip=%s, status=%s, port=%s, node_id=%s>") % (
            self.id,
            self.name,
            self.ip,
            self.status,
            self.port,
            self.node_id,
        )


class NttCisVIPNode:
    def __init__(
        self,
        id,
        name,
        status,
        ip,
        connection_limit="10000",
        connection_rate_limit="10000",
        health_monitor=None,
    ):
        """
        Initialize an instance of :class:`NttCisVIPNode`

        :param id: The ID of the node
        :type  id: ``str``

        :param name: The name of the node
        :type  name: ``str``

        :param status: The status of the node
        :type  status: :class:`NttCisStatus`

        :param ip: The IP of the node
        :type  ip: ``str``

        :param connection_limit: The total connection limit for the node
        :type  connection_limit: ``int``

        :param connection_rate_limit: The rate limit for the node
        :type  connection_rate_limit: ``int``
        """
        self.id = str(id)
        self.name = name
        self.status = status
        self.ip = ip
        self.connection_limit = connection_limit
        self.connection_rate_limit = connection_rate_limit
        if health_monitor is not None:
            self.health_monitor_id = health_monitor

    def __repr__(self):
        return ("<NttCisVIPNode: id=%s, name=%s, " "status=%s, ip=%s>") % (
            self.id,
            self.name,
            self.status,
            self.ip,
        )


class NttCisVirtualListener:
    """
    NTTCIS Virtual Listener.
    """

    def __init__(self, id, name, status, ip):
        """
        Initialize an instance of :class:`NttCisVirtualListener`

        :param id: The ID of the listener
        :type  id: ``str``

        :param name: The name of the listener
        :type  name: ``str``

        :param status: The status of the listener
        :type  status: :class:`NttCisStatus`

        :param ip: The IP of the listener
        :type  ip: ``str``
        """
        self.id = str(id)
        self.name = name
        self.status = status
        self.ip = ip

    def __repr__(self):
        return ("<NttCisVirtualListener: id=%s, name=%s, " "status=%s, ip=%s>") % (
            self.id,
            self.name,
            self.status,
            self.ip,
        )


class NttCisDefaultHealthMonitor:
    """
    A default health monitor for a VIP (node, pool or listener)
    """

    def __init__(self, id, name, node_compatible, pool_compatible):
        """
        Initialize an instance of :class:`NttCisDefaultHealthMonitor`

        :param id: The ID of the monitor
        :type  id: ``str``

        :param name: The name of the monitor
        :type  name: ``str``

        :param node_compatible: Is a monitor capable of monitoring nodes
        :type  node_compatible: ``bool``

        :param pool_compatible: Is a monitor capable of monitoring pools
        :type  pool_compatible: ``bool``
        """
        self.id = id
        self.name = name
        self.node_compatible = node_compatible
        self.pool_compatible = pool_compatible

    def __repr__(self):
        return ("<NttCisDefaultHealthMonitor: id=%s, name=%s>") % (self.id, self.name)


class NttCisPersistenceProfile:
    """
    Each Persistence Profile declares the combination of Virtual Listener
    type and protocol with which it is
    compatible and whether or not it is compatible as a
    Fallback Persistence Profile.
    """

    def __init__(self, id, name, compatible_listeners, fallback_compatible):
        """
        Initialize an instance of :class:`NttCisPersistenceProfile`

        :param id: The ID of the profile
        :type  id: ``str``

        :param name: The name of the profile
        :type  name: ``str``

        :param compatible_listeners: List of compatible Virtual Listener types
        :type  compatible_listeners: ``list`` of
            :class:`NttCisVirtualListenerCompatibility`

        :param fallback_compatible: Is capable as a fallback profile
        :type  fallback_compatible: ``bool``
        """
        self.id = id
        self.name = name
        self.compatible_listeners = compatible_listeners
        self.fallback_compatible = fallback_compatible

    def __repr__(self):
        return ("NttCisPersistenceProfile: id=%s, name=%s>") % (self.id, self.name)


class NttCisDefaultiRule:
    """
    A default iRule for a network domain, can be applied to a listener
    """

    def __init__(self, id, name, compatible_listeners):
        """
        Initialize an instance of :class:`NttCisefaultiRule`

        :param id: The ID of the iRule
        :type  id: ``str``

        :param name: The name of the iRule
        :type  name: ``str``

        :param compatible_listeners: List of compatible Virtual Listener types
        :type  compatible_listeners: ``list`` of
            :class:`NttCisVirtualListenerCompatibility`
        """
        self.id = id
        self.name = name
        self.compatible_listeners = compatible_listeners

    def __repr__(self):
        return ("<NttCisDefaultiRule: id=%s, name=%s>") % (self.id, self.name)


class NttCisVirtualListenerCompatibility:
    """
    A compatibility preference for a persistence profile or iRule
    specifies which virtual listener types this profile or iRule can be
    applied to.
    """

    def __init__(self, type, protocol):
        self.type = type
        self.protocol = protocol

    def __repr__(self):
        return ("<NttCisVirtualListenerCompatibility: " "type=%s, protocol=%s>") % (
            self.type,
            self.protocol,
        )


class NttCisBackupDetails:
    """
    NTTCIS Backup Details represents information about
    a targets backups configuration
    """

    def __init__(self, asset_id, service_plan, status, clients=None):
        """
        Initialize an instance of :class:`NttCisBackupDetails`

        :param asset_id: Asset identification for backups
        :type  asset_id: ``str``

        :param service_plan: The service plan for backups. i.e (Essentials)
        :type  service_plan: ``str``

        :param status: The overall status this backup target.
                       i.e. (unregistered)
        :type  status: ``str``

        :param clients: Backup clients attached to this target
        :type  clients: ``list`` of :class:`NttCisBackupClient`
        """
        self.asset_id = asset_id
        self.service_plan = service_plan
        self.status = status
        self.clients = clients

    def __repr__(self):
        return ("<NttCisBackupDetails: id=%s>") % (self.asset_id)


class NttCisBackupClient:
    """
    An object that represents a backup client
    """

    def __init__(
        self,
        id,
        type,
        status,
        schedule_policy,
        storage_policy,
        download_url,
        alert=None,
        running_job=None,
    ):
        """
        Initialize an instance of this class.

        :param id: Unique ID for the client
        :type  id: ``str``
        :param type: The type of client that this client is
        :type  type: :class:`NttCisBackupClientType`
        :param status: The states of this particular backup client.
                       i.e. (Unregistered)
        :type  status: ``str``
        :param schedule_policy: The schedule policy for this client
                                NOTE: NTTCIS only sends back the name
                                of the schedule policy, no further details
        :type  schedule_policy: ``str``
        :param storage_policy: The storage policy for this client
                               NOTE: NTTCIS only sends back the name
                               of the storage policy, no further details
        :type  storage_policy: ``str``
        :param download_url: The download url for this client
        :type  download_url: ``str``
        :param alert: The alert configured for this backup client (optional)
        :type  alert: :class:`NttCisBackupClientAlert`
        :param alert: The running job for the client (optional)
        :type  alert: :class:`NttCisBackupClientRunningJob`
        """
        self.id = id
        self.type = type
        self.status = status
        self.schedule_policy = schedule_policy
        self.storage_policy = storage_policy
        self.download_url = download_url
        self.alert = alert
        self.running_job = running_job

    def __repr__(self):
        return ("<NttCisBackupClient: id=%s>") % (self.id)


class NttCisBackupClientAlert:
    """
    An alert for a backup client
    """

    def __init__(self, trigger, notify_list=[]):
        """
        Initialize an instance of :class:`NttCisBackupClientAlert`

        :param trigger: Trigger type for the client i.e. ON_FAILURE
        :type  trigger: ``str``

        :param notify_list: List of email addresses that are notified
                            when the alert is fired
        :type  notify_list: ``list`` of ``str``
        """
        self.trigger = trigger
        self.notify_list = notify_list

    def __repr__(self):
        return ("<NttCisBackupClientAlert: trigger=%s>") % (self.trigger)


class NttCisBackupClientRunningJob:
    """
    A running job for a given backup client
    """

    def __init__(self, id, status, percentage=0):
        """
        Initialize an instance of :class:`NttCisBackupClientRunningJob`

        :param id: The unique ID of the job
        :type  id: ``str``

        :param status: The status of the job i.e. Waiting
        :type  status: ``str``

        :param percentage: The percentage completion of the job
        :type  percentage: ``int``
        """
        self.id = id
        self.percentage = percentage
        self.status = status

    def __repr__(self):
        return ("<NttCisBackupClientRunningJob: id=%s>") % (self.id)


class NttCisBackupClientType:
    """
    A client type object for backups
    """

    def __init__(self, type, is_file_system, description):
        """
        Initialize an instance of :class:`NttCisBackupClientType`

        :param type: The type of client i.e. (FA.Linux, MySQL, etc.)
        :type  type: ``str``

        :param is_file_system: The name of the iRule
        :type  is_file_system: ``bool``

        :param description: Description of the client
        :type  description: ``str``
        """
        self.type = type
        self.is_file_system = is_file_system
        self.description = description

    def __repr__(self):
        return ("<NttCisBackupClientType: type=%s>") % (self.type)


class NttCisBackupStoragePolicy:
    """
    A representation of a storage policy
    """

    def __init__(self, name, retention_period, secondary_location):
        """
        Initialize an instance of :class:`NttCisBackupStoragePolicy`

        :param name: The name of the storage policy i.e. 14 Day Storage Policy
        :type  name: ``str``

        :param retention_period: How long to keep the backup in days
        :type  retention_period: ``int``

        :param secondary_location: The secondary location i.e. Primary
        :type  secondary_location: ``str``
        """
        self.name = name
        self.retention_period = retention_period
        self.secondary_location = secondary_location

    def __repr__(self):
        return ("<NttCisBackupStoragePolicy: name=%s>") % (self.name)


class NttCisBackupSchedulePolicy:
    """
    A representation of a schedule policy
    """

    def __init__(self, name, description):
        """
        Initialize an instance of :class:`NttCisBackupSchedulePolicy`

        :param name: The name of the policy i.e 12AM - 6AM
        :type  name: ``str``

        :param description: Short summary of the details of the policy
        :type  description: ``str``
        """
        self.name = name
        self.description = description

    def __repr__(self):
        return ("<NttCisBackupSchedulePolicy: name=%s>") % (self.name)


class NttCisTag:
    """
    A representation of a Tag in NTTCIS
    A Tag first must have a Tag Key, then an asset is tag with
    a key and an option value.  Tags can be queried later to filter assets
    and also show up on usage report if so desired.
    """

    def __init__(self, asset_type, asset_id, asset_name, datacenter, key, value):
        """
        Initialize an instance of :class:`NttCisTag`

        :param asset_type: The type of asset.  Current asset types:
                           SERVER, VLAN, NETWORK_DOMAIN, CUSTOMER_IMAGE,
                           PUBLIC_IP_BLOCK, ACCOUNT
        :type  asset_type: ``str``

        :param asset_id: The GUID of the asset that is tagged
        :type  asset_id: ``str``

        :param asset_name: The name of the asset that is tagged
        :type  asset_name: ``str``

        :param datacenter: The short datacenter name of the tagged asset
        :type  datacenter: ``str``

        :param key: The tagged key
        :type  key: :class:`NttCisTagKey`

        :param value: The tagged value
        :type  value: ``None`` or ``str``
        """
        self.asset_type = asset_type
        self.asset_id = asset_id
        self.asset_name = asset_name
        self.datacenter = datacenter
        self.key = key
        self.value = value

    def __repr__(self):
        return ("<NttCisTag: asset_name=%s, tag_name=%s, value=%s>") % (
            self.asset_name,
            self.key.name,
            self.value,
        )


class NttCisTagKey:
    """
    A representation of a Tag Key in NTTCIS
    A tag key is required to tag an asset
    """

    def __init__(self, id, name, description, value_required, display_on_report):
        """
        Initialize an instance of :class:`NttCisTagKey`

        :param id: GUID of the tag key
        :type  id: ``str``

        :param name: Name of the tag key
        :type  name: ``str``

        :param description: Description of the tag key
        :type  description: ``str``

        :param value_required: If a value is required for this tag key
        :type  value_required: ``bool``

        :param display_on_report: If this tag key should be displayed on
                                  usage reports
        :type  display_on_report: ``bool``
        """
        self.id = id
        self.name = name
        self.description = description
        self.value_required = value_required
        self.display_on_report = display_on_report

    def __repr__(self):
        return ("NttCisTagKey: id=%s name=%s>") % (self.id, self.name)


class NttCisIpAddressList:
    """
    NttCis IP Address list
    """

    def __init__(
        self,
        id,
        name,
        description,
        ip_version,
        ip_address_collection,
        state,
        create_time,
        child_ip_address_lists=None,
    ):
        """ "
        Initialize an instance of :class:`NttCisIpAddressList`

        :param id: GUID of the IP Address List key
        :type  id: ``str``

        :param name: Name of the IP Address List
        :type  name: ``str``

        :param description: Description of the IP Address List
        :type  description: ``str``

        :param ip_version: IP version. E.g. IPV4, IPV6
        :type  ip_version: ``str``

        :param ip_address_collection: Collection of NttCisIpAddress
        :type  ip_address_collection: ``List``

        :param state: IP Address list state
        :type  state: ``str``

        :param create_time: IP Address List created time
        :type  create_time: ``date time``

        :param child_ip_address_lists: List of IP address list to be included
        :type  child_ip_address_lists: List
        of :class:'NttCisIpAddressList'
        """
        self.id = id
        self.name = name
        self.description = description
        self.ip_version = ip_version
        self.ip_address_collection = ip_address_collection
        self.state = state
        self.create_time = create_time
        self.child_ip_address_lists = child_ip_address_lists

    def __repr__(self):
        return (
            "<NttCisIpAddressList: id=%s, name=%s, description=%s, "
            "ip_version=%s, ip_address_collection=%s, state=%s, "
            "create_time=%s, child_ip_address_lists=%s>"
            % (
                self.id,
                self.name,
                self.description,
                self.ip_version,
                self.ip_address_collection,
                self.state,
                self.create_time,
                self.child_ip_address_lists,
            )
        )


class NttCisChildIpAddressList:
    """
    NttCis Child IP Address list
    """

    def __init__(self, id, name):
        """ "
        Initialize an instance of :class:`NttCisDataChildIpAddressList`

        :param id: GUID of the IP Address List key
        :type  id: ``str``

        :param name: Name of the IP Address List
        :type  name: ``str``

        """
        self.id = id
        self.name = name

    def __repr__(self):
        return "<NttCisChildIpAddressList: id={}, name={}>".format(self.id, self.name)


class NttCisIpAddress:
    """
    A representation of IP Address in NttCis
    """

    def __init__(self, begin, end=None, prefix_size=None):
        """
        Initialize an instance of :class:`NttCisIpAddress`

        :param begin: IP Address Begin
        :type  begin: ``str``

        :param end: IP Address end
        :type  end: ``str``

        :param prefixSize: IP Address prefix size
        :type  prefixSize: ``int``
        """
        self.begin = begin
        self.end = end
        self.prefix_size = prefix_size

    def __repr__(self):
        return "<NttCisIpAddress: begin={}, end={}, prefix_size={}>".format(
            self.begin,
            self.end,
            self.prefix_size,
        )


class NttCisPortList:
    """
    NttCis Port list
    """

    def __init__(
        self,
        id,
        name,
        description,
        port_collection,
        child_portlist_list,
        state,
        create_time,
    ):
        """ "
        Initialize an instance of :class:`DNttCisPortList`

        :param id: GUID of the Port List key
        :type  id: ``str``

        :param name: Name of the Port List
        :type  name: ``str``

        :param description: Description of the Port List
        :type  description: ``str``

        :param port_collection: Collection of NttCisPort
        :type  port_collection: ``List``

        :param child_portlist_list: Collection of NttCisChildPort
        :type  child_portlist_list: ``List``

        :param state: Port list state
        :type  state: ``str``

        :param create_time: Port List created time
        :type  create_time: ``date time``
        """
        self.id = id
        self.name = name
        self.description = description
        self.port_collection = port_collection
        self.child_portlist_list = child_portlist_list
        self.state = state
        self.create_time = create_time

    def __repr__(self):
        return (
            "<NttCisPortList: id=%s, name=%s, description=%s, "
            "port_collection=%s, child_portlist_list=%s, state=%s, "
            "create_time=%s>"
            % (
                self.id,
                self.name,
                self.description,
                self.port_collection,
                self.child_portlist_list,
                self.state,
                self.create_time,
            )
        )


class NttCisChildPortList:
    """
    NttCis Child Port list
    """

    def __init__(self, id, name):
        """ "
        Initialize an instance of :class:`NttCisChildIpAddressList`

        :param id: GUID of the child port list key
        :type  id: ``str``

        :param name: Name of the child port List
        :type  name: ``str``

        """
        self.id = id
        self.name = name

    def __repr__(self):
        return "<NttCisChildPortList: id={}, name={}>".format(self.id, self.name)


class NttCisPort:
    """
    A representation of Port in NTTCIS
    """

    def __init__(self, begin, end=None):
        """
        Initialize an instance of :class:`NttCisPort`

        :param begin: Port Number Begin
        :type  begin: ``str``

        :param end: Port Number end
        :type  end: ``str``
        """
        self.begin = begin
        self.end = end

    def __repr__(self):
        return "<NttCisPort: begin={}, end={}>".format(self.begin, self.end)


class NttCisNic:
    """
    A representation of Network Adapter in NTTCIS
    """

    def __init__(self, private_ip_v4=None, vlan=None, network_adapter_name=None):
        """
        Initialize an instance of :class:`NttCisNic`

        :param private_ip_v4: IPv4
        :type  private_ip_v4: ``str``

        :param vlan: Network VLAN
        :type  vlan: class: NttCisVlan or ``str``

        :param network_adapter_name: Network Adapter Name
        :type  network_adapter_name: ``str``
        """
        self.private_ip_v4 = private_ip_v4
        self.vlan = vlan
        self.network_adapter_name = network_adapter_name

    def __repr__(self):
        return "<NttCisNic: private_ip_v4=%s, vlan=%s," "network_adapter_name=%s>" % (
            self.private_ip_v4,
            self.vlan,
            self.network_adapter_name,
        )


# Dynamically create classes from returned XML. Leaves the API as the
# single authoritative source.


class ClassFactory:
    pass


attrs = {}  # type: Dict[str, str]


def processor(mapping, name=None):
    """
    Closure that keeps the deepcopy of the original dict
    converted to XML current.
    :param mapping: The converted XML to dict/lists
    :type mapping: ``dict``
    :param name: (Optional) what becomes the class name if provided
    :type: ``str``
    :return: Nothing
    """
    mapping = mapping
    # the map_copy will have keys deleted after the key and value are processed
    map_copy = deepcopy(mapping)

    def add_items(key, value, name=None):
        """
        Add items to the global attr dict, then delete key, value from map copy
        :param key: from the process function becomes the attribute name
        :type key: ``str``
        :param value: The value of the property and may be a dict
        :type value: ``str``
        :param name: Name of class, often same as key
        :type: name" ``str``
        """
        if name in attrs:
            attrs[name].update({key: value})
        elif name is not None:
            attrs[name] = value

        else:
            attrs.update({key: value})
        # trim the copy of the mapping
        if key in map_copy:
            del map_copy[key]
        elif key in map_copy[name]:
            del map_copy[name][key]
            if len(map_copy[name]) == 0:
                del map_copy[name]

    def handle_map(map, name):
        tmp = {}
        types = [type(x) for x in map.values()]
        if XmlListConfig not in types and XmlDictConfig not in types and dict not in types:
            return map

        elif XmlListConfig in types:
            result = handle_seq(map, name)
            return result
        else:
            for k, v in map.items():
                if isinstance(v, str):
                    tmp.update({k: v})
                if isinstance(v, dict):
                    cls = build_class(k.capitalize(), v)
                    tmp.update({k: cls})
                elif isinstance(v, XmlDictConfig):
                    cls = build_class(k.capitalize(), v)
                    return (k, cls)
            return tmp

    def handle_seq(seq, name):
        tmp = {}
        if isinstance(seq, list):
            tmp = []
            for _ in seq:
                cls = build_class(name.capitalize(), _)
                tmp.append(cls)
            return tmp
        for k, v in seq.items():
            if isinstance(v, MutableSequence):
                for _ in v:
                    if isinstance(_, Mapping):
                        types = [type(x) for x in _.values()]
                        if XmlDictConfig in types:
                            result = handle_map(_, k)
                            if isinstance(result, tuple):
                                tmp.update({result[0]: result[1]})
                            else:
                                tmp.update({k: result})
                        else:
                            tmp_list = [build_class(k.capitalize(), i) for i in v]
                            tmp[k] = tmp_list
            elif isinstance(v, str):
                tmp.update({k: v})
        return tmp

    def build_class(key, value):
        klass = class_factory(key.capitalize(), value)
        return klass(value)

    def process(mapping):
        """
        This function is recursive, creating attributes for the class factory
        by taking apart the elements in the  dictionary.  Thus, the calls to
        handle_seq or handle_map
        :param mapping: the dictionary converted from XML
        :return: itself (recursive)
        """
        for k1, v1 in mapping.items():
            if isinstance(v1, Mapping):
                types = [type(v) for v in v1.values()]
                if MutableSequence not in types and dict not in types:
                    result = handle_map(v1, k1)
                    cls = build_class(k1.capitalize(), result)
                    add_items(k1, cls)
                elif XmlListConfig in types:
                    result = handle_seq(v1, k1)
                    cls = build_class(list(v1)[0], result)
                    add_items(k1, cls)
                elif dict in types:
                    result = handle_map(v1, k1)
                    cls = build_class(k1.capitalize(), result)
                    add_items(k1, cls, k1)
            elif isinstance(v1, list):
                tmp1 = {}
                tmp2 = {}
                tmp2[k1] = []
                for i, j in enumerate(v1):
                    if isinstance(j, dict):
                        key = list(j)[0]
                        result = handle_map(j, key)
                        tmp1[k1 + str(i)] = build_class(k1, result)
                        tmp2[k1].append(tmp1[k1 + str(i)])
                if tmp2:
                    add_items(k1, tmp2[k1], k1)
            elif isinstance(v1, str):
                add_items(k1, v1)

    if len(map_copy) == 0:
        return 1
    return process(mapping)


def class_factory(cls_name, attrs):
    """
    This class takes a name and a dictionary to create a class.
    The clkass has an init method, an iter for retrieving properties,
    and, finally, a repr for returning the instance
    :param cls_name: The name to be tacked onto the suffix NttCis
    :type cls_name: ``str``
    :param attrs: The attributes and values for an instance
    :type attrs: ``dict``
    :return:  a class that inherits from ClassFactory
    :rtype: ``ClassFactory``
    """

    def __init__(self, *args, **kwargs):
        for key in attrs:
            setattr(self, key, attrs[key])
        if cls_name == "NttCisServer":
            self.state = self._get_state()

    def __iter__(self):
        for name in self.__dict__:
            yield getattr(self, name)

    def __repr__(self):
        values = ", ".join("{}={!r}".format(*i) for i in zip(self.__dict__, self))
        return "{}({})".format(self.__class__.__name__, values)

    cls_attrs = dict(__init__=__init__, __iter__=__iter__, __repr__=__repr__)

    return type("NttCis{}".format(cls_name), (ClassFactory,), cls_attrs)


class XmlListConfig(list):
    """
    Creates a class from XML elements that make a list.  If a list of
    XML elements with attributes, the attributes are passed to XmlDictConfig.
    """

    def __init__(self, elem_list):
        for element in elem_list:
            if element is not None and len(element) > 1:
                # treat like dict
                if element[0].tag != element[1].tag:
                    self.append(XmlDictConfig(element))
                # treat like list
                else:
                    # property refers to an element used repeatedly
                    #  in the XML for data centers only
                    if "property" in element.tag:
                        self.append({element.attrib.get("name"): element.attrib.get("value")})
                    else:
                        self.append(element.attrib)
            elif element.text:
                text = element.text.strip()
                if text:
                    self.append(text)


class XmlDictConfig(dict):
    """
    Inherits from dict.  Looks for XML elements, such as attrib, that
    can be converted to a dictionary.  Any XML element that contains
    other XML elements, will be passed to XmlListConfig
    """

    def __init__(self, parent_element):
        if parent_element.items():
            if "property" in parent_element.tag:
                self.update({parent_element.attrib.get("name"): parent_element.attrib.get("value")})
            else:
                self.update(dict(parent_element.items()))
        for element in parent_element:
            if len(element) > 0:
                # treat like dict - we assume that if the first two tags
                # in a series are different, then they are all different.
                if len(element) == 1 or element[0].tag != element[1].tag:
                    elem_dict = XmlDictConfig(element)

                # treat like list - we assume that if the first two tags
                # in a series are the same, then the rest are the same.
                else:
                    # here, we put the list in dictionary; the key is the
                    # tag name the list elements all share in common, and
                    # the value is the list itself
                    elem_dict = {element[0].tag.split("}")[1]: XmlListConfig(element)}

                # if the tag has attributes, add those to the dict
                if element.items():
                    elem_dict.update(dict(element.items()))
                self.update({element.tag.split("}")[1]: elem_dict})
            # this assumes that if you've got an attribute in a tag,
            # you won't be having any text. This may or may not be a
            # good idea -- time will tell. It works for the way we are
            # currently doing XML configuration files...
            elif element.items():
                # It is possible to have duplicate element tags.
                # If so, convert to a dict of lists
                if element.tag.split("}")[1] in self:
                    if isinstance(self[element.tag.split("}")[1]], list):
                        self[element.tag.split("}")[1]].append(dict(element.items()))
                    else:
                        tmp_list = list()
                        tmp_dict = dict()
                        for k, v in self[element.tag.split("}")[1]].items():
                            if isinstance(k, XmlListConfig):
                                tmp_list.append(k)
                            else:
                                tmp_dict.update({k: v})
                        tmp_list.append(tmp_dict)
                        tmp_list.append(dict(element.items()))
                        self[element.tag.split("}")[1]] = tmp_list
                else:
                    self.update({element.tag.split("}")[1]: dict(element.items())})
            # finally, if there are no child tags and no attributes, extract
            # the text
            else:
                self.update({element.tag.split("}")[1]: element.text})


def process_xml(xml):
    """
    Take the xml and put it into a dictionary. The process the dictionary
    recursively.  This returns a class based on the XML API.  Thus, properties
    will have the camel case found in the Java XML.  This a trade-off
    to reduce the number of "static" classes that all have to be synchronized
    with any changes in the API.
    :param xml: The serialized version of the XML returned from Cloud Control
    :return:  a dynamic class that inherits from ClassFactory
    :rtype: `ClassFactory`
    """
    global attrs
    tree = etree.parse(BytesIO(xml))
    root = tree.getroot()
    elem = root.tag.split("}")[1].capitalize()
    items = dict(root.items())

    if "pageNumber" in items:
        converted_xml = XmlListConfig(root)
        processor(converted_xml[0])
    else:
        converted_xml = XmlDictConfig(root)
        processor(converted_xml)
    klass = class_factory(elem.capitalize(), attrs)
    cls = klass(attrs)
    attrs = {}
    return cls
