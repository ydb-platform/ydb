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

__all__ = ["SLB_API_VERSION", "SLBDriver"]

try:
    import simplejson as json
except ImportError:
    import json

from libcloud.utils.py3 import u
from libcloud.utils.xml import findall, findattr, findtext
from libcloud.utils.misc import ReprMixin
from libcloud.common.types import LibcloudError
from libcloud.common.aliyun import AliyunXmlResponse, SignedAliyunConnection
from libcloud.loadbalancer.base import Driver, Member, Algorithm, LoadBalancer
from libcloud.loadbalancer.types import State

SLB_API_VERSION = "2014-05-15"
SLB_API_HOST = "slb.aliyuncs.com"
DEFAULT_SIGNATURE_VERSION = "1.0"


STATE_MAPPINGS = {
    "inactive": State.UNKNOWN,
    "active": State.RUNNING,
    "locked": State.PENDING,
}


RESOURCE_EXTRA_ATTRIBUTES_MAP = {
    "balancer": {
        "create_timestamp": {"xpath": "CreateTimeStamp", "transform_func": int},
        "address_type": {"xpath": "AddressType", "transform_func": u},
        "region_id": {"xpath": "RegionId", "transform_func": u},
        "region_id_alias": {"xpath": "RegionIdAlias", "transform_func": u},
        "create_time": {"xpath": "CreateTime", "transform_func": u},
        "master_zone_id": {"xpath": "MasterZoneId", "transform_func": u},
        "slave_zone_id": {"xpath": "SlaveZoneId", "transform_func": u},
        "network_type": {"xpath": "NetworkType", "transform_func": u},
    }
}


SLB_SCHEDULER_TO_ALGORITHM = {
    "wrr": Algorithm.WEIGHTED_ROUND_ROBIN,
    "wlc": Algorithm.WEIGHTED_LEAST_CONNECTIONS,
}


ALGORITHM_TO_SLB_SCHEDULER = {
    Algorithm.WEIGHTED_ROUND_ROBIN: "wrr",
    Algorithm.WEIGHTED_LEAST_CONNECTIONS: "wlc",
}


class SLBConnection(SignedAliyunConnection):
    api_version = SLB_API_VERSION
    host = SLB_API_HOST
    responseCls = AliyunXmlResponse
    service_name = "slb"


class SLBLoadBalancerAttribute:
    """
    This class used to get listeners and backend servers related to a balancer
    listeners is a ``list`` of ``dict``, each element contains
    'ListenerPort' and 'ListenerProtocol' keys.
    backend_servers is a ``list`` of ``dict``, each element contains
    'ServerId' and 'Weight' keys.
    """

    def __init__(self, balancer, listeners, backend_servers, extra=None):
        self.balancer = balancer
        self.listeners = listeners or []
        self.backend_servers = backend_servers or []
        self.extra = extra or {}

    def is_listening(self, port):
        for listener in self.listeners:
            if listener.get("ListenerPort") == port:
                return True
        return False

    def is_attached(self, member):
        for server in self.backend_servers:
            if server.get("Serverid") == member.id:
                return True
        return False

    def __repr__(self):
        return "<SLBLoadBalancerAttribute id={}, ports={}, servers={} ...>".format(
            self.balancer.id,
            self.listeners,
            self.backend_servers,
        )


class SLBLoadBalancerListener(ReprMixin):
    """
    Base SLB load balancer listener class
    """

    _repr_attributes = ["port", "backend_port", "scheduler", "bandwidth"]
    action = None
    option_keys = []

    def __init__(self, port, backend_port, algorithm, bandwidth, extra=None):
        self.port = port
        self.backend_port = backend_port
        self.scheduler = ALGORITHM_TO_SLB_SCHEDULER.get(algorithm, "wrr")
        self.bandwidth = bandwidth
        self.extra = extra or {}

    @classmethod
    def create(cls, port, backend_port, algorithm, bandwidth, extra=None):
        return cls(port, backend_port, algorithm, bandwidth, extra=extra)

    def get_create_params(self):
        params = self.get_required_params()
        options = self.get_optional_params()
        options.update(params)
        return options

    def get_required_params(self):
        params = {
            "Action": self.action,
            "ListenerPort": self.port,
            "BackendServerPort": self.backend_port,
            "Scheduler": self.scheduler,
            "Bandwidth": self.bandwidth,
        }
        return params

    def get_optional_params(self):
        options = {}
        for option in self.option_keys:
            if self.extra and option in self.extra:
                options[option] = self.extra[option]
        return options


class SLBLoadBalancerHttpListener(SLBLoadBalancerListener):
    """
    This class represents a rule to route http request to the backends.
    """

    action = "CreateLoadBalancerHTTPListener"
    option_keys = [
        "XForwardedFor",
        "StickySessionType",
        "CookieTimeout",
        "Cookie",
        "HealthCheckDomain",
        "HealthCheckURI",
        "HealthCheckConnectPort",
        "HealthyThreshold",
        "UnhealthyThreshold",
        "HealthCheckTimeout",
        "HealthCheckInterval",
        "HealthCheckHttpCode",
    ]

    def __init__(
        self,
        port,
        backend_port,
        algorithm,
        bandwidth,
        sticky_session,
        health_check,
        extra=None,
    ):
        super().__init__(port, backend_port, algorithm, bandwidth, extra=extra)
        self.sticky_session = sticky_session
        self.health_check = health_check

    def get_required_params(self):
        params = super().get_required_params()
        params["StickySession"] = self.sticky_session
        params["HealthCheck"] = self.health_check
        return params

    @classmethod
    def create(cls, port, backend_port, algorithm, bandwidth, extra={}):
        if "StickySession" not in extra:
            raise AttributeError("StickySession is required")
        if "HealthCheck" not in extra:
            raise AttributeError("HealthCheck is required")
        sticky_session = extra["StickySession"]
        health_check = extra["HealthCheck"]
        return cls(
            port,
            backend_port,
            algorithm,
            bandwidth,
            sticky_session,
            health_check,
            extra=extra,
        )


class SLBLoadBalancerHttpsListener(SLBLoadBalancerListener):
    """
    This class represents a rule to route https request to the backends.
    """

    action = "CreateLoadBalancerHTTPSListener"
    option_keys = [
        "XForwardedFor",
        "StickySessionType",
        "CookieTimeout",
        "Cookie",
        "HealthCheckDomain",
        "HealthCheckURI",
        "HealthCheckConnectPort",
        "HealthyThreshold",
        "UnhealthyThreshold",
        "HealthCheckTimeout",
        "HealthCheckInterval",
        "HealthCheckHttpCode",
    ]

    def __init__(
        self,
        port,
        backend_port,
        algorithm,
        bandwidth,
        sticky_session,
        health_check,
        certificate_id,
        extra=None,
    ):
        super().__init__(port, backend_port, algorithm, bandwidth, extra=extra)
        self.sticky_session = sticky_session
        self.health_check = health_check
        self.certificate_id = certificate_id

    def get_required_params(self):
        params = super().get_required_params()
        params["StickySession"] = self.sticky_session
        params["HealthCheck"] = self.health_check
        params["ServerCertificateId"] = self.certificate_id
        return params

    @classmethod
    def create(cls, port, backend_port, algorithm, bandwidth, extra={}):
        if "StickySession" not in extra:
            raise AttributeError("StickySession is required")
        if "HealthCheck" not in extra:
            raise AttributeError("HealthCheck is required")
        if "ServerCertificateId" not in extra:
            raise AttributeError("ServerCertificateId is required")
        sticky_session = extra["StickySession"]
        health_check = extra["HealthCheck"]
        certificate_id = extra["ServerCertificateId"]
        return cls(
            port,
            backend_port,
            algorithm,
            bandwidth,
            sticky_session,
            health_check,
            certificate_id,
            extra=extra,
        )


class SLBLoadBalancerTcpListener(SLBLoadBalancerListener):
    """
    This class represents a rule to route tcp request to the backends.
    """

    action = "CreateLoadBalancerTCPListener"
    option_keys = [
        "PersistenceTimeout",
        "HealthCheckType",
        "HealthCheckDomain",
        "HealthCheckURI",
        "HealthCheckConnectPort",
        "HealthyThreshold",
        "UnhealthyThreshold",
        "HealthCheckConnectTimeout",
        "HealthCheckInterval",
        "HealthCheckHttpCode",
    ]


class SLBLoadBalancerUdpListener(SLBLoadBalancerTcpListener):
    """
    This class represents a rule to route udp request to the backends.
    """

    action = "CreateLoadBalancerUDPListener"
    option_keys = [
        "PersistenceTimeout",
        "HealthCheckConnectPort",
        "HealthyThreshold",
        "UnhealthyThreshold",
        "HealthCheckConnectTimeout",
        "HealthCheckInterval",
    ]


class SLBServerCertificate(ReprMixin):
    _repr_attributes = ["id", "name", "fingerprint"]

    def __init__(self, id, name, fingerprint):
        self.id = id
        self.name = name
        self.fingerprint = fingerprint


PROTOCOL_TO_LISTENER_MAP = {
    "http": SLBLoadBalancerHttpListener,
    "https": SLBLoadBalancerHttpsListener,
    "tcp": SLBLoadBalancerTcpListener,
    "udp": SLBLoadBalancerUdpListener,
}


class SLBDriver(Driver):
    """
    Aliyun SLB load balancer driver.
    """

    name = "Aliyun Server Load Balancer"
    website = "https://www.aliyun.com/product/slb"
    connectionCls = SLBConnection
    path = "/"
    namespace = None

    _VALUE_TO_ALGORITHM_MAP = SLB_SCHEDULER_TO_ALGORITHM

    _ALGORITHM_TO_VALUE_MAP = ALGORITHM_TO_SLB_SCHEDULER

    def __init__(self, access_id, secret, region):
        super().__init__(access_id, secret)
        self.region = region

    def list_protocols(self):
        return list(PROTOCOL_TO_LISTENER_MAP.keys())

    def list_balancers(self, ex_balancer_ids=None, ex_filters=None):
        """
        List all loadbalancers

        @inherits :class:`Driver.list_balancers`

        :keyword ex_balancer_ids: a list of balancer ids to filter results
                                  Only balancers which's id in this list
                                  will be returned
        :type ex_balancer_ids: ``list`` of ``str``

        :keyword ex_filters: attributes to filter results. Only balancers
                             which have all the desired attributes
                             and values will be returned
        :type ex_filters: ``dict``
        """

        params = {"Action": "DescribeLoadBalancers", "RegionId": self.region}
        if ex_balancer_ids and isinstance(ex_balancer_ids, list):
            params["LoadBalancerId"] = ",".join(ex_balancer_ids)

        if ex_filters and isinstance(ex_filters, dict):
            ex_filters.update(params)
            params = ex_filters
        resp_body = self.connection.request(self.path, params=params).object
        return self._to_balancers(resp_body)

    def create_balancer(
        self,
        name,
        port,
        protocol,
        algorithm,
        members,
        ex_bandwidth=None,
        ex_internet_charge_type=None,
        ex_address_type=None,
        ex_vswitch_id=None,
        ex_master_zone_id=None,
        ex_slave_zone_id=None,
        ex_client_token=None,
        **kwargs,
    ):
        """
        Create a new load balancer instance

        @inherits: :class:`Driver.create_balancer`

        :keyword ex_bandwidth: The max bandwidth limit for `paybybandwidth`
                               internet charge type, in Mbps unit
        :type ex_bandwidth: ``int`` in range [1, 1000]

        :keyword ex_internet_charge_type: The internet charge type
        :type ex_internet_charge_type: a ``str`` of `paybybandwidth`
                                       or `paybytraffic`

        :keyword ex_address_type: The listening IP address type
        :type ex_address_type: a ``str`` of `internet` or `intranet`

        :keyword ex_vswitch_id: The vswitch id in a VPC network
        :type ex_vswitch_id: ``str``

        :keyword ex_master_zone_id: The id of the master availability zone
        :type ex_master_zone_id: ``str``

        :keyword ex_slave_zone_id: The id of the slave availability zone
        :type ex_slave_zone_id: ``str``

        :keyword ex_client_token: The token generated by client to
                                  identify requests
        :type ex_client_token: ``str``
        """

        # 1.Create load balancer
        params = {"Action": "CreateLoadBalancer", "RegionId": self.region}
        if name:
            params["LoadBalancerName"] = name
        if not port:
            raise AttributeError("port is required")
        if not protocol:
            # NOTE(samsong8610): Use http listener as default
            protocol = "http"
        if protocol not in PROTOCOL_TO_LISTENER_MAP:
            raise AttributeError("unsupported protocol %s" % protocol)

        # Bandwidth in range [1, 1000] Mbps
        bandwidth = -1
        if ex_bandwidth:
            try:
                bandwidth = int(ex_bandwidth)
            except ValueError:
                raise AttributeError("ex_bandwidth should be a integer in " "range [1, 1000].")
            params["Bandwidth"] = bandwidth

        if ex_internet_charge_type:
            if ex_internet_charge_type.lower() == "paybybandwidth":
                if bandwidth == -1:
                    raise AttributeError(
                        "PayByBandwidth internet charge type" " need ex_bandwidth be set"
                    )
            params["InternetChargeType"] = ex_internet_charge_type

        if ex_address_type:
            if ex_address_type.lower() not in ("internet", "intranet"):
                raise AttributeError('ex_address_type should be "internet" ' 'or "intranet"')
            params["AddressType"] = ex_address_type

        if ex_vswitch_id:
            params["VSwitchId"] = ex_vswitch_id

        if ex_master_zone_id:
            params["MasterZoneId"] = ex_master_zone_id
        if ex_slave_zone_id:
            params["SlaveZoneId"] = ex_slave_zone_id

        if ex_client_token:
            params["ClientToken"] = ex_client_token

        if members and isinstance(members, list):
            backend_ports = [member.port for member in members]
            if len(set(backend_ports)) != 1:
                raise AttributeError("the ports of members should be unique")
            # NOTE(samsong8610): If members do not provide backend port,
            #                    default to listening port
            backend_port = backend_ports[0] or port
        else:
            backend_port = port

        balancer = None
        try:
            resp_body = self.connection.request(self.path, params).object
            balancer = self._to_balancer(resp_body)
            balancer.port = port

            # 2.Add backend servers
            if members is None:
                members = []
            for member in members:
                self.balancer_attach_member(balancer, member)
            # 3.Create listener
            # NOTE(samsong8610): Assume only create a listener which uses all
            #                    the bandwidth.
            self.ex_create_listener(
                balancer, backend_port, protocol, algorithm, bandwidth, **kwargs
            )
            self.ex_start_listener(balancer, port)
            return balancer
        except Exception as e:
            if balancer is not None:
                try:
                    self.destroy_balancer(balancer)
                except Exception:
                    pass
            raise e

    def destroy_balancer(self, balancer):
        params = {"Action": "DeleteLoadBalancer", "LoadBalancerId": balancer.id}
        resp = self.connection.request(self.path, params)
        return resp.success()

    def get_balancer(self, balancer_id):
        balancers = self.list_balancers(ex_balancer_ids=[balancer_id])
        if len(balancers) != 1:
            raise LibcloudError("could not find load balancer with id %s" % balancer_id)
        return balancers[0]

    def balancer_attach_compute_node(self, balancer, node):
        if len(node.public_ips) > 0:
            ip = node.public_ips[0]
        else:
            ip = node.private_ips[0]
        member = Member(id=node.id, ip=ip, port=balancer.port)
        return self.balancer_attach_member(balancer, member)

    def balancer_attach_member(self, balancer, member):
        params = {"Action": "AddBackendServers", "LoadBalancerId": balancer.id}
        if member and isinstance(member, Member):
            params["BackendServers"] = self._to_servers_json([member])
        self.connection.request(self.path, params)
        return member

    def balancer_detach_member(self, balancer, member):
        params = {"Action": "RemoveBackendServers", "LoadBalancerId": balancer.id}
        if member and isinstance(member, Member):
            params["BackendServers"] = self._list_to_json([member.id])
        self.connection.request(self.path, params)
        return member

    def balancer_list_members(self, balancer):
        attribute = self.ex_get_balancer_attribute(balancer)
        members = [
            Member(
                server["ServerId"],
                None,
                None,
                balancer=balancer,
                extra={"Weight": server["Weight"]},
            )
            for server in attribute.backend_servers
        ]
        return members

    def ex_get_balancer_attribute(self, balancer):
        """
        Get balancer attribute

        :param balancer: the balancer to get attribute
        :type balancer: ``LoadBalancer``

        :return: the balancer attribute
        :rtype: ``SLBLoadBalancerAttribute``
        """

        params = {
            "Action": "DescribeLoadBalancerAttribute",
            "LoadBalancerId": balancer.id,
        }
        resp_body = self.connection.request(self.path, params).object
        attribute = self._to_balancer_attribute(resp_body)
        return attribute

    def ex_list_listeners(self, balancer):
        """
        Get all listener related to the given balancer

        :param balancer: the balancer to list listeners
        :type balancer: ``LoadBalancer``

        :return: a list of listeners
        :rtype: ``list`` of ``SLBLoadBalancerListener``
        """

        attribute = self.ex_get_balancer_attribute(balancer)
        listeners = [
            SLBLoadBalancerListener(each["ListenerPort"], None, None, None)
            for each in attribute.listeners
        ]
        return listeners

    def ex_create_listener(self, balancer, backend_port, protocol, algorithm, bandwidth, **kwargs):
        """
        Create load balancer listening rule.

        :param balancer: the balancer which the rule belongs to.
                         The listener created will listen on the port of the
                         the balancer as default. 'ListenerPort' in kwargs
                         will *OVERRIDE* it.
        :type balancer: ``LoadBalancer``

        :param backend_port: the backend server port
        :type backend_port: ``int``

        :param protocol: the balancer protocol, default to http
        :type protocol: ``str``

        :param algorithm: the balancer routing algorithm
        :type algorithm: ``Algorithm``

        :param bandwidth: the listener bandwidth limits
        :type bandwidth: ``str``

        :return: the created listener
        :rtype: ``SLBLoadBalancerListener``
        """

        cls = PROTOCOL_TO_LISTENER_MAP.get(protocol, SLBLoadBalancerHttpListener)
        if "ListenerPort" in kwargs:
            port = kwargs["ListenerPort"]
        else:
            port = balancer.port
        listener = cls.create(port, backend_port, algorithm, bandwidth, extra=kwargs)
        params = listener.get_create_params()
        params["LoadBalancerId"] = balancer.id
        params["RegionId"] = self.region
        resp = self.connection.request(self.path, params)
        return resp.success()

    def ex_start_listener(self, balancer, port):
        """
        Start balancer's listener listening the given port.

        :param balancer: a load balancer
        :type balancer: ``LoadBalancer``

        :param port: listening port
        :type port: ``int``

        :return: whether operation is success
        :rtype: ``bool``
        """

        params = {
            "Action": "StartLoadBalancerListener",
            "LoadBalancerId": balancer.id,
            "ListenerPort": port,
        }
        resp = self.connection.request(self.path, params)
        return resp.success()

    def ex_stop_listener(self, balancer, port):
        """
        Stop balancer's listener listening the given port.

        :param balancer: a load balancer
        :type balancer: ``LoadBalancer``

        :param port: listening port
        :type port: ``int``

        :return: whether operation is success
        :rtype: ``bool``
        """

        params = {
            "Action": "StopLoadBalancerListener",
            "LoadBalancerId": balancer.id,
            "ListenerPort": port,
        }
        resp = self.connection.request(self.path, params)
        return resp.success()

    def ex_upload_certificate(self, name, server_certificate, private_key):
        """
        Upload certificate and private key for https load balancer listener

        :param name: the certificate name
        :type name: ``str``

        :param server_certificate: the content of the certificate to upload
                                   in PEM format
        :type server_certificate: ``str``

        :param private_key: the content of the private key to upload
                            in PEM format
        :type private_key: ``str``

        :return: new created certificate info
        :rtype: ``SLBServerCertificate``
        """

        params = {
            "Action": "UploadServerCertificate",
            "RegionId": self.region,
            "ServerCertificate": server_certificate,
            "PrivateKey": private_key,
        }
        if name:
            params["ServerCertificateName"] = name
        resp_body = self.connection.request(self.path, params).object
        return self._to_server_certificate(resp_body)

    def ex_list_certificates(self, certificate_ids=[]):
        """
        List all server certificates

        :param certificate_ids: certificate ids to filter results
        :type certificate_ids: ``str``

        :return: certificates
        :rtype: ``SLBServerCertificate``
        """

        params = {"Action": "DescribeServerCertificates", "RegionId": self.region}
        if certificate_ids and isinstance(certificate_ids, list):
            params["ServerCertificateId"] = ",".join(certificate_ids)

        resp_body = self.connection.request(self.path, params).object
        cert_elements = findall(
            resp_body, "ServerCertificates/ServerCertificate", namespace=self.namespace
        )
        certificates = [self._to_server_certificate(el) for el in cert_elements]
        return certificates

    def ex_delete_certificate(self, certificate_id):
        """
        Delete the given server certificate

        :param certificate_id: the id of the certificate to delete
        :type certificate_id: ``str``

        :return: whether process is success
        :rtype: ``bool``
        """

        params = {
            "Action": "DeleteServerCertificate",
            "RegionId": self.region,
            "ServerCertificateId": certificate_id,
        }
        resp = self.connection.request(self.path, params)
        return resp.success()

    def ex_set_certificate_name(self, certificate_id, name):
        """
        Set server certificate name.

        :param certificate_id: the id of the server certificate to update
        :type certificate_id: ``str``

        :param name: the new name
        :type name: ``str``

        :return: whether updating is success
        :rtype: ``bool``
        """

        params = {
            "Action": "SetServerCertificateName",
            "RegionId": self.region,
            "ServerCertificateId": certificate_id,
            "ServerCertificateName": name,
        }
        resp = self.connection.request(self.path, params)
        return resp.success()

    def _to_balancers(self, element):
        xpath = "LoadBalancers/LoadBalancer"
        return [
            self._to_balancer(el)
            for el in findall(element=element, xpath=xpath, namespace=self.namespace)
        ]

    def _to_balancer(self, el):
        _id = findtext(element=el, xpath="LoadBalancerId", namespace=self.namespace)
        name = findtext(element=el, xpath="LoadBalancerName", namespace=self.namespace)
        status = findtext(element=el, xpath="LoadBalancerStatus", namespace=self.namespace)
        state = STATE_MAPPINGS.get(status, State.UNKNOWN)
        address = findtext(element=el, xpath="Address", namespace=self.namespace)
        extra = self._get_extra_dict(el, RESOURCE_EXTRA_ATTRIBUTES_MAP["balancer"])

        balancer = LoadBalancer(
            id=_id,
            name=name,
            state=state,
            ip=address,
            port=None,
            driver=self,
            extra=extra,
        )
        return balancer

    def _create_list_params(self, params, items, label):
        """
        return parameter list
        """
        if isinstance(items, str):
            items = [items]
        for index, item in enumerate(items):
            params[label % (index + 1)] = item
        return params

    def _get_extra_dict(self, element, mapping):
        """
        Extract attributes from the element based on rules provided in the
        mapping dictionary.

        :param      element: Element to parse the values from.
        :type       element: xml.etree.ElementTree.Element.

        :param      mapping: Dictionary with the extra layout
        :type       node: :class:`Node`

        :rtype: ``dict``
        """
        extra = {}
        for attribute, values in mapping.items():
            transform_func = values["transform_func"]
            value = findattr(element=element, xpath=values["xpath"], namespace=self.namespace)
            if value:
                try:
                    extra[attribute] = transform_func(value)
                except Exception:
                    extra[attribute] = None
            else:
                extra[attribute] = value

        return extra

    def _to_servers_json(self, members):
        servers = []
        for each in members:
            server = {"ServerId": each.id, "Weight": "100"}
            if "Weight" in each.extra:
                server["Weight"] = each.extra["Weight"]
            servers.append(server)
        try:
            return json.dumps(servers)
        except Exception:
            raise AttributeError("could not convert member to backend server")

    def _to_balancer_attribute(self, element):
        balancer = self._to_balancer(element)
        port_proto_elements = findall(
            element,
            "ListenerPortsAndProtocol/ListenerPortAndProtocol",
            namespace=self.namespace,
        )
        if len(port_proto_elements) > 0:
            listeners = [self._to_port_and_protocol(el) for el in port_proto_elements]
        else:
            port_elements = findall(element, "ListenerPorts/ListenerPort", namespace=self.namespace)
            listeners = [
                {"ListenerPort": el.text, "ListenerProtocol": "http"} for el in port_elements
            ]
        server_elements = findall(element, "BackendServers/BackendServer", namespace=self.namespace)
        backend_servers = [self._to_server_and_weight(el) for el in server_elements]
        return SLBLoadBalancerAttribute(balancer, listeners, backend_servers)

    def _to_port_and_protocol(self, el):
        port = findtext(el, "ListenerPort", namespace=self.namespace)
        protocol = findtext(el, "ListenerProtocol", namespace=self.namespace)
        return {"ListenerPort": port, "ListenerProtocol": protocol}

    def _to_server_and_weight(self, el):
        server_id = findtext(el, "ServerId", namespace=self.namespace)
        weight = findtext(el, "Weight", namespace=self.namespace)
        return {"ServerId": server_id, "Weight": weight}

    def _to_server_certificate(self, el):
        _id = findtext(el, "ServerCertificateId", namespace=self.namespace)
        name = findtext(el, "ServerCertificateName", namespace=self.namespace)
        fingerprint = findtext(el, "Fingerprint", namespace=self.namespace)
        return SLBServerCertificate(id=_id, name=name, fingerprint=fingerprint)

    def _list_to_json(self, value):
        try:
            return json.dumps(value)
        except Exception:
            return "[]"
