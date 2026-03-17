"""Nomad Python library"""

import os
from typing import Optional

import requests

from nomad import api


class Nomad:  # pylint: disable=too-many-public-methods,too-many-instance-attributes
    """
    Nomad API
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        host: str = "127.0.0.1",
        secure: bool = False,
        port: int = 4646,
        address: Optional[str] = None,
        user_agent: Optional[str] = None,
        namespace: Optional[str] = None,
        token: Optional[str] = None,
        timeout: int = 5,
        region: Optional[str] = None,
        version: str = "v1",
        verify: bool = False,
        cert: tuple = (),
        session: requests.Session = None,
    ):
        """Nomad api client

        https://github.com/jrxFive/python-nomad/

         optional arguments:
          - host (defaults 127.0.0.1), string ip or name of the nomad api server/agent that will be used.
          - port (defaults 4646), integer port that will be used to connect.
          - user_agent (defaults None), custom user agent for requests to Nomad.
          - secure (defaults False), define if the protocol is secured or not (https or http)
          - version (defaults v1), version of the api of nomad.
          - verify (defaults False), verify the certificate when tls/ssl is enabled
                              at nomad.
          - cert (defaults empty), cert, or key and cert file to validate the certificate
                              configured at nomad.
          - region (defaults None), version of the region to use. It will be used then
                              regions of the current agent of the connection.
          - namespace (defaults to None), Specifies the enterprise namespace that will
                              be use to deploy or to ask info to nomad.
          - token (defaults to None), Specifies to append ACL token to the headers to
                              make authentication on secured based nomad environments.
          - session (defaults to None), allows for injecting a prepared requests.Session object that
                              all requests to Nomad should use.
         returns: Nomad api client object

         raises:
           - nomad.api.exceptions.BaseNomadException
           - nomad.api.exceptions.URLNotFoundNomadException
           - nomad.api.exceptions.URLNotAuthorizedNomadException
        """
        cert = cert or (
            os.getenv("NOMAD_CLIENT_CERT", None),
            os.getenv("NOMAD_CLIENT_KEY", None),
        )

        self.host = host
        self.secure = secure
        self.port = port
        self.address = address or os.getenv("NOMAD_ADDR", None)
        self.user_agent = user_agent
        self.region = region or os.getenv("NOMAD_REGION", None)
        self.timeout = timeout
        self.version = version
        self.token = token or os.getenv("NOMAD_TOKEN", None)
        self.verify = verify
        self.cert = cert if all(cert) else ()
        self.session = session
        self.__namespace = namespace or os.getenv("NOMAD_NAMESPACE", None)

        self.requester_settings = {
            "address": self.address,
            "uri": self.get_uri(),
            "port": self.port,
            "user_agent": self.user_agent,
            "namespace": self.__namespace,
            "token": self.token,
            "timeout": self.timeout,
            "version": self.version,
            "verify": self.verify,
            "cert": self.cert,
            "region": self.region,
            "session": self.session,
        }

        self._acl = api.Acl(**self.requester_settings)
        self._agent = api.Agent(**self.requester_settings)
        self._allocation = api.Allocation(**self.requester_settings)
        self._allocations = api.Allocations(**self.requester_settings)
        self._client = api.Client(**self.requester_settings)
        self._deployment = api.Deployment(**self.requester_settings)
        self._deployments = api.Deployments(**self.requester_settings)
        self._evaluation = api.Evaluation(**self.requester_settings)
        self._evaluations = api.Evaluations(**self.requester_settings)
        self._event = api.Event(**self.requester_settings)
        self._job = api.Job(**self.requester_settings)
        self._jobs = api.Jobs(**self.requester_settings)
        self._metrics = api.Metrics(**self.requester_settings)
        self._namespace = api.Namespace(**self.requester_settings)
        self._namespaces = api.Namespaces(**self.requester_settings)
        self._node = api.Node(**self.requester_settings)
        self._nodes = api.Nodes(**self.requester_settings)
        self._operator = api.Operator(**self.requester_settings)
        self._regions = api.Regions(**self.requester_settings)
        self._scaling = api.Scaling(**self.requester_settings)
        self._sentinel = api.Sentinel(**self.requester_settings)
        self._search = api.Search(**self.requester_settings)
        self._status = api.Status(**self.requester_settings)
        self._system = api.System(**self.requester_settings)
        self._validate = api.Validate(**self.requester_settings)
        self._variable = api.Variable(**self.requester_settings)
        self._variables = api.Variables(**self.requester_settings)

    def get_uri(self):
        """
        Get Nomad host
        """
        if self.secure:
            protocol = "https"
        else:
            protocol = "http"
        return f"{protocol}://{self.host}"

    def get_namespace(self):
        """
        Get Nomad namaspace
        """
        return self.__namespace

    def get_token(self):
        """
        Get Nomad token
        """
        return self.token

    @property
    def jobs(self):
        """
        Jobs API
        """
        return self._jobs

    @property
    def job(self):
        """
        Job API
        """
        return self._job

    @property
    def nodes(self):
        """
        Nodes API
        """
        return self._nodes

    @property
    def node(self):
        """
        Node API
        """
        return self._node

    @property
    def allocations(self):
        """
        Allocations API
        """
        return self._allocations

    @property
    def allocation(self):
        """
        Allocation API
        """
        return self._allocation

    @property
    def evaluations(self):
        """
        Evaluations API
        """
        return self._evaluations

    @property
    def evaluation(self):
        """
        Evaluation API
        """
        return self._evaluation

    @property
    def event(self):
        """
        Event API
        """
        return self._event

    @property
    def agent(self):
        """
        Agent API
        """
        return self._agent

    @property
    def client(self):
        """
        Client API
        """
        return self._client

    @property
    def deployments(self):
        """
        Deployments API
        """
        return self._deployments

    @property
    def deployment(self):
        """
        Deployment API
        """
        return self._deployment

    @property
    def regions(self):
        """
        Regions API
        """
        return self._regions

    @property
    def scaling(self):
        """
        Scaling API
        """
        return self._scaling

    @property
    def status(self):
        """
        Status API
        """
        return self._status

    @property
    def system(self):
        """
        System API
        """
        return self._system

    @property
    def operator(self):
        """
        Operator API
        """
        return self._operator

    @property
    def validate(self):
        """
        Validate API
        """
        return self._validate

    @property
    def namespaces(self):
        """
        Namespaces API
        """
        return self._namespaces

    @property
    def namespace(self):
        """
        Namespace API
        """
        return self._namespace

    @property
    def acl(self):
        """
        ACL API
        """
        return self._acl

    @property
    def sentinel(self):
        """
        Sentinel API
        """
        return self._sentinel

    @property
    def search(self):
        """
        Search API
        """
        return self._search

    @property
    def metrics(self):
        """
        Metrics API
        """
        return self._metrics

    @property
    def variable(self):
        """
        Variable API
        """
        return self._variable

    @property
    def variables(self):
        """
        Variables API
        """
        return self._variables
