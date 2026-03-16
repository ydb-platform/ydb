from __future__ import annotations

from typing import TYPE_CHECKING

from .actions import Actions
from .attack_protection import AttackProtection
from .blacklists import Blacklists
from .branding import Branding
from .client_credentials import ClientCredentials
from .client_grants import ClientGrants
from .clients import Clients
from .connections import Connections
from .custom_domains import CustomDomains
from .device_credentials import DeviceCredentials
from .email_templates import EmailTemplates
from .emails import Emails
from .grants import Grants
from .guardian import Guardian
from .hooks import Hooks
from .jobs import Jobs
from .log_streams import LogStreams
from .logs import Logs
from .network_acls import NetworkAcls
from .organizations import Organizations
from .prompts import Prompts
from .resource_servers import ResourceServers
from .roles import Roles
from .rules import Rules
from .rules_configs import RulesConfigs
from .self_service_profiles import SelfServiceProfiles
from .stats import Stats
from .tenants import Tenants
from .tickets import Tickets
from .user_blocks import UserBlocks
from .users import Users
from .users_by_email import UsersByEmail

if TYPE_CHECKING:
    from auth0.rest import RestClientOptions


class Auth0:
    """Provides easy access to all endpoint classes

    Args:
        domain (str): Your Auth0 domain, e.g: 'username.auth0.com'

        token (str): Management API v2 Token

        rest_options (RestClientOptions): Pass an instance of
            RestClientOptions to configure additional RestClient
            options, such as rate-limit retries.
            (defaults to None)
    """

    def __init__(
        self, domain: str, token: str, rest_options: RestClientOptions | None = None
    ):
        self.actions = Actions(domain, token, rest_options=rest_options)
        self.attack_protection = AttackProtection(
            domain, token, rest_options=rest_options
        )
        self.blacklists = Blacklists(domain, token, rest_options=rest_options)
        self.branding = Branding(domain, token, rest_options=rest_options)
        self.client_credentials = ClientCredentials(
            domain, token, rest_options=rest_options
        )
        self.client_grants = ClientGrants(domain, token, rest_options=rest_options)
        self.clients = Clients(domain, token, rest_options=rest_options)
        self.connections = Connections(domain, token, rest_options=rest_options)
        self.custom_domains = CustomDomains(domain, token, rest_options=rest_options)
        self.device_credentials = DeviceCredentials(
            domain, token, rest_options=rest_options
        )
        self.email_templates = EmailTemplates(domain, token, rest_options=rest_options)
        self.emails = Emails(domain, token, rest_options=rest_options)
        self.grants = Grants(domain, token, rest_options=rest_options)
        self.guardian = Guardian(domain, token, rest_options=rest_options)
        self.hooks = Hooks(domain, token, rest_options=rest_options)
        self.jobs = Jobs(domain, token, rest_options=rest_options)
        self.log_streams = LogStreams(domain, token, rest_options=rest_options)
        self.logs = Logs(domain, token, rest_options=rest_options)
        self.network_acls = NetworkAcls(domain, token, rest_options=rest_options)
        self.organizations = Organizations(domain, token, rest_options=rest_options)
        self.prompts = Prompts(domain, token, rest_options=rest_options)
        self.resource_servers = ResourceServers(
            domain, token, rest_options=rest_options
        )
        self.roles = Roles(domain, token, rest_options=rest_options)
        self.rules_configs = RulesConfigs(domain, token, rest_options=rest_options)
        self.rules = Rules(domain, token, rest_options=rest_options)
        self.self_service_profiles = SelfServiceProfiles(
            domain, token, rest_options=rest_options
        )
        self.stats = Stats(domain, token, rest_options=rest_options)
        self.tenants = Tenants(domain, token, rest_options=rest_options)
        self.tickets = Tickets(domain, token, rest_options=rest_options)
        self.user_blocks = UserBlocks(domain, token, rest_options=rest_options)
        self.users_by_email = UsersByEmail(domain, token, rest_options=rest_options)
        self.users = Users(domain, token, rest_options=rest_options)
