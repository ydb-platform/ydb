![Auth0 SDK for Python](https://cdn.auth0.com/website/sdks/banners/auth0-python-banner.png)

![Release](https://img.shields.io/pypi/v/auth0-python)
[![Codecov](https://img.shields.io/codecov/c/github/auth0/auth0-python)](https://codecov.io/gh/auth0/auth0-python)
![Downloads](https://img.shields.io/pypi/dw/auth0-python)
[![License](https://img.shields.io/:license-MIT-blue.svg?style=flat)](https://opensource.org/licenses/MIT)
[![CircleCI](https://img.shields.io/circleci/build/github/auth0/auth0-python)](https://circleci.com/gh/auth0/auth0-python)

<div>
📚 <a href="#documentation">Documentation</a> - 🚀 <a href="#getting-started">Getting started</a> - 💻 <a href="#api-reference">API reference</a> - 💬 <a href="#feedback">Feedback</a>
</div>


Learn how to integrate Auth0 with Python.
## Documentation
- [Docs site](https://www.auth0.com/docs) - explore our docs site and learn more about Auth0.

## Getting started
### Installation
You can install the auth0 Python SDK using the following command.
```
pip install auth0-python
```

> Requires Python 3.7 or higher.

### Usage

#### Authentication SDK
The Authentication SDK is organized into components that mirror the structure of the
[API documentation](https://auth0.com/docs/auth-api).

If you need to sign up a user using their email and password, you can use the Database object.

```python
from auth0.authentication import Database

database = Database('my-domain.us.auth0.com', 'my-client-id')

database.signup(email='user@domain.com', password='secr3t', connection='Username-Password-Authentication')
```

If you need to authenticate a user using their email and password, you can use the `GetToken` object, which enables making requests to the `/oauth/token` endpoint.

```python
from auth0.authentication import GetToken

token = GetToken('my-domain.us.auth0.com', 'my-client-id', client_secret='my-client-secret')

token.login(username='user@domain.com', password='secr3t', realm='Username-Password-Authentication')
```

#### Management SDK
To use the management library you will need to instantiate an Auth0 object with a domain and a [Management API v2 token](https://auth0.com/docs/api/management/v2/tokens). Please note that these token last 24 hours, so if you need it constantly you should ask for it programmatically using the client credentials grant with a [non interactive client](https://auth0.com/docs/api/management/v2/tokens#1-create-and-authorize-a-client) authorized to access the API. For example:

```python
from auth0.authentication import GetToken

domain = 'myaccount.auth0.com'
non_interactive_client_id = 'exampleid'
non_interactive_client_secret = 'examplesecret'

get_token = GetToken(domain, non_interactive_client_id, client_secret=non_interactive_client_secret)
token = get_token.client_credentials('https://{}/api/v2/'.format(domain))
mgmt_api_token = token['access_token']
```

Then use the token you've obtained as follows:

```python
from auth0.management import Auth0

domain = 'myaccount.auth0.com'
mgmt_api_token = 'MGMT_API_TOKEN'

auth0 = Auth0(domain, mgmt_api_token)
```

The `Auth0()` object is now ready to take orders, see our [connections example](https://github.com/auth0/auth0-python/blob/master/EXAMPLES.md#connections) to find out how to use it!

For more code samples on how to integrate the auth0-python SDK in your Python application, have a look at our [examples](https://github.com/auth0/auth0-python/blob/master/EXAMPLES.md).

## API reference

### Authentication Endpoints

- Database ( `authentication.Database` )
- Delegated ( `authentication.Delegated` )
- Enterprise ( `authentication.Enterprise` )
- API Authorization - Get Token ( `authentication.GetToken`)
- BackChannelLogin ( `authentication.BackChannelLogin`)
- Passwordless ( `authentication.Passwordless` )
- PushedAuthorizationRequests ( `authentication.PushedAuthorizationRequests` )
- RevokeToken ( `authentication.RevokeToken` )
- Social ( `authentication.Social` )
- Users ( `authentication.Users` )


### Management Endpoints

- Actions() (`Auth0().action`)
- AttackProtection() (`Auth0().attack_protection`)
- Blacklists() ( `Auth0().blacklists` )
- Branding() ( `Auth0().branding` )
- ClientCredentials() ( `Auth0().client_credentials` )
- ClientGrants() ( `Auth0().client_grants` )
- Clients() ( `Auth0().clients` )
- Connections() ( `Auth0().connections` )
- CustomDomains() ( `Auth0().custom_domains` )
- DeviceCredentials() ( `Auth0().device_credentials` )
- EmailTemplates() ( `Auth0().email_templates` )
- Emails() ( `Auth0().emails` )
- Grants() ( `Auth0().grants` )
- Guardian() ( `Auth0().guardian` )
- Hooks() ( `Auth0().hooks` )
- Jobs() ( `Auth0().jobs` )
- LogStreams() ( `Auth0().log_streams` )
- Logs() ( `Auth0().logs` )
- NetworkAcls() ( `Auth0().network_acls` )
- Organizations() ( `Auth0().organizations` )
- Prompts() ( `Auth0().prompts` )
- ResourceServers() (`Auth0().resource_servers` )
- Roles() ( `Auth0().roles` )
- RulesConfigs() ( `Auth0().rules_configs` )
- Rules() ( `Auth0().rules` )
- SelfServiceProfiles() ( `Auth0().self_service_profiles` )
- Stats() ( `Auth0().stats` )
- Tenants() ( `Auth0().tenants` )
- Tickets() ( `Auth0().tickets` )
- UserBlocks() (`Auth0().user_blocks` )
- UsersByEmail() ( `Auth0().users_by_email` )
- Users() ( `Auth0().users` )

## Support Policy

Our support lifecycle policy mirrors the [Python support schedule](https://devguide.python.org/versions/). We do not support running the SDK on unsupported versions of Python that have ceased to receive security updates. Please ensure your environment remains up to date and running the latest Python version possible.

| SDK Version | Python Version | Support Ends |
|-------------|----------------|--------------|
| 4.x         | 3.12           | Oct 2028     |
|             | 3.11           | Oct 2027     |
|             | 3.10           | Oct 2026     |
|             | 3.9            | Oct 2025     |
|             | 3.8            | Oct 2024     |

> As `pip` [reliably avoids](https://packaging.python.org/en/latest/tutorials/packaging-projects/#configuring-metadata) installing package updates that target incompatible Python versions, we may opt to remove support for [end-of-life](https://en.wikipedia.org/wiki/CPython#Version_history) Python versions during minor SDK updates. These are not considered breaking changes by this SDK.

The following is a list of unsupported Python versions, and the last SDK version supporting them:

| Python Version | Last SDK Version Supporting |
|----------------|-----------------------------|
| >= 3.7         | 4.6.1                       |
| >= 2.0, <= 3.6 | 3.x                         |

You can determine what version of Python you have installed by running:

```
python --version
```

## Feedback

### Contributing

We appreciate feedback and contribution to this repo! Before you get started, please see the following:

- [Auth0's general contribution guidelines](https://github.com/auth0/open-source-template/blob/master/GENERAL-CONTRIBUTING.md)
- [Auth0's code of conduct guidelines](https://github.com/auth0/open-source-template/blob/master/CODE-OF-CONDUCT.md)

### Raise an issue

To provide feedback or report a bug, please [raise an issue on our issue tracker](https://github.com/auth0/auth0-python/issues).

### Vulnerability Reporting

Please do not report security vulnerabilities on the public GitHub issue tracker. The [Responsible Disclosure Program](https://auth0.com/responsible-disclosure-policy) details the procedure for disclosing security issues.

---

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="https://cdn.auth0.com/website/sdks/logos/auth0_light_mode.png"   width="150">
    <source media="(prefers-color-scheme: dark)" srcset="https://cdn.auth0.com/website/sdks/logos/auth0_dark_mode.png" width="150">
    <img alt="Auth0 Logo" src="https://cdn.auth0.com/website/sdks/logos/auth0_light_mode.png" width="150">
  </picture>
</p>
<p align="center">Auth0 is an easy to implement, adaptable authentication and authorization platform. To learn more checkout <a href="https://auth0.com/why-auth0">Why Auth0?</a></p>
<p align="center">
This project is licensed under the MIT license. See the <a href="https://github.com/auth0/auth0-python/blob/master/LICENSE"> LICENSE</a> file for more info.</p>
