## whodap

[![PyPI version](https://badge.fury.io/py/whodap.svg)](https://badge.fury.io/py/whodap)
![example workflow](https://github.com/pogzyb/whodap/actions/workflows/run-build-and-test.yml/badge.svg)
[![codecov](https://codecov.io/gh/pogzyb/whodap/branch/main/graph/badge.svg?token=NCfdf6ftb9)](https://codecov.io/gh/pogzyb/whodap)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

`whodap` | Simple RDAP Utility for Python

- Support for asyncio HTTP requests ([`httpx`](https://www.python-httpx.org/))
- Leverages the [`SimpleNamespace`](https://docs.python.org/3/library/types.html#types.SimpleNamespace) type for cleaner RDAP Response traversal
- Keeps the familiar look of WHOIS via the `to_whois_dict` method for DNS lookups

#### Quickstart

```python
import asyncio
from pprint import pprint

import whodap

# Looking up a domain name
response = whodap.lookup_domain(domain='bitcoin', tld='org') 
# Equivalent asyncio call
loop = asyncio.get_event_loop()
response = loop.run_until_complete(whodap.aio_lookup_domain(domain='bitcoin', tld='org'))
# "response" is a DomainResponse object. It contains the output from the RDAP lookup.
print(response)
# Traverse the DomainResponse via "dot" notation
print(response.events)
"""
[{
  "eventAction": "last update of RDAP database",
  "eventDate": "2021-04-23T21:50:03"
},
 {
  "eventAction": "registration",
  "eventDate": "2008-08-18T13:19:55"
}, ... ]
"""
# Retrieving the registration date from above:
print(response.events[1].eventDate)
"""
2008-08-18 13:19:55
"""
# Don't want "dot" notation? Use `to_dict` to get the RDAP response as a dictionary
pprint(response.to_dict())
# Use `to_whois_dict` for the familiar look of WHOIS output
pprint(response.to_whois_dict())
"""
{abuse_email: 'abuse@namecheap.com',
 abuse_phone: 'tel:+1.6613102107',
 admin_address: 'P.O. Box 0823-03411, Panama, Panama, PA',
 admin_email: '2603423f6ed44178a3b9d728827aa19a.protect@whoisguard.com',
 admin_fax: 'fax:+51.17057182',
 admin_name: 'WhoisGuard Protected',
 admin_organization: 'WhoisGuard, Inc.',
 admin_phone: 'tel:+507.8365503',
 billing_address: None,
 billing_email: None,
 billing_fax: None,
 billing_name: None,
 billing_organization: None,
 billing_phone: None,
 created_date: datetime.datetime(2008, 8, 18, 13, 19, 55),
 domain_name: 'bitcoin.org',
 expires_date: datetime.datetime(2029, 8, 18, 13, 19, 55),
 nameservers: ['dns1.registrar-servers.com', 'dns2.registrar-servers.com'],
 registrant_address: 'P.O. Box 0823-03411, Panama, Panama, PA',
 registrant_email: '2603423f6ed44178a3b9d728827aa19a.protect@whoisguard.com',
 registrant_fax: 'fax:+51.17057182',
 registrant_name: 'WhoisGuard Protected',
 registrant_organization: None,
 registrant_phone: 'tel:+507.8365503',
 registrar_address: '4600 E Washington St #305, Phoenix, Arizona, 85034',
 registrar_email: 'support@namecheap.com',
 registrar_fax: None,
 registrar_name: 'NAMECHEAP INC',
 registrar_phone: 'tel:+1.6613102107',
 status: ['client transfer prohibited'],
 technical_address: 'P.O. Box 0823-03411, Panama, Panama, PA',
 technical_email: '2603423f6ed44178a3b9d728827aa19a.protect@whoisguard.com',
 technical_fax: 'fax:+51.17057182',
 technical_name: 'WhoisGuard Protected',
 technical_organization: 'WhoisGuard, Inc.',
 technical_phone: 'tel:+507.8365503',
 updated_date: datetime.datetime(2019, 11, 24, 13, 58, 35)}
"""
```

#### Exported Functions and Classes

| Object      | Description |
| ----------- | ----------- |
|  `lookup_domain`      | Performs an RDAP query for the given Domain and TLD                     |
|  `lookup_ipv4`        | Performs an RDAP query for the given IPv4 address                       |
|  `lookup_ipv6`        | Performs an RDAP query for the given IPv6 address                       |
|  `lookup_asn`         | Performs an RDAP query for the Autonomous System with the given Number  |
|  `aio_lookup_domain`  | async counterpart to `lookup_domain`  |
|  `aio_lookup_ipv4`    | async counterpart to `lookup_ipv4`    |
|  `aio_lookup_ipv6`    | async counterpart to `lookup_ipv6`    |
|  `aio_lookup_asn`     | async counterpart to `lookup_asn`     |
|  `DNSClient`     | Reusable client for RDAP DNS queries    |
|  `IPv4Client`     | Reusable client for RDAP IPv4 queries     |
|  `IPv6Client`     | Reusable client for RDAP IPv6 queries     |
|  `ASNClient`     | Reusable client for RDAP ASN queries     |


#### Common Usage Patterns

- Using the DNSClient:
```python
import whodap

# Initialize an instance of DNSClient using classmethods: `new_client` or `new_aio_client`
dns_client = whodap.DNSClient.new_client()
for domain, tld in [('google', 'com'), ('google', 'buzz')]:
    response = dns_client.lookup(domain, tld)
    
# Equivalent asyncio call
dns_client = await whodap.DNSClient.new_aio_client()
for domain, tld in [('google', 'com'), ('google', 'buzz')]:
    response = await dns_client.aio_lookup(domain, tld)
    
# Use the DNSClient contextmanagers: `new_client_context` or `new_aio_client_context`
with whodap.DNSClient.new_client_context() as dns_client:
    for domain, tld in [('google', 'com'), ('google', 'buzz')]:
        response = dns_client.lookup(domain, tld)

# Equivalent asyncio call
async with whodap.DNSClient.new_aio_client_context() as dns_client:
    for domain, tld in [('google', 'com'), ('google', 'buzz')]:
        response = await dns_client.aio_lookup(domain, tld)
```

- Configurable `httpx` client:

```python
import asyncio

import httpx
import whodap

# Initialize a custom, pre-configured httpx client ...
httpx_client = httpx.Client(proxies=httpx.Proxy('https://user:pw@proxy_url.net'))
# ... or an async client
aio_httpx_client = httpx.AsyncClient(proxies=httpx.Proxy('http://user:pw@proxy_url.net'))

# Three common methods for leveraging httpx clients are outlined below:

# 1) Pass the httpx client directly into the convenience functions: `lookup_domain` or `aio_lookup_domain`
# Important: In this scenario, you are responsible for closing the httpx client.
# In this example, the given httpx client is used as a contextmanager; ensuring it is "closed" when finished.
async with aio_httpx_client:
    futures = []
    for domain, tld in [('google', 'com'), ('google', 'buzz')]:
        task = whodap.aio_lookup_domain(domain, tld, httpx_client=aio_httpx_client)
        futures.append(task)
    await asyncio.gather(*futures)

# 2) Pass the httpx_client into the DNSClient classmethod: `new_client` or `new_aio_client`
aio_dns_client = await whodap.DNSClient.new_aio_client(aio_httpx_client)
result = await aio_dns_client.aio_lookup('google', 'buzz')
await aio_httpx_client.aclose()

# 3) Pass the httpx_client into the DNSClient contextmanagers: `new_client_context` or `new_aio_client_context`
# This method ensures the underlying httpx_client is closed when exiting the "with" block.
async with whodap.DNSClient.new_aio_client_context(aio_httpx_client) as dns_client:
    for domain, tld in [('google', 'com'), ('google', 'buzz')]:
        response = await dns_client.aio_lookup(domain, tld)
```

- Using the `to_whois_dict` method and `RDAPConformanceException`
```python
import logging

from whodap import lookup_domain
from whodap.errors import RDAPConformanceException

logger = logging.getLogger(__name__)

# strict = False (default)
rdap_response = lookup_domain("example", "com")
whois_format = rdap_response.to_whois_dict()
logger.info(f"whois={whois_format}")
# Given a valid RDAP response, the `to_whois_dict` method will attempt to
# convert the RDAP format into a flattened dictionary of WHOIS key/values

# strict = True
try:
    # Unfortunately, there are instances in which the RDAP protocol is not
    # properly implemented by the registrar. By default, the `to_whois_dict`
    # will still attempt to parse the into the WHOIS dictionary. However,
    # there is no guarantee that the information will be correct or non-null. 
    # If your applications rely on accurate information, the `strict=True`
    # parameter will raise an `RDAPConformanceException` when encountering
    # invalid or incorrectly formatted RDAP responses.
    rdap_response = lookup_domain("example", "com")
    whois_format = rdap_response.to_whois_dict(strict=True)
except RDAPConformanceException:
    logger.exception("RDAP response is incorrectly formatted.")
```

#### Contributions
- Interested in contributing? 
- Have any questions or comments? 
- Anything that you'd like to see?
- Anything that doesn't look right?

Please post a question or comment.

#### Roadmap

[alpha] 0.1.X Release:
- ~~Support for RDAP "domain" queries~~
- ~~Support for RDAP "ipv4" and "ipv6" queries~~
- ~~Support for RDAP ASN queries~~
- Abstract the HTTP Client (`httpx` is the defacto client for now)
- Add parser utils/helpers for IPv4, IPv6, and ASN Responses (if someone shows interest)
- Add RDAP response validation support leveraging [ICANN's tool](https://github.com/icann/rdap-conformance-tool/)

#### RDAP Resources:
- [rdap.org](https://rdap.org/)
- [RFC 9082](https://datatracker.ietf.org/doc/html/rfc9082) 
