### asyncwhois

[![PyPI version](https://badge.fury.io/py/asyncwhois.svg)](https://badge.fury.io/py/asyncwhois)
![build-workflow](https://github.com/pogzyb/asyncwhois/actions/workflows/build-and-test.yml/badge.svg)
[![codecov](https://codecov.io/gh/pogzyb/asyncwhois/branch/main/graph/badge.svg?token=Q4xtgezXGX)](https://codecov.io/gh/pogzyb/asyncwhois)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

`asyncwhois` | Python utility for WHOIS and RDAP queries.

#### Quickstart

```python
import asyncio
from pprint import pprint

import asyncwhois

# pick a domain
domain = 'bitcoin.org'
# domain could also be a URL; asyncwhois uses tldextract to parse the URL
# domain = 'https://www.google.com?q=asyncwhois'

# basic example
query_string, parsed_dict = asyncwhois.whois(domain)
# query_string  # The semi-free text output from the whois server
# parsed_dict   # A dictionary of key:values extracted from `query_string`

# asyncio example
loop = asyncio.get_event_loop()
query_string, parsed_dict = loop.run_until_complete(asyncwhois.aio_whois(domain))

pprint(parsed_dict)
"""
{created: datetime.datetime(2008, 8, 18, 13, 19, 55),
 dnssec: 'unsigned',
 domain_name: 'bitcoin.org',
 expires: datetime.datetime(2029, 8, 18, 13, 19, 55),
 name_servers: ['dns1.registrar-servers.com', 'dns2.registrar-servers.com'],
 registrant_address: 'P.O. Box 0823-03411',
 registrant_city: 'Panama',
 registrant_country: 'PA',
 registrant_name: 'WhoisGuard Protected',
 registrant_organization: 'WhoisGuard, Inc.',
 registrant_state: 'Panama',
 registrant_zipcode: '',
 registrar: 'NAMECHEAP INC',
 status: ['clientTransferProhibited '
          'https://icann.org/epp#clientTransferProhibited'],
 updated: datetime.datetime(2019, 11, 24, 13, 58, 35, 940000)}
 ...
 """

# set `find_authoritative_server=False` to only query the TLD's whois server as specified in the IANA root db. 
# this will generally speed up execution times, but responses may not include complete information. 
query_string, parsed_dict = asyncwhois.whois("google.com", find_authoritative_server=False)

# support for IPv4, IPv6, and ASNs too
ipv4 = "8.8.8.8"
query_string, parsed_dict = asyncwhois.whois(ipv4)
pprint(parsed_dict)
"""
{abuse_address: None,
 abuse_email: 'network-abuse@google.com',
 abuse_handle: 'ABUSE5250-ARIN',
 abuse_name: 'Abuse',
 abuse_phone: '+1-650-253-0000',
 abuse_rdap_ref: 'https://rdap.arin.net/registry/entity/ABUSE5250-ARIN',
 cidr: '8.8.8.0/24',
 net_handle: 'NET-8-8-8-0-2',
 net_name: 'GOGL',
 net_range: '8.8.8.0 - 8.8.8.255',
 net_type: 'Direct Allocation',
 org_address: '1600 Amphitheatre Parkway',
 org_city: 'Mountain View',
 org_country: 'US',
 org_id: 'GOGL',
 ...
"""
```

#### RDAP

The [whodap](https://github.com/pogzyb/whodap) project is used behind the scenes to perform RDAP queries.

```python
domain = "https://google.com"
query_string, parsed_dict = asyncwhois.rdap(domain)
# OR with asyncio
query_string, parsed_dict = loop.run_until_complete(asyncwhois.aio_rdap(domain))

# Reusable client (caches the RDAP bootstrap server list, so it is faster for doing multiple calls)
client = asyncwhois.DomainClient()
for domain in ["google.com", "tesla.coffee", "bitcoin.org"]:
    query_string, parsed_dict = client.rdap(domain)
    # query_string, parsed_dict = await client.aio_rdap(domain)

# Using a proxy or need to configure something HTTP related? Try reconfiguring the client:
whodap_client = whodap.DNSClient.new_client(
    httpx_client=httpx.Client(proxies="https://proxy:8080")
)
# whodap_client = await whodap.DNSClient.new_aio_client(httpx_client=httpx.AsyncClient(proxies="https://proxy:8080"))
client = asyncwhois.DomainClient(whodap_client=whodap_client)

```

#### Proxies

SOCKS proxies are supported for WHOIS and RDAP queries.

```python
import whodap

tor_host = "localhost"
tor_port = 9050

# WHOIS
query_string, parsed_dict = asyncwhois.whois(
    "8.8.8.8", proxy_url=f"socks5://{tor_host}:{tor_port}"
)

# RDAP
import httpx
from httpx_socks import SyncProxyTransport, AsyncProxyTransport  # EXTERNAL DEPENDENCY for SOCKS Proxies 

transport = SyncProxyTransport.from_url(f"socks5://{tor_host}:{tor_port}")
httpx_client = httpx.Client(transport=transport)
whodap_client = whodap.IPv6Client.new_client(httpx_client=httpx_client)
query_string, parsed_dict = asyncwhois.rdap('2001:4860:4860::8888', whodap_client=whodap_client)

transport = AsyncProxyTransport.from_url(f"socks5://{tor_user}:{tor_pw}@{tor_host}:{tor_port}")
async with httpx.AsyncClient(transport=transport) as httpx_client:
    whodap_client = await whodap.DNSClient.new_aio_client(httpx_client=httpx_client)
    query_string, parsed_dict = await asyncwhois.aio_rdap('bitcoin.org', whodap_client=whodap_client)

```

#### Exported Functions

| Function/Object    | Description                                             |
|--------------------|---------------------------------------------------------|
| `DomainClient`     | Reusable client for  WHOIS or RDAP domain queries       |
| `NumberClient`     | Reusable client for WHOIS or RDAP ipv4/ipv6 queries     |
| `ASNClient`        | Reusable client for RDAP asn queries                    |
| `whois`            | WHOIS entrypoint for domain, ipv4, or ipv6 queries      |
| `rdap`             | RDAP entrypoint for domain, ipv4, ipv6, or asn queries  |
| `aio_whois`        | async counterpart to `whois`                            |
| `aio_rdap`         | async counterpart to `rdap`                             |
| `whois_ipv4`       | [DEPRECATED] WHOIS lookup for ipv4 addresses            |
| `whois_ipv6`       | [DEPRECATED] WHOIS lookup for ipv6 addresses            |
| `rdap_domain`      | [DEPRECATED] RDAP lookup for domain names               |
| `rdap_ipv4`        | [DEPRECATED] RDAP lookup for ipv4 addresses             |
| `rdap_ipv6`        | [DEPRECATED] RDAP lookup for ipv6 addresses             |
| `rdap_asn`         | [DEPRECATED] RDAP lookup for Autonomous System Numbers  |
| `aio_whois_domain` | [DEPRECATED] async counterpart to `whois_domain`        |
| `aio_whois_ipv4`   | [DEPRECATED] async counterpart to `whois_ipv4`          |
| `aio_whois_ipv6`   | [DEPRECATED] async counterpart to `whois_ipv6`          |
| `aio_rdap_domain`  | [DEPRECATED] async counterpart to `rdap_domain`         |
| `aio_rdap_ipv4`    | [DEPRECATED] async counterpart to `rdap_ipv4`           |
| `aio_rdap_ipv6`    | [DEPRECATED] async counterpart to `rdap_ipv6`           |
| `aio_rdap_asn`     | [DEPRECATED] async counterpart to `rdap_asn`            |

#### Contributions

Parsed output not what you expected? Unfortunately, "the format of responses [from a WHOIS server] follow a semi-free text format". Therefore,
situations will arise where this module does not support parsing the output from a specific server, and you may find
yourself needing more control over how parsing happens. Fortunately, you can create customized parsers to suit your needs.

Example: This is a snippet of the output from running the "whois google.be" command.
```python
Domain:	google.be
Status:	NOT AVAILABLE
Registered:	Tue Dec 12 2000

Registrant:
    Not shown, please visit www.dnsbelgium.be for webbased whois.

Registrar Technical Contacts:
    Organisation:	MarkMonitor Inc.
    Language:	en
    Phone:	+1.2083895740
    Fax:	+1.2083895771

Registrar:
    Name:	 MarkMonitor Inc.
    Website: http://www.markmonitor.com

Nameservers:
    ns2.google.com
    ns1.google.com
    ns4.google.com
    ns3.google.com

Keys:

Flags:
    clientTransferProhibited
...
```
In this case, the "name servers" are listed on separate lines. The default BaseParser regexes
won't find all of these server names. In order to accommodate this extra step, the "parse" method was
overwritten within the parser subclass as seen below:
```python
class RegexBE(BaseParser):
    _be_expressions = {  # the base class (BaseParser) will handle these regexes
        BaseKeys.CREATED: r'Registered: *(.+)',
        BaseKeys.REGISTRAR: r'Registrar:\n.+Name: *(.+)',
        BaseKeys.REGISTRANT_NAME: r'Registrant:\n *(.+)'
    }
    
    def __init__(self):
        super().__init__()
        self.update_reg_expressions(self._be_expressions)
    
    def parse(self, blob: str) -> Dict[str, Any]:
        # run base class parsing for other keys
        parsed_output = super().parse(blob)
        # custom parsing is needed to extract all the name servers
        ns_match = re.search(r"Name servers: *(.+)Keys: ", blob, re.DOTALL)
        if ns_match:
            parsed_output[BaseKeys.NAME_SERVERS] = [m.strip() for m in ns_match.group(1).split('\n') if m.strip()]
        return parsed_output
```