# Features

- [Dynamic Server Timeout Calculation](#dynamic-server-timeout-calculation)
- [Failed Server Isolation](#failed-server-isolation)
- [Query Cache](#query-cache)
- [DNS 0x20 Query Name Case Randomization](#dns-0x20-query-name-case-randomization)
- [DNS Cookies](#dns-cookies)
- [TCP FastOpen (0-RTT)](#tcp-fastopen-0-rtt)
- [Event Thread](#event-thread)
- [System Configuration Change Monitoring](#system-configuration-change-monitoring)


## Dynamic Server Timeout Calculation

Metrics are stored for every server in time series buckets for both the current
time span and prior time span in 1 minute, 15 minute, 1 hour, and 1 day
intervals, plus a single since-inception bucket (of the server in the c-ares
channel).

These metrics are then used to calculate the average latency for queries on
each server, which automatically adjusts to network conditions.  This average
is then multiplied by 5 to come up with a timeout to use for the query before
re-queuing it.  If there is not sufficient data yet to calculate a timeout
(need at least 3 prior queries), then the default of 2000ms is used (or an 
administrator-set `ARES_OPT_TIMEOUTMS`).

The timeout is then adjusted to a minimum bound of 250ms which is the
approximate RTT of network traffic half-way around the world, to account for the
upstream server needing to recurse to a DNS server far away.  It is also
bounded on the upper end to 5000ms (or an administrator-set 
`ARES_OPT_MAXTIMEOUTMS`).

If a server does not reply within the given calculated timeout, the next time
the query is re-queued to the same server, the timeout will approximately
double thus leading to adjustments in timeouts automatically when a successful
reply is recorded.

In order to calculate the optimal timeout, it is highly recommended to ensure
`ARES_OPT_QUERY_CACHE` is enabled with a non-zero `qcache_max_ttl` (which it
is enabled by default with a 3600s default max ttl).  The goal is to record
the recursion time as part of query latency as the upstream server will also
cache results.

This feature requires the c-ares channel to persist for the lifetime of the
application.


## Failed Server Isolation

Each server is tracked for failures relating to consecutive connectivity issues
or unrecoverable response codes.  Servers are sorted in priority order based
on this metric.  Downed servers will be brought back online either when the
current highest priority server has failed, or has been determined to be online
when a query is randomly selected to probe a downed server.

By default a downed server won't be retried for 5 seconds, and queries will
have a 10% chance of being chosen after this timeframe to test a downed server.
When a downed server is selected to be probed, the query will be duplicated
and sent to the downed server independent of the original query itself.  This
means that probing a downed server will always use an intended legitimate
query, but not have a negative impact of a delayed response in case that server
is still down.

Administrators may customize these settings via `ARES_OPT_SERVER_FAILOVER`.

Additionally, when using `ARES_OPT_ROTATE` or a system configuration option of
`rotate`, c-ares will randomly select a server from the list of highest priority
servers based on failures.  Any servers in any lower priority bracket will be
omitted from the random selection.

This feature requires the c-ares channel to persist for the lifetime of the
application.


## Query Cache

Every successful query response, as well as `NXDOMAIN` responses containing
an `SOA` record are cached using the `TTL` returned or the SOA Minimum as
appropriate.  This timeout is bounded by the `ARES_OPT_QUERY_CACHE`
`qcache_max_ttl`, which defaults to 1hr.

The query is cached at the lowest possible layer, meaning a call into
`ares_search_dnsrec()` or `ares_getaddrinfo()` may spawn multiple queries
in order to complete its lookup, each individual backend query result will
be cached.

Any server list change will automatically invalidate the cache in order to
purge any possible stale data.  For example, if `NXDOMAIN` is cached but system
configuration has changed due to a VPN connection, the same query might now
result in a valid response.

This feature is not expected to cause any issues that wouldn't already be
present due to the upstream DNS server having substantially similar caching
already.  However if desired it can be disabled by setting `qcache_max_ttl` to
`0`.

This feature requires the c-ares channel to persist for the lifetime of the
application.


## DNS 0x20 Query Name Case Randomization

DNS 0x20 is the name of the feature which automatically randomizes the case
of the characters in a UDP query as defined in
[draft-vixie-dnsext-dns0x20-00](https://datatracker.ietf.org/doc/html/draft-vixie-dnsext-dns0x20-00).

For example, if name resolution is performed for `www.example.com`, the actual
query sent to the upstream name server may be `Www.eXaMPlE.cOM`.

The reason to randomize case characters is to provide additional entropy in the
query to be able to detect off-path cache poisoning attacks for UDP.  This is
not used for TCP connections which are not known to be vulnerable to such
attacks due to their stateful nature.

Much research has been performed by
[Google](https://groups.google.com/g/public-dns-discuss/c/KxIDPOydA5M)
on case randomization and in general have found it to be effective and widely
supported.

This feature is disabled by default and can be enabled via `ARES_FLAG_DNS0x20`.
There are some instances where servers do not properly facilitate this feature
and unlike in a recursive resolver where it may be possible to determine an
authoritative server is incapable, its much harder to come to any reliable
conclusion as a stub resolver as to where in the path the issue resides.  Due to
the recent wide deployment of DNS 0x20 in large public DNS servers, it is
expected compatibility will improve rapidly where this feature, in time, may be
able to be enabled by default.

Another feature which can be used to prevent off-path cache poisoning attacks
is [DNS Cookies](#dns-cookies).


## DNS Cookies

DNS Cookies are are a method of learned mutual authentication between a server
and a client as defined in
[RFC7873](https://datatracker.ietf.org/doc/html/rfc7873)
and [RFC9018](https://datatracker.ietf.org/doc/html/rfc9018).

This mutual authentication ensures clients are protected from off-path cache
poisoning attacks, and protects servers from being used as DNS amplification
attack sources.  Many servers will disable query throttling limits when DNS
Cookies are in use. It only applies to UDP connections.

Since DNS Cookies are optional and learned dynamically, this is an always-on
feature and will automatically adjust based on the upstream server state.  The
only potential issue is if a server has once supported DNS Cookies then stops
supporting them, it must clear a regression timeout of 2 minutes before it can
accept responses without cookies.  Such a scenario would be exceedingly rare.

Interestingly, the large public recursive DNS servers such as provided by
[Google](https://developers.google.com/speed/public-dns/docs/using),
[CloudFlare](https://one.one.one.one/), and
[OpenDNS](https://opendns.com) do not have this feature enabled.  That said,
most DNS products like [BIND](https://www.isc.org/bind/) enable DNS Cookies
by default.

This feature requires the c-ares channel to persist for the lifetime of the
application.


## TCP FastOpen (0-RTT)

TCP Fast Open is defined in [RFC7413](https://datatracker.ietf.org/doc/html/rfc7413)
and enables data to be sent with the TCP SYN packet when establishing the
connection, thus rivaling the performance of UDP.  A previous connection must
have already have been established in order to obtain the client cookie to
allow the server to trust the data sent in the first packet and know it was not
an off-path attack.

TCP FastOpen can only be used with idempotent requests since in timeout
conditions the SYN packet with data may be re-sent which may cause the server
to process the packet more than once.  Luckily DNS requests are idempotent by
nature.

TCP FastOpen is supported on Linux, MacOS, and FreeBSD. Most other systems do
not support this feature, or like on Windows require use of completion
notifications to use it whereas c-ares relies on readiness notifications.

Supported systems also need to be configured appropriately on both the client
and server systems.

### Linux TFO
In linux a single sysctl value is used with flags to set the desired fastopen
behavior.

It is recommended to make any changes permanent by creating a file in
`/etc/sysctl.d/` with the appropriate key and value.  Legacy Linux systems
might need to update `/etc/sysctl.conf` directly.  After modifying the
configuration, it can be loaded via `sysctl -p`.

`net.ipv4.tcp_fastopen`:
   - `1` = client only (typically default)
   - `2` = server only
   - `3` = client and server

### MacOS TFO
In MacOS, TCP FastOpen is enabled by default for clients and servers.  You can
verify via the `net.inet.tcp.fastopen` sysctl.

If any change is needed, you should make it persistent as per this guidance:
[Persistent Sysctl Settings](https://discussions.apple.com/thread/253840320?)

`net.inet.tcp.fastopen`
   - `1` = client only
   - `2` = server only
   - `3` = client and server (typically default)

### FreeBSD TFO
In FreeBSD, server mode TCP FastOpen is typically enabled by default but
client mode is disabled.  It is recommended to edit `/etc/sysctl.conf` and
place in the values you wish to persist to enable or disable TCP Fast Open.
Once the file is modified, it can be loaded via `sysctl -f /etc/sysctl.conf`.

- `net.inet.tcp.fastopen.server_enable` (boolean) - enable/disable server
- `net.inet.tcp.fastopen.client_enable` (boolean) - enable/disable client


## Event Thread

Historic c-ares integrations required integrators to have their own event loop
which would be required to notify c-ares of read and write events for each
socket.  It was also required to notify c-ares at the appropriate timeout if
no events had occurred.  This could be difficult to do correctly and could
lead to stalls or other issues.

The Event Thread is currently supported on all systems except DOS which does
not natively support threading (however it could in theory be possible to
enable with something like [FSUpthreads](https://arcb.csc.ncsu.edu/~mueller/pthreads/)).

c-ares is built by default with threading support enabled, however it may
disabled at compile time.  The event thread must also be specifically enabled
via `ARES_OPT_EVENT_THREAD`.

Using the Event Thread feature also facilitates some other features like
[System Configuration Change Monitoring](#system-configuration-change-monitoring),
and automatically enables the `ares_set_pending_write_cb()` feature to optimize
multi-query writing.


## System Configuration Change Monitoring

The system configuration is automatically monitored for changes to the network
and DNS settings.  When a change is detected a thread is spawned to read the
new configuration then apply it to the current c-ares configuration.

This feature requires the [Event Thread](#event-thread) to be enabled via
`ARES_OPT_EVENT_THREAD`.  Otherwise it is up to the integrator to do their own
configuration monitoring and call `ares_reinit()` to reload the system
configuration.

It is supported on Windows, MacOS, iOS and any system configuration that uses
`/etc/resolv.conf` and similar files such as Linux and FreeBSD.  Specifically
excluded are DOS and Android due to missing mechanisms to support such a
feature.  On linux file monitoring will result in immediate change detection,
however on other unix-like systems a polling mechanism is used that checks every
30s for changes.

This feature requires the c-ares channel to persist for the lifetime of the
application.
