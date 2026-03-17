# dnspython

[![Build Status](https://travis-ci.org/rthalley/dnspython.svg?branch=master)](https://travis-ci.org/rthalley/dnspython)

## INTRODUCTION

dnspython is a DNS toolkit for Python. It supports almost all record types. It
can be used for queries, zone transfers, and dynamic updates. It supports TSIG
authenticated messages and EDNS0.

dnspython provides both high and low level access to DNS. The high level classes
perform queries for data of a given name, type, and class, and return an answer
set. The low level classes allow direct manipulation of DNS zones, messages,
names, and records.

To see a few of the ways dnspython can be used, look in the `examples/` directory.

dnspython is a utility to work with DNS, `/etc/hosts` is thus not used. For
simple forward DNS lookups, it's better to use `socket.gethostbyname()`.

dnspython originated at Nominum where it was developed
to facilitate the testing of DNS software.

## INSTALLATION

* Many distributions have dnspython packaged for you, so you should
  check there first.
* If you have pip installed, you can do `pip install dnspython`
* If not just download the source file and unzip it, then run
  `sudo python setup.py install`

## ABOUT THIS RELEASE

This is dnspython 1.16.0

### Notices

Python 2.x support ends with the release of 1.16.0, unless there are
critical bugs in 1.16.0.  Future versions of dnspython will only
support Python 3.

Version numbering of future dnspython releases will also start at 2.0, as
incompatible changes will be permitted.  We're not planning huge changes at
this time, but we'd like to do a better job at IDNA, and there are other
API improvements to be made.

The ChangeLog has been discontinued.  Please see the git history for detailed
change information.

### New since 1.15.0:

* Much of the internals of dns.query.udp() and dns.query.tcp() have
  been factored out into dns.query.send_udp(),
  dns.query.receive_udp(), dns.query.send_tcp(), and
  dns.query.receive_tcp().  Applications which want more control over
  the socket may find the new routines helpful; for example it would
  be easy to send multiple queries over a single TCP connection.
  
* The OPENPGPKEY RR, and the CHAOS class A RR are now supported.

* EDNS0 client-subnet is supported.

* dns.resover.query() now has a lifetime timeout optional parameter.

* pycryptodome and pycryptodomex are now supported and recommended for use
  instead of pycrypto.
  
* dns.message.from_wire() now has an ignore_trailing option.

* type signatures have been provided.

* module dns.hash is now deprecated, use standard Python libraries instead.

* setup.py supports Cythonization to improve performance.

### Bugs fixed since 1.15.0:

* DNSSEC signature validation didn't check names correctly.  [Issue #295]

* The NXDOMAIN exception should not use its docstring.  [Issue #253]

* Fixed regression where trailing zeros in APL RRs were not
  suppressed, and then fixed the problem where trailing zeros
  were not added back properly on python 3 when needed.
  
* Masterfile TTL defaulting is now harmonized with BIND practice.

* dns.query.xfr() now raises on a non-zero rcode.

* Rdata module importing is now locked to avoid races.

* Several Python 3 incompatibilities have been fixed.

* NSEC3 bitmap parsing now works with mulitple NSEC3 windows.

* dns.renderer.Render supports TSIG on DNS envelope sequences.

* DNSSEC validation now checks names properly [Issue #295]

### New since 1.14.0:

* IDNA 2008 support is now available if the "idna" module has been
  installed and IDNA 2008 is requested.  The default IDNA behavior is
  still IDNA 2003.  The new IDNA codec mechanism is currently only
  useful for direct calls to dns.name.from_text() or
  dns.name.from_unicode(), but in future releases it will be deployed
  throughout dnspython, e.g. so that you can read a masterfile with an
  IDNA 2008 codec in force.

* By default, dns.name.to_unicode() is not strict about which
  version of IDNA the input complies with.  Strictness can be
  requested by using one of the strict IDNA codecs.

* The AVC RR is now supported.

### Bugs fixed since 1.14.0:

* Some problems with newlines in various output modes have been
  addressed.

* dns.name.to_text() now returns text and not bytes on Python 3.x

* Miscellaneous fixes for the Python 2/3 codeline merge.

* Many "lint" fixes after the addition of pylint support.

* The random number generator reseeds after a fork().


## REQUIREMENTS

Python 2.7 or 3.4+.


## HOME PAGE

For the latest in releases, documentation, and information, visit the dnspython
home page at http://www.dnspython.org/


## BUG REPORTS

Bug reports may be opened at
https://github.com/rthalley/dnspython/issues or sent to
bugs@dnspython.org


## MAILING LISTS

A number of mailing lists are available. Visit the dnspython home page to
subscribe or unsubscribe.


## PRIOR RELEASE INFORMATION

### New since 1.13.0:

* CSYNC RRs are now supported.

* `dns/message.py` (`make_query`): Setting any value which implies EDNS will
  turn on EDNS if `use_edns` has not been specified.

### Bugs fixed since 1.13.0:

* TSIG signature algorithm setting was broken by the Python 2 and Python 3 code
  line merge.

* A bug in the LOC RR destroyed N/S and E/W distinctions within a degree of the
  equator or prime merdian respectively.

* Misc. fixes to deal with fallout from the Python 2 & 3 merge.
  Fixes #156, #157, #158, #159, #160.

* Running with python optimization on caused issues when stripped docstrings
  were referenced. Fixes #154

* `dns.zone.from_text()` erroneously required the zone to be provided.
  Fixes #153

### New since 1.12.0:

* Dnspython now uses a single source for Python 2 and Python 3, eliminating the
  painful merging between the Python 2 and Python 3 branches. Thank you so much
  to Arthur Gautier for taking on this challenge and making it work! It was a
  big job!

* Support for Python older than 2.6 dropped.

* Support for Python older than 3.3 dropped.

* Zone origin can be specified as a string.

* A rich string representation for all DNSExceptions.

* setuptools has replaced distutils

* Added support for CAA, CDS, CDNSKEY, EUI48, EUI64, and URI RR types.

* Names now support the pickle protocol.

* Ports can be specified per-nameserver in the stub resolver.

### Bugs fixed since 1.12.0:

* A number of Unicode name bugs have been fixed.

* `resolv.conf` processing now rejects lines with too few tokens.

* NameDicts now keep the max-depth value correct, and update properly.

### New since 1.11.1:

* Added `dns.zone.to_text()`.

* Added support for "options rotate" in `/etc/resolv.conf`.

* `dns.rdtypes.ANY.DNSKEY` now has helpers functions to convert between the
  numeric form of the flags and a set of human-friendly strings

* The reverse name of an IPv6 mapped IPv4 address is now in the IPv4 reverse
  namespace.

* The test system can now run the tests without requiring dnspython to be
  installed.

* Preliminary Elliptic Curve DNSSEC Validation (requires ecdsa module)

### Bugs fixed since 1.11.1:

* dnspython raised an exception when reading a masterfile starting with leading
  whitespace

* dnspython was affected by a python slicing API bug present on 64-bit windows.

* Unicode escaping was applied at the wrong time.

* RRSIG `to_text()` did not respect the relativize setting.

* APL RRs with zero rdlength were rejected.

* The tokenizer could put back an unescaped token.

* Making a response to a message signed with TSIG was broken.

* The IXFR state machine didn't handle long IXFR diffs.

### New since 1.11.0:

* Nothing

### Bugs fixed since 1.11.0:

* `dns.resolver.Resolver` erroneously referred to `retry_servfail`
  instead of `self.retry_servfail`.

* `dns.tsigkeyring.to_text()` would fail trying to convert the keyname to text.

* Multi-message TSIGs were broken for algorithms other than HMAC-MD5 because we
  weren't passing the right digest module to the HMAC code.

* `dns.dnssec._find_candidate_keys()` tried to extract the key from the wrong
  variable name.

* $GENERATE tests were not backward compatible with python 2.4.

### New since 1.10.0:

* $GENERATE support

* TLSA RR support

* Added set_flags() method to dns.resolver.Resolver

### Bugs fixed since 1.10.0:

* Names with offsets >= 2^14 are no longer added to the compression table.

* The "::" syntax is not used to shorten a single 16-bit section of the text
  form an IPv6 address.

* Caches are now locked.

* YXDOMAIN is raised if seen by the resolver.

* Empty rdatasets are not printed.

* DNSKEY key tags are no longer assumed to be unique.

### New since 1.9.4:

* Added dns.resolver.LRUCache. In this cache implementation, the cache size is
  limited to a user-specified number of nodes, and when adding a new node to a
  full cache the least-recently used node is removed. If you're crawling the web
  or otherwise doing lots of resolutions and you are using a cache, switching
  to the LRUCache is recommended.

* `dns.resolver.query()` will try TCP if a UDP response is truncated.

* The python socket module's DNS methods can be now be overridden with
  implementations that use dnspython's resolver.

* Old DNSSEC types KEY, NXT, and SIG have been removed.

* Whitespace is allowed in SSHFP fingerprints.

* Origin checking in `dns.zone.from_xfr()` can be disabled.

* Trailing junk checking can be disabled.

* A source port can be specified when creating a resolver query.

* All EDNS values may now be specified to `dns.message.make_query()`.

### Bugs fixed since 1.9.4:

* IPv4 and IPv6 address processing is now stricter.

* Bounds checking of slices in rdata wire processing is now more strict, and
  bounds errors (e.g. we got less data than was expected) now raise
  `dns.exception.FormError` rather than `IndexError`.

* Specifying a source port without specifying source used to have no effect, but
  now uses the wildcard address and the specified port.

### New since 1.9.3:

* Nothing.

### Bugs fixed since 1.9.3:

* The rdata `_wire_cmp()` routine now handles relative names.

* The SIG RR implementation was missing `import struct`.

### New since 1.9.2:

* A boolean parameter, `raise_on_no_answer`, has been added to the `query()`
  methods. In no-error, no-data situations, this parameter determines whether
  `NoAnswer` should be raised or not. If True, `NoAnswer` is raised. If False,
  then an `Answer()` object with a None rrset will be returned.

* Resolver `Answer()` objects now have a canonical_name field.

* Rdata now has a `__hash__` method.

### Bugs fixed since 1.9.2:

* Dnspython was erroneously doing case-insensitive comparisons of the names in
  NSEC and RRSIG RRs.

* We now use `is` and not `==` when testing what section an RR is in.

* The resolver now disallows metaqueries.

### New since 1.9.1:

* Nothing.

### Bugs fixed since 1.9.1:

* The `dns.dnssec` module didn't work at all due to missing imports that escaped
  detection in testing because the test suite also did the imports. The third
  time is the charm!

### New since 1.9.0:

* Nothing.

### Bugs fixed since 1.9.0:

* The `dns.dnssec` module didn't work with DSA due to namespace contamination
  from a "from"-style import.

### New since 1.8.0:

* dnspython now uses `poll()` instead of `select()` when available.

* Basic DNSSEC validation can be done using `dns.dnsec.validate()` and
  `dns.dnssec.validate_rrsig()` if you have PyCrypto 2.3 or later installed.
  Complete secure resolution is not yet available.

* Added `key_id()` to the DNSSEC module, which computes the DNSSEC key id of a
  DNSKEY rdata.

* Added `make_ds()` to the DNSSEC module, which returns the DS RR for a given
  DNSKEY rdata.

* dnspython now raises an exception if HMAC-SHA284 or HMAC-SHA512 are used with
  a Python older than 2.5.2. (Older Pythons do not compute the correct value.)

* Symbolic constants are now available for TSIG algorithm names.

### Bugs fixed since 1.8.0

* `dns.resolver.zone_for_name()` didn't handle a query response with a CNAME or
  DNAME correctly in some cases.

* When specifying rdata types and classes as text, Unicode strings may now be
  used.

* Hashlib compatibility issues have been fixed.

* `dns.message` now imports `dns.edns`.

* The TSIG algorithm value was passed incorrectly to `use_tsig()` in some cases.

### New since 1.7.1:

* Support for hmac-sha1, hmac-sha224, hmac-sha256, hmac-sha384 and hmac-sha512
  has been contributed by Kevin Chen.

* The tokenizer's tokens are now Token objects instead of (type, value) tuples.

### Bugs fixed since 1.7.1:

* Escapes in masterfiles now work correctly. Previously they were only working
  correctly when the text involved was part of a domain name.

* When constructing a DDNS update, if the `present()` method was used with a
  single rdata, a zero TTL was not added.

* The entropy pool needed locking to be thread safe.

* The entropy pool's reading of `/dev/random` could cause dnspython to block.

* The entropy pool did buffered reads, potentially consuming more randomness
  than we needed.

* The entropy pool did not seed with high quality randomness on Windows.

* SRV records were compared incorrectly.

* In the e164 query function, the resolver parameter was not used.

### New since 1.7.0:

* Nothing

### Bugs fixed since 1.7.0:

* The 1.7.0 kitting process inadvertently omitted the code for the DLV RR.

* Negative DDNS prerequisites are now handled correctly.

### New since 1.6.0:

* Rdatas now have a `to_digestable()` method, which returns the DNSSEC canonical
  form of the rdata, suitable for use in signature computations.

* The NSEC3, NSEC3PARAM, DLV, and HIP RR types are now supported.

* An entropy module has been added and is used to randomize query ids.

* EDNS0 options are now supported.

* UDP IXFR is now supported.

* The wire format parser now has a `one_rr_per_rrset` mode, which suppresses the
  usual coalescing of all RRs of a given type into a single RRset.

* Various helpful DNSSEC-related constants are now defined.

* The resolver's `query()` method now has an optional `source` parameter,
  allowing the source IP address to be specified.

### Bugs fixed since 1.6.0:

* On Windows, the resolver set the domain incorrectly.

* DS RR parsing only allowed one Base64 chunk.

* TSIG validation didn't always use absolute names.

* `NSEC.to_text()` only printed the last window.

* We did not canonicalize IPv6 addresses before comparing them; we
  would thus treat equivalent but different textual forms, e.g.
  "1:00::1" and "1::1" as being non-equivalent.

* If the peer set a TSIG error, we didn't raise an exception.

* Some EDNS bugs in the message code have been fixed (see the ChangeLog
  for details).

### New since 1.5.0:

* Added dns.inet.is_multicast().

### Bugs fixed since 1.5.0:

* If `select()` raises an exception due to EINTR, we should just `select()`
  again.

* If the queried address is a multicast address, then don't check that the
  address of the response is the same as the address queried.

* NAPTR comparisons didn't compare the preference field due to a typo.

* Testing of whether a Windows NIC is enabled now works on Vista thanks to code
  contributed by Paul Marks.

### New since 1.4.0:

* Answer objects now support more of the python sequence protocol, forwarding
  the requests to the answer rrset. E.g. `for a in answer` is equivalent to
  `for a in  answer.rrset`, `answer[i]` is equivalent to `answer.rrset[i]`, and
  `answer[i:j]` is equivalent to `answer.rrset[i:j]`.

* Making requests using EDNS, including indicating DNSSEC awareness,
  is now easier. For example, you can now say:
  `q = dns.message.make_query('www.dnspython.org', 'MX', want_dnssec=True)`

* `dns.query.xfr()` can now be used for IXFR.

* Support has been added for the DHCID, IPSECKEY, and SPF RR types.

* UDP messages from unexpected sources can now be ignored by setting
  `ignore_unexpected` to True when calling `dns.query.udp`.

### Bugs fixed since 1.4.0:

* If `/etc/resolv.conf` didn't exist, we raised an exception instead of simply
  using the default resolver configuration.

* In `dns.resolver.Resolver._config_win32_fromkey()`, we were passing the wrong
  variable to `self._config_win32_search()`.

### New since 1.3.5:

* You can now convert E.164 numbers to/from their ENUM name forms:
  ```python
  >>> import dns.e164
  >>> n = dns.e164.from_e164("+1 555 1212")
  >>> n
  <DNS name 2.1.2.1.5.5.5.1.e164.arpa.>
  >>> dns.e164.to_e164(n)
  '+15551212'
  ```

* You can now convert IPv4 and IPv6 address to/from their corresponding DNS
  reverse map names:
  ```python
  >>> import dns.reversename
  >>> n = dns.reversename.from_address("127.0.0.1")
  >>> n
  <DNS name 1.0.0.127.in-addr.arpa.>
  >>> dns.reversename.to_address(n)
  '127.0.0.1'
  ```

* You can now convert between Unicode strings and their IDN ACE form:
  ```python
  >>> n = dns.name.from_text(u'les-\u00e9l\u00e8ves.example.')
  >>> n
  <DNS name xn--les-lves-50ai.example.>
  >>> n.to_unicode()
  u'les-\xe9l\xe8ves.example.'
  ```

* The origin parameter to `dns.zone.from_text()` and `dns.zone.to_text()` is now
  optional. If not specified, the origin will be taken from the first $ORIGIN
  statement in the master file.

* Sanity checking of a zone can be disabled; this is useful when working with
  files which are zone fragments.

### Bugs fixed since 1.3.5:

* The correct delimiter was not used when retrieving the list of nameservers
  from the registry in certain versions of windows.

* The floating-point version of latitude and longitude in LOC RRs
  (`float_latitude` and `float_longitude`) had incorrect signs for south
  latitudes and west longitudes.

* BIND 8 TTL syntax is now accepted in all TTL-like places (i.e. SOA fields
  refresh, retry, expire, and minimum; SIG/RRSIG field original_ttl).

* TTLs are now bounds checked when their text form is parsed, and their values
  must be in the closed interval `[0, 2^31 - 1]`.

### New since 1.3.4:

* In the resolver, if time goes backward a little bit, ignore it.

* `zone_for_name()` has been added to the resolver module. It returns the zone
  which is authoritative for the specified name, which is handy for dynamic
  update. E.g.

      import dns.resolver
      print dns.resolver.zone_for_name('www.dnspython.org')

  will output `"dnspython.org."` and
  `print dns.resolver.zone_for_name('a.b.c.d.e.f.example.')`
  will output `"."`.

* The default resolver can be fetched with the `get_default_resolver()` method.

* You can now get the parent (immediate superdomain) of a name by using the
  `parent()` method.

* `Zone.iterate_rdatasets()` and `Zone.iterate_rdatas()` now have a default
  rdtype of `dns.rdatatype.ANY` like the documentation says.

* A Dynamic DNS example, ddns.py, has been added.

### New since 1.3.3:

* The source address and port may now be specified when calling
  `dns.query.{udp,tcp,xfr}`.

* The resolver now does exponential backoff each time it runs through all of the
  nameservers.

* Rcodes which indicate a nameserver is likely to be a "permanent failure" for a
  query cause the nameserver to be removed from the mix for that query.

### New since 1.3.2:

* `dns.message.Message.find_rrset()` now uses an index, vastly improving the
  `from_wire()` performance of large messages such as zone transfers.

* Added `dns.message.make_response()`, which creates a skeletal response for the
  specified query.

* Added `opcode()` and `set_opcode()` convenience methods to the
  `dns.message.Message` class. Added the `request_payload` attribute to the
  Message class.

* The `file` parameter of `dns.name.Name.to_wire()` is now optional; if omitted,
  the wire form will be returned as the value of the function.

* `dns.zone.from_xfr()` in relativization mode incorrectly set `zone.origin` to
  the empty name.

* The masterfile parser incorrectly rejected TXT records where a value was not
  quoted.

### New since 1.3.1:

* The NSEC format doesn't allow specifying types by number, so we shouldn't
  either. (Using the unknown type format is still OK though.)

* The resolver wasn't catching `dns.exception.Timeout`, so a timeout erroneously
  caused the whole resolution to fail instead of just going on to the next
  server.

* The renderer module didn't import random, causing an exception to be raised if
  a query id wasn't provided when a Renderer was created.

* The conversion of LOC milliseconds values from text to binary was incorrect if
  the length of the milliseconds string was not 3.

### New since 1.3.0:

* Added support for the SSHFP type.

### New since 1.2.0:

* Added support for new DNSSEC types RRSIG, NSEC, and DNSKEY.

* This release fixes all known bugs.

* See the ChangeLog file for more detailed information on changes since the
  prior release.
