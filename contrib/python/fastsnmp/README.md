# fastsnmp
SNMP poller oriented to poll bunch of hosts in short time. Package include poller and SNMP coder/encoder library.

[Reference manual] (http://fastsnmp.readthedocs.org/)

Example:
```python
from fastsnmp import snmp_poller

hosts = ("127.0.0.1",)
# oids in group must be with same indexes
oid_group = {"1.3.6.1.2.1.2.2.1.2": "ifDescr",
             "1.3.6.1.2.1.2.2.1.10": "ifInOctets",
             }

community = "public"
snmp_data = snmp_poller.poller(hosts, [list(oid_group)], community)
for d in snmp_data:
    print ("host=%s oid=%s.%s value=%s" % (d[0], oid_group[d[1]], d[2], d[3]))
```
Output:
```
host=127.0.0.1 oid=ifInOctets.1 value=243203744
host=127.0.0.1 oid=ifDescr.1 value=b'lo'
host=127.0.0.1 oid=ifInOctets.2 value=1397428486
host=127.0.0.1 oid=ifDescr.2 value=b'eth0'
```
Type conversion:

| SNMP | Python |
| --- | --- |
| octetstring, ipaddress | bytes |
| null | None |
| objectid | str |
| counter32, unsigned32, gauge32, counter64, integer | int |
| noSuchInstance | None |
| noSuchObject | None |
| endOfMibView | None |

Notices:

- ipaddress can be converted to string using ``str(ipaddress.IPv4Address(b"\x01\x01\x01\x01"))`` or ``socket.inet_ntoa(b"\x01\x01\x01\x01")``

Another python SNMP libraries:

* [PySNMP](http://pysnmp.sourceforge.net/) - very good SNMP library
* [libsnmp](https://pypi.python.org/pypi/libsnmp) - SNMP coder/decoder (abandoned project)
* [Bindings to Net-SNMP](http://net-snmp.sourceforge.net/wiki/index.php/Python_Bindings)
