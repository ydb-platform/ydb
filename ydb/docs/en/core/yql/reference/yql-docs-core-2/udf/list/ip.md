# Ip

The `Ip`  module supports both the IPv4 and IPv6 addresses. By default, they are represented as binary strings of 4 and 16 bytes, respectively.

**List of functions**

* ```Ip::FromString(String{Flags:AutoMap}) -> String?``` - From a human-readable representation to a binary representation.
* ```Ip::ToString(String{Flags:AutoMap}) -> String?``` - From a binary representation to a human-readable representation.
* ```Ip::IsIPv4(String?) -> Bool```
* ```Ip::IsIPv6(String?) -> Bool```
* ```Ip::IsEmbeddedIPv4(String?) -> Bool```
* ```Ip::ConvertToIPv6(String{Flags:AutoMap}) -> String```: IPv6 remains unchanged, and IPv4 becomes embedded in IPv6
* ```Ip::GetSubnet(String{Flags:AutoMap}, [Uint8?]) -> String```: The second argument is the subnet size, by default it's 24 for IPv4 and 64 for IPv6

**Examples**

```sql
SELECT Ip::IsEmbeddedIPv4(
  Ip::FromString("::ffff:77.75.155.3")
); -- true

SELECT
  Ip::ToString(
    Ip::GetSubnet(
      Ip::FromString("213.180.193.3")
    )
  ); -- "213.180.193.0"
```

