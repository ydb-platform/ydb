# Ip
В модуле `Ip` поддерживаются как IPv4, так и IPv6 адреса. По умолчанию они представляются в виде бинарных строк длиной 4 и 16 байт, соответственно.

**Список функций**

* ```Ip::FromString(String{Flags:AutoMap}) -> String?``` - из человекочитаемого представления в бинарное
* ```Ip::ToString(String{Flags:AutoMap}) -> String?``` - из бинарного представления в человекочитаемое
* ```Ip::IsIPv4(String?) -> Bool```
* ```Ip::IsIPv6(String?) -> Bool```
* ```Ip::IsEmbeddedIPv4(String?) -> Bool```
* ```Ip::ConvertToIPv6(String{Flags:AutoMap}) -> String``` - IPv6 остается без изменений, а IPv4 становится embedded в IPv6
* ```Ip::GetSubnet(String{Flags:AutoMap}, [Uint8?]) -> String``` - во втором аргументе размер подсети, по умолчанию 24 для IPv4 и 64 для IPv6

**Примеры**

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
