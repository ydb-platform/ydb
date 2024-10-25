/* syntax version 1 */
$dict = '\x00\x00\x00\x00\x01\x00\x00\x00\x0a\x00\x00\x00\x00\x00\x00\x80';
SELECT IpLookup::IpPrefix(key) AS prefix, IpLookup::LookupIp(key, $dict) as result FROM Input
