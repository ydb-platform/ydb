/* syntax version 1 */
-- One address, dictionary id=1. Bridge pin path (no RFC 005 pragmas).
$dict = SELECT Unwrap(MIN(acl)) FROM `ip_dict` WHERE id = 1ul;

SELECT id, ip,
       Trie::LookupWithStringPinned(addr, $dict) AS org
FROM `ip_addr`
WHERE id = 1ul;
