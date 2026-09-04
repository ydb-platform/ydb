/* syntax version 1 */
-- Full scan of addresses against dictionary id=1 (1 MiB).
-- Scalar subquery, not a JOIN: $dict is pinned once via bridge EnsureString.
$dict = SELECT Unwrap(MIN(acl)) FROM `ip_dict` WHERE id = 1ul;

SELECT SUM(Trie::LookupPinned(addr, $dict)) AS checksum
FROM `ip_addr`;
