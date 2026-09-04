/* syntax version 1 */
-- Full scan of addresses against dictionary id=7 (7 MiB).
-- Scalar subquery, not a JOIN: $dict is pinned once via bridge EnsureString.
$dict = SELECT Unwrap(MIN(acl)) FROM `ip_dict` WHERE id = 7ul;

SELECT SUM(Trie::LookupPinned(addr, $dict)) AS checksum
FROM `ip_addr`;
