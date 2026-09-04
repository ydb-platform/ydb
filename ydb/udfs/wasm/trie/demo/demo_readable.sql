/* syntax version 1 */
-- First addresses looked up in dictionary id=1 (1 MiB). No JOIN: scalar subquery.
-- Bridge LookupWithStringPinned: TryReuse handle + BridgeRef + BridgeEnsureString
-- pins $dict into compartment linear memory once per query generation.

$dict = SELECT Unwrap(MIN(acl)) FROM `ip_dict` WHERE id = 1ul;

SELECT
    id,
    ip,
    Trie::LookupWithStringPinned(addr, $dict) AS org
FROM `ip_addr`
WHERE id <= 10ul
ORDER BY id;
