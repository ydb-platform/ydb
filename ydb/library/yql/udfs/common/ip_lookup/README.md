Allows searching ip in prefix database (e.g. geoip) compiled to binary blob

Interface:

IpLookup::LoolupIp(ip STRING, dictBlob STRING) -> Int64
        arguments:
                ip: ipv4 or ipv6 ip as a string
                dictBlob: dictionary blob
        returns:
                non-negative value between 0 and INT64_MAX on match
                negative number (-1) on mismatch
                NULL on internal error (corrupt trie)

IpLookup::IpPrefix(ip STRING [, ipv6_prefix_size Int [, ipv4_prefix_size Int ] ]) -> Uint64
        arguments:
                ip: ipv4 or ipv6 ip as a string
                ipv6_prefix_size: optional number between 0 and 56, default 32:
                        size of prefix for ipv6
                ipv4_prefix_size: optional number between 0 and 24, default 8:
                        size of prefix for ipv4
                *_prefix_size must match parameters for ip-dict-compile
        returns:
                Uint64 suitable as key for dictionary separation

Blob format: [[[ PRELIMIMNARY ]]]
        Header (16 bytes):
                array<ui8, 8> signature: "Trie0000"
                ui32 dictSize: number of nodes in trie (one past last index in trie)
                ui8 trieBits: number of bits (must be from 1 to 8 and be divisor of 32)
                array<ui8, 3> reserved
        Dictionary:
                array<TrieEntry, dictSize>
        union TrieEntry {
                array<ui32, (1u<<trieBits)> childs;
                ui64 ip_key;\
        };
        trie[node].childs[bit] index of child of node (0 if no child)
        trie[node].ip_key is payload key with 63th bit set
