#include <algorithm>
#include <string>
#include <unordered_map>
#include <array>
#include <vector>
#include <iostream>
#include <iomanip>
#include <cassert>
#include <cstdint>
#include <bit>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <byteswap.h>
#include <cstring>

constexpr uint32_t eol = uint32_t(1)<<(32 - 1);

// squeeze structurally-same subtrees
size_t compressTrie(uint32_t u, const auto& trie, auto& ctrie, auto& uctrie) {
    const auto &head = trie[u];
    std::decay_t<decltype(head)> chead {};
    if (head[1] & eol) {
        chead = head;
    } else {
        for (unsigned i = 0; i < head.size(); ++i) {
            if (head[i]) chead[i] = compressTrie(head[i], trie, ctrie, uctrie);
        }
    }
    auto j = uctrie.emplace(chead, ctrie.size());
    if (j.second)
        ctrie.emplace_back(chead);
    return j.first->second;
}

constexpr unsigned TrieBits = 1;
constexpr unsigned TrieSize = 1u<<TrieBits;
static_assert(TrieBits > 0);

int main([[maybe_unused]] int argc, [[maybe_unused]] char *argv[]) {
    std::cin.tie(nullptr);
    std::ios_base::sync_with_stdio(false);
    std::string r1, r2;
    using TPayload = std::string;
    TPayload asn;
    std::unordered_map<uint64_t, std::vector<std::array<unsigned, TrieSize >>> prefixes;
    uint64_t masks = 0;
    std::unordered_map<TPayload, uint64_t> payloads;
    payloads.emplace(TPayload{}, 0u);
    std::string s;
    while(std::getline(std::cin, s)) {
        const auto c1 = s.find(',');
        assert(c1 != std::string::npos);
        const auto c2 = s.find(',', c1 + 1);
        assert(c2 != std::string::npos);
        r1 = s.substr(0, c1);
        r2 = s.substr(c1 + 1, c2 - c1 - 1);
        asn = s.substr(c2 + 1);
        struct in6_addr a1 {}, a2 {};
        int rc;
        constexpr unsigned ipv6PrefixLength = 32;
        constexpr unsigned ipv4PrefixLength = 8;
        if (inet_pton(AF_INET6, r1.c_str(), &a1) > 0)
            rc = inet_pton(AF_INET6, r2.c_str(), &a2);
        else {
            size_t offset = (ipv6PrefixLength - ipv4PrefixLength)/8;
            rc = inet_pton(AF_INET, r1.c_str(), ((char *)&a1) + offset);
            assert(rc > 0);
            rc = inet_pton(AF_INET, r2.c_str(), ((char *)&a2) + offset);
            memset((char *)&a2 + offset + 4, 0xff, sizeof(a2) - (offset + 4));
        }
        assert(rc > 0);
        std::cerr << r1 << '|' << r2 << std::endl;
        uint64_t p1, p2;
        uint64_t s2;
        auto id = payloads.emplace(asn, payloads.size());
        memcpy(&p1, &a1, sizeof(p1));
        p1 = bswap_64(p1);
        memcpy(&p2, &a2, sizeof(p2));
        p2 = bswap_64(p2);
        std::cerr << std::hex << p1 << '|' << std::endl << p2 << std::endl;
        std::memcpy(&s2, ((const char *)&a2) + sizeof(p1), sizeof(s2));
        while(p1 <= p2) {
            unsigned bits = std::min<unsigned>(std::countr_zero(p1), 64 - ipv6PrefixLength);
            while(p1 + (uint64_t(1)<<bits) - 1 > p2)
                --bits;
            unsigned len = (128 - (bits + 64));
            std::cerr << std::hex << p1 << std::dec << '/' << len << ' ' << (p1 >> (64 - ipv6PrefixLength)) << std::endl;
            if (id.first->second != 0) { 
                unsigned h = 0;
                auto &trie = prefixes.emplace(p1 >> (64 - ipv6PrefixLength), 1).first->second;

                std::decay_t<decltype(trie[0])> alt {};
                //assert(len >= 64 - prefixlen);
                unsigned i = 0;
                for (; i + TrieBits <= len; i += TrieBits) {
                    auto bit = (p1 >> (64 - (i + TrieBits))) & (TrieSize - 1);
#if 0
                    std::assert(!(trie[h][1] & eol));
#else
                    if ((trie[h][1] & eol)) {
                        alt = std::exchange(trie[h], decltype(alt){});
                    }
#endif
                    if (alt[1]) {
                        for (unsigned altbit = 0; altbit < TrieBits; ++altbit)
                            if (altbit != bit) {
                                assert(trie[h][bit] == 0);
                                trie[h][altbit] = trie.size();
                                trie.emplace_back(alt);
                            }
                        std::cerr << "push alt" << std::endl;
                    }
                    auto &next = trie[h][bit];
                    if (next) {
                        h = next;
                    } else {
                        next = h = trie.size();
                        trie.emplace_back();
                    }
                }
                if (auto rem = len % TrieBits) {
                    assert(!alt[1]);
                    assert(i == len - rem);
                    i += TrieBits;
                    auto bit = (p1 >> (64 - i)) & (TrieSize - 1);
                    for (unsigned j = 0; j < (1u<<(TrieBits - rem)); ++j) {
                        auto &next = trie[h][bit + j];
                        assert(next == 0);
                        auto hh = next = trie.size();
                        trie.emplace_back();
                        trie[hh][0] = uint32_t(id.first->second);
                        trie[hh][1] = eol | uint32_t(id.first->second >> 32);
                    }
                } else {
                    trie[h][0] = uint32_t(id.first->second);
                    trie[h][1] = eol | uint32_t(id.first->second >> 32);
                }
                ++masks;
            } else {
                std::cerr << '!' << std::endl;
            }
            p1 += (uint64_t(1)<<(bits - 0));
        }
    }
    size_t maxSizeBytes = 0;
    size_t sumSizeBytes = 0;
    for (auto &[s,trie]: prefixes) {
        auto sizeb = trie.size()*sizeof(trie[0]);
        std::cerr << std::hex << s << std::dec << ' ' << trie.size() << ' ' << sizeb << std::endl;
        maxSizeBytes = std::max(maxSizeBytes, sizeb);
        sumSizeBytes += sizeb;
    }
    std::cerr 
        << "nprefix=" << prefixes.size() << ' '
        << "maxsize=" << maxSizeBytes << ' '
        << "masks=" << masks << ' '
        << "payloads=" << payloads.size() << ' '
        << "sizeb=" << sumSizeBytes << ' '
        << std::endl;
    size_t maxCompressedSize = 0;
    size_t csum = 0;
    auto hashop = [](const auto &a) {
        return std::hash<std::string_view>{}(std::string_view((const char *)&a, sizeof(a)));
    };
    for (auto &[prefix, trie]: prefixes) {
        std::decay_t<decltype(trie)> ctrie(1);
        std::unordered_map<decltype(ctrie)::value_type, size_t, decltype(hashop)> uctrie;
        auto root = compressTrie(0, trie, ctrie, uctrie);
        ctrie[0] = ctrie[root];
        if (root + 1 == ctrie.size()) ctrie.pop_back();
        auto sizeb = ctrie.size()*sizeof(ctrie[0]);
        maxCompressedSize = std::max(maxCompressedSize, sizeb);
        csum += sizeb;
        std::cout << "{\"prefix\":" << prefix << ",\"ipdict\":\"" << std::hex;
        std::cout << "Trie0000";
        auto dumpHex = [](auto it, auto end) {
            for (; it != end; ++it)
                std::cout << "\\u00" << std::setw(2) << std::setfill('0') << (unsigned)*it;
        };
        uint32_t ctrieSize = ctrie.size();
        uint8_t ctrieSizeBytes[sizeof(ctrieSize)];
        memcpy(ctrieSizeBytes, &ctrieSize, sizeof(ctrieSize));
        uint8_t trieBitsBytes[4] = { TrieBits };
        dumpHex(ctrieSizeBytes, ctrieSizeBytes + sizeof(ctrieSize));
        dumpHex(trieBitsBytes, trieBitsBytes + sizeof(trieBitsBytes));
        dumpHex((const uint8_t *)ctrie.data(), (const uint8_t *)(ctrie.data() + ctrie.size()));
        std::cout << "\"}\n" << std::dec;
        std::cerr << std::hex << prefix << std::dec << ' ' << trie.size()*sizeof(trie[0]) << '>' << sizeb << std::endl;
    }
    std::cerr 
        << "csizeb=" << csum << ' '
        << "maxcsize=" << maxCompressedSize << ' '
        << std::endl;
    return 0;
}
