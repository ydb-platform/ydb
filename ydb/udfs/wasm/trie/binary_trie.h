#pragma once

#include <util/generic/yexception.h>
#include <util/system/unaligned_mem.h>

#include <string_view>

namespace NBinaryTrie {

inline i64 LookupTriev0(const auto& text, const auto& dictBlob) {
    constexpr size_t headerSize = 16;
    if (dictBlob.size() < headerSize) {
        throw yexception() << "Corrupt trie: too small: " << dictBlob.size() << " < " << headerSize;
    }
    if (!std::string_view(dictBlob).starts_with("Trie0000")) {
        throw yexception() << "Corrupt trie: invalid signature";
    }
    const auto trie = dictBlob.data();
    const ui32 dictSize = ReadUnaligned<ui32>(trie + 8);
    const ui8 trieBits = ReadUnaligned<ui8>(trie + 8 + 4);
    if (trieBits < 1 || trieBits > 8 || 8 % trieBits != 0) {
        throw yexception() << "Corrupt trie: trieBits = " << (int)trieBits;
    }
    const ui32 trieSize = ui32(1) << trieBits;
    const auto itemSize = sizeof(ui32) << trieBits;
    if ((dictBlob.size() - headerSize) / itemSize < dictSize) {
        throw yexception() << "Corrupt trie: too small: " << (dictBlob.size() - headerSize) / itemSize << " < " << dictSize;
    }
    if (dictSize == 0) {
        return -1;
    }
    ui32 h = 0;
    constexpr ui32 eol = ui32(1) << (32 - 1);
    auto readTrie = [&trie, trieBits](ui32 h, ui32 bit) {
        return ReadUnaligned<ui32>(trie + headerSize + ((h << trieBits) + bit) * sizeof(ui32));
    };
    for (auto&& data : text) {
        for (auto i = sizeof(data) * CHAR_BIT;;) {
            auto item1 = readTrie(h, 1);
            if (item1 & eol) {
                return (int64_t(item1 ^ eol) << 32) ^ readTrie(h, 0);
            }
            if (!i) {
                break;
            }
            i -= trieBits;
            auto bit = (data >> i) & (trieSize - 1);
            auto next = readTrie(h, bit);
            if (next >= dictSize) {
                throw yexception() << "Corrupt trie: " << h << "[" << bit << "] = " << next << " >= " << dictSize;
            }
            if (next == 0) {
                return -1;
            }
            h = next;
        }
    }
    return -1;
}

namespace NImpl {

constexpr size_t HeaderSize = 16;

inline void LookupTrie(const auto& text, const auto& dictBlob, ui8 flagsAnd, ui8 flagsXor, auto&& hook) {
    const auto trie = dictBlob.data() + HeaderSize;
    const ui32 dictSize = ReadUnaligned<ui32>(trie - 8);
    const ui8 trieBits = ReadUnaligned<ui8>(trie - 8 + 4);
    const ui8 flags = ReadUnaligned<ui8>(trie - 8 + 4 + 1);
    if (((flags & flagsAnd) ^ flagsXor) != 0) {
        throw yexception() << "Incompatible trie: flags = " << (int)flags;
    }
    if (trieBits < 1 || trieBits > 8 || 8 % trieBits != 0) {
        throw yexception() << "Corrupt trie: trieBits = " << (int)trieBits;
    }
    if (dictBlob.size() - HeaderSize < dictSize) {
        throw yexception() << "Corrupt trie: too small: " << dictBlob.size() << " < " << dictSize + HeaderSize;
    }
    const ui32 trieSize = ui32(1) << trieBits;
    Y_DEBUG_ABORT_UNLESS(trieBits <= trieSize);
    if (dictSize == 0) {
        return;
    }
    if (dictSize < trieSize * sizeof(ui32)) {
        throw yexception() << "Corrupt trie: too small: " << dictSize << " < " << (trieSize * sizeof(ui32));
    }
    ui32 head = 0;
    constexpr ui32 match = ui32(1) << (32 - 1);
    auto readTrie = [&trie](ui32 head, ui32 bit) {
        return ReadUnaligned<ui32>(trie + head + bit * sizeof(ui32));
    };
    uint32_t matchedLength = 0;
    if ((readTrie(head, 0) & match)) { // special-case: 0/0
        hook(head + trieSize * sizeof(ui32), 0u, matchedLength);
    }
    for (auto&& data : text) {
        for (auto i = sizeof(data) * CHAR_BIT; i;) {
            i -= trieBits;
            auto bit = (data >> i) & (trieSize - 1);
            auto next = readTrie(head, bit) & ~match;
            if (next > dictSize - trieSize * sizeof(ui32)) {
                throw yexception() << "Corrupt trie: " << head << "[" << bit << "] = " << next << " > " << dictSize << " - " << (trieSize * sizeof(ui32));
            }
            if (next == 0) {
                return;
            }
            head = next;
            matchedLength += trieBits;
            for (size_t j = trieBits; j--;) {
                if ((readTrie(head, j) & match)) {
                    hook(head + trieSize * sizeof(ui32), j, matchedLength);
                }
            }
        }
    }
}

} // namespace NImpl

inline i64 LookupTrie(const auto& text, const auto& dictBlob) {
    static_assert(sizeof(dictBlob.data()[0]) == 1);
    using TPayload = ui64;
    if (dictBlob.size() < NImpl::HeaderSize) {
        throw yexception() << "Corrupt trie: too small: " << dictBlob.size() << " < " << NImpl::HeaderSize;
    }
    if (!std::string_view(dictBlob).starts_with("Trie0001")) {
        if (std::string_view(dictBlob).starts_with("Trie0000")) {
            return LookupTriev0(text, dictBlob);
        }
        throw yexception() << "Corrupt trie: invalid signature";
    }
    ui32 payloadPos = 0;
    NImpl::LookupTrie(text, dictBlob, 1u, 0u, [&payloadPos](auto pos, auto j, auto) {
        payloadPos = pos + j * sizeof(TPayload);
    });
    if (payloadPos == 0) {
        return -1;
    }
    const auto trie = dictBlob.data() + NImpl::HeaderSize;
    const auto dictSize = dictBlob.size() - NImpl::HeaderSize;
    if (payloadPos + sizeof(TPayload) > dictSize) {
        throw yexception() << "Corrupt trie: " << payloadPos << " > " << dictSize << " - " << sizeof(TPayload);
    }
    return ReadUnaligned<TPayload>(trie + payloadPos);
}

template <class TResult>
TResult LookupTrieAllMatchesv0(const auto& text, const auto& dictBlob) {
    static_assert(sizeof(dictBlob.data()[0]) == 1);
    TResult matches;
    constexpr size_t headerSize = 16;
    if (dictBlob.size() < headerSize) {
        throw yexception() << "Corrupt trie: too small: " << dictBlob.size() << " < " << headerSize;
    }
    if (!std::string_view(dictBlob).starts_with("TrieM000")) {
        throw yexception() << "Corrupt trie: invalid signature";
    }
    const auto trie = dictBlob.data();
    const ui32 dictSize = ReadUnaligned<ui32>(trie + 8);
    const ui8 trieBits = ReadUnaligned<ui8>(trie + 8 + 4);
    if (trieBits < 1 || trieBits > 8 || 8 % trieBits != 0) {
        throw yexception() << "Corrupt trie: trieBits = " << (int)trieBits;
    }
    const ui32 trieSize = ui32(1) << trieBits;
    Y_DEBUG_ABORT_UNLESS(trieBits <= trieSize);
    const auto itemSize = sizeof(ui32) << trieBits;
    if ((dictBlob.size() - headerSize) / itemSize < dictSize) {
        throw yexception() << "Corrupt trie: too small: " << (dictBlob.size() - headerSize) / itemSize << " < " << dictSize;
    }
    if (dictSize == 0) {
        return matches;
    }
    ui32 h = 0;
    constexpr ui32 match = ui32(1) << (32 - 1);
    auto readTrie = [&trie, trieBits](ui32 h, ui32 bit) {
        return ReadUnaligned<ui32>(trie + headerSize + ((h << trieBits) + bit) * sizeof(ui32));
    };
    ui32 matchedLength = 0;
    if ((readTrie(h, 0) & match)) {
        matches.push_back(0);
    }
    for (auto&& data : text) {
        for (auto i = sizeof(data) * CHAR_BIT; i;) {
            i -= trieBits;
            auto bit = (data >> i) & (trieSize - 1);
            auto next = readTrie(h, bit) & ~match;
            if (next >= dictSize) {
                throw yexception() << "Corrupt trie: " << h << "[" << bit << "] = " << next << " >= " << dictSize;
            }
            if (next == 0) {
                return matches;
            }
            matchedLength += trieBits;
            h = next;
            for (size_t j = 0; j < trieBits; ++j) {
                if ((readTrie(h, j) & match)) {
                    matches.push_back(matchedLength - j);
                }
            }
        }
    }
    return matches;
}

template <class TResult>
TResult LookupTrieAllMatches(const auto& text, const auto& dictBlob) {
    static_assert(sizeof(dictBlob.data()[0]) == 1);
    if (dictBlob.size() < NImpl::HeaderSize) {
        throw yexception() << "Corrupt trie: too small: " << dictBlob.size() << " < " << NImpl::HeaderSize;
    }
    if (!std::string_view(dictBlob).starts_with("Trie0001")) {
        if (std::string_view(dictBlob).starts_with("TrieM000")) {
            return LookupTrieAllMatchesv0<TResult>(text, dictBlob);
        }
        throw yexception() << "Corrupt trie: invalid signature";
    }
    TResult matches;
    NImpl::LookupTrie(text, dictBlob, 0u, 0u, [&matches](auto, auto j, auto matchedLength) {
        matches.push_back(matchedLength - j);
    });
    return matches;
}

template <class TResult>
TResult LookupTrieAllMatchesKeys(const auto& text, const auto& dictBlob) {
    static_assert(sizeof(dictBlob.data()[0]) == 1);
    using TPayload = ui64;
    if (dictBlob.size() < NImpl::HeaderSize) {
        throw yexception() << "Corrupt trie: too small: " << dictBlob.size() << " < " << NImpl::HeaderSize;
    }
    if (!std::string_view(dictBlob).starts_with("Trie0001")) {
        throw yexception() << "Corrupt trie: invalid signature";
    }
    TResult matches;
    const auto trie = dictBlob.data() + NImpl::HeaderSize;
    const auto dictSize = dictBlob.size() - NImpl::HeaderSize;
    NImpl::LookupTrie(text, dictBlob, 1u, 0u, [&matches, trie, dictSize](auto pos, auto j, auto) {
        ui32 payloadPos = pos + j * sizeof(TPayload);
        if (payloadPos + sizeof(TPayload) > dictSize) {
            throw yexception() << "Corrupt trie: " << payloadPos << " > " << dictSize << " - " << sizeof(TPayload);
        }
        matches.push_back(ReadUnaligned<TPayload>(trie + payloadPos));
    });
    return matches;
}

} // namespace NBinaryTrie
