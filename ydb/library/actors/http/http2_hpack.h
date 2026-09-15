#pragma once

#include "http2_frames.h"

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/deque.h>

#include <cstdint>
#include <utility>

namespace NHttp::NHttp2 {

// HPACK: Header Compression for HTTP/2 (RFC 7541)

// Huffman decoder node for trie-based decoding
class THuffmanDecoder {
public:
    THuffmanDecoder();
    // Decode Huffman-encoded data. Returns false on error.
    bool Decode(const uint8_t* data, size_t len, TString& out) const;

private:
    // Binary trie for fast decoding
    // Each node has two children (0 and 1)
    // If Symbol >= 0, it's a leaf node
    struct TNode {
        int32_t Children[2] = {-1, -1};
        int16_t Symbol = -1; // -1 = not a leaf
    };
    TVector<TNode> Nodes;
    int32_t AllocNode();
};

// HPACK dynamic table (RFC 7541 Section 2.3.2)
class THPackDynamicTable {
public:
    explicit THPackDynamicTable(uint32_t maxSize = DEFAULT_HEADER_TABLE_SIZE);

    void Add(TString name, TString value);
    bool Get(size_t index, TStringBuf& name, TStringBuf& value) const; // 1-based index into dynamic table
    size_t Size() const { return Entries.size(); }
    size_t TableSize() const { return CurrentSize; } // in bytes (per HPACK rules)
    void SetMaxSize(uint32_t maxSize);

private:
    struct TDynEntry {
        TString Name;
        TString Value;
        size_t EntrySize() const { return Name.size() + Value.size() + 32; } // RFC 7541 Section 4.1
    };

    TDeque<TDynEntry> Entries; // front = most recently added
    uint32_t MaxSize;
    size_t CurrentSize = 0;

    void Evict();
};

// HPACK Encoder
class THPackEncoder {
public:
    explicit THPackEncoder(uint32_t maxTableSize = DEFAULT_HEADER_TABLE_SIZE);

    // Encode a list of headers into an HPACK block
    TString Encode(const TVector<std::pair<TString, TString>>& headers);

    void SetMaxTableSize(uint32_t maxSize);

    static void EncodeInteger(TString& out, uint32_t value, uint8_t prefix, uint8_t pattern);
    static void EncodeString(TString& out, TStringBuf str, bool huffman = false);
    static TString HuffmanEncode(TStringBuf str);

private:
    THPackDynamicTable DynTable;
    bool TableSizeChanged = false;
    uint32_t PendingMaxSize = DEFAULT_HEADER_TABLE_SIZE;

    // Find header in static or dynamic table
    // Returns: {index, nameOnly} where index=0 means not found
    std::pair<size_t, bool> FindHeader(TStringBuf name, TStringBuf value) const;
};

// HPACK Decoder
class THPackDecoder {
public:
    explicit THPackDecoder(uint32_t maxTableSize = DEFAULT_HEADER_TABLE_SIZE);

    // Decode an HPACK block into a list of headers
    // Returns false on decode error
    bool Decode(TStringBuf data, TVector<std::pair<TString, TString>>& headers);

    void SetMaxTableSize(uint32_t maxSize);

    bool DecodeInteger(const uint8_t*& pos, const uint8_t* end, uint8_t prefix, uint32_t& value);
    bool DecodeString(const uint8_t*& pos, const uint8_t* end, TString& out);
    bool LookupIndex(size_t index, TString& name, TString& value) const;
    bool LookupIndexName(size_t index, TString& name) const;

private:
    THPackDynamicTable DynTable;
    THuffmanDecoder HuffDecoder;
};

} // namespace NHttp::NHttp2
