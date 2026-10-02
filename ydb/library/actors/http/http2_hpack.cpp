#include "http2_hpack.h"

#include <util/generic/yexception.h>

namespace NHttp::NHttp2 {

// Static table entry
struct THPackEntry {
    TStringBuf Name;
    TStringBuf Value;
};

// RFC 7541 Appendix A - Static Table
static constexpr THPackEntry STATIC_TABLE[] = {
    {"",                          ""},             // index 0 (unused, 1-based)
    {":authority",                ""},             // 1
    {":method",                   "GET"},          // 2
    {":method",                   "POST"},         // 3
    {":path",                     "/"},            // 4
    {":path",                     "/index.html"},  // 5
    {":scheme",                   "http"},         // 6
    {":scheme",                   "https"},        // 7
    {":status",                   "200"},          // 8
    {":status",                   "204"},          // 9
    {":status",                   "206"},          // 10
    {":status",                   "304"},          // 11
    {":status",                   "400"},          // 12
    {":status",                   "404"},          // 13
    {":status",                   "500"},          // 14
    {"accept-charset",            ""},             // 15
    {"accept-encoding",           "gzip, deflate"},// 16
    {"accept-language",           ""},             // 17
    {"accept-ranges",             ""},             // 18
    {"accept",                    ""},             // 19
    {"access-control-allow-origin",""},            // 20
    {"age",                       ""},             // 21
    {"allow",                     ""},             // 22
    {"authorization",             ""},             // 23
    {"cache-control",             ""},             // 24
    {"content-disposition",       ""},             // 25
    {"content-encoding",          ""},             // 26
    {"content-language",          ""},             // 27
    {"content-length",            ""},             // 28
    {"content-location",          ""},             // 29
    {"content-range",             ""},             // 30
    {"content-type",              ""},             // 31
    {"cookie",                    ""},             // 32
    {"date",                      ""},             // 33
    {"etag",                      ""},             // 34
    {"expect",                    ""},             // 35
    {"expires",                   ""},             // 36
    {"from",                      ""},             // 37
    {"host",                      ""},             // 38
    {"if-match",                  ""},             // 39
    {"if-modified-since",         ""},             // 40
    {"if-none-match",             ""},             // 41
    {"if-range",                  ""},             // 42
    {"if-unmodified-since",       ""},             // 43
    {"last-modified",             ""},             // 44
    {"link",                      ""},             // 45
    {"location",                  ""},             // 46
    {"max-forwards",              ""},             // 47
    {"proxy-authenticate",        ""},             // 48
    {"proxy-authorization",       ""},             // 49
    {"range",                     ""},             // 50
    {"referer",                   ""},             // 51
    {"refresh",                   ""},             // 52
    {"retry-after",               ""},             // 53
    {"server",                    ""},             // 54
    {"set-cookie",                ""},             // 55
    {"strict-transport-security", ""},             // 56
    {"transfer-encoding",         ""},             // 57
    {"user-agent",                ""},             // 58
    {"vary",                      ""},             // 59
    {"via",                       ""},             // 60
    {"www-authenticate",          ""},             // 61
};

static constexpr size_t STATIC_TABLE_SIZE = 61; // entries 1..61

// Huffman code table (RFC 7541 Appendix B)
struct THuffmanCode {
    uint32_t Code;
    uint8_t BitLen;
};

// RFC 7541 Appendix B - Huffman Code
static constexpr THuffmanCode HUFFMAN_TABLE[257] = {
    {0x1ff8,     13}, // (  0)
    {0x7fffd8,   23}, // (  1)
    {0xfffffe2,  28}, // (  2)
    {0xfffffe3,  28}, // (  3)
    {0xfffffe4,  28}, // (  4)
    {0xfffffe5,  28}, // (  5)
    {0xfffffe6,  28}, // (  6)
    {0xfffffe7,  28}, // (  7)
    {0xfffffe8,  28}, // (  8)
    {0xffffea,   24}, // (  9)
    {0x3ffffffc, 30}, // ( 10)
    {0xfffffe9,  28}, // ( 11)
    {0xfffffea,  28}, // ( 12)
    {0x3ffffffd, 30}, // ( 13)
    {0xfffffeb,  28}, // ( 14)
    {0xfffffec,  28}, // ( 15)
    {0xfffffed,  28}, // ( 16)
    {0xfffffee,  28}, // ( 17)
    {0xfffffef,  28}, // ( 18)
    {0xffffff0,  28}, // ( 19)
    {0xffffff1,  28}, // ( 20)
    {0xffffff2,  28}, // ( 21)
    {0x3ffffffe, 30}, // ( 22)
    {0xffffff3,  28}, // ( 23)
    {0xffffff4,  28}, // ( 24)
    {0xffffff5,  28}, // ( 25)
    {0xffffff6,  28}, // ( 26)
    {0xffffff7,  28}, // ( 27)
    {0xffffff8,  28}, // ( 28)
    {0xffffff9,  28}, // ( 29)
    {0xffffffa,  28}, // ( 30)
    {0xffffffb,  28}, // ( 31)
    {0x14,        6}, // ' ' ( 32)
    {0x3f8,      10}, // '!' ( 33)
    {0x3f9,      10}, // '"' ( 34)
    {0xffa,      12}, // '#' ( 35)
    {0x1ff9,     13}, // '$' ( 36)
    {0x15,        6}, // '%' ( 37)
    {0xf8,        8}, // '&' ( 38)
    {0x7fa,      11}, // '\'' ( 39)
    {0x3fa,      10}, // '(' ( 40)
    {0x3fb,      10}, // ')' ( 41)
    {0xf9,        8}, // '*' ( 42)
    {0x7fb,      11}, // '+' ( 43)
    {0xfa,        8}, // ',' ( 44)
    {0x16,        6}, // '-' ( 45)
    {0x17,        6}, // '.' ( 46)
    {0x18,        6}, // '/' ( 47)
    {0x0,         5}, // '0' ( 48)
    {0x1,         5}, // '1' ( 49)
    {0x2,         5}, // '2' ( 50)
    {0x19,        6}, // '3' ( 51)
    {0x1a,        6}, // '4' ( 52)
    {0x1b,        6}, // '5' ( 53)
    {0x1c,        6}, // '6' ( 54)
    {0x1d,        6}, // '7' ( 55)
    {0x1e,        6}, // '8' ( 56)
    {0x1f,        6}, // '9' ( 57)
    {0x5c,        7}, // ':' ( 58)
    {0xfb,        8}, // ';' ( 59)
    {0x7ffc,     15}, // '<' ( 60)
    {0x20,        6}, // '=' ( 61)
    {0xffb,      12}, // '>' ( 62)
    {0x3fc,      10}, // '?' ( 63)
    {0x1ffa,     13}, // '@' ( 64)
    {0x21,        6}, // 'A' ( 65)
    {0x5d,        7}, // 'B' ( 66)
    {0x5e,        7}, // 'C' ( 67)
    {0x5f,        7}, // 'D' ( 68)
    {0x60,        7}, // 'E' ( 69)
    {0x61,        7}, // 'F' ( 70)
    {0x62,        7}, // 'G' ( 71)
    {0x63,        7}, // 'H' ( 72)
    {0x64,        7}, // 'I' ( 73)
    {0x65,        7}, // 'J' ( 74)
    {0x66,        7}, // 'K' ( 75)
    {0x67,        7}, // 'L' ( 76)
    {0x68,        7}, // 'M' ( 77)
    {0x69,        7}, // 'N' ( 78)
    {0x6a,        7}, // 'O' ( 79)
    {0x6b,        7}, // 'P' ( 80)
    {0x6c,        7}, // 'Q' ( 81)
    {0x6d,        7}, // 'R' ( 82)
    {0x6e,        7}, // 'S' ( 83)
    {0x6f,        7}, // 'T' ( 84)
    {0x70,        7}, // 'U' ( 85)
    {0x71,        7}, // 'V' ( 86)
    {0x72,        7}, // 'W' ( 87)
    {0xfc,        8}, // 'X' ( 88)
    {0x73,        7}, // 'Y' ( 89)
    {0xfd,        8}, // 'Z' ( 90)
    {0x1ffb,     13}, // '[' ( 91)
    {0x7fff0,    19}, // '\' ( 92)
    {0x1ffc,     13}, // ']' ( 93)
    {0x3ffc,     14}, // '^' ( 94)
    {0x22,        6}, // '_' ( 95)
    {0x7ffd,     15}, // '`' ( 96)
    {0x3,         5}, // 'a' ( 97)
    {0x23,        6}, // 'b' ( 98)
    {0x4,         5}, // 'c' ( 99)
    {0x24,        6}, // 'd' (100)
    {0x5,         5}, // 'e' (101)
    {0x25,        6}, // 'f' (102)
    {0x26,        6}, // 'g' (103)
    {0x27,        6}, // 'h' (104)
    {0x6,         5}, // 'i' (105)
    {0x74,        7}, // 'j' (106)
    {0x75,        7}, // 'k' (107)
    {0x28,        6}, // 'l' (108)
    {0x29,        6}, // 'm' (109)
    {0x2a,        6}, // 'n' (110)
    {0x7,         5}, // 'o' (111)
    {0x2b,        6}, // 'p' (112)
    {0x76,        7}, // 'q' (113)
    {0x2c,        6}, // 'r' (114)
    {0x8,         5}, // 's' (115)
    {0x9,         5}, // 't' (116)
    {0x2d,        6}, // 'u' (117)
    {0x77,        7}, // 'v' (118)
    {0x78,        7}, // 'w' (119)
    {0x79,        7}, // 'x' (120)
    {0x7a,        7}, // 'y' (121)
    {0x7b,        7}, // 'z' (122)
    {0x7fffe,    19}, // '{' (123)
    {0x7fc,      11}, // '|' (124)
    {0x3ffd,     14}, // '}' (125)
    {0x1ffd,     13}, // '~' (126)
    {0xffffffc,  28}, // (127)
    {0xfffe6,    20}, // (128)
    {0x3fffd2,   22}, // (129)
    {0xfffe7,    20}, // (130)
    {0xfffe8,    20}, // (131)
    {0x3fffd3,   22}, // (132)
    {0x3fffd4,   22}, // (133)
    {0x3fffd5,   22}, // (134)
    {0x7fffd9,   23}, // (135)
    {0x3fffd6,   22}, // (136)
    {0x7fffda,   23}, // (137)
    {0x7fffdb,   23}, // (138)
    {0x7fffdc,   23}, // (139)
    {0x7fffdd,   23}, // (140)
    {0x7fffde,   23}, // (141)
    {0xffffeb,   24}, // (142)
    {0x7fffdf,   23}, // (143)
    {0xffffec,   24}, // (144)
    {0xffffed,   24}, // (145)
    {0x3fffd7,   22}, // (146)
    {0x7fffe0,   23}, // (147)
    {0xffffee,   24}, // (148)
    {0x7fffe1,   23}, // (149)
    {0x7fffe2,   23}, // (150)
    {0x7fffe3,   23}, // (151)
    {0x7fffe4,   23}, // (152)
    {0x1fffdc,   21}, // (153)
    {0x3fffd8,   22}, // (154)
    {0x7fffe5,   23}, // (155)
    {0x3fffd9,   22}, // (156)
    {0x7fffe6,   23}, // (157)
    {0x7fffe7,   23}, // (158)
    {0xffffef,   24}, // (159)
    {0x3fffda,   22}, // (160)
    {0x1fffdd,   21}, // (161)
    {0xfffe9,    20}, // (162)
    {0x3fffdb,   22}, // (163)
    {0x3fffdc,   22}, // (164)
    {0x7fffe8,   23}, // (165)
    {0x7fffe9,   23}, // (166)
    {0x1fffde,   21}, // (167)
    {0x7fffea,   23}, // (168)
    {0x3fffdd,   22}, // (169)
    {0x3fffde,   22}, // (170)
    {0xfffff0,   24}, // (171)
    {0x1fffdf,   21}, // (172)
    {0x3fffdf,   22}, // (173)
    {0x7fffeb,   23}, // (174)
    {0x7fffec,   23}, // (175)
    {0x1fffe0,   21}, // (176)
    {0x1fffe1,   21}, // (177)
    {0x3fffe0,   22}, // (178)
    {0x1fffe2,   21}, // (179)
    {0x7fffed,   23}, // (180)
    {0x3fffe1,   22}, // (181)
    {0x7fffee,   23}, // (182)
    {0x7fffef,   23}, // (183)
    {0xfffea,    20}, // (184)
    {0x3fffe2,   22}, // (185)
    {0x3fffe3,   22}, // (186)
    {0x3fffe4,   22}, // (187)
    {0x7ffff0,   23}, // (188)
    {0x3fffe5,   22}, // (189)
    {0x3fffe6,   22}, // (190)
    {0x7ffff1,   23}, // (191)
    {0x3ffffe0,  26}, // (192)
    {0x3ffffe1,  26}, // (193)
    {0xfffeb,    20}, // (194)
    {0x7fff1,    19}, // (195)
    {0x3fffe7,   22}, // (196)
    {0x7ffff2,   23}, // (197)
    {0x3fffe8,   22}, // (198)
    {0x1ffffec,  25}, // (199)
    {0x3ffffe2,  26}, // (200)
    {0x3ffffe3,  26}, // (201)
    {0x3ffffe4,  26}, // (202)
    {0x7ffffde,  27}, // (203)
    {0x7ffffdf,  27}, // (204)
    {0x3ffffe5,  26}, // (205)
    {0xfffff1,   24}, // (206)
    {0x1ffffed,  25}, // (207)
    {0x7fff2,    19}, // (208)
    {0x1fffe3,   21}, // (209)
    {0x3ffffe6,  26}, // (210)
    {0x7ffffe0,  27}, // (211)
    {0x7ffffe1,  27}, // (212)
    {0x3ffffe7,  26}, // (213)
    {0x7ffffe2,  27}, // (214)
    {0xfffff2,   24}, // (215)
    {0x1fffe4,   21}, // (216)
    {0x1fffe5,   21}, // (217)
    {0x3ffffe8,  26}, // (218)
    {0x3ffffe9,  26}, // (219)
    {0xffffffd,  28}, // (220)
    {0x7ffffe3,  27}, // (221)
    {0x7ffffe4,  27}, // (222)
    {0x7ffffe5,  27}, // (223)
    {0xfffec,    20}, // (224)
    {0xfffff3,   24}, // (225)
    {0xfffed,    20}, // (226)
    {0x1fffe6,   21}, // (227)
    {0x3fffe9,   22}, // (228)
    {0x1fffe7,   21}, // (229)
    {0x1fffe8,   21}, // (230)
    {0x7ffff3,   23}, // (231)
    {0x3fffea,   22}, // (232)
    {0x3fffeb,   22}, // (233)
    {0x1ffffee,  25}, // (234)
    {0x1ffffef,  25}, // (235)
    {0xfffff4,   24}, // (236)
    {0xfffff5,   24}, // (237)
    {0x3ffffea,  26}, // (238)
    {0x7ffff4,   23}, // (239)
    {0x3ffffeb,  26}, // (240)
    {0x7ffffe6,  27}, // (241)
    {0x3ffffec,  26}, // (242)
    {0x3ffffed,  26}, // (243)
    {0x7ffffe7,  27}, // (244)
    {0x7ffffe8,  27}, // (245)
    {0x7ffffe9,  27}, // (246)
    {0x7ffffea,  27}, // (247)
    {0x7ffffeb,  27}, // (248)
    {0xffffffe,  28}, // (249)
    {0x7ffffec,  27}, // (250)
    {0x7ffffed,  27}, // (251)
    {0x7ffffee,  27}, // (252)
    {0x7ffffef,  27}, // (253)
    {0x7fffff0,  27}, // (254)
    {0x3ffffee,  26}, // (255)
    {0x3fffffff, 30}, // EOS (256)
};

// ============== THuffmanDecoder ==============

THuffmanDecoder::THuffmanDecoder() {
    // Build binary trie from Huffman table
    Nodes.reserve(1024);
    AllocNode(); // root = 0
    for (int sym = 0; sym < 257; ++sym) {
        const auto& hc = HUFFMAN_TABLE[sym];
        int32_t node = 0;
        for (int bit = hc.BitLen - 1; bit >= 0; --bit) {
            int child = (hc.Code >> bit) & 1;
            if (Nodes[node].Children[child] < 0) {
                Nodes[node].Children[child] = AllocNode();
            }
            node = Nodes[node].Children[child];
        }
        Nodes[node].Symbol = static_cast<int16_t>(sym);
    }
}

int32_t THuffmanDecoder::AllocNode() {
    Nodes.emplace_back();
    return static_cast<int32_t>(Nodes.size() - 1);
}

bool THuffmanDecoder::Decode(const uint8_t* data, size_t len, TString& out) const {
    out.clear();
    int32_t node = 0;
    int bitsLeft = 0; // track padding bits

    for (size_t i = 0; i < len; ++i) {
        uint8_t byte = data[i];
        for (int bit = 7; bit >= 0; --bit) {
            int child = (byte >> bit) & 1;
            if (Nodes[node].Children[child] < 0) {
                return false; // invalid encoding
            }
            node = Nodes[node].Children[child];
            if (Nodes[node].Symbol >= 0) {
                if (Nodes[node].Symbol == 256) {
                    return false; // EOS in the middle of data is an error
                }
                out.push_back(static_cast<char>(Nodes[node].Symbol));
                node = 0;
                bitsLeft = 0;
            } else {
                bitsLeft++;
            }
        }
    }
    // Remaining bits must be padding (all 1s) and <= 7 bits
    if (bitsLeft > 7) {
        return false;
    }
    // Per RFC 7541 §5.2: padding must be the most-significant bits of EOS (all 1s)
    if (bitsLeft > 0 && len > 0) {
        uint8_t lastByte = data[len - 1];
        uint8_t mask = static_cast<uint8_t>((1 << bitsLeft) - 1);
        if ((lastByte & mask) != mask) {
            return false;
        }
    }
    return true;
}

// ============== THPackDynamicTable ==============

THPackDynamicTable::THPackDynamicTable(uint32_t maxSize)
    : MaxSize(maxSize)
{}

void THPackDynamicTable::Add(TString name, TString value) {
    size_t entrySize = name.size() + value.size() + 32;
    // Evict entries if needed to make room
    while (CurrentSize + entrySize > MaxSize && !Entries.empty()) {
        CurrentSize -= Entries.back().EntrySize();
        Entries.pop_back();
    }
    if (entrySize <= MaxSize) {
        Entries.push_front({std::move(name), std::move(value)});
        CurrentSize += entrySize;
    }
    // If entrySize > MaxSize, the entry is not added (table is emptied)
}

bool THPackDynamicTable::Get(size_t index, TStringBuf& name, TStringBuf& value) const {
    if (index < 1 || index > Entries.size()) {
        return false;
    }
    const auto& entry = Entries[index - 1];
    name = entry.Name;
    value = entry.Value;
    return true;
}

void THPackDynamicTable::SetMaxSize(uint32_t maxSize) {
    MaxSize = maxSize;
    Evict();
}

void THPackDynamicTable::Evict() {
    while (CurrentSize > MaxSize && !Entries.empty()) {
        CurrentSize -= Entries.back().EntrySize();
        Entries.pop_back();
    }
}

// ============== THPackEncoder ==============

THPackEncoder::THPackEncoder(uint32_t maxTableSize)
    : DynTable(maxTableSize)
    , PendingMaxSize(maxTableSize)
{}

std::pair<size_t, bool> THPackEncoder::FindHeader(TStringBuf name, TStringBuf value) const {
    size_t nameMatch = 0;

    // Search static table
    for (size_t i = 1; i <= STATIC_TABLE_SIZE; ++i) {
        if (STATIC_TABLE[i].Name == name) {
            if (STATIC_TABLE[i].Value == value) {
                return {i, false}; // exact match
            }
            if (nameMatch == 0) {
                nameMatch = i;
            }
        }
    }

    // Search dynamic table
    for (size_t i = 0; i < DynTable.Size(); ++i) {
        TStringBuf dName, dValue;
        DynTable.Get(i + 1, dName, dValue);
        if (dName == name) {
            if (dValue == value) {
                return {STATIC_TABLE_SIZE + i + 1, false}; // exact match
            }
            if (nameMatch == 0) {
                nameMatch = STATIC_TABLE_SIZE + i + 1;
            }
        }
    }

    return {nameMatch, nameMatch > 0}; // nameOnly=true if we found only a name match
}

void THPackEncoder::EncodeInteger(TString& out, uint32_t value, uint8_t prefix, uint8_t pattern) {
    uint8_t maxPrefix = (1 << prefix) - 1;
    if (value < maxPrefix) {
        out.push_back(static_cast<char>(pattern | value));
    } else {
        out.push_back(static_cast<char>(pattern | maxPrefix));
        value -= maxPrefix;
        while (value >= 128) {
            out.push_back(static_cast<char>((value & 0x7F) | 0x80));
            value >>= 7;
        }
        out.push_back(static_cast<char>(value));
    }
}

TString THPackEncoder::HuffmanEncode(TStringBuf str) {
    TString result;
    uint64_t buffer = 0;
    int bufBits = 0;

    for (uint8_t ch : str) {
        const auto& hc = HUFFMAN_TABLE[ch];
        buffer = (buffer << hc.BitLen) | hc.Code;
        bufBits += hc.BitLen;
        while (bufBits >= 8) {
            bufBits -= 8;
            result.push_back(static_cast<char>((buffer >> bufBits) & 0xFF));
        }
    }
    // Pad with EOS prefix bits (all 1s)
    if (bufBits > 0) {
        buffer = (buffer << (8 - bufBits)) | ((1 << (8 - bufBits)) - 1);
        result.push_back(static_cast<char>(buffer & 0xFF));
    }
    return result;
}

void THPackEncoder::EncodeString(TString& out, TStringBuf str, bool huffman) {
    if (huffman) {
        TString encoded = HuffmanEncode(str);
        EncodeInteger(out, encoded.size(), 7, 0x80); // H=1
        out.append(encoded);
    } else {
        EncodeInteger(out, str.size(), 7, 0x00); // H=0
        out.append(str);
    }
}

TString THPackEncoder::Encode(const TVector<std::pair<TString, TString>>& headers) {
    TString result;

    // Emit dynamic table size update if changed
    if (TableSizeChanged) {
        EncodeInteger(result, PendingMaxSize, 5, 0x20);
        TableSizeChanged = false;
    }

    for (const auto& [name, value] : headers) {
        auto [index, nameOnly] = FindHeader(name, value);

        if (index > 0 && !nameOnly) {
            // Indexed Header Field (RFC 7541 Section 6.1)
            EncodeInteger(result, index, 7, 0x80);
        } else if (index > 0) {
            // Literal Header Field with Incremental Indexing - Indexed Name (RFC 7541 Section 6.2.1)
            EncodeInteger(result, index, 6, 0x40);
            EncodeString(result, value);
            DynTable.Add(TString(name), TString(value));
        } else {
            // Literal Header Field with Incremental Indexing - New Name (RFC 7541 Section 6.2.1)
            result.push_back(static_cast<char>(0x40));
            EncodeString(result, name);
            EncodeString(result, value);
            DynTable.Add(TString(name), TString(value));
        }
    }
    return result;
}

void THPackEncoder::SetMaxTableSize(uint32_t maxSize) {
    DynTable.SetMaxSize(maxSize);
    PendingMaxSize = maxSize;
    TableSizeChanged = true;
}

// ============== THPackDecoder ==============

THPackDecoder::THPackDecoder(uint32_t maxTableSize)
    : DynTable(maxTableSize)
{}

bool THPackDecoder::DecodeInteger(const uint8_t*& pos, const uint8_t* end, uint8_t prefix, uint32_t& value) {
    if (pos >= end) return false;
    uint8_t maxPrefix = (1 << prefix) - 1;
    value = *pos & maxPrefix;
    pos++;

    if (value < maxPrefix) {
        return true;
    }

    uint32_t m = 0;
    while (pos < end) {
        uint8_t b = *pos;
        pos++;
        if (m >= 28) return false; // overflow protection
        value += static_cast<uint32_t>(b & 0x7F) << m;
        m += 7;
        if ((b & 0x80) == 0) {
            return true;
        }
    }
    return false; // incomplete
}

bool THPackDecoder::DecodeString(const uint8_t*& pos, const uint8_t* end, TString& out) {
    if (pos >= end) return false;
    bool huffman = (*pos & 0x80) != 0;

    uint32_t length;
    if (!DecodeInteger(pos, end, 7, length)) return false;
    if (pos + length > end) return false;

    if (huffman) {
        if (!HuffDecoder.Decode(pos, length, out)) return false;
    } else {
        out.assign(reinterpret_cast<const char*>(pos), length);
    }
    pos += length;
    return true;
}

bool THPackDecoder::LookupIndex(size_t index, TString& name, TString& value) const {
    if (index <= STATIC_TABLE_SIZE) {
        if (index < 1) return false;
        name = TString(STATIC_TABLE[index].Name);
        value = TString(STATIC_TABLE[index].Value);
        return true;
    }
    size_t dynIndex = index - STATIC_TABLE_SIZE;
    TStringBuf n, v;
    if (!DynTable.Get(dynIndex, n, v)) return false;
    name = TString(n);
    value = TString(v);
    return true;
}

bool THPackDecoder::LookupIndexName(size_t index, TString& name) const {
    if (index <= STATIC_TABLE_SIZE) {
        if (index < 1) return false;
        name = TString(STATIC_TABLE[index].Name);
        return true;
    }
    size_t dynIndex = index - STATIC_TABLE_SIZE;
    TStringBuf n, v;
    if (!DynTable.Get(dynIndex, n, v)) return false;
    name = TString(n);
    return true;
}

bool THPackDecoder::Decode(TStringBuf data, TVector<std::pair<TString, TString>>& headers) {
    const uint8_t* pos = reinterpret_cast<const uint8_t*>(data.data());
    const uint8_t* end = pos + data.size();

    while (pos < end) {
        uint8_t byte = *pos;

        if (byte & 0x80) {
            // Indexed Header Field (Section 6.1)
            uint32_t index;
            if (!DecodeInteger(pos, end, 7, index)) return false;
            if (index == 0) return false;
            TString name, value;
            if (!LookupIndex(index, name, value)) return false;
            headers.emplace_back(std::move(name), std::move(value));
        } else if (byte & 0x40) {
            // Literal Header Field with Incremental Indexing (Section 6.2.1)
            uint32_t index;
            if (!DecodeInteger(pos, end, 6, index)) return false;
            TString name, value;
            if (index > 0) {
                if (!LookupIndexName(index, name)) return false;
            } else {
                if (!DecodeString(pos, end, name)) return false;
            }
            if (!DecodeString(pos, end, value)) return false;
            DynTable.Add(TString(name), TString(value));
            headers.emplace_back(std::move(name), std::move(value));
        } else if (byte & 0x20) {
            // Dynamic Table Size Update (Section 6.3)
            uint32_t maxSize;
            if (!DecodeInteger(pos, end, 5, maxSize)) return false;
            DynTable.SetMaxSize(maxSize);
        } else {
            // Literal Header Field without Indexing (Section 6.2.2) or
            // Literal Header Field Never Indexed (Section 6.2.3)
            uint8_t prefix = 4; // Both "without indexing" (6.2.2) and "never indexed" (6.2.3) use 4-bit prefix
            uint32_t index;
            if (!DecodeInteger(pos, end, prefix, index)) return false;
            TString name, value;
            if (index > 0) {
                if (!LookupIndexName(index, name)) return false;
            } else {
                if (!DecodeString(pos, end, name)) return false;
            }
            if (!DecodeString(pos, end, value)) return false;
            // These are not added to the dynamic table
            headers.emplace_back(std::move(name), std::move(value));
        }
    }
    return true;
}

void THPackDecoder::SetMaxTableSize(uint32_t maxSize) {
    DynTable.SetMaxSize(maxSize);
}

} // namespace NHttp::NHttp2
