/*
 * Deserializer for the dict_builder binary format.
 *
 * Reverses dict_builder: reconstructs the original NDJSON rows from the
 * per-column dictionary sections and writes them to stdout (or --output).
 *
 * Sections (see dict_builder/main.cpp):
 *   keys      <uint32 count> then count column names, each <varint len><bytes>
 *   values    M concatenated per-column dicts, same self-delimiting format
 *   presence  <uint64 N> then M bitmaps of ceil(N/8) bytes (bit r => present)
 *   indexes   per column, in column order: one unsigned-LEB128 dict id for
 *             every present row (row order); together they replace "rows"
 *
 * Usage:
 *   dict_reader --input INPUT.bin [--output OUTPUT.ndjson]
 */

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/streams/zstd/zstd.h>

#include <util/stream/file.h>
#include <util/stream/str.h>

#include <cstdint>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// Section container reading (identical layout to compact_kv).
// ---------------------------------------------------------------------------

struct TSection {
    std::string Name;
    std::vector<uint8_t> Data;
};

static std::vector<uint8_t> ZstdDecompress(const uint8_t* data, size_t size) {
    TString decompressed;
    {
        TString raw(reinterpret_cast<const char*>(data), size);
        TStringInput src(raw);
        TZstdDecompress zstd(&src);
        TStringOutput sink(decompressed);
        TransferData(&zstd, &sink);
    }
    const auto* p = reinterpret_cast<const uint8_t*>(decompressed.data());
    return std::vector<uint8_t>(p, p + decompressed.size());
}

static bool IsZstd(const uint8_t* data, size_t size) {
    return size >= 4 && data[0] == 0x28 && data[1] == 0xB5 && data[2] == 0x2F && data[3] == 0xFD;
}

static std::vector<TSection> ReadSections(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f) {
        throw std::runtime_error("Cannot open: " + path);
    }
    auto readU32 = [&]() -> uint32_t {
        uint32_t v = 0;
        f.read(reinterpret_cast<char*>(&v), 4);
        return v;
    };
    auto readU64 = [&]() -> uint64_t {
        uint64_t v = 0;
        f.read(reinterpret_cast<char*>(&v), 8);
        return v;
    };

    uint32_t numSections = readU32();
    std::vector<TSection> sections;
    sections.reserve(numSections);
    for (uint32_t i = 0; i < numSections; ++i) {
        TSection s;
        uint32_t nameLen = readU32();
        s.Name.resize(nameLen);
        f.read(s.Name.data(), nameLen);
        uint64_t dataLen = readU64();
        std::vector<uint8_t> raw(dataLen);
        f.read(reinterpret_cast<char*>(raw.data()), dataLen);
        s.Data = IsZstd(raw.data(), raw.size()) ? ZstdDecompress(raw.data(), raw.size()) : std::move(raw);
        sections.push_back(std::move(s));
    }
    return sections;
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

static uint64_t ReadUVarInt(const std::vector<uint8_t>& d, size_t& pos) {
    uint64_t v = 0;
    int shift = 0;
    while (pos < d.size()) {
        uint8_t b = d[pos++];
        v |= static_cast<uint64_t>(b & 0x7F) << shift;
        if (!(b & 0x80)) {
            return v;
        }
        shift += 7;
    }
    throw std::runtime_error("varint: truncated");
}

// Parse one self-delimiting dict starting at pos: <uint32 count> then count
// <varint len><bytes> strings. Advances pos past the dict.
static std::vector<std::string> ParseDict(const std::vector<uint8_t>& data, size_t& pos) {
    if (pos + 4 > data.size()) {
        throw std::runtime_error("dict: truncated count");
    }
    uint32_t count = 0;
    std::memcpy(&count, data.data() + pos, 4);
    pos += 4;
    std::vector<std::string> result;
    result.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        uint64_t len = ReadUVarInt(data, pos);
        if (pos + len > data.size()) {
            throw std::runtime_error("dict: string overruns section");
        }
        result.emplace_back(reinterpret_cast<const char*>(data.data() + pos), len);
        pos += len;
    }
    return result;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

int main(int argc, const char* argv[]) {
    NLastGetopt::TOpts opts;
    opts.SetTitle("Decode dict_builder binary format back to NDJSON");

    TString inputPath;
    TString outputPath;
    opts.AddLongOption("input", "Input binary file").Required().StoreResult(&inputPath);
    opts.AddLongOption("output", "Output NDJSON file (default: stdout)").Optional().StoreResult(&outputPath);
    opts.AddHelpOption();
    NLastGetopt::TOptsParseResult parseResult(&opts, argc, argv);

    auto sections = ReadSections(std::string(inputPath.data(), inputPath.size()));

    const std::vector<uint8_t>* keysData = nullptr;
    const std::vector<uint8_t>* valsData = nullptr;
    const std::vector<uint8_t>* presData = nullptr;
    const std::vector<uint8_t>* idxData = nullptr;
    for (const auto& s : sections) {
        if (s.Name == "keys") keysData = &s.Data;
        else if (s.Name == "values") valsData = &s.Data;
        else if (s.Name == "presence") presData = &s.Data;
        else if (s.Name == "indexes") idxData = &s.Data;
    }
    if (!keysData || !valsData || !presData || !idxData) {
        throw std::runtime_error("Missing one of keys/values/presence/indexes sections");
    }

    // Column names.
    size_t kpos = 0;
    std::vector<std::string> colNames = ParseDict(*keysData, kpos);
    const size_t m = colNames.size();

    // Per-column value dictionaries.
    std::vector<std::vector<std::string>> colDicts;
    colDicts.reserve(m);
    size_t vpos = 0;
    for (size_t c = 0; c < m; ++c) {
        colDicts.push_back(ParseDict(*valsData, vpos));
    }

    // Row count + per-column presence bitmaps.
    if (presData->size() < 8) {
        throw std::runtime_error("presence: truncated row count");
    }
    uint64_t n = 0;
    std::memcpy(&n, presData->data(), 8);
    const size_t stride = (n + 7) / 8;
    if (presData->size() != 8 + m * stride) {
        throw std::runtime_error("presence: size mismatch");
    }
    const uint8_t* bitmaps = presData->data() + 8;

    // Reconstruct rows: walk each column, consuming one index per present row.
    std::vector<NJson::TJsonValue> rows(n, NJson::TJsonValue(NJson::JSON_MAP));
    size_t ipos = 0;
    for (size_t c = 0; c < m; ++c) {
        const uint8_t* bm = bitmaps + c * stride;
        const auto& dict = colDicts[c];
        for (uint64_t r = 0; r < n; ++r) {
            if ((bm[r >> 3] >> (r & 7)) & 1) {
                uint64_t id = ReadUVarInt(*idxData, ipos);
                if (id >= dict.size()) {
                    throw std::runtime_error("index out of range for column " + colNames[c]);
                }
                rows[r][colNames[c]] = dict[id];
            }
        }
    }

    auto write = [&](IOutputStream& os) {
        for (const auto& row : rows) {
            os << NJson::WriteJson(row, false) << "\n";
        }
    };
    if (outputPath.empty()) {
        TFileOutput fout(Duplicate(1));
        write(fout);
    } else {
        TFileOutput fout(outputPath);
        write(fout);
    }
    return 0;
}
