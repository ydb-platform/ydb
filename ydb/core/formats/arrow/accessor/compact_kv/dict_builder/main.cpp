/*
 * Per-column dictionary builder (comparison baseline for compact_kv).
 *
 * Instead of compact_kv's single global value dictionary + Avro "rows" stream,
 * this encodes every leaf column independently:
 *   - keys:     newline-joined column-name dictionary, uint32 count prefix
 *   - values:   concatenation of each column's own value dictionary
 *   - presence: concatenation of each column's presence bitmap (1 bit / row)
 *   - indexes:  concatenation of each column's varint dict-ids, for present
 *               rows only (this is what replaces compact_kv's "rows" section)
 * Each section is optionally zstd-compressed independently.
 *
 * Input is NDJSON (one object per line; relaxed parsing accepts python-dict
 * reprs). Rows are assumed to be flat maps of scalar values; nested values are
 * stringified and treated as opaque leaf strings.
 *
 * Output binary format matches the compact_kv builder:
 *   <uint32 num_sections>
 *   per section: <uint32 name_len><name><uint64 data_len><data>
 *
 * Usage:
 *   dict_builder --input IN.ndjson --output OUT.bin [--zstd-level N] [--json-stats]
 */

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/streams/zstd/zstd.h>

#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/string/strip.h>

#include <cstdint>
#include <cstring>
#include <deque>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <unordered_map>
#include <vector>

// ---------------------------------------------------------------------------
// String dictionary (insertion order, stable string_view keys via deque).
// ---------------------------------------------------------------------------
class TStringDict {
public:
    uint32_t GetOrAdd(std::string_view s) {
        auto it = Index.find(s);
        if (it != Index.end()) {
            return it->second;
        }
        uint32_t id = static_cast<uint32_t>(Strings.size());
        Strings.emplace_back(s);
        Index.emplace(std::string_view(Strings.back()), id);
        return id;
    }
    const std::deque<std::string>& GetStrings() const {
        return Strings;
    }
    size_t Size() const {
        return Strings.size();
    }

private:
    struct SvHash {
        using is_transparent = void;
        size_t operator()(std::string_view sv) const noexcept {
            return std::hash<std::string_view>{}(sv);
        }
    };
    struct SvEqual {
        using is_transparent = void;
        bool operator()(std::string_view a, std::string_view b) const noexcept {
            return a == b;
        }
    };
    std::unordered_map<std::string_view, uint32_t, SvHash, SvEqual> Index;
    std::deque<std::string> Strings;
};

// ---------------------------------------------------------------------------
// Per-column state.
// ---------------------------------------------------------------------------
struct TColumn {
    TStringDict Dict;
    std::vector<uint8_t> Bitmap;     // ceil(N/8) bytes; bit r set iff present in row r
    std::vector<uint32_t> Indices;   // dict-id per present row, in row order
    uint32_t PresentCount = 0;
};

// Append an unsigned LEB128 varint.
static void WriteUVarInt(std::vector<uint8_t>& buf, uint64_t v) {
    while (v > 0x7F) {
        buf.push_back(static_cast<uint8_t>((v & 0x7F) | 0x80));
        v >>= 7;
    }
    buf.push_back(static_cast<uint8_t>(v));
}

// Render a JSON leaf as the string we dictionary-encode.
static std::string ValueToString(const NJson::TJsonValue& v) {
    if (v.GetType() == NJson::JSON_STRING) {
        const auto sb = v.GetString();
        return std::string(sb.data(), sb.size());
    }
    const TString s = v.GetStringRobust();
    return std::string(s.data(), s.size());
}

// <uint32 count> then for each string: <varint len><bytes>.
// Length-prefixing (rather than a newline delimiter) keeps each dict
// self-delimiting AND binary-safe, so several dicts can be concatenated in one
// section (the per-column value dicts in "values") and values may contain any
// byte, including '\n' (common in meta stack traces / embedded JSON bodies).
static void AppendStrings(std::vector<uint8_t>& buf, const std::deque<std::string>& strings) {
    uint32_t count = static_cast<uint32_t>(strings.size());
    size_t at = buf.size();
    buf.resize(at + 4);
    std::memcpy(buf.data() + at, &count, 4);
    for (const auto& s : strings) {
        WriteUVarInt(buf, s.size());
        buf.insert(buf.end(), s.begin(), s.end());
    }
}

static std::vector<uint8_t> ZstdCompress(const std::vector<uint8_t>& buf, int level) {
    TString compressed;
    {
        TStringOutput sink(compressed);
        TZstdCompress zstd(&sink, level);
        zstd.Write(buf.data(), buf.size());
        zstd.Finish();
    }
    const auto* p = reinterpret_cast<const uint8_t*>(compressed.data());
    return std::vector<uint8_t>(p, p + compressed.size());
}

struct TSectionInfo {
    std::string Name;
    size_t RawSize;
    std::vector<uint8_t> Data;   // compressed if zstd_level >= 0, else raw
};

static void WriteSections(const std::string& outputPath, const std::vector<TSectionInfo>& sections) {
    std::ofstream out(outputPath, std::ios::binary);
    if (!out) {
        throw std::runtime_error("Cannot open output file: " + outputPath);
    }
    uint32_t numSections = static_cast<uint32_t>(sections.size());
    out.write(reinterpret_cast<const char*>(&numSections), 4);
    for (const auto& s : sections) {
        uint32_t nameLen = static_cast<uint32_t>(s.Name.size());
        out.write(reinterpret_cast<const char*>(&nameLen), 4);
        out.write(s.Name.data(), s.Name.size());
        uint64_t dataLen = static_cast<uint64_t>(s.Data.size());
        out.write(reinterpret_cast<const char*>(&dataLen), 8);
        out.write(reinterpret_cast<const char*>(s.Data.data()), s.Data.size());
    }
}

static std::string FmtNum(size_t n) {
    std::string s = std::to_string(n);
    int insertPos = static_cast<int>(s.size()) - 3;
    while (insertPos > 0) {
        s.insert(insertPos, ",");
        insertPos -= 3;
    }
    return s;
}

int main(int argc, const char* argv[]) {
    NLastGetopt::TOpts opts;
    opts.SetTitle("Per-column dictionary encoder (presence bitmap + indexes), comparison baseline for compact_kv");

    TString inputPath;
    TString outputPath;
    bool jsonStats = false;
    int zstdLevel = -1;

    opts.AddLongOption("input", "Input NDJSON file").Required().StoreResult(&inputPath);
    opts.AddLongOption("output", "Output binary file").Required().StoreResult(&outputPath);
    opts.AddLongOption("zstd-level", "ZSTD compression level (1..22). If omitted, no compression.")
        .Optional().StoreResult(&zstdLevel);
    opts.AddLongOption("json-stats", "Print machine-readable stats as a single JSON line to stdout")
        .Optional().NoArgument().SetFlag(&jsonStats);
    opts.AddHelpOption();
    NLastGetopt::TOptsParseResult parseResult(&opts, argc, argv);

    // Read non-empty lines.
    std::vector<std::string> lines;
    {
        TFileInput fileInput(inputPath);
        TString line;
        while (fileInput.ReadLine(line)) {
            TString stripped = StripString(line);
            if (!stripped.empty()) {
                lines.emplace_back(stripped.data(), stripped.size());
            }
        }
    }
    if (lines.empty()) {
        std::cerr << "No rows to process." << std::endl;
        return 1;
    }

    const size_t n = lines.size();
    const size_t bitmapBytes = (n + 7) / 8;

    TStringDict keyDict;
    std::vector<TColumn> columns;

    for (size_t r = 0; r < n; ++r) {
        NJson::TJsonValue doc;
        if (!NJson::ReadJsonFastTree(TStringBuf(lines[r].data(), lines[r].size()), &doc)) {
            std::cerr << "JSON parse error in line: " << lines[r].substr(0, 80) << std::endl;
            return 1;
        }
        if (doc.GetType() != NJson::JSON_MAP) {
            continue;
        }
        for (const auto& [k, v] : doc.GetMap()) {
            uint32_t col = keyDict.GetOrAdd(std::string_view(k.data(), k.size()));
            if (col >= columns.size()) {
                columns.resize(col + 1);
            }
            TColumn& c = columns[col];
            if (c.Bitmap.empty()) {
                c.Bitmap.assign(bitmapBytes, 0);
            }
            c.Bitmap[r >> 3] |= static_cast<uint8_t>(1u << (r & 7));
            ++c.PresentCount;
            c.Indices.push_back(c.Dict.GetOrAdd(ValueToString(v)));
        }
    }

    // Build sections.
    std::vector<uint8_t> keysRaw;
    AppendStrings(keysRaw, keyDict.GetStrings());

    std::vector<uint8_t> valuesRaw;
    std::vector<uint8_t> presenceRaw;
    std::vector<uint8_t> indexesRaw;
    // The presence section starts with the uint64 row count so the reader can
    // recover the per-column bitmap stride (ceil(N/8)).
    {
        uint64_t nn = n;
        presenceRaw.resize(8);
        std::memcpy(presenceRaw.data(), &nn, 8);
    }
    size_t totalDistinctValues = 0;
    for (const auto& c : columns) {
        AppendStrings(valuesRaw, c.Dict.GetStrings());
        totalDistinctValues += c.Dict.Size();
        presenceRaw.insert(presenceRaw.end(), c.Bitmap.begin(), c.Bitmap.end());
        for (uint32_t idx : c.Indices) {
            WriteUVarInt(indexesRaw, idx);
        }
    }

    struct TRawSection {
        std::string Name;
        std::vector<uint8_t> Raw;
    };
    std::vector<TRawSection> rawSections = {
        {"keys", std::move(keysRaw)},
        {"values", std::move(valuesRaw)},
        {"presence", std::move(presenceRaw)},
        {"indexes", std::move(indexesRaw)},
    };

    std::vector<TSectionInfo> sections;
    sections.reserve(rawSections.size());
    for (auto& rs : rawSections) {
        TSectionInfo si;
        si.Name = rs.Name;
        si.RawSize = rs.Raw.size();
        si.Data = (zstdLevel >= 0) ? ZstdCompress(rs.Raw, zstdLevel) : std::move(rs.Raw);
        sections.push_back(std::move(si));
    }

    const std::string outStr(outputPath.data(), outputPath.size());
    WriteSections(outStr, sections);

    std::ifstream checkIn(std::string(inputPath.data(), inputPath.size()), std::ios::binary | std::ios::ate);
    std::ifstream checkOut(outStr, std::ios::binary | std::ios::ate);
    int64_t inputFileSize = checkIn ? static_cast<int64_t>(checkIn.tellg()) : -1;
    int64_t outputFileSize = checkOut ? static_cast<int64_t>(checkOut.tellg()) : -1;

    size_t totalRaw = 0;
    size_t totalComp = 0;
    for (const auto& s : sections) {
        totalRaw += s.RawSize;
        totalComp += s.Data.size();
    }

    std::cerr << "Rows: " << n << ", Keys (columns): " << keyDict.Size()
              << ", Values (sum per-column): " << totalDistinctValues << "\n";
    if (zstdLevel >= 0) {
        std::cerr << "\n";
        std::cerr << std::left << std::setw(12) << "Section" << std::right << std::setw(14) << "Raw"
                  << std::right << std::setw(14) << "Compressed" << std::right << std::setw(8) << "Ratio" << "\n";
        std::cerr << std::string(48, '-') << "\n";
        for (const auto& s : sections) {
            double ratio = s.Data.size() ? 1.0 * s.RawSize / s.Data.size() : 0.0;
            std::cerr << std::left << std::setw(12) << s.Name << std::right << std::setw(14) << FmtNum(s.RawSize)
                      << std::right << std::setw(14) << FmtNum(s.Data.size()) << std::right << std::setw(7)
                      << std::fixed << std::setprecision(2) << ratio << "x\n";
        }
    }
    if (inputFileSize >= 0) {
        std::cerr << "\nInput file:  " << FmtNum((size_t)inputFileSize) << " bytes\n";
    }
    if (outputFileSize >= 0) {
        std::cerr << "Output file: " << FmtNum((size_t)outputFileSize) << " bytes\n";
    }

    if (jsonStats) {
        auto jsonEscape = [](std::string_view s) {
            std::string r;
            r.reserve(s.size() + 2);
            for (char c : s) {
                switch (c) {
                    case '"': r += "\\\""; break;
                    case '\\': r += "\\\\"; break;
                    case '\n': r += "\\n"; break;
                    case '\t': r += "\\t"; break;
                    case '\r': r += "\\r"; break;
                    default: r += c; break;
                }
            }
            return r;
        };
        std::ostringstream js;
        js << "{"
           << "\"input\":\"" << jsonEscape(std::string_view(inputPath.data(), inputPath.size())) << "\","
           << "\"rows\":" << n << ","
           << "\"keys\":" << keyDict.Size() << ","
           << "\"values\":" << totalDistinctValues << ","
           << "\"zstd_level\":" << zstdLevel << ","
           << "\"sections\":[";
        for (size_t i = 0; i < sections.size(); ++i) {
            const auto& s = sections[i];
            if (i) {
                js << ",";
            }
            js << "{\"name\":\"" << jsonEscape(s.Name) << "\","
               << "\"raw\":" << s.RawSize << ","
               << "\"compressed\":" << s.Data.size() << "}";
        }
        js << "],"
           << "\"total_raw\":" << totalRaw << ","
           << "\"total_compressed\":" << totalComp << ","
           << "\"input_file_size\":" << inputFileSize << ","
           << "\"output_file_size\":" << outputFileSize << "}";
        std::cout << js.str() << std::endl;
    }

    return 0;
}
