/*
 * Deserializer for the compact_kv/builder binary format.
 *
 * Reads the binary file produced by builder and writes NDJSON to stdout
 * (or --output file), reconstructing the original JSON rows.
 *
 * Two decoding modes (default: native):
 *   --native  Use hand-written Avro binary decoder (default)
 *   --avro    Use the Avro library's DataFileReader + GenericDatum
 *
 * Usage:
 *   reader --input INPUT.bin [--output OUTPUT.ndjson] [--avro|--native]
 */

#include <avro/Compiler.hh>
#include <avro/DataFile.hh>
#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>
#include <avro/NodeImpl.hh>
#include <avro/Schema.hh>
#include <avro/Stream.hh>
#include <avro/Types.hh>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/streams/zstd/zstd.h>

#include <util/stream/file.h>
#include <util/stream/str.h>

#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// Section reading
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
    if (!f) throw std::runtime_error("Cannot open: " + path);

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
        s.Data = IsZstd(raw.data(), raw.size())
            ? ZstdDecompress(raw.data(), raw.size())
            : std::move(raw);
        sections.push_back(std::move(s));
    }
    return sections;
}

// ---------------------------------------------------------------------------
// String dictionary parsing
// Format: <uint32_t count> \n <str0> \n <str1> ... \n <strN-1>
// ---------------------------------------------------------------------------

static std::vector<std::string> ParseStringDict(const std::vector<uint8_t>& data) {
    if (data.size() < 4) throw std::runtime_error("String dict too short");
    uint32_t count = 0;
    std::memcpy(&count, data.data(), 4);

    std::vector<std::string> result;
    result.reserve(count);

    size_t pos = 4;
    for (uint32_t i = 0; i < count; ++i) {
        if (pos >= data.size() || data[pos] != '\n')
            throw std::runtime_error("Expected newline separator in string dict");
        ++pos;
        size_t start = pos;
        while (pos < data.size() && data[pos] != '\n') ++pos;
        result.emplace_back(reinterpret_cast<const char*>(data.data() + start), pos - start);
    }
    return result;
}

// ---------------------------------------------------------------------------
// Native Avro binary decoder
// ---------------------------------------------------------------------------

static int64_t ReadVarInt(const uint8_t* buf, size_t size, size_t& pos) {
    uint64_t uv = 0;
    int shift = 0;
    while (pos < size) {
        uint8_t b = buf[pos++];
        uv |= static_cast<uint64_t>(b & 0x7F) << shift;
        shift += 7;
        if (!(b & 0x80)) break;
    }
    return static_cast<int64_t>((uv >> 1) ^ -(uv & 1));
}

static void DecodeNative(
    const avro::NodePtr& nodeIn,
    const uint8_t* buf, size_t size, size_t& pos,
    const std::vector<std::string>& valDict,
    NJson::TJsonValue& out)
{
    using namespace avro;
    // Resolve symbolic references (back-references to named types like string_ref)
    const avro::NodePtr node = (nodeIn->type() == AVRO_SYMBOLIC)
        ? avro::resolveSymbol(nodeIn)
        : nodeIn;
    switch (node->type()) {
        case AVRO_NULL:
            out = NJson::TJsonValue(NJson::JSON_NULL);
            return;
        case AVRO_BOOL:
            out = NJson::TJsonValue(buf[pos++] != 0);
            return;
        case AVRO_INT:
        case AVRO_LONG: {
            int64_t v = ReadVarInt(buf, size, pos);
            out = NJson::TJsonValue(static_cast<long long>(v));
            return;
        }
        case AVRO_FLOAT: {
            float v; std::memcpy(&v, buf + pos, 4); pos += 4;
            out = NJson::TJsonValue(static_cast<double>(v));
            return;
        }
        case AVRO_DOUBLE: {
            double v; std::memcpy(&v, buf + pos, 8); pos += 8;
            out = NJson::TJsonValue(v);
            return;
        }
        case AVRO_STRING:
        case AVRO_BYTES: {
            int64_t len = ReadVarInt(buf, size, pos);
            out = NJson::TJsonValue(std::string(reinterpret_cast<const char*>(buf + pos), len));
            pos += len;
            return;
        }
        case AVRO_RECORD: {
            if (node->name().simpleName() == "string_ref") {
                int64_t id = ReadVarInt(buf, size, pos);
                out = NJson::TJsonValue(valDict.at(id));
                return;
            }
            out = NJson::TJsonValue(NJson::JSON_MAP);
            for (size_t i = 0; i < node->leaves(); ++i) {
                NJson::TJsonValue fv;
                DecodeNative(node->leafAt(i), buf, size, pos, valDict, fv);
                if (fv.GetType() != NJson::JSON_NULL)
                    out[node->nameAt(i)] = std::move(fv);
            }
            return;
        }
        case AVRO_UNION: {
            int64_t branch = ReadVarInt(buf, size, pos);
            DecodeNative(node->leafAt(branch), buf, size, pos, valDict, out);
            return;
        }
        case AVRO_ARRAY: {
            out = NJson::TJsonValue(NJson::JSON_ARRAY);
            while (true) {
                int64_t cnt = ReadVarInt(buf, size, pos);
                if (cnt == 0) break;
                if (cnt < 0) { ReadVarInt(buf, size, pos); cnt = -cnt; }
                for (int64_t j = 0; j < cnt; ++j) {
                    NJson::TJsonValue item;
                    DecodeNative(node->leafAt(0), buf, size, pos, valDict, item);
                    out.AppendValue(std::move(item));
                }
            }
            return;
        }
        case AVRO_MAP: {
            out = NJson::TJsonValue(NJson::JSON_MAP);
            while (true) {
                int64_t cnt = ReadVarInt(buf, size, pos);
                if (cnt == 0) break;
                if (cnt < 0) { ReadVarInt(buf, size, pos); cnt = -cnt; }
                for (int64_t j = 0; j < cnt; ++j) {
                    int64_t klen = ReadVarInt(buf, size, pos);
                    std::string key(reinterpret_cast<const char*>(buf + pos), klen);
                    pos += klen;
                    NJson::TJsonValue val;
                    DecodeNative(node->leafAt(0), buf, size, pos, valDict, val);
                    out[key] = std::move(val);
                }
            }
            return;
        }
        default:
            throw std::runtime_error("Unsupported Avro type: " + std::to_string(static_cast<int>(node->type())));
    }
}

static void DecodeAvroNative(
    const std::vector<uint8_t>& data,
    const std::vector<std::string>& valDict,
    IOutputStream& out)
{
    const uint8_t* buf = data.data();
    size_t size = data.size();
    size_t pos = 0;

    if (size < 4 || buf[0] != 'O' || buf[1] != 'b' || buf[2] != 'j' || buf[3] != 0x01)
        throw std::runtime_error("Invalid Avro magic");
    pos += 4;

    std::string schemaJson;
    {
        int64_t mapCount = ReadVarInt(buf, size, pos);
        while (mapCount != 0) {
            if (mapCount < 0) { ReadVarInt(buf, size, pos); mapCount = -mapCount; }
            for (int64_t i = 0; i < mapCount; ++i) {
                int64_t klen = ReadVarInt(buf, size, pos);
                std::string key(reinterpret_cast<const char*>(buf + pos), klen); pos += klen;
                int64_t vlen = ReadVarInt(buf, size, pos);
                std::string val(reinterpret_cast<const char*>(buf + pos), vlen); pos += vlen;
                if (key == "avro.schema") schemaJson = val;
            }
            mapCount = ReadVarInt(buf, size, pos);
        }
    }
    if (schemaJson.empty()) throw std::runtime_error("No avro.schema in metadata");

    avro::ValidSchema schema = avro::compileJsonSchemaFromString(schemaJson);
    const avro::NodePtr& root = schema.root();

    const uint8_t* syncMarker = buf + pos;
    pos += 16;

    while (pos < size) {
        int64_t objCount = ReadVarInt(buf, size, pos);
        if (objCount == 0) break;
        ReadVarInt(buf, size, pos); // byte count (unused)
        for (int64_t i = 0; i < objCount; ++i) {
            NJson::TJsonValue row;
            DecodeNative(root, buf, size, pos, valDict, row);
            out << NJson::WriteJson(row, false) << "\n";
        }
        if (std::memcmp(buf + pos, syncMarker, 16) != 0)
            throw std::runtime_error("Sync marker mismatch");
        pos += 16;
    }
}

// ---------------------------------------------------------------------------
// Avro-library path: DataFileReader<GenericDatum>
// ---------------------------------------------------------------------------

static void GenericDatumToJson(
    const avro::GenericDatum& datum,
    const std::vector<std::string>& valDict,
    NJson::TJsonValue& out)
{
    using namespace avro;
    switch (datum.type()) {
        case AVRO_NULL:
            out = NJson::TJsonValue(NJson::JSON_NULL);
            return;
        case AVRO_BOOL:
            out = NJson::TJsonValue(datum.value<bool>());
            return;
        case AVRO_INT:
            out = NJson::TJsonValue(static_cast<long long>(datum.value<int32_t>()));
            return;
        case AVRO_LONG:
            out = NJson::TJsonValue(static_cast<long long>(datum.value<int64_t>()));
            return;
        case AVRO_FLOAT:
            out = NJson::TJsonValue(static_cast<double>(datum.value<float>()));
            return;
        case AVRO_DOUBLE:
            out = NJson::TJsonValue(datum.value<double>());
            return;
        case AVRO_STRING:
            out = NJson::TJsonValue(datum.value<std::string>());
            return;
        case AVRO_BYTES: {
            const auto& bytes = datum.value<std::vector<uint8_t>>();
            out = NJson::TJsonValue(std::string(bytes.begin(), bytes.end()));
            return;
        }
        case AVRO_RECORD: {
            const GenericRecord& rec = datum.value<GenericRecord>();
            // string_ref: single field "id" -> look up in valDict
            if (rec.schema()->name().simpleName() == "string_ref") {
                int64_t id = rec.fieldAt(0).value<int32_t>();
                out = NJson::TJsonValue(valDict.at(id));
                return;
            }
            out = NJson::TJsonValue(NJson::JSON_MAP);
            for (size_t i = 0; i < rec.fieldCount(); ++i) {
                NJson::TJsonValue fv;
                GenericDatumToJson(rec.fieldAt(i), valDict, fv);
                if (fv.GetType() != NJson::JSON_NULL)
                    out[rec.schema()->nameAt(i)] = std::move(fv);
            }
            return;
        }
        case AVRO_ARRAY: {
            out = NJson::TJsonValue(NJson::JSON_ARRAY);
            const GenericArray& arr = datum.value<GenericArray>();
            for (const auto& item : arr.value()) {
                NJson::TJsonValue iv;
                GenericDatumToJson(item, valDict, iv);
                out.AppendValue(std::move(iv));
            }
            return;
        }
        case AVRO_MAP: {
            out = NJson::TJsonValue(NJson::JSON_MAP);
            const GenericMap& m = datum.value<GenericMap>();
            for (const auto& [k, v] : m.value()) {
                NJson::TJsonValue mv;
                GenericDatumToJson(v, valDict, mv);
                out[k] = std::move(mv);
            }
            return;
        }
        default:
            throw std::runtime_error("Unsupported GenericDatum type");
    }
}

static void DecodeAvroLibrary(
    const std::vector<uint8_t>& data,
    const std::vector<std::string>& valDict,
    IOutputStream& out)
{
    auto stream = avro::memoryInputStream(data.data(), data.size());
    avro::DataFileReader<avro::GenericDatum> reader(std::move(stream));

    avro::GenericDatum datum(reader.dataSchema());
    while (reader.read(datum)) {
        NJson::TJsonValue row;
        GenericDatumToJson(datum, valDict, row);
        out << NJson::WriteJson(row, false) << "\n";
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

int main(int argc, const char* argv[]) {
    NLastGetopt::TOpts opts;
    opts.SetTitle("Decode compact_kv binary format back to NDJSON");

    TString inputPath;
    TString outputPath;
    bool useAvro = false;

    opts.AddLongOption("input", "Input binary file").Required().StoreResult(&inputPath);
    opts.AddLongOption("output", "Output NDJSON file (default: stdout)")
        .Optional().StoreResult(&outputPath);
    opts.AddLongOption("avro", "Use Avro library decoder (default: native decoder)")
        .NoArgument().SetFlag(&useAvro);
    opts.AddHelpOption();

    NLastGetopt::TOptsParseResult parseResult(&opts, argc, argv);

    auto sections = ReadSections(std::string(inputPath.data(), inputPath.size()));

    const std::vector<uint8_t>* valsData = nullptr;
    const std::vector<uint8_t>* rowsData = nullptr;
    for (const auto& s : sections) {
        if (s.Name == "values") valsData = &s.Data;
        if (s.Name == "rows")   rowsData = &s.Data;
    }
    if (!valsData) throw std::runtime_error("Missing 'values' section");
    if (!rowsData) throw std::runtime_error("Missing 'rows' section");

    auto valDict = ParseStringDict(*valsData);

    auto decode = [&](IOutputStream& os) {
        if (useAvro) {
            DecodeAvroLibrary(*rowsData, valDict, os);
        } else {
            DecodeAvroNative(*rowsData, valDict, os);
        }
    };

    if (outputPath.empty()) {
        TFileOutput fout(Duplicate(1));
        decode(fout);
    } else {
        TFileOutput fout(outputPath);
        decode(fout);
    }

    return 0;
}
