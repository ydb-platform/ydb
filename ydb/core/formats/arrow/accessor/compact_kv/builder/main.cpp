/*
 * C++ port of json_single_table.py
 *
 * Encodes NDJSON rows into a binary container with three sections:
 *   - keys:   newline-joined key strings (field name dictionary), prefixed with uint32 count
 *   - values: newline-joined value strings (leaf string dictionary), prefixed with uint32 count
 *   - rows:   Avro container with inferred complex schema (strings encoded as int32 IDs)
 *
 * String values that look like embedded JSON objects/arrays are NOT recursively
 * expanded before encoding (unless --parse-nested is given).
 *
 * The Avro schema is inferred from the union of all observed JSON structures.
 * Leaf string values are dictionary-encoded as int32 IDs (string_ref record).
 * Numeric promotions: int -> long -> double.
 * Incompatible types fall back to string.
 *
 * Output binary format:
 *   <uint32_t num_sections>
 *   For each section:
 *     <uint32_t name_len> <name bytes>
 *     <uint64_t data_len> <data bytes>
 *
 * Usage:
 *   builder --input INPUT.ndjson --output OUTPUT.bin [--parse-nested]
 */

#include <avro/Compiler.hh>
#include <avro/Schema.hh>
#include <avro/Types.hh>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/streams/zstd/zstd.h>

#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/string/strip.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <deque>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

// ---------------------------------------------------------------------------
// String dictionary
// ---------------------------------------------------------------------------

class TStringDict {
public:
    uint32_t GetOrAdd(std::string_view s) {
        auto it = Index.find(s);
        if (it != Index.end()) {
            return it->second;
        }
        uint32_t id = static_cast<uint32_t>(Strings.size());
        // deque never invalidates references on push_back, so string_view keys
        // in Index remain valid after growth — unlike vector which may reallocate.
        Strings.emplace_back(s);
        Index.emplace(std::string_view(Strings.back()), id);
        return id;
    }

    const std::deque<std::string>& GetStrings() const {
        return Strings;
    }

private:
    // Heterogeneous lookup: hash and compare string_view against stored strings.
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
// Inferred type system — arena-allocated to avoid per-node heap overhead
// ---------------------------------------------------------------------------

enum class EKind {
    Null, Boolean, Int, Long, Double, String,
    Nullable, Record, Array,
};

struct TInferredType;

// Simple bump-pointer arena for TInferredType nodes.
// All nodes are freed at once when the arena is destroyed.
class TTypeArena {
public:
    TInferredType* Alloc(EKind k);

    // Prevent accidental copy.
    TTypeArena() = default;
    TTypeArena(const TTypeArena&) = delete;
    TTypeArena& operator=(const TTypeArena&) = delete;

private:
    // Each slab holds a fixed number of nodes; we never move slabs so
    // pointers into them remain stable.
    static constexpr size_t kSlabSize = 4096;
    std::vector<std::unique_ptr<TInferredType[]>> Slabs;
    size_t SlabPos = kSlabSize; // force first alloc to create a slab
};

struct TInferredType {
    EKind Kind;
    TInferredType* Inner = nullptr;  // Nullable
    TInferredType* Items = nullptr;  // Array
    // Record: ordered fields with owned name strings.
    std::vector<std::pair<std::string, TInferredType*>> Fields;

    TInferredType() : Kind(EKind::Null) {}
    explicit TInferredType(EKind k) : Kind(k) {}

    bool IsNullable() const {
        return Kind == EKind::Null || Kind == EKind::Nullable;
    }
};

TInferredType* TTypeArena::Alloc(EKind k) {
    if (SlabPos >= kSlabSize) {
        Slabs.push_back(std::make_unique<TInferredType[]>(kSlabSize));
        SlabPos = 0;
    }
    TInferredType* p = &Slabs.back()[SlabPos++];
    // Placement-construct to reset fields (unique_ptr<[]> default-initialises).
    new (p) TInferredType(k);
    return p;
}

static TInferredType* MakeNullable(TTypeArena& arena, TInferredType* t) {
    if (t->Kind == EKind::Null || t->Kind == EKind::Nullable) return t;
    TInferredType* r = arena.Alloc(EKind::Nullable);
    r->Inner = t;
    return r;
}

// Returns true if b is already "subsumed" by a — i.e. merging would return a
// unchanged. This lets us skip the expensive merge for the common case where
// the unified schema has already seen every field/type in the new row.
static bool IsSubsumed(const TInferredType* a, const TInferredType* b);

static bool IsSubsumed(const TInferredType* a, const TInferredType* b) {
    if (a == b) return true;
    if (a->Kind != b->Kind) {
        // Nullable(X) subsumes X and Null.
        if (a->Kind == EKind::Nullable) {
            if (b->Kind == EKind::Null) return true;
            return IsSubsumed(a->Inner, b);
        }
        // Numeric widening: double subsumes long/int, long subsumes int.
        auto isNum = [](EKind k) {
            return k == EKind::Int || k == EKind::Long || k == EKind::Double;
        };
        if (isNum(a->Kind) && isNum(b->Kind)) {
            if (a->Kind == EKind::Double) return true;
            if (a->Kind == EKind::Long && b->Kind == EKind::Int) return true;
        }
        return false;
    }
    // Same kind.
    switch (a->Kind) {
        case EKind::Record: {
            // Build index into a's fields once.
            // For the hot path (schema stable), b->Fields is a subset of a->Fields
            // and all types are already subsumed — we just need to verify that.
            // Use a linear scan since field counts are typically small (<100).
            for (const auto& [bk, bv] : b->Fields) {
                bool found = false;
                for (const auto& [ak, av] : a->Fields) {
                    if (ak == bk) {
                        if (!IsSubsumed(av, bv)) return false;
                        found = true;
                        break;
                    }
                }
                if (!found) return false; // new field — must merge
            }
            return true;
        }
        case EKind::Array:
            return IsSubsumed(a->Items, b->Items);
        case EKind::Nullable:
            return IsSubsumed(a->Inner, b->Inner);
        default:
            return true; // scalar kinds match
    }
}

static TInferredType* MergeTypes(TTypeArena& arena, TInferredType* a, TInferredType* b) {
    // Fast path: if b adds nothing new to a, return a unchanged — no allocation.
    if (IsSubsumed(a, b)) return a;

    if (a->Kind == b->Kind) {
        if (a->Kind == EKind::Record) {
            std::unordered_map<std::string_view, size_t> bIdx;
            bIdx.reserve(b->Fields.size());
            for (size_t i = 0; i < b->Fields.size(); ++i)
                bIdx.emplace(std::string_view(b->Fields[i].first), i);

            std::vector<bool> bSeen(b->Fields.size(), false);
            std::vector<std::pair<std::string, TInferredType*>> merged;
            merged.reserve(a->Fields.size() + b->Fields.size());

            for (const auto& [k, v] : a->Fields) {
                auto it = bIdx.find(std::string_view(k));
                if (it != bIdx.end()) {
                    bSeen[it->second] = true;
                    merged.emplace_back(k, MergeTypes(arena, v, b->Fields[it->second].second));
                } else {
                    merged.emplace_back(k, MakeNullable(arena, v));
                }
            }
            for (size_t i = 0; i < b->Fields.size(); ++i) {
                if (!bSeen[i])
                    merged.emplace_back(b->Fields[i].first, MakeNullable(arena, b->Fields[i].second));
            }
            TInferredType* r = arena.Alloc(EKind::Record);
            r->Fields = std::move(merged);
            return r;
        }
        if (a->Kind == EKind::Array) {
            TInferredType* r = arena.Alloc(EKind::Array);
            r->Items = MergeTypes(arena, a->Items, b->Items);
            return r;
        }
        if (a->Kind == EKind::Nullable) {
            return MakeNullable(arena, MergeTypes(arena, a->Inner, b->Inner));
        }
        return a;
    }

    if (a->Kind == EKind::Null) return MakeNullable(arena, b);
    if (b->Kind == EKind::Null) return MakeNullable(arena, a);

    auto isNum = [](EKind k) {
        return k == EKind::Int || k == EKind::Long || k == EKind::Double;
    };
    if (isNum(a->Kind) && isNum(b->Kind)) {
        if (a->Kind == EKind::Double || b->Kind == EKind::Double)
            return arena.Alloc(EKind::Double);
        if (a->Kind == EKind::Long || b->Kind == EKind::Long)
            return arena.Alloc(EKind::Long);
        return arena.Alloc(EKind::Int);
    }

    if (a->Kind == EKind::Nullable) return MakeNullable(arena, MergeTypes(arena, a->Inner, b));
    if (b->Kind == EKind::Nullable) return MakeNullable(arena, MergeTypes(arena, a, b->Inner));

    return arena.Alloc(EKind::String);
}

// ---------------------------------------------------------------------------
// Combined expand-and-infer pass
//
// Instead of first copying the entire JSON tree via ExpandNestedJson and then
// walking it again in InferType, we do both in a single recursive traversal.
// The expanded tree is written into `out` (caller-provided storage) and the
// inferred type is returned.
// ---------------------------------------------------------------------------

static TInferredType* ExpandAndInfer(
    const NJson::TJsonValue& val,
    bool parseNested,
    TTypeArena& arena,
    TStringDict& keyDict,
    TStringDict& valDict,
    NJson::TJsonValue& out)
{
    switch (val.GetType()) {
        case NJson::JSON_NULL:
            out = val;
            return arena.Alloc(EKind::Null);

        case NJson::JSON_BOOLEAN:
            out = val;
            return arena.Alloc(EKind::Boolean);

        case NJson::JSON_INTEGER: {
            out = val;
            int64_t v = val.GetInteger();
            return (v >= INT32_MIN && v <= INT32_MAX)
                ? arena.Alloc(EKind::Int)
                : arena.Alloc(EKind::Long);
        }

        case NJson::JSON_UINTEGER: {
            out = val;
            uint64_t v = val.GetUInteger();
            return (v <= (uint64_t)INT32_MAX)
                ? arena.Alloc(EKind::Int)
                : arena.Alloc(EKind::Long);
        }

        case NJson::JSON_DOUBLE:
            out = val;
            return arena.Alloc(EKind::Double);

        case NJson::JSON_STRING: {
            const TStringBuf sv = val.GetString();
            if (parseNested && !sv.empty() && (sv[0] == '{' || sv[0] == '[')) {
                NJson::TJsonValue parsed;
                if (NJson::ReadJsonFastTree(sv, &parsed)) {
                    if (parsed.GetType() == NJson::JSON_MAP ||
                        parsed.GetType() == NJson::JSON_ARRAY) {
                        return ExpandAndInfer(parsed, parseNested, arena, keyDict, valDict, out);
                    }
                }
            }
            // Store the dict id directly so SerializeValue can read it without a second lookup.
            uint32_t id = valDict.GetOrAdd(std::string_view(sv.data(), sv.size()));
            out = NJson::TJsonValue(static_cast<long long>(id));
            return arena.Alloc(EKind::String);
        }

        case NJson::JSON_MAP: {
            out = NJson::TJsonValue(NJson::JSON_MAP);
            TInferredType* r = arena.Alloc(EKind::Record);
            for (const auto& [k, v] : val.GetMap()) {
                keyDict.GetOrAdd(std::string_view(k.data(), k.size()));
                NJson::TJsonValue& child = out[k];
                TInferredType* ft = ExpandAndInfer(v, parseNested, arena, keyDict, valDict, child);
                r->Fields.emplace_back(std::string(k.data(), k.size()), ft);
            }
            // Sort fields by name for deterministic schema and serialization order.
            std::sort(r->Fields.begin(), r->Fields.end(),
                [](const auto& a, const auto& b) { return a.first < b.first; });
            return r;
        }

        case NJson::JSON_ARRAY: {
            out = NJson::TJsonValue(NJson::JSON_ARRAY);
            const auto& arr = val.GetArray();
            TInferredType* r = arena.Alloc(EKind::Array);
            if (arr.empty()) {
                r->Items = arena.Alloc(EKind::String);
            } else {
                NJson::TJsonValue tmp;
                r->Items = ExpandAndInfer(arr[0], parseNested, arena, keyDict, valDict, tmp);
                out.AppendValue(std::move(tmp));
                for (size_t i = 1; i < arr.size(); ++i) {
                    NJson::TJsonValue tmp2;
                    TInferredType* it = ExpandAndInfer(arr[i], parseNested, arena, keyDict, valDict, tmp2);
                    r->Items = MergeTypes(arena, r->Items, it);
                    out.AppendValue(std::move(tmp2));
                }
            }
            return r;
        }

        default:
            out = val;
            return arena.Alloc(EKind::String);
    }
}

// ---------------------------------------------------------------------------
// Avro schema JSON generation
// ---------------------------------------------------------------------------

static int gRecordCounter = 0;

static std::string ToAvroSchemaJson(const TInferredType& t,
                                    size_t numVals,
                                    bool& stringRefDefined) {
    switch (t.Kind) {
        case EKind::Null:    return "\"null\"";
        case EKind::Boolean: return "\"boolean\"";
        case EKind::Int:     return "\"int\"";
        case EKind::Long:    return "\"long\"";
        case EKind::Double:  return "\"double\"";
        case EKind::String: {
            if (!stringRefDefined) {
                stringRefDefined = true;
                const char* idType = (numVals <= (size_t)INT32_MAX) ? "int" : "long";
                return std::string(
                    "{\"type\":\"record\",\"name\":\"string_ref\","
                    "\"fields\":[{\"name\":\"id\",\"type\":\"") + idType + "\"}]}";
            }
            return "\"string_ref\"";
        }
        case EKind::Nullable: {
            std::string inner = ToAvroSchemaJson(*t.Inner, numVals, stringRefDefined);
            return "[\"null\"," + inner + "]";
        }
        case EKind::Record: {
            std::string name = "Record_" + std::to_string(++gRecordCounter);
            std::string result = "{\"type\":\"record\",\"name\":\"" + name + "\",\"fields\":[";
            bool first = true;
            for (const auto& [fname, ftype] : t.Fields) {
                if (!first) result += ",";
                first = false;
                std::string fieldSchema = ToAvroSchemaJson(*ftype, numVals, stringRefDefined);
                result += "{\"name\":\"";
                result.append(fname.data(), fname.size());
                result += "\"";
                if (ftype->IsNullable())
                    result += ",\"type\":" + fieldSchema + ",\"default\":null";
                else
                    result += ",\"type\":" + fieldSchema;
                result += "}";
            }
            result += "]}";
            return result;
        }
        case EKind::Array: {
            std::string items = ToAvroSchemaJson(*t.Items, numVals, stringRefDefined);
            return "{\"type\":\"array\",\"items\":" + items + "}";
        }
    }
    return "\"string_ref\"";
}

// ---------------------------------------------------------------------------
// Fast Avro binary serializer
//
// Writes directly into a std::vector<uint8_t> without any GenericDatum
// allocations or virtual-dispatch encoder calls.
// ---------------------------------------------------------------------------

// Zigzag-encode and write a variable-length int64 (Avro wire format).
static void WriteVarInt(std::vector<uint8_t>& buf, int64_t value) {
    // Zigzag encode: map signed -> unsigned so small negatives are compact.
    uint64_t uv = (static_cast<uint64_t>(value) << 1) ^ static_cast<uint64_t>(value >> 63);
    // Write 7 bits at a time, MSB set means more bytes follow.
    while (uv > 0x7F) {
        buf.push_back(static_cast<uint8_t>((uv & 0x7F) | 0x80));
        uv >>= 7;
    }
    buf.push_back(static_cast<uint8_t>(uv));
}

static void WriteBytes(std::vector<uint8_t>& buf, const uint8_t* data, size_t len) {
    buf.insert(buf.end(), data, data + len);
}

static void WriteString(std::vector<uint8_t>& buf, const std::string& s) {
    WriteVarInt(buf, static_cast<int64_t>(s.size()));
    WriteBytes(buf, reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

// Recursively serialize a JSON value according to the inferred schema directly
// into buf. No heap allocations beyond the output buffer itself.
static void SerializeValue(std::vector<uint8_t>& buf,
                           const NJson::TJsonValue& val,
                           const TInferredType& schema) {
    switch (schema.Kind) {
        case EKind::Null:
            // null encodes as zero bytes
            return;

        case EKind::Boolean:
            buf.push_back((val.GetType() == NJson::JSON_BOOLEAN && val.GetBoolean()) ? 1 : 0);
            return;

        case EKind::Int: {
            int32_t v = 0;
            if (val.GetType() == NJson::JSON_INTEGER)
                v = static_cast<int32_t>(val.GetInteger());
            else if (val.GetType() == NJson::JSON_UINTEGER)
                v = static_cast<int32_t>(val.GetUInteger());
            else if (val.GetType() == NJson::JSON_DOUBLE)
                v = static_cast<int32_t>(val.GetDouble());
            WriteVarInt(buf, v);
            return;
        }

        case EKind::Long: {
            int64_t v = 0;
            if (val.GetType() == NJson::JSON_INTEGER)
                v = val.GetInteger();
            else if (val.GetType() == NJson::JSON_UINTEGER)
                v = static_cast<int64_t>(val.GetUInteger());
            else if (val.GetType() == NJson::JSON_DOUBLE)
                v = static_cast<int64_t>(val.GetDouble());
            WriteVarInt(buf, v);
            return;
        }

        case EKind::Double: {
            double v = 0.0;
            if (val.GetType() == NJson::JSON_INTEGER)
                v = static_cast<double>(val.GetInteger());
            else if (val.GetType() == NJson::JSON_UINTEGER)
                v = static_cast<double>(val.GetUInteger());
            else if (val.GetType() == NJson::JSON_DOUBLE)
                v = val.GetDouble();
            WriteBytes(buf, reinterpret_cast<const uint8_t*>(&v), sizeof(double));
            return;
        }

        case EKind::String: {
            // The id was pre-computed by ExpandAndInfer and stored as an integer.
            WriteVarInt(buf, val.GetIntegerRobust());
            return;
        }

        case EKind::Nullable: {
            if (val.GetType() == NJson::JSON_NULL) {
                // Union branch 0 = null
                WriteVarInt(buf, 0);
                // null value encodes as zero bytes
            } else {
                // Union branch 1 = inner type
                WriteVarInt(buf, 1);
                SerializeValue(buf, val, *schema.Inner);
            }
            return;
        }

        case EKind::Record: {
            // Records: fields written in order, no framing.
            // Use the JSON map directly via GetMap() (hash map lookup) instead
            // of GetValueByPath() which re-parses the path string each call.
            const NJson::TJsonValue::TMapType* jsonMap = nullptr;
            if (val.GetType() == NJson::JSON_MAP) {
                jsonMap = &val.GetMap();
            }

            for (const auto& [fname, ftype] : schema.Fields) {
                const NJson::TJsonValue* fieldVal = nullptr;
                if (jsonMap) {
                    auto it = jsonMap->find(TStringBuf(fname.data(), fname.size()));
                    if (it != jsonMap->end()) {
                        fieldVal = &it->second;
                    }
                }

                if (fieldVal) {
                    SerializeValue(buf, *fieldVal, *ftype);
                } else if (ftype->IsNullable()) {
                    // Missing field in nullable union -> write null branch (index 0).
                    WriteVarInt(buf, 0);
                }
            }
            return;
        }

        case EKind::Array: {
            if (val.GetType() == NJson::JSON_ARRAY) {
                const auto& arr = val.GetArray();
                if (!arr.empty()) {
                    // Block count (positive = count of items in this block)
                    WriteVarInt(buf, static_cast<int64_t>(arr.size()));
                    for (const auto& item : arr) {
                        SerializeValue(buf, item, *schema.Items);
                    }
                }
            }
            // End-of-array marker: block count 0
            WriteVarInt(buf, 0);
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// Avro container file writer (in-memory, no temp file)
//
// Format (from Avro spec):
//   magic:    4 bytes  "Obj\x01"
//   metadata: avro map<bytes>  (avro.schema, avro.codec)
//   sync:     16 bytes random
//   [blocks]:
//     object_count: varint
//     byte_count:   varint
//     data:         <byte_count bytes of serialized objects>
//     sync:         16 bytes
// ---------------------------------------------------------------------------

using TSync = std::array<uint8_t, 16>;

static TSync MakeSync() {
    // Fixed sync marker — randomness is not required for correctness,
    // only uniqueness within the file for block boundary detection.
    return {0x42, 0x4b, 0x56, 0x5f, 0x53, 0x59, 0x4e, 0x43,
            0x4d, 0x41, 0x52, 0x4b, 0x45, 0x52, 0x30, 0x31};
}

// Write an Avro map<bytes> (used for the file header metadata).
static void WriteAvroMetaMap(std::vector<uint8_t>& buf,
                             const std::string& schemaJson) {
    // Two entries: avro.codec and avro.schema.
    WriteVarInt(buf, 2);

    const std::string codecKey = "avro.codec";
    const std::string codecVal = "null";
    WriteString(buf, codecKey);
    WriteVarInt(buf, static_cast<int64_t>(codecVal.size()));
    WriteBytes(buf, reinterpret_cast<const uint8_t*>(codecVal.data()), codecVal.size());

    const std::string schemaKey = "avro.schema";
    WriteString(buf, schemaKey);
    WriteVarInt(buf, static_cast<int64_t>(schemaJson.size()));
    WriteBytes(buf, reinterpret_cast<const uint8_t*>(schemaJson.data()), schemaJson.size());

    // End-of-map marker
    WriteVarInt(buf, 0);
}

// Serialize all rows into an Avro object container file stored in memory.
// syncInterval: flush a block every this many bytes of row data.
static std::vector<uint8_t> SerializeAvroContainer(
    const std::vector<NJson::TJsonValue>& docs,
    const TInferredType& schema,
    const std::string& schemaJson,
    size_t syncInterval = 64 * 1024)
{
    const TSync sync = MakeSync();

    std::vector<uint8_t> out;
    // Reserve a generous initial capacity to avoid repeated reallocations.
    out.reserve(docs.size() * 64);

    // --- Header ---
    // Magic
    const uint8_t magic[4] = {'O', 'b', 'j', 0x01};
    WriteBytes(out, magic, 4);

    // Metadata map (avro.codec + avro.schema)
    WriteAvroMetaMap(out, schemaJson);

    // Sync marker
    WriteBytes(out, sync.data(), 16);

    // --- Data blocks ---
    // We accumulate serialized rows into a temporary block buffer, then flush
    // when it exceeds syncInterval.
    std::vector<uint8_t> blockBuf;
    blockBuf.reserve(syncInterval + 4096);
    int64_t blockCount = 0;

    auto FlushBlock = [&]() {
        if (blockCount == 0) return;
        WriteVarInt(out, blockCount);
        WriteVarInt(out, static_cast<int64_t>(blockBuf.size()));
        WriteBytes(out, blockBuf.data(), blockBuf.size());
        WriteBytes(out, sync.data(), 16);
        blockBuf.clear();
        blockCount = 0;
    };

    for (const auto& doc : docs) {
        SerializeValue(blockBuf, doc, schema);
        ++blockCount;
        if (blockBuf.size() >= syncInterval) {
            FlushBlock();
        }
    }
    FlushBlock();

    return out;
}

// ---------------------------------------------------------------------------
// Section serialization
// ---------------------------------------------------------------------------

// Matches Python: b"\n".join([struct.pack("<I", len(strings))] + [s.encode() for s in strings])
// i.e. <4-byte count> \n <str0> \n <str1> \n ... \n <strN-1>
static std::vector<uint8_t> SerializeStrings(const std::deque<std::string>& strings) {
    std::vector<uint8_t> buf;
    uint32_t count = static_cast<uint32_t>(strings.size());
    buf.resize(4);
    std::memcpy(buf.data(), &count, 4);
    for (const auto& s : strings) {
        buf.push_back('\n');
        buf.insert(buf.end(), s.begin(), s.end());
    }
    return buf;
}

// Compress buf with ZSTD at the given level; returns compressed bytes.
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
    std::vector<uint8_t> Data; // compressed if zstd_level >= 0, else raw
};

static void WriteSections(
    const std::string& outputPath,
    const std::vector<TSectionInfo>& sections) {
    std::ofstream out(outputPath, std::ios::binary);
    if (!out) throw std::runtime_error("Cannot open output file: " + outputPath);
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

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

// Format a size_t with thousands separators (e.g. 1,234,567).
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
    opts.SetTitle(
        "Encode NDJSON into Avro with inferred complex schema + key/value dictionaries");

    TString inputPath;
    TString outputPath;
    bool parseNested = false;
    int zstdLevel = -1;

    opts.AddLongOption("input", "Input NDJSON file").Required().StoreResult(&inputPath);
    opts.AddLongOption("output", "Output binary file").Required().StoreResult(&outputPath);
    opts.AddLongOption("parse-nested",
        "Enable recursive expansion of string values that look like JSON")
        .Optional().NoArgument().SetFlag(&parseNested);
    opts.AddLongOption("zstd-level",
        "ZSTD compression level (1..22). If omitted, no compression.")
        .Optional().StoreResult(&zstdLevel);
    opts.AddHelpOption();

    NLastGetopt::TOptsParseResult parseResult(&opts, argc, argv);

    // Read all lines.
    std::vector<std::string> lines;
    {
        TFileInput fileInput(inputPath);
        TString line;
        while (fileInput.ReadLine(line)) {
            TString stripped = StripString(line);
            if (!stripped.empty())
                lines.emplace_back(stripped.data(), stripped.size());
        }
    }

    if (lines.empty()) {
        std::cerr << "No rows to process." << std::endl;
        return 1;
    }

    TStringDict keyDict;
    TStringDict valDict;
    TTypeArena arena;

    // Pass 1: parse, expand, infer unified schema — all in one traversal.
    std::vector<NJson::TJsonValue> docs;
    docs.reserve(lines.size());
    TInferredType* unifiedType = nullptr;

    for (const auto& line : lines) {
        NJson::TJsonValue raw;
        if (!NJson::ReadJsonFastTree(TStringBuf(line.data(), line.size()), &raw)) {
            std::cerr << "JSON parse error in line: " << line.substr(0, 80) << std::endl;
            return 1;
        }
        docs.emplace_back();
        TInferredType* rowType = ExpandAndInfer(raw, parseNested, arena, keyDict, valDict, docs.back());
        unifiedType = unifiedType ? MergeTypes(arena, unifiedType, rowType) : rowType;
    }

    if (!unifiedType) {
        std::cerr << "No type inferred." << std::endl;
        return 1;
    }

    // Make all top-level record fields nullable.
    if (unifiedType->Kind == EKind::Record) {
        for (auto& [k, v] : unifiedType->Fields)
            v = MakeNullable(arena, v);
    }

    // Build Avro schema JSON.
    gRecordCounter = 0;
    bool stringRefDefined = false;
    std::string schemaJson = ToAvroSchemaJson(*unifiedType, valDict.GetStrings().size(),
                                              stringRefDefined);

    // Wrap non-record top-level types.
    if (unifiedType->Kind != EKind::Record) {
        schemaJson = "{\"type\":\"record\",\"name\":\"Row\",\"fields\":"
                     "[{\"name\":\"value\",\"type\":[\"null\"," + schemaJson + "],"
                     "\"default\":null}]}";
    }

    // Validate schema via avro library (compile-only, no serialization).
    try {
        avro::compileJsonSchemaFromString(schemaJson);
    } catch (const std::exception& e) {
        std::cerr << "Avro schema compile error: " << e.what() << std::endl;
        std::cerr << "Schema: " << schemaJson << std::endl;
        return 1;
    }

    // Serialize rows directly into an in-memory Avro container (no temp file,
    // no GenericDatum allocations).
    std::vector<uint8_t> rowsRaw = SerializeAvroContainer(
        docs, *unifiedType, schemaJson);

    const auto& keys = keyDict.GetStrings();
    const auto& vals = valDict.GetStrings();

    auto keysRaw = SerializeStrings(keys);
    auto valsRaw = SerializeStrings(vals);

    // Build sections, optionally compressing each one.
    struct TRawSection {
        std::string Name;
        std::vector<uint8_t> Raw;
    };
    std::vector<TRawSection> rawSections = {
        {"keys",   std::move(keysRaw)},
        {"values", std::move(valsRaw)},
        {"rows",   std::move(rowsRaw)},
    };

    std::vector<TSectionInfo> sections;
    sections.reserve(rawSections.size());
    for (auto& rs : rawSections) {
        TSectionInfo si;
        si.Name = rs.Name;
        si.RawSize = rs.Raw.size();
        if (zstdLevel >= 0) {
            si.Data = ZstdCompress(rs.Raw, zstdLevel);
        } else {
            si.Data = std::move(rs.Raw);
        }
        sections.push_back(std::move(si));
    }

    const std::string outStr(outputPath.data(), outputPath.size());
    WriteSections(outStr, sections);

    // Compute file sizes for stats.
    std::ifstream checkIn(std::string(inputPath.data(), inputPath.size()),
                          std::ios::binary | std::ios::ate);
    std::ifstream checkOut(outStr, std::ios::binary | std::ios::ate);
    int64_t inputFileSize  = checkIn  ? static_cast<int64_t>(checkIn.tellg())  : -1;
    int64_t outputFileSize = checkOut ? static_cast<int64_t>(checkOut.tellg()) : -1;

    size_t totalRaw  = 0;
    size_t totalComp = 0;
    for (const auto& s : sections) {
        totalRaw  += s.RawSize;
        totalComp += s.Data.size();
    }

    std::cerr << "Rows: " << docs.size()
              << ", Keys: " << keys.size()
              << ", Values: " << vals.size() << "\n";

    if (zstdLevel >= 0) {
        // Print compression table only when compression is enabled.
        std::cerr << "\n";
        std::cerr << std::left  << std::setw(12) << "Section"
                  << std::right << std::setw(12) << "Raw"
                  << std::right << std::setw(12) << "Compressed"
                  << std::right << std::setw(8)  << "Ratio"
                  << "\n";
        std::cerr << std::string(48, '-') << "\n";
        for (const auto& s : sections) {
            double ratio = s.Data.size() > 0
                ? static_cast<double>(s.RawSize) / static_cast<double>(s.Data.size())
                : 0.0;
            std::cerr << std::left  << std::setw(12) << s.Name
                      << std::right << std::setw(12) << FmtNum(s.RawSize)
                      << std::right << std::setw(12) << FmtNum(s.Data.size())
                      << std::right << std::setw(7)  << std::fixed << std::setprecision(2)
                      << ratio << "x\n";
        }
        std::cerr << std::string(48, '-') << "\n";
        double totalRatio = totalComp > 0
            ? static_cast<double>(totalRaw) / static_cast<double>(totalComp)
            : 0.0;
        std::cerr << std::left  << std::setw(12) << "TOTAL"
                  << std::right << std::setw(12) << FmtNum(totalRaw)
                  << std::right << std::setw(12) << FmtNum(totalComp)
                  << std::right << std::setw(7)  << std::fixed << std::setprecision(2)
                  << totalRatio << "x\n";
    }

    if (inputFileSize >= 0)
        std::cerr << "\nInput file:  " << FmtNum(static_cast<size_t>(inputFileSize)) << " bytes\n";
    if (outputFileSize >= 0)
        std::cerr << "Output file: " << FmtNum(static_cast<size_t>(outputFileSize)) << " bytes\n";
    if (inputFileSize > 0 && outputFileSize > 0) {
        double ioRatio = static_cast<double>(inputFileSize) / static_cast<double>(outputFileSize);
        std::cerr << "Input/Output ratio: " << std::fixed << std::setprecision(2) << ioRatio << "x\n";
    }

    return 0;
}
