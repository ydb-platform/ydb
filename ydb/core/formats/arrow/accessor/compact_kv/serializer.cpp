#include "serializer.h"

#include <ydb/core/formats/arrow/serializer/native.h>

#include <ydb/library/formats/arrow/protos/accessor.pb.h>
#include <ydb/library/formats/arrow/validation/validation.h>

#include <ydb/library/actors/core/log.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/streams/zstd/zstd.h>

#include <util/generic/yexception.h>
#include <util/stream/str.h>

#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>

#include <avro/Compiler.hh>
#include <avro/NodeImpl.hh>
#include <avro/Schema.hh>
#include <avro/Types.hh>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <deque>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace NKikimr::NArrow::NAccessor::NCompactKV {

namespace {

// ===========================================================================
// String dictionary (insertion-ordered, deduplicated)
// ===========================================================================

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

// ===========================================================================
// Inferred type system (arena-allocated)
// ===========================================================================

enum class EKind {
    Null, Boolean, Int, Long, Double, String,
    Nullable, Record, Array,
};

struct TInferredType;

class TTypeArena {
public:
    TInferredType* Alloc(EKind k);

    TTypeArena() = default;
    TTypeArena(const TTypeArena&) = delete;
    TTypeArena& operator=(const TTypeArena&) = delete;

private:
    static constexpr size_t kSlabSize = 4096;
    std::vector<std::unique_ptr<TInferredType[]>> Slabs;
    size_t SlabPos = kSlabSize;
};

struct TInferredType {
    EKind Kind;
    TInferredType* Inner = nullptr;
    TInferredType* Items = nullptr;
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
    new (p) TInferredType(k);
    return p;
}

TInferredType* MakeNullable(TTypeArena& arena, TInferredType* t) {
    if (t->Kind == EKind::Null || t->Kind == EKind::Nullable) return t;
    TInferredType* r = arena.Alloc(EKind::Nullable);
    r->Inner = t;
    return r;
}

bool IsSubsumed(const TInferredType* a, const TInferredType* b) {
    if (a == b) return true;
    if (a->Kind != b->Kind) {
        if (a->Kind == EKind::Nullable) {
            if (b->Kind == EKind::Null) return true;
            return IsSubsumed(a->Inner, b);
        }
        auto isNum = [](EKind k) {
            return k == EKind::Int || k == EKind::Long || k == EKind::Double;
        };
        if (isNum(a->Kind) && isNum(b->Kind)) {
            if (a->Kind == EKind::Double) return true;
            if (a->Kind == EKind::Long && b->Kind == EKind::Int) return true;
        }
        return false;
    }
    switch (a->Kind) {
        case EKind::Record: {
            for (const auto& [bk, bv] : b->Fields) {
                bool found = false;
                for (const auto& [ak, av] : a->Fields) {
                    if (ak == bk) {
                        if (!IsSubsumed(av, bv)) return false;
                        found = true;
                        break;
                    }
                }
                if (!found) return false;
            }
            return true;
        }
        case EKind::Array:
            return IsSubsumed(a->Items, b->Items);
        case EKind::Nullable:
            return IsSubsumed(a->Inner, b->Inner);
        default:
            return true;
    }
}

TInferredType* MergeTypes(TTypeArena& arena, TInferredType* a, TInferredType* b) {
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
            // Keep fields sorted (both inputs are sorted; merge preserves a-order
            // then appends new b-fields, so re-sort to stay deterministic).
            std::sort(merged.begin(), merged.end(),
                [](const auto& x, const auto& y) { return x.first < y.first; });
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

// Builds the (possibly nested-expanded) document `out` and infers its type.
//
// String leaves are kept verbatim here; their conversion into value-dictionary
// ids is deferred to CoerceValues(), which runs against the final unified
// schema. This matters because MergeTypes() can widen a value to String after
// the fact (e.g. a key that is a number in one row and a string in another):
// only the final schema knows which values must become string ids, and storing
// ids eagerly here would make a raw integer indistinguishable from a string id.
TInferredType* ExpandAndInfer(
    const NJson::TJsonValue& val, bool parseNested, TTypeArena& arena,
    TStringDict& keyDict, NJson::TJsonValue& out)
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
            return (v >= INT32_MIN && v <= INT32_MAX) ? arena.Alloc(EKind::Int) : arena.Alloc(EKind::Long);
        }
        case NJson::JSON_UINTEGER: {
            out = val;
            uint64_t v = val.GetUInteger();
            return (v <= (uint64_t)INT32_MAX) ? arena.Alloc(EKind::Int) : arena.Alloc(EKind::Long);
        }
        case NJson::JSON_DOUBLE:
            out = val;
            return arena.Alloc(EKind::Double);
        case NJson::JSON_STRING: {
            const TStringBuf sv = val.GetString();
            if (parseNested && !sv.empty() && (sv[0] == '{' || sv[0] == '[')) {
                NJson::TJsonValue parsed;
                if (NJson::ReadJsonFastTree(sv, &parsed)) {
                    if (parsed.GetType() == NJson::JSON_MAP || parsed.GetType() == NJson::JSON_ARRAY) {
                        return ExpandAndInfer(parsed, parseNested, arena, keyDict, out);
                    }
                }
            }
            out = val;
            return arena.Alloc(EKind::String);
        }
        case NJson::JSON_MAP: {
            out = NJson::TJsonValue(NJson::JSON_MAP);
            TInferredType* r = arena.Alloc(EKind::Record);
            for (const auto& [k, v] : val.GetMap()) {
                keyDict.GetOrAdd(std::string_view(k.data(), k.size()));
                NJson::TJsonValue& child = out[k];
                TInferredType* ft = ExpandAndInfer(v, parseNested, arena, keyDict, child);
                r->Fields.emplace_back(std::string(k.data(), k.size()), ft);
            }
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
                r->Items = ExpandAndInfer(arr[0], parseNested, arena, keyDict, tmp);
                out.AppendValue(std::move(tmp));
                for (size_t i = 1; i < arr.size(); ++i) {
                    NJson::TJsonValue tmp2;
                    TInferredType* it = ExpandAndInfer(arr[i], parseNested, arena, keyDict, tmp2);
                    r->Items = MergeTypes(arena, r->Items, it);
                    out.AppendValue(std::move(tmp2));
                }
            }
            return r;
        }
        default: {
            std::string s = NJson::WriteJson(val, false);
            out = NJson::TJsonValue(TString(s));
            return arena.Alloc(EKind::String);
        }
    }
}

// Resolves String-typed leaves of `val` (interpreted against the final unified
// `schema`) into value-dictionary ids, mutating `val` in place. A value lands
// here as a real string only when its path stayed String; when the schema
// widened a non-string value to String, its JSON text representation is stored
// instead. The traversal mirrors SerializeValue() exactly so encoder and
// decoder stay in sync.
void CoerceValues(NJson::TJsonValue& val, const TInferredType& schema, TStringDict& valDict) {
    switch (schema.Kind) {
        case EKind::Null:
        case EKind::Boolean:
        case EKind::Int:
        case EKind::Long:
        case EKind::Double:
            return;
        case EKind::String: {
            std::string text;
            std::string_view repr;
            if (val.GetType() == NJson::JSON_STRING) {
                const TStringBuf sv = val.GetString();
                repr = std::string_view(sv.data(), sv.size());
            } else {
                text = NJson::WriteJson(val, false);
                repr = text;
            }
            uint32_t id = valDict.GetOrAdd(repr);
            val = NJson::TJsonValue(static_cast<long long>(id));
            return;
        }
        case EKind::Nullable:
            if (val.GetType() != NJson::JSON_NULL) {
                CoerceValues(val, *schema.Inner, valDict);
            }
            return;
        case EKind::Record: {
            if (val.GetType() != NJson::JSON_MAP) {
                return;
            }
            auto& map = val.GetMapSafe();
            for (const auto& [fname, ftype] : schema.Fields) {
                auto it = map.find(TString(fname.data(), fname.size()));
                if (it != map.end()) {
                    CoerceValues(it->second, *ftype, valDict);
                }
            }
            return;
        }
        case EKind::Array: {
            if (val.GetType() != NJson::JSON_ARRAY) {
                return;
            }
            for (auto& item : val.GetArraySafe()) {
                CoerceValues(item, *schema.Items, valDict);
            }
            return;
        }
    }
}

// ===========================================================================
// Avro schema JSON generation
// ===========================================================================

std::string ToAvroSchemaJson(const TInferredType& t, size_t numVals, bool& stringRefDefined, int& recordCounter) {
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
            std::string inner = ToAvroSchemaJson(*t.Inner, numVals, stringRefDefined, recordCounter);
            return "[\"null\"," + inner + "]";
        }
        case EKind::Record: {
            std::string name = "Record_" + std::to_string(++recordCounter);
            std::string result = "{\"type\":\"record\",\"name\":\"" + name + "\",\"fields\":[";
            bool first = true;
            for (const auto& [fname, ftype] : t.Fields) {
                if (!first) result += ",";
                first = false;
                std::string fieldSchema = ToAvroSchemaJson(*ftype, numVals, stringRefDefined, recordCounter);
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
            std::string items = ToAvroSchemaJson(*t.Items, numVals, stringRefDefined, recordCounter);
            return "{\"type\":\"array\",\"items\":" + items + "}";
        }
    }
    return "\"string_ref\"";
}

// ===========================================================================
// Avro binary encoder
// ===========================================================================

void WriteVarInt(std::vector<uint8_t>& buf, int64_t value) {
    uint64_t uv = (static_cast<uint64_t>(value) << 1) ^ static_cast<uint64_t>(value >> 63);
    while (uv > 0x7F) {
        buf.push_back(static_cast<uint8_t>((uv & 0x7F) | 0x80));
        uv >>= 7;
    }
    buf.push_back(static_cast<uint8_t>(uv));
}

void WriteBytes(std::vector<uint8_t>& buf, const uint8_t* data, size_t len) {
    buf.insert(buf.end(), data, data + len);
}

void WriteAvroString(std::vector<uint8_t>& buf, const std::string& s) {
    WriteVarInt(buf, static_cast<int64_t>(s.size()));
    WriteBytes(buf, reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

void SerializeValue(std::vector<uint8_t>& buf, const NJson::TJsonValue& val, const TInferredType& schema) {
    switch (schema.Kind) {
        case EKind::Null:
            return;
        case EKind::Boolean:
            buf.push_back((val.GetType() == NJson::JSON_BOOLEAN && val.GetBoolean()) ? 1 : 0);
            return;
        case EKind::Int: {
            int32_t v = 0;
            if (val.GetType() == NJson::JSON_INTEGER) v = static_cast<int32_t>(val.GetInteger());
            else if (val.GetType() == NJson::JSON_UINTEGER) v = static_cast<int32_t>(val.GetUInteger());
            else if (val.GetType() == NJson::JSON_DOUBLE) v = static_cast<int32_t>(val.GetDouble());
            WriteVarInt(buf, v);
            return;
        }
        case EKind::Long: {
            int64_t v = 0;
            if (val.GetType() == NJson::JSON_INTEGER) v = val.GetInteger();
            else if (val.GetType() == NJson::JSON_UINTEGER) v = static_cast<int64_t>(val.GetUInteger());
            else if (val.GetType() == NJson::JSON_DOUBLE) v = static_cast<int64_t>(val.GetDouble());
            WriteVarInt(buf, v);
            return;
        }
        case EKind::Double: {
            double v = 0.0;
            if (val.GetType() == NJson::JSON_INTEGER) v = static_cast<double>(val.GetInteger());
            else if (val.GetType() == NJson::JSON_UINTEGER) v = static_cast<double>(val.GetUInteger());
            else if (val.GetType() == NJson::JSON_DOUBLE) v = val.GetDouble();
            WriteBytes(buf, reinterpret_cast<const uint8_t*>(&v), sizeof(double));
            return;
        }
        case EKind::String:
            WriteVarInt(buf, val.GetIntegerRobust());
            return;
        case EKind::Nullable: {
            if (val.GetType() == NJson::JSON_NULL) {
                WriteVarInt(buf, 0);
            } else {
                WriteVarInt(buf, 1);
                SerializeValue(buf, val, *schema.Inner);
            }
            return;
        }
        case EKind::Record: {
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
                    WriteVarInt(buf, 0);
                }
            }
            return;
        }
        case EKind::Array: {
            if (val.GetType() == NJson::JSON_ARRAY) {
                const auto& arr = val.GetArray();
                if (!arr.empty()) {
                    WriteVarInt(buf, static_cast<int64_t>(arr.size()));
                    for (const auto& item : arr) {
                        SerializeValue(buf, item, *schema.Items);
                    }
                }
            }
            WriteVarInt(buf, 0);
            return;
        }
    }
}

using TSync = std::array<uint8_t, 16>;

TSync MakeSync() {
    return {0x42, 0x4b, 0x56, 0x5f, 0x53, 0x59, 0x4e, 0x43,
            0x4d, 0x41, 0x52, 0x4b, 0x45, 0x52, 0x30, 0x31};
}

void WriteAvroMetaMap(std::vector<uint8_t>& buf, const std::string& schemaJson) {
    WriteVarInt(buf, 2);
    const std::string codecKey = "avro.codec";
    const std::string codecVal = "null";
    WriteAvroString(buf, codecKey);
    WriteVarInt(buf, static_cast<int64_t>(codecVal.size()));
    WriteBytes(buf, reinterpret_cast<const uint8_t*>(codecVal.data()), codecVal.size());
    const std::string schemaKey = "avro.schema";
    WriteAvroString(buf, schemaKey);
    WriteVarInt(buf, static_cast<int64_t>(schemaJson.size()));
    WriteBytes(buf, reinterpret_cast<const uint8_t*>(schemaJson.data()), schemaJson.size());
    WriteVarInt(buf, 0);
}

std::vector<uint8_t> SerializeAvroContainer(
    const std::vector<NJson::TJsonValue>& docs, const TInferredType& schema,
    const std::string& schemaJson, size_t syncInterval = 64 * 1024)
{
    const TSync sync = MakeSync();
    std::vector<uint8_t> out;
    out.reserve(docs.size() * 64);

    const uint8_t magic[4] = {'O', 'b', 'j', 0x01};
    WriteBytes(out, magic, 4);
    WriteAvroMetaMap(out, schemaJson);
    WriteBytes(out, sync.data(), 16);

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

// ===========================================================================
// String-dictionary section: <uint32 count> \n <s0> \n <s1> ...
// ===========================================================================

std::vector<uint8_t> SerializeStrings(const std::deque<std::string>& strings) {
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

std::vector<std::string> ParseStringDict(const char* data, size_t size) {
    if (size < 4) ythrow yexception() << "compact_kv: string dict too short";
    uint32_t count = 0;
    std::memcpy(&count, data, 4);
    std::vector<std::string> result;
    result.reserve(count);
    size_t pos = 4;
    for (uint32_t i = 0; i < count; ++i) {
        if (pos >= size || data[pos] != '\n') {
            ythrow yexception() << "compact_kv: expected newline in string dict";
        }
        ++pos;
        size_t start = pos;
        while (pos < size && data[pos] != '\n') ++pos;
        result.emplace_back(data + start, pos - start);
    }
    return result;
}

// ===========================================================================
// Compression helpers
// ===========================================================================

std::vector<uint8_t> CompressWithCodec(const std::vector<uint8_t>& buf, arrow::util::Codec& codec) {
    const int64_t maxCompressed = codec.MaxCompressedLen(buf.size(), buf.data());
    std::vector<uint8_t> compressed(maxCompressed);
    const int64_t actualSize = NArrow::TStatusValidator::GetValid(
        codec.Compress(buf.size(), buf.data(), maxCompressed, compressed.data()));
    compressed.resize(actualSize);
    return compressed;
}

std::shared_ptr<arrow::util::Codec> BuildCodecFromSerializer(const std::shared_ptr<NArrow::NSerialization::ISerializer>& serializer) {
    if (!serializer) {
        return nullptr;
    }
    auto* native = dynamic_cast<const NArrow::NSerialization::TNativeSerializer*>(serializer.get());
    if (!native) {
        return nullptr;
    }
    const auto codecType = native->GetCodecType();
    if (codecType == arrow::Compression::UNCOMPRESSED) {
        return nullptr;
    }
    auto level = native->GetCodecLevel();
    if (level) {
        return NArrow::TStatusValidator::GetValid(arrow::util::Codec::Create(codecType, *level));
    }
    return NArrow::TStatusValidator::GetValid(arrow::util::Codec::Create(codecType));
}

bool IsZstd(const char* data, size_t size) {
    return size >= 4 && (uint8_t)data[0] == 0x28 && (uint8_t)data[1] == 0xB5 &&
           (uint8_t)data[2] == 0x2F && (uint8_t)data[3] == 0xFD;
}

std::vector<uint8_t> MaybeDecompress(const char* data, size_t size) {
    if (IsZstd(data, size)) {
        TString out;
        {
            TString raw(data, size);
            TStringInput src(raw);
            TZstdDecompress zstd(&src);
            TStringOutput sink(out);
            TransferData(&zstd, &sink);
        }
        const auto* p = reinterpret_cast<const uint8_t*>(out.data());
        return std::vector<uint8_t>(p, p + out.size());
    }
    return std::vector<uint8_t>(reinterpret_cast<const uint8_t*>(data), reinterpret_cast<const uint8_t*>(data) + size);
}

// ===========================================================================
// Avro binary decoder (native, schema-driven)
// ===========================================================================

int64_t ReadVarInt(const uint8_t* buf, size_t size, size_t& pos) {
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

void DecodeNative(
    const avro::NodePtr& nodeIn, const uint8_t* buf, size_t size, size_t& pos,
    const std::vector<std::string>& valDict, NJson::TJsonValue& out)
{
    using namespace avro;
    const avro::NodePtr node = (nodeIn->type() == AVRO_SYMBOLIC) ? avro::resolveSymbol(nodeIn) : nodeIn;
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
        default:
            ythrow yexception() << "compact_kv: unsupported avro type " << (int)node->type();
    }
}

// Decode the Avro container into a vector of binary-JSON strings (one per record).
std::vector<TString> DecodeAvroContainer(const std::vector<uint8_t>& data, const std::vector<std::string>& valDict) {
    const uint8_t* buf = data.data();
    size_t size = data.size();
    size_t pos = 0;

    if (size < 4 || buf[0] != 'O' || buf[1] != 'b' || buf[2] != 'j' || buf[3] != 0x01)
        ythrow yexception() << "compact_kv: invalid avro magic";
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
                std::string v(reinterpret_cast<const char*>(buf + pos), vlen); pos += vlen;
                if (key == "avro.schema") schemaJson = v;
            }
            mapCount = ReadVarInt(buf, size, pos);
        }
    }
    if (schemaJson.empty()) ythrow yexception() << "compact_kv: no avro.schema";

    avro::ValidSchema schema = avro::compileJsonSchemaFromString(schemaJson);
    const avro::NodePtr& root = schema.root();

    const uint8_t* syncMarker = buf + pos;
    pos += 16;

    std::vector<TString> result;
    while (pos < size) {
        int64_t objCount = ReadVarInt(buf, size, pos);
        if (objCount == 0) break;
        ReadVarInt(buf, size, pos); // byte count
        for (int64_t i = 0; i < objCount; ++i) {
            NJson::TJsonValue row;
            DecodeNative(root, buf, size, pos, valDict, row);
            TString jsonText = NJson::WriteJson(&row, false, false, false);
            auto bj = NBinaryJson::SerializeToBinaryJson(jsonText);
            if (const TString* err = std::get_if<TString>(&bj)) {
                ythrow yexception() << "compact_kv: binary json reserialize error: " << *err;
            }
            const auto& binaryJson = std::get<NBinaryJson::TBinaryJson>(bj);
            result.emplace_back(TString(binaryJson.data(), binaryJson.size()));
        }
        if (std::memcmp(buf + pos, syncMarker, 16) != 0)
            ythrow yexception() << "compact_kv: sync marker mismatch";
        pos += 16;
    }
    return result;
}

}   // anonymous namespace

// ===========================================================================
// Public API
// ===========================================================================

TString TSerializer::SerializeArray(const std::shared_ptr<arrow::Array>& binaryJsonArray, const TSettings& settings,
    const std::shared_ptr<NSerialization::ISerializer>& columnSerializer) {
    AFL_VERIFY(binaryJsonArray);
    AFL_VERIFY(binaryJsonArray->type_id() == arrow::Type::BINARY)("type", binaryJsonArray->type()->ToString());
    const auto* arr = static_cast<const arrow::BinaryArray*>(binaryJsonArray.get());
    const ui32 recordsCount = arr->length();
    const bool parseNested = settings.GetParseNested();

    TStringDict keyDict;
    TStringDict valDict;
    TTypeArena arena;

    // Build the null bitmap (1 bit per record, LSB-first). Only non-null rows
    // are encoded in the Avro container; the bitmap lets us restore null positions.
    std::vector<uint8_t> nullBitmap;
    ui32 notNullCount = 0;
    const bool hasNulls = arr->null_count() > 0;
    if (hasNulls) {
        nullBitmap.assign((recordsCount + 7) / 8, 0);
    }

    std::vector<NJson::TJsonValue> docs;
    docs.reserve(recordsCount);
    TInferredType* unifiedType = nullptr;

    for (ui32 i = 0; i < recordsCount; ++i) {
        if (arr->IsNull(i)) {
            continue;
        }
        if (hasNulls) {
            nullBitmap[i >> 3] |= (1u << (i & 7));
        }
        ++notNullCount;

        const auto view = arr->GetView(i);
        const TString jsonText = NBinaryJson::SerializeToJson(TStringBuf(view.data(), view.size()));
        NJson::TJsonValue raw;
        if (!NJson::ReadJsonFastTree(TStringBuf(jsonText.data(), jsonText.size()), &raw)) {
            ythrow yexception() << "compact_kv: cannot parse json text from binary json at row " << i;
        }
        docs.emplace_back();
        TInferredType* rowType = ExpandAndInfer(raw, parseNested, arena, keyDict, docs.back());
        unifiedType = unifiedType ? MergeTypes(arena, unifiedType, rowType) : rowType;
    }

    // Finalize the unified type. When there are no non-null rows (unifiedType is
    // null), fall back to an empty record.
    if (!unifiedType) {
        unifiedType = arena.Alloc(EKind::Record);
    } else if (unifiedType->Kind == EKind::Record) {
        for (auto& [k, v] : unifiedType->Fields) {
            v = MakeNullable(arena, v);
        }
    }

    // Resolve string leaves into value-dictionary ids against the final type.
    // This must run after MergeTypes() so values widened to String are stored
    // as ids, and before the schema/dictionary are emitted below.
    for (auto& doc : docs) {
        CoerceValues(doc, *unifiedType, valDict);
    }

    // Build the Avro schema.
    std::string schemaJson;
    {
        int recordCounter = 0;
        bool stringRefDefined = false;
        schemaJson = ToAvroSchemaJson(*unifiedType, valDict.GetStrings().size(), stringRefDefined, recordCounter);
        if (unifiedType->Kind != EKind::Record) {
            schemaJson = "{\"type\":\"record\",\"name\":\"Row\",\"fields\":"
                         "[{\"name\":\"value\",\"type\":[\"null\"," + schemaJson + "],\"default\":null}]}";
        }
    }

    std::vector<uint8_t> rowsRaw = SerializeAvroContainer(docs, *unifiedType, schemaJson);
    std::vector<uint8_t> keysRaw = SerializeStrings(keyDict.GetStrings());
    std::vector<uint8_t> valsRaw = SerializeStrings(valDict.GetStrings());

    auto codec = BuildCodecFromSerializer(columnSerializer);
    auto maybeCompress = [&](std::vector<uint8_t>&& raw) -> std::vector<uint8_t> {
        if (codec) {
            return CompressWithCodec(raw, *codec);
        }
        return std::move(raw);
    };

    const ui32 keysRawSize = keysRaw.size();
    const ui32 valsRawSize = valsRaw.size();
    const ui64 rowsRawSize = rowsRaw.size();

    std::vector<uint8_t> keysSec = maybeCompress(std::move(keysRaw));
    std::vector<uint8_t> valsSec = maybeCompress(std::move(valsRaw));
    std::vector<uint8_t> rowsSec = maybeCompress(std::move(rowsRaw));

    // Build the proto header declaring the size of each part.
    NKikimrArrowAccessorProto::TCompactKVAccessor proto;
    proto.SetKeysSize(keysSec.size());
    proto.SetValuesSize(valsSec.size());
    proto.SetRowsSize(rowsSec.size());
    proto.SetNullBitmapSize(nullBitmap.size());
    proto.SetNotNullCount(notNullCount);
    if (codec) {
        proto.SetKeysRawSize(keysRawSize);
        proto.SetValuesRawSize(valsRawSize);
        proto.SetRowsRawSize(rowsRawSize);
    }

    const TString protoString = proto.SerializeAsString();
    const ui32 protoSize = protoString.size();

    TString result;
    TStringOutput so(result);
    constexpr ui32 magic = TSerializer::Magic;
    so.Reserve(sizeof(magic) + sizeof(protoSize) + protoSize + nullBitmap.size() + keysSec.size() + valsSec.size() + rowsSec.size());
    so.Write(&magic, sizeof(magic));
    so.Write(&protoSize, sizeof(protoSize));
    so.Write(protoString.data(), protoSize);
    if (!nullBitmap.empty()) {
        so.Write(nullBitmap.data(), nullBitmap.size());
    }
    so.Write(keysSec.data(), keysSec.size());
    so.Write(valsSec.data(), valsSec.size());
    so.Write(rowsSec.data(), rowsSec.size());
    so.Finish();
    return result;
}

bool TSerializer::IsCompactKV(const TString& blob) {
    if (blob.size() < sizeof(ui32)) {
        return false;
    }
    ui32 magic = 0;
    std::memcpy(&magic, blob.data(), sizeof(magic));
    return magic == Magic;
}

std::shared_ptr<arrow::Array> TSerializer::DeserializeArray(const TString& blob, const ui32 recordsCount,
    const std::shared_ptr<NSerialization::ISerializer>& columnSerializer) {
    if (blob.size() < 2 * sizeof(ui32)) {
        ythrow yexception() << "compact_kv: blob too small";
    }
    // Skip the magic (already verified by the caller via IsCompactKV).
    size_t pos = sizeof(ui32);

    ui32 protoSize = 0;
    std::memcpy(&protoSize, blob.data() + pos, sizeof(protoSize));
    pos += sizeof(protoSize);

    NKikimrArrowAccessorProto::TCompactKVAccessor proto;
    if (!proto.ParseFromArray(blob.data() + pos, protoSize)) {
        ythrow yexception() << "compact_kv: cannot parse proto header";
    }
    pos += protoSize;

    // Build decompression codec from the column serializer (same codec used during serialization).
    auto decompCodec = BuildCodecFromSerializer(columnSerializer);

    auto decompress = [&](const char* data, size_t compressedSize, int64_t rawSize) -> std::vector<uint8_t> {
        if (!decompCodec) {
            // No compression — return data as-is. Also handle legacy blobs with ZSTD.
            return MaybeDecompress(data, compressedSize);
        }
        if (rawSize == 0) {
            // Raw size not stored (legacy blob) — fall back to old path.
            return MaybeDecompress(data, compressedSize);
        }
        std::vector<uint8_t> out(rawSize);
        const int64_t actual = NArrow::TStatusValidator::GetValid(
            decompCodec->Decompress(compressedSize, reinterpret_cast<const uint8_t*>(data), rawSize, out.data()));
        out.resize(actual);
        return out;
    };

    // Null bitmap
    std::vector<uint8_t> nullBitmap;
    if (proto.GetNullBitmapSize()) {
        nullBitmap.assign(
            reinterpret_cast<const uint8_t*>(blob.data() + pos),
            reinterpret_cast<const uint8_t*>(blob.data() + pos + proto.GetNullBitmapSize()));
        pos += proto.GetNullBitmapSize();
    }

    // keys section (not needed for reconstruction, only values)
    pos += proto.GetKeysSize();

    // values section
    std::vector<uint8_t> valsData = decompress(blob.data() + pos, proto.GetValuesSize(), proto.GetValuesRawSize());
    pos += proto.GetValuesSize();
    std::vector<std::string> valDict = ParseStringDict(
        reinterpret_cast<const char*>(valsData.data()), valsData.size());

    // rows section
    std::vector<uint8_t> rowsData = decompress(blob.data() + pos, proto.GetRowsSize(), proto.GetRowsRawSize());
    pos += proto.GetRowsSize();

    std::vector<TString> binaryJsons = DecodeAvroContainer(rowsData, valDict);

    // Rebuild the arrow binary array, restoring nulls from the bitmap.
    arrow::BinaryBuilder builder;
    NArrow::TStatusValidator::Validate(builder.Reserve(recordsCount));
    const bool hasNulls = !nullBitmap.empty();
    ui32 nextNotNull = 0;
    for (ui32 i = 0; i < recordsCount; ++i) {
        const bool isNull = hasNulls ? !((nullBitmap[i >> 3] >> (i & 7)) & 1) : false;
        if (isNull) {
            NArrow::TStatusValidator::Validate(builder.AppendNull());
        } else {
            AFL_VERIFY(nextNotNull < binaryJsons.size())("not_null", nextNotNull)("decoded", binaryJsons.size());
            const TString& bj = binaryJsons[nextNotNull++];
            NArrow::TStatusValidator::Validate(builder.Append(bj.data(), bj.size()));
        }
    }
    AFL_VERIFY(nextNotNull == binaryJsons.size())("consumed", nextNotNull)("decoded", binaryJsons.size());

    std::shared_ptr<arrow::Array> result;
    NArrow::TStatusValidator::Validate(builder.Finish(&result));
    return result;
}

}   // namespace NKikimr::NArrow::NAccessor::NCompactKV
