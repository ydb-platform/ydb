#include "write.h"

#include <contrib/libs/simdjson/include/simdjson.h>
#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>
#include <library/cpp/json/json_reader.h>
#include <util/generic/algorithm.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/stack.h>
#include <util/generic/vector.h>
#include <yql/essentials/minikql/dom/node.h>
#include <yql/essentials/utils/parse_double.h>

#include <cmath>

namespace NKikimr::NBinaryJson {

/**
 * Serialization is done in 2 steps:
 *  1. Parse textual JSON into TJsonIndex
 *  2. Serialze TJsonIndex into TBinaryJson
 *
 * During the first step we:
 *  1. Intern all strings found in JSON (both keys of objects and string values)
 *  2. Intern all numbers found in JSON
 *  3. Record JSON structure using sequence of TContainer. During this steps we store
 *     indices instead of offsets inside TEntry
 *
 * During the second step we:
 *  1. Write Header
 *  2. Write all interned strings into String index
 *  3. Write all interned numbers into Number index
 *  4. Serialize sequence of TContainer into Tree section. During this step we also
 *     replace all indices inside TEntry with actual offsets
 */

using namespace NJson;
using namespace NYql::NDom;

namespace {

struct TContainer {
    explicit TContainer(EContainerType type)
        : Type(type)
    {
    }

    EContainerType Type;
    TVector<TEntry> Header;
};

/**
 * @brief Intermediate representation of textual JSON convenient for serialization into BinaryJson. Each string and number
 * is assigned to unique index and stored in THashMap. JSON structure is stored as a sequence of TContainer
 *
 * Let us consider and example of how we store JSON structure. Given JSON:
 * ```
 * {
 *   "array": [1, 2]
 * }
 * ```
 *
 * It will be stored as following sequence of TContainer:
 * ```
 *  TContainer (Object, size 2)
 *    TEntry (String, "array")
 *    TEntry (Container, index 1)
 *  TContainer (Array, size 2)
 *    TEntry (Number, 1)
 *    TEntry (Number, 2)
 * ```
 *
 * Note that we store containers in a flat manner. Array is not nested inside object container, it is referenced by
 * container index instead. This is exactly how containers are stored in serialized BinaryJson (but with offsets instead of indices)
 */
struct TJsonIndex {
    ui32 InternKey(const TStringBuf& value) {
        TotalKeysCount++;

        const auto [it, emplaced] = Keys.emplace(value, LastFreeStringIndex);
        if (emplaced) {
            ++LastFreeStringIndex;
            TotalKeyLength += value.length() + 1;
        }
        return it->second;
    }

    ui32 InternString(const TStringBuf& value) {
        const auto [it, emplaced] = Strings.emplace(value, LastFreeStringIndex);
        if (emplaced) {
            ++LastFreeStringIndex;
            TotalStringLength += value.length() + 1;
        }
        return it->second;
    }

    ui32 InternNumber(double value) {
        const auto [it, emplaced] = Numbers.emplace(value, LastFreeNumberIndex);
        if (emplaced) {
            ++LastFreeNumberIndex;
        }
        return it->second;
    }

    void AddContainer(EContainerType type) {
        Containers.emplace_back(type);
        const ui32 index = Containers.size() - 1;
        if (!ContainerIndex.empty()) {
            // Add new container to parent container
            AddEntry(TEntry(EEntryType::Container, index));
        }
        ContainerIndex.push(index);
    }

    void RemoveContainer() {
        ContainerIndex.pop();
    }

    void AddEntry(TEntry entry, bool createTopLevel = false) {
        if (createTopLevel && ContainerIndex.empty()) {
            AddContainer(EContainerType::TopLevelScalar);
        }
        Containers[ContainerIndex.top()].Header.push_back(entry);
        TotalEntriesCount++;
    }

    TStack<ui32> ContainerIndex;
    TVector<TContainer> Containers;

    TMap<std::string, ui32> Keys;
    ui32 TotalKeyLength = 0;
    ui32 TotalKeysCount = 0;

    absl::flat_hash_map<std::string, ui32> Strings;
    ui32 LastFreeStringIndex = 0;
    ui32 TotalStringLength = 0;

    absl::flat_hash_map<double, ui32> Numbers;
    ui32 LastFreeNumberIndex = 0;

    ui32 TotalEntriesCount = 0;
};

/**
 * @brief Convenient interface to write POD datastructures into buffer
 */
struct TPODWriter {
    TBinaryJson& Buffer;
    ui32 Offset;

    TPODWriter(TBinaryJson& buffer, ui32 offset)
        : Buffer(buffer)
        , Offset(offset)
    {
    }

    template <typename T>
    void Write(const T& value) {
        Y_DEBUG_ABORT_UNLESS(Offset + sizeof(T) <= Buffer.Size());
        MemCopy(Buffer.Data() + Offset, reinterpret_cast<const char*>(&value), sizeof(T));
        Offset += sizeof(T);
    }

    void Write(const char* source, ui32 length) {
        Y_DEBUG_ABORT_UNLESS(Offset + length <= Buffer.Size());
        MemCopy(Buffer.Data() + Offset, source, length);
        Offset += length;
    }

    template <typename T>
    void Skip(ui32 count) {
        Y_DEBUG_ABORT_UNLESS(Offset + count * sizeof(T) <= Buffer.Size());
        Offset += count * sizeof(T);
    }
};

/**
 * @brief Serializes TJsonIndex into BinaryJson buffer
 */
class TBinaryJsonSerializer {
public:
    explicit TBinaryJsonSerializer(TJsonIndex&& json)
        : Json_(std::move(json))
    {
    }

    /**
     * @brief Performs actual serialization
     *
     * BinaryJson structure:
     *  +--------+------+--------------+--------------+
     *  | Header | Tree | String index | Number index |
     *  +--------+------+--------------+--------------+
     *
     * Serialization consists of the following steps:
     *  1. Reserve memory for the whole BinaryJson in 1 allocation
     *  2. Write Header
     *  3. Write String index and record offsets to all strings
     *  4. Write Number index and record offsets to all numbers
     *  5. Write Tree and replace all indices to strings and numbers with actual offsets
     */
    TBinaryJson Serialize() && {
        // Header consists only of THeader
        const ui32 headerSize = sizeof(THeader);
        // Each container consists of 1 TMeta and multiple TEntry. Objects also have multiple TKeyEntry
        const ui32 keysSize = Json_.TotalKeysCount * sizeof(TKeyEntry);
        const ui32 entriesSize = (Json_.TotalEntriesCount - Json_.TotalKeysCount) * sizeof(TEntry);
        const ui32 treeSize = Json_.Containers.size() * sizeof(TMeta) + entriesSize + keysSize;

        // String index consists of Count and TSEntry/string body pair for each string
        const ui32 stringIndexSize = sizeof(ui32) + (Json_.Strings.size() + Json_.Keys.size()) * sizeof(TSEntry) + (Json_.TotalStringLength + Json_.TotalKeyLength);
        // Number index consists of multiple doubles
        const ui32 numberIndexSize = Json_.Numbers.size() * sizeof(double);

        // Allocate space for all sections
        const ui32 totalSize = headerSize + treeSize + stringIndexSize + numberIndexSize;
        Buffer_.Advance(totalSize);

        TPODWriter writer(Buffer_, 0);

        // Write Header
        const ui32 stringIndexStart = headerSize + treeSize;
        writer.Write(THeader(CURRENT_VERSION, stringIndexStart));

        // To get offsets to index elements we first need to write String index and Number index.
        // We save current position for later use and skip Tree for now
        TPODWriter treeWriter(writer);
        writer.Skip<char>(treeSize);

        // Write String index and record offsets to all strings written
        WriteStringIndex(writer);

        // Write Number index and record offsets to all numbers written
        WriteNumberIndex(writer);

        // Write Tree
        WriteContainer(treeWriter, 0);

        return std::move(Buffer_);
    }

private:
    /**
     * @brief Writes container and all its children recursively
     */
    void WriteContainer(TPODWriter& valueWriter, ui32 index) {
        Y_DEBUG_ABORT_UNLESS(index < Json_.Containers.size());
        const auto& container = Json_.Containers[index];

        switch (container.Type) {
            case EContainerType::Array:
            case EContainerType::TopLevelScalar:
                WriteArray(valueWriter, container);
                break;

            case EContainerType::Object:
                WriteObject(valueWriter, container);
                break;
        };
    }

    /**
     * @brief Writes array and all its children recursively
     *
     * Structure:
     *  +------+---------+-----+------------+
     *  | Meta | Entry 1 | ... | Entry Size |
     *  +------+---------+-----+------------+
     */
    void WriteArray(TPODWriter& valueWriter, const TContainer& container) {
        const ui32 size = container.Header.size();
        valueWriter.Write(TMeta(container.Type, size));

        TPODWriter entryWriter(valueWriter);
        valueWriter.Skip<TEntry>(size);

        for (const auto entry : container.Header) {
            WriteValue(entry, entryWriter, valueWriter);
        }
    }

    /**
     * @brief Writes object and all its children recursively
     *
     * Structure:
     *  +------+------------+-----+---------------+---------+-----+------------+
     *  | Meta | KeyEntry 1 | ... | KeyEntry Size | Entry 1 | ... | Entry Size |
     *  +------+------------+-----+---------------+---------+-----+------------+
     */
    void WriteObject(TPODWriter& valueWriter, const TContainer& container) {
        const ui32 entriesCount = container.Header.size();
        const ui32 size = entriesCount / 2;
        valueWriter.Write(TMeta(container.Type, size));

        TVector<std::pair<TKeyEntry, TEntry>> keyValuePairs;
        keyValuePairs.reserve(size);
        for (ui32 i = 0; i < entriesCount; i += 2) {
            const auto keyIndex = container.Header[i].Value;
            const auto keyOffset = StringOffsets_[keyIndex];
            const auto& value = container.Header[i + 1];
            keyValuePairs.emplace_back(TKeyEntry(keyOffset), value);
        }

        // We need to sort all elements by key before writing them to buffer.
        // All keys are already sorted in Key index so we can just compare
        // offsets to them instead of actual keys
        SortBy(keyValuePairs, [](const auto& pair) { return pair.first; });

        TPODWriter keyWriter(valueWriter);
        valueWriter.Skip<TKeyEntry>(size);

        TPODWriter entryWriter(valueWriter);
        valueWriter.Skip<TEntry>(size);

        for (const auto& pair : keyValuePairs) {
            keyWriter.Write(pair.first);
            WriteValue(pair.second, entryWriter, valueWriter);
        }
    }

    void WriteValue(TEntry entry, TPODWriter& entryWriter, TPODWriter& valueWriter) {
        TEntry result = entry;

        if (entry.Type == EEntryType::Container) {
            const ui32 childIndex = entry.Value;
            result.Value = valueWriter.Offset;
            WriteContainer(valueWriter, childIndex);
        } else if (entry.Type == EEntryType::String) {
            const ui32 stringIndex = entry.Value;
            result.Value = StringOffsets_[stringIndex];
        } else if (entry.Type == EEntryType::Number) {
            const ui32 numberIndex = entry.Value;
            result.Value = NumberOffsets_[numberIndex];
        }

        entryWriter.Write(result);
    }

    /**
     * @brief Writes String index and returns offsets to all strings
     *
     * Structure:
     *  +----------------+----------+-----+--------------+---------+-----+-------------+
     *  | Count, 32 bits | SEntry 1 | ... | SEntry Count | SData 1 | ... | SData Count |
     *  +----------------+----------+-----+--------------+---------+-----+-------------+
     */
    void WriteStringIndex(TPODWriter& writer) {
        const ui32 stringCount = Json_.Keys.size() + Json_.Strings.size();
        writer.Write(stringCount);

        TPODWriter entryWriter(writer);
        writer.Skip<TSEntry>(stringCount);

        // Write SData and SEntry for each string
        StringOffsets_.resize(stringCount);

        for (const auto& it : Json_.Keys) {
            const auto& currentString = it.first;
            const auto currentIndex = it.second;

            StringOffsets_[currentIndex] = entryWriter.Offset;

            // Append SData to the end of the buffer
            writer.Write(currentString.data(), currentString.length());
            writer.Write("\0", 1);

            // Rewrite SEntry in string index
            entryWriter.Write(TSEntry(EStringType::RawNullTerminated, writer.Offset));
        }

        for (const auto& it : Json_.Strings) {
            const auto& currentString = it.first;
            const auto currentIndex = it.second;

            StringOffsets_[currentIndex] = entryWriter.Offset;

            // Append SData to the end of the buffer
            writer.Write(currentString.data(), currentString.length());
            writer.Write("\0", 1);

            // Rewrite SEntry in string index
            entryWriter.Write(TSEntry(EStringType::RawNullTerminated, writer.Offset));
        }
    }

    /**
     * @brief Writes Number index and returns offsets to all numbers
     *
     * Structure:
     *  +----------+-----+----------+
     *  | double 1 | ... | double N |
     *  +----------+-----+----------+
     */
    void WriteNumberIndex(TPODWriter& writer) {
        const ui32 numberCount = Json_.Numbers.size();

        NumberOffsets_.resize(numberCount);
        for (const auto it : Json_.Numbers) {
            NumberOffsets_[it.second] = writer.Offset;
            writer.Write(it.first);
        }
    }

    TJsonIndex Json_;
    TBinaryJson Buffer_;
    TVector<ui32> StringOffsets_;
    TVector<ui32> NumberOffsets_;
};

/**
 * @brief Callbacks for textual JSON parser. Essentially wrapper around TJsonIndex methods
 */
class TBinaryJsonCallbacks: public TJsonCallbacks {
public:
    TBinaryJsonCallbacks(bool throwException, bool allowInf)
        : TJsonCallbacks(/* throwException */ throwException)
        , AllowInf_(allowInf)
    {
    }

    bool OnNull() override {
        Json_.AddEntry(TEntry(EEntryType::Null), /* createTopLevel */ true);
        return true;
    }

    bool OnBoolean(bool value) override {
        auto type = EEntryType::BoolFalse;
        if (value) {
            type = EEntryType::BoolTrue;
        }
        Json_.AddEntry(TEntry(type), /* createTopLevel */ true);
        return true;
    }

    bool OnInteger(long long value) override {
        Json_.AddEntry(TEntry(EEntryType::Number, Json_.InternNumber(static_cast<double>(value))), /* createTopLevel */ true);
        return true;
    }

    bool OnUInteger(unsigned long long value) override {
        Json_.AddEntry(TEntry(EEntryType::Number, Json_.InternNumber(static_cast<double>(value))), /* createTopLevel */ true);
        return true;
    }

    bool OnDouble(double value) override {
        if (Y_UNLIKELY(std::isinf(value) && !AllowInf_)) {
            if (ThrowException) {
                ythrow yexception() << "JSON number is infinite";
            } else {
                return false;
            }
        }
        Json_.AddEntry(TEntry(EEntryType::Number, Json_.InternNumber(value)), /* createTopLevel */ true);
        return true;
    }

    bool OnString(const TStringBuf& value) override {
        Json_.AddEntry(TEntry(EEntryType::String, Json_.InternString(value)), /* createTopLevel */ true);
        return true;
    }

    bool OnOpenMap() override {
        Json_.AddContainer(EContainerType::Object);
        return true;
    }

    bool OnMapKey(const TStringBuf& value) override {
        Json_.AddEntry(TEntry(EEntryType::String, Json_.InternKey(value)));
        return true;
    }

    bool OnCloseMap() override {
        Json_.RemoveContainer();
        return true;
    }

    bool OnOpenArray() override {
        Json_.AddContainer(EContainerType::Array);
        return true;
    }

    bool OnCloseArray() override {
        Json_.RemoveContainer();
        return true;
    }

    TJsonIndex GetResult() && {
        return std::move(Json_);
    }

private:
    TJsonIndex Json_;
    bool AllowInf_;
};

void DomToJsonIndex(const NUdf::TUnboxedValue& value, TBinaryJsonCallbacks& callbacks) {
    switch (GetNodeType(value)) {
        case ENodeType::String:
            callbacks.OnString(value.AsStringRef());
            break;
        case ENodeType::Bool:
            callbacks.OnBoolean(value.Get<bool>());
            break;
        case ENodeType::Int64:
            callbacks.OnInteger(value.Get<i64>());
            break;
        case ENodeType::Uint64:
            callbacks.OnUInteger(value.Get<ui64>());
            break;
        case ENodeType::Double:
            callbacks.OnDouble(value.Get<double>());
            break;
        case ENodeType::Entity:
            callbacks.OnNull();
            break;
        case ENodeType::List: {
            callbacks.OnOpenArray();

            if (value.IsBoxed()) {
                const auto it = value.GetListIterator();
                TUnboxedValue current;
                while (it.Next(current)) {
                    DomToJsonIndex(current, callbacks);
                }
            }

            callbacks.OnCloseArray();
            break;
        }
        case ENodeType::Dict:
        case ENodeType::Attr: {
            callbacks.OnOpenMap();

            if (value.IsBoxed()) {
                const auto it = value.GetDictIterator();
                TUnboxedValue key;
                TUnboxedValue value;
                while (it.NextPair(key, value)) {
                    callbacks.OnMapKey(key.AsStringRef());
                    DomToJsonIndex(value, callbacks);
                }
            }

            callbacks.OnCloseMap();
            break;
        }
    }
}

template <typename TOnDemandValue>
    requires std::is_same_v<TOnDemandValue, simdjson::ondemand::value> || std::is_same_v<TOnDemandValue, simdjson::ondemand::document>
[[nodiscard]] simdjson::error_code SimdJsonToJsonIndex(TOnDemandValue& value, TBinaryJsonCallbacks& callbacks) {
#define RETURN_IF_NOT_SUCCESS(expr)                                           \
    if (const auto& status = expr; Y_UNLIKELY(status != simdjson::SUCCESS)) { \
        return status;                                                        \
    }

    switch (value.type()) {
        case simdjson::ondemand::json_type::string: {
            std::string_view v;
            RETURN_IF_NOT_SUCCESS(value.get(v));
            callbacks.OnString(v);
            break;
        }
        case simdjson::ondemand::json_type::boolean: {
            bool v;
            RETURN_IF_NOT_SUCCESS(value.get(v));
            callbacks.OnBoolean(v);
            break;
        }
        case simdjson::ondemand::json_type::number: {
            switch (value.get_number_type()) {
                case simdjson::builtin::number_type::floating_point_number: {
                    double v;
                    if (const auto& error = value.get(v); Y_UNLIKELY(error != simdjson::SUCCESS)) {
                        if (!NYql::TryDoubleFromString((std::string_view)value.raw_json_token(), v)) {
                            return error;
                        }
                    };
                    if (Y_UNLIKELY(!callbacks.OnDouble(v))) {
                        return simdjson::error_code::NUMBER_ERROR;
                    }
                    break;
                }
                case simdjson::builtin::number_type::signed_integer: {
                    int64_t v;
                    RETURN_IF_NOT_SUCCESS(value.get(v));
                    callbacks.OnInteger(v);
                    break;
                }
                case simdjson::builtin::number_type::unsigned_integer: {
                    uint64_t v;
                    RETURN_IF_NOT_SUCCESS(value.get(v));
                    callbacks.OnUInteger(v);
                    break;
                }
                case simdjson::builtin::number_type::big_integer:
                    double v;
                    RETURN_IF_NOT_SUCCESS(value.get(v));
                    if (Y_UNLIKELY(!callbacks.OnDouble(v))) {
                        return simdjson::error_code::NUMBER_ERROR;
                    }
                    break;
            }
            break;
        }
        case simdjson::ondemand::json_type::null: {
            auto is_null = value.is_null();
            RETURN_IF_NOT_SUCCESS(is_null.error());
            if (Y_UNLIKELY(!is_null.value_unsafe())) {
                return simdjson::error_code::N_ATOM_ERROR;
            }
            callbacks.OnNull();
            break;
        }
        case simdjson::ondemand::json_type::array: {
            callbacks.OnOpenArray();

            simdjson::ondemand::array v;
            RETURN_IF_NOT_SUCCESS(value.get(v));
            for (auto item : v) {
                RETURN_IF_NOT_SUCCESS(item.error());
                RETURN_IF_NOT_SUCCESS(SimdJsonToJsonIndex(item.value_unsafe(), callbacks));
            }

            callbacks.OnCloseArray();
            break;
        }
        case simdjson::ondemand::json_type::object: {
            callbacks.OnOpenMap();

            simdjson::ondemand::object v;
            RETURN_IF_NOT_SUCCESS(value.get(v));
            for (auto item : v) {
                RETURN_IF_NOT_SUCCESS(item.error());
                auto& keyValue = item.value_unsafe();
                const auto key = keyValue.unescaped_key();
                RETURN_IF_NOT_SUCCESS(key.error());
                callbacks.OnMapKey(key.value_unsafe());
                RETURN_IF_NOT_SUCCESS(SimdJsonToJsonIndex(keyValue.value(), callbacks));
            }

            callbacks.OnCloseMap();
            break;
        }
        case simdjson::ondemand::json_type::unknown:
            return simdjson::UNEXPECTED_ERROR;
    }

    return simdjson::SUCCESS;

#undef RETURN_IF_NOT_SUCCESS
}

// unused, left for performance comparison
[[nodiscard]] [[maybe_unused]] simdjson::error_code SimdJsonToJsonIndexImpl(const simdjson::dom::element& value, TBinaryJsonCallbacks& callbacks) {
#define RETURN_IF_NOT_SUCCESS(status)              \
    if (Y_UNLIKELY(status != simdjson::SUCCESS)) { \
        return status;                             \
    }

    switch (value.type()) {
        case simdjson::dom::element_type::STRING: {
            std::string_view v;
            RETURN_IF_NOT_SUCCESS(value.get(v));
            callbacks.OnString(v);
            break;
        }
        case simdjson::dom::element_type::BOOL: {
            bool v;
            RETURN_IF_NOT_SUCCESS(value.get(v));
            callbacks.OnBoolean(v);
            break;
        }
        case simdjson::dom::element_type::INT64: {
            int64_t v;
            RETURN_IF_NOT_SUCCESS(value.get(v));
            callbacks.OnInteger(v);
            break;
        }
        case simdjson::dom::element_type::UINT64: {
            uint64_t v;
            RETURN_IF_NOT_SUCCESS(value.get(v));
            callbacks.OnUInteger(v);
            break;
        }
        case simdjson::dom::element_type::DOUBLE: {
            double v;
            RETURN_IF_NOT_SUCCESS(value.get(v));
            callbacks.OnDouble(v);
            break;
        }
        case simdjson::dom::element_type::NULL_VALUE:
            callbacks.OnNull();
            break;
        case simdjson::dom::element_type::ARRAY: {
            callbacks.OnOpenArray();

            simdjson::dom::array v;
            RETURN_IF_NOT_SUCCESS(value.get(v));
            for (const auto& item : v) {
                RETURN_IF_NOT_SUCCESS(SimdJsonToJsonIndexImpl(item, callbacks));
            }

            callbacks.OnCloseArray();
            break;
        }
        case simdjson::dom::element_type::OBJECT: {
            callbacks.OnOpenMap();

            simdjson::dom::object v;
            RETURN_IF_NOT_SUCCESS(value.get(v));
            for (const auto& item : v) {
                callbacks.OnMapKey(item.key);
                RETURN_IF_NOT_SUCCESS(SimdJsonToJsonIndexImpl(item.value, callbacks));
            }

            callbacks.OnCloseMap();
            break;
        }
    }
    return simdjson::SUCCESS;
#undef RETURN_IF_NOT_SUCCESS
}

void SerializeEntryCursorToBinaryJson(TBinaryJsonCallbacks& callbacks, const NBinaryJson::TEntryCursor& value) {
    switch (value.GetType()) {
        case NBinaryJson::EEntryType::BoolFalse:
            callbacks.OnBoolean(false);
            break;
        case NBinaryJson::EEntryType::BoolTrue:
            callbacks.OnBoolean(true);
            break;
        case NBinaryJson::EEntryType::Null:
            callbacks.OnNull();
            break;
        case NBinaryJson::EEntryType::String:
            callbacks.OnString(value.GetString());
            break;
        case NBinaryJson::EEntryType::Number:
            callbacks.OnDouble(value.GetNumber());
            break;
        case NBinaryJson::EEntryType::Container: {
            auto container = value.GetContainer();
            if (container.GetType() == NBinaryJson::EContainerType::Array) {
                callbacks.OnOpenArray();

                auto it = container.GetArrayIterator();
                while (it.HasNext()) {
                    auto value = it.Next();
                    SerializeEntryCursorToBinaryJson(callbacks, value);
                }

                callbacks.OnCloseArray();

            } else if (container.GetType() == NBinaryJson::EContainerType::Object) {
                callbacks.OnOpenMap();

                auto it = container.GetObjectIterator();
                while (it.HasNext()) {
                    auto [key, value] = it.Next();
                    if (key.GetType() != NBinaryJson::EEntryType::String) {
                        throw yexception() << "Unexpected non-string key: " << key.GetType();
                    }

                    callbacks.OnMapKey(key.GetString());
                    SerializeEntryCursorToBinaryJson(callbacks, value);
                }

                callbacks.OnCloseMap();
            } else {
                throw yexception() << "Unexpected type in container iterator: " << container.GetType();
            }
            break;
        }
        default:
            throw yexception() << "Unexpected entry type: " << value.GetType();
    }
}
} // namespace

std::variant<TBinaryJson, TString> SerializeToBinaryJsonImpl(const TStringBuf json, bool allowInf) {
    std::variant<TBinaryJson, TString> res;
    TBinaryJsonCallbacks callbacks(/* throwException */ false, allowInf);
    const simdjson::padded_string paddedJson(json);
    simdjson::ondemand::parser parser;
    try {
        auto doc = parser.iterate(paddedJson);
        if (auto status = doc.error(); status != simdjson::SUCCESS) {
            res = TString(simdjson::error_message(status));
            return res;
        }
        if (auto status = SimdJsonToJsonIndex(doc.value_unsafe(), callbacks); status != simdjson::SUCCESS) {
            res = TString(simdjson::error_message(status));
            return res;
        }
    } catch (const simdjson::simdjson_error& e) {
        res = TString(e.what());
        return res;
    }
    TBinaryJsonSerializer serializer(std::move(callbacks).GetResult());
    res = std::move(serializer).Serialize();
    return res;
}

std::variant<TBinaryJson, TString> SerializeToBinaryJson(const TStringBuf json, bool allowInf) {
    return SerializeToBinaryJsonImpl(json, allowInf);
}

TBinaryJson SerializeToBinaryJson(const NUdf::TUnboxedValue& value) {
    TBinaryJsonCallbacks callbacks(/* throwException */ false, /* allowInf */ false);
    DomToJsonIndex(value, callbacks);
    TBinaryJsonSerializer serializer(std::move(callbacks).GetResult());
    return std::move(serializer).Serialize();
}

std::variant<TBinaryJson, TString> SerializeToBinaryJson(const NBinaryJson::TEntryCursor& value) {
    TBinaryJsonCallbacks callbacks(/* throwException */ true, /* allowInf */ false);

    try {
        SerializeEntryCursorToBinaryJson(callbacks, value);
    } catch (const yexception& ex) {
        return TString(ex.what());
    }

    TBinaryJsonSerializer serializer(std::move(callbacks).GetResult());
    return std::move(serializer).Serialize();
}

} // namespace NKikimr::NBinaryJson
