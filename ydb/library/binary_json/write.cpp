#include "write.h"

#include <library/cpp/json/json_reader.h>

#include <util/generic/vector.h>
#include <util/generic/stack.h>
#include <util/generic/set.h>
#include <util/generic/algorithm.h>
#include <util/generic/map.h>

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
    TContainer(EContainerType type)
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
    ui32 InternKey(const TStringBuf value) {
        TotalKeysCount++;

        const auto it = Keys.find(value);
        if (it == Keys.end()) {
            const ui32 currentIndex = LastFreeStringIndex++;
            Keys[TString(value)] = currentIndex;
            TotalKeyLength += value.length() + 1;
            return currentIndex;
        } else {
            return it->second;
        }
    }

    ui32 InternString(const TStringBuf value) {
        const auto it = Strings.find(value);
        if (it == Strings.end()) {
            const ui32 currentIndex = LastFreeStringIndex++;
            Strings[value] = currentIndex;
            TotalStringLength += value.length() + 1;
            return currentIndex;
        } else {
            return it->second;
        }
    }

    ui32 InternNumber(double value) {
        const auto it = Numbers.find(value);
        if (it == Numbers.end()) {
            const ui32 currentIndex = LastFreeNumberIndex++;
            Numbers[value] = currentIndex;
            return currentIndex;
        } else {
            return it->second;
        }
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

    TMap<TString, ui32> Keys;
    ui32 TotalKeyLength = 0;
    ui32 TotalKeysCount = 0;

    THashMap<TString, ui32> Strings;
    ui32 LastFreeStringIndex = 0;
    ui32 TotalStringLength = 0;

    THashMap<double, ui32> Numbers;
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
    TBinaryJsonSerializer(TJsonIndex&& json)
        : Json(std::move(json))
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
        const ui32 keysSize = Json.TotalKeysCount * sizeof(TKeyEntry);
        const ui32 entriesSize = (Json.TotalEntriesCount - Json.TotalKeysCount) * sizeof(TEntry);
        const ui32 treeSize = Json.Containers.size() * sizeof(TMeta) + entriesSize + keysSize;

        // String index consists of Count and TSEntry/string body pair for each string
        const ui32 stringIndexSize = sizeof(ui32) + (Json.Strings.size() + Json.Keys.size()) * sizeof(TSEntry) + (Json.TotalStringLength + Json.TotalKeyLength);
        // Number index consists of multiple doubles
        const ui32 numberIndexSize = Json.Numbers.size() * sizeof(double);

        // Allocate space for all sections
        const ui32 totalSize = headerSize + treeSize + stringIndexSize + numberIndexSize;
        Buffer.Advance(totalSize);

        TPODWriter writer(Buffer, 0);

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

        return std::move(Buffer);
    }

private:
    /**
     * @brief Writes container and all its children recursively
     */
    void WriteContainer(TPODWriter& valueWriter, ui32 index) {
        Y_DEBUG_ABORT_UNLESS(index < Json.Containers.size());
        const auto& container = Json.Containers[index];

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
            const auto keyOffset = StringOffsets[keyIndex];
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
            result.Value = StringOffsets[stringIndex];
        } else if (entry.Type == EEntryType::Number) {
            const ui32 numberIndex = entry.Value;
            result.Value = NumberOffsets[numberIndex];
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
        const ui32 stringCount = Json.Keys.size() + Json.Strings.size();
        writer.Write(stringCount);

        TPODWriter entryWriter(writer);
        writer.Skip<TSEntry>(stringCount);

        // Write SData and SEntry for each string
        StringOffsets.resize(stringCount);

        for (const auto& it : Json.Keys) {
            const auto& currentString = it.first;
            const auto currentIndex = it.second;

            StringOffsets[currentIndex] = entryWriter.Offset;

            // Append SData to the end of the buffer
            writer.Write(currentString.data(), currentString.length());
            writer.Write("\0", 1);

            // Rewrite SEntry in string index
            entryWriter.Write(TSEntry(EStringType::RawNullTerminated, writer.Offset));
        }

        for (const auto& it : Json.Strings) {
            const auto& currentString = it.first;
            const auto currentIndex = it.second;

            StringOffsets[currentIndex] = entryWriter.Offset;

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
        const ui32 numberCount = Json.Numbers.size();

        NumberOffsets.resize(numberCount);
        for (const auto it : Json.Numbers) {
            NumberOffsets[it.second] = writer.Offset;
            writer.Write(it.first);
        }
    }

    TJsonIndex Json;
    TBinaryJson Buffer;
    TVector<ui32> StringOffsets;
    TVector<ui32> NumberOffsets;
};

/**
 * @brief Callbacks for textual JSON parser. Essentially wrapper around TJsonIndex methods
 */
class TBinaryJsonCallbacks : public TJsonCallbacks {
public:
    TBinaryJsonCallbacks(bool throwException)
        : TJsonCallbacks(/* throwException */ throwException)
    {
    }

    bool OnNull() override {
        Json.AddEntry(TEntry(EEntryType::Null), /* createTopLevel */ true);
        return true;
    }

    bool OnBoolean(bool value) override {
        auto type = EEntryType::BoolFalse;
        if (value) {
            type = EEntryType::BoolTrue;
        }
        Json.AddEntry(TEntry(type), /* createTopLevel */ true);
        return true;
    }

    bool OnInteger(long long value) override {
        Json.AddEntry(TEntry(EEntryType::Number, Json.InternNumber(static_cast<double>(value))), /* createTopLevel */ true);
        return true;
    }

    bool OnUInteger(unsigned long long value) override {
        Json.AddEntry(TEntry(EEntryType::Number, Json.InternNumber(static_cast<double>(value))), /* createTopLevel */ true);
        return true;
    }

    bool OnDouble(double value) override {
        if (Y_UNLIKELY(std::isinf(value))) {
            if (ThrowException) {
                ythrow yexception() << "JSON number is infinite";
            } else {
                return false;
            }
        }
        Json.AddEntry(TEntry(EEntryType::Number, Json.InternNumber(value)), /* createTopLevel */ true);
        return true;
    }

    bool OnString(const TStringBuf& value) override {
        Json.AddEntry(TEntry(EEntryType::String, Json.InternString(value)), /* createTopLevel */ true);
        return true;
    }

    bool OnOpenMap() override {
        Json.AddContainer(EContainerType::Object);
        return true;
    }

    bool OnMapKey(const TStringBuf& value) override {
        Json.AddEntry(TEntry(EEntryType::String, Json.InternKey(value)));
        return true;
    }

    bool OnCloseMap() override {
        Json.RemoveContainer();
        return true;
    }

    bool OnOpenArray() override {
        Json.AddContainer(EContainerType::Array);
        return true;
    }

    bool OnCloseArray() override {
        Json.RemoveContainer();
        return true;
    }

    TJsonIndex GetResult() && {
        return std::move(Json);
    }

private:
    TJsonIndex Json;
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

}

TMaybe<TBinaryJson> SerializeToBinaryJsonImpl(const TStringBuf json) {
    TMemoryInput input(json.data(), json.size());
    TBinaryJsonCallbacks callbacks(/* throwException */ false);
    if (!ReadJson(&input, &callbacks)) {
        return Nothing();
    }
    TBinaryJsonSerializer serializer(std::move(callbacks).GetResult());
    return std::move(serializer).Serialize();

}

TMaybe<TBinaryJson> SerializeToBinaryJson(const TStringBuf json) {
    return SerializeToBinaryJsonImpl(json);
}

TBinaryJson SerializeToBinaryJson(const NUdf::TUnboxedValue& value) {
    TBinaryJsonCallbacks callbacks(/* throwException */ false);
    DomToJsonIndex(value, callbacks);
    TBinaryJsonSerializer serializer(std::move(callbacks).GetResult());
    return std::move(serializer).Serialize();
}

}
