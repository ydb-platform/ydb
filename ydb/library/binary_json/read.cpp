#include "read.h"

#include <library/cpp/json/json_writer.h>

#include <util/stream/str.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <cmath>

namespace NKikimr::NBinaryJson {

using namespace NUdf;
using namespace NYql::NDom;
using namespace NJson;

TEntryCursor::TEntryCursor(const TBinaryJsonReaderPtr reader, TEntry entry)
    : Reader(reader)
    , Entry(entry)
{
}

EEntryType TEntryCursor::GetType() const {
    return Entry.Type;
}

TContainerCursor TEntryCursor::GetContainer() const {
    Y_DEBUG_ABORT_UNLESS(Entry.Type == EEntryType::Container, "Expected container type");
    return TContainerCursor(Reader, Entry.Value);
}

TStringBuf TEntryCursor::GetString() const {
    Y_DEBUG_ABORT_UNLESS(Entry.Type == EEntryType::String, "Expected string type");
    return Reader->ReadString(Entry.Value);
}

double TEntryCursor::GetNumber() const {
    Y_DEBUG_ABORT_UNLESS(Entry.Type == EEntryType::Number, "Expected number type");
    return Reader->ReadNumber(Entry.Value);
}

TArrayIterator::TArrayIterator(const TBinaryJsonReaderPtr reader, ui32 startOffset, ui32 count)
    : Reader(reader)
    , Offset(startOffset)
{
    EndOffset = Offset + count * sizeof(TEntry);
}

TEntryCursor TArrayIterator::Next() {
    Y_DEBUG_ABORT_UNLESS(HasNext());
    TEntryCursor element(Reader, Reader->ReadEntry(Offset));
    Offset += sizeof(TEntry);
    return element;
}

bool TArrayIterator::HasNext() const {
    return Offset < EndOffset;
}

TObjectIterator::TObjectIterator(const TBinaryJsonReaderPtr reader, ui32 startOffset, ui32 count)
    : Reader(reader)
{
    KeyOffset = startOffset;
    ValueOffset = KeyOffset + count * sizeof(TKeyEntry);
    ValueEndOffset = ValueOffset + count * sizeof(TEntry);
}

std::pair<TEntryCursor, TEntryCursor> TObjectIterator::Next() {
    Y_DEBUG_ABORT_UNLESS(HasNext());
    // Here we create fake Entry to return Entry cursor
    const auto stringOffset = static_cast<ui32>(Reader->ReadKeyEntry(KeyOffset));
    TEntryCursor key(Reader, TEntry(EEntryType::String, stringOffset));
    TEntryCursor value(Reader, Reader->ReadEntry(ValueOffset));
    KeyOffset += sizeof(TKeyEntry);
    ValueOffset += sizeof(TEntry);
    return std::make_pair(std::move(key), std::move(value));
}

bool TObjectIterator::HasNext() const {
    return ValueOffset < ValueEndOffset;
}

TContainerCursor::TContainerCursor(const TBinaryJsonReaderPtr reader, ui32 startOffset)
    : Reader(reader)
    , StartOffset(startOffset)
{
    Meta = Reader->ReadMeta(StartOffset);
    StartOffset += sizeof(Meta);
}

EContainerType TContainerCursor::GetType() const {
    return Meta.Type;
}

ui32 TContainerCursor::GetSize() const {
    return Meta.Size;
}

TEntryCursor TContainerCursor::GetElement(ui32 index) const {
    Y_DEBUG_ABORT_UNLESS(Meta.Type == EContainerType::Array || Meta.Type == EContainerType::TopLevelScalar, "Expected array");
    Y_DEBUG_ABORT_UNLESS(index < GetSize(), "Invalid index");
    const ui32 offset = StartOffset + index * sizeof(TEntry);
    return TEntryCursor(Reader, Reader->ReadEntry(offset));
}

TArrayIterator TContainerCursor::GetArrayIterator() const {
    Y_DEBUG_ABORT_UNLESS(Meta.Type == EContainerType::Array || Meta.Type == EContainerType::TopLevelScalar, "Expected array");
    return TArrayIterator(Reader, StartOffset, Meta.Size);
}

TMaybe<TEntryCursor> TContainerCursor::Lookup(const TStringBuf key) const {
    if (Meta.Size == 0) {
        return Nothing();
    }

    i32 left = 0;
    i32 right = Meta.Size - 1;
    while (left <= right) {
        const i32 middle = (left + right) / 2;
        const ui32 keyEntryOffset = StartOffset + middle * sizeof(TKeyEntry);
        const auto keyStringOffset = Reader->ReadKeyEntry(keyEntryOffset);

        const int compare = Reader->ReadString(keyStringOffset).compare(key);
        if (compare == 0) {
            const ui32 entryOffset = StartOffset + Meta.Size * sizeof(TKeyEntry) + middle * sizeof(TEntry);
            return TEntryCursor(Reader, Reader->ReadEntry(entryOffset));
        } else if (compare < 0) {
            left = middle + 1;
        } else {
            right = middle - 1;
        }
    }
    return Nothing();
}

TObjectIterator TContainerCursor::GetObjectIterator() const {
    Y_DEBUG_ABORT_UNLESS(Meta.Type == EContainerType::Object, "Expected object");
    return TObjectIterator(Reader, StartOffset, Meta.Size);
}

TBinaryJsonReader::TBinaryJsonReader(const TBinaryJson& buffer)
    : TBinaryJsonReader(TStringBuf(buffer.Data(), buffer.Size()))
{
}

TBinaryJsonReader::TBinaryJsonReader(TStringBuf buffer)
    : Buffer(buffer)
{
    // Header is stored at the beginning of BinaryJson
    Header = ReadPOD<THeader>(0);

    Y_ENSURE(
        Header.Version == CURRENT_VERSION,
        TStringBuilder() << "Version in BinaryJson `" << static_cast<ui64>(Header.Version) << "` "
        << "does not match current version `" << static_cast<ui64>(CURRENT_VERSION) << "`"
    );

    Y_ENSURE(
        Header.StringOffset < Buffer.Size(),
        "StringOffset must be inside buffer"
    );

    // Tree starts right after Header
    TreeStart = sizeof(Header);

    // SEntry sequence starts right after count of strings
    StringCount = ReadPOD<ui32>(Header.StringOffset);
    StringSEntryStart = Header.StringOffset + sizeof(ui32);
}

TContainerCursor TBinaryJsonReader::GetRootCursor() {
    return TContainerCursor(TIntrusivePtr(this), TreeStart);
}

TMeta TBinaryJsonReader::ReadMeta(ui32 offset) const {
    Y_DEBUG_ABORT_UNLESS(TreeStart <= offset && offset < Header.StringOffset, "Offset is not inside Tree section");
    return ReadPOD<TMeta>(offset);
}

TEntry TBinaryJsonReader::ReadEntry(ui32 offset) const {
    Y_DEBUG_ABORT_UNLESS(TreeStart <= offset && offset < Header.StringOffset, "Offset is not inside Tree section");
    return ReadPOD<TEntry>(offset);
}

TKeyEntry TBinaryJsonReader::ReadKeyEntry(ui32 offset) const {
    Y_DEBUG_ABORT_UNLESS(TreeStart <= offset && offset < Header.StringOffset, "Offset is not inside Tree section");
    return ReadPOD<TKeyEntry>(offset);
}

const TStringBuf TBinaryJsonReader::ReadString(ui32 offset) const {
    Y_DEBUG_ABORT_UNLESS(StringSEntryStart <= offset && offset < StringSEntryStart + StringCount * sizeof(TSEntry), "Offset is not inside string index");
    ui32 startOffset = 0;
    if (offset == StringSEntryStart) {
        startOffset = StringSEntryStart + StringCount * sizeof(TSEntry);
    } else {
        ui32 previousOffset = offset - sizeof(TSEntry);
        const auto previousEntry = ReadPOD<TSEntry>(previousOffset);
        startOffset = previousEntry.Value;
    }
    const auto entry = ReadPOD<TSEntry>(offset);
    const ui32 endOffset = entry.Value - 1;
    Y_ENSURE(startOffset <= endOffset && startOffset <= Buffer.Size() && endOffset <= Buffer.Size(), "Incorrect string bounds");
    return TStringBuf(Buffer.Data() + startOffset, endOffset - startOffset);
}

double TBinaryJsonReader::ReadNumber(ui32 offset) const {
    double result;
    Y_ENSURE(offset <= Buffer.Size() && offset + sizeof(result) <= Buffer.Size(), "Incorrect number bounds");
    MemCopy(reinterpret_cast<char*>(&result), Buffer.Data() + offset, sizeof(result));
    return result;
}

TUnboxedValue ReadElementToJsonDom(const TEntryCursor& cursor, const NUdf::IValueBuilder* valueBuilder) {
    switch (cursor.GetType()) {
        case EEntryType::BoolFalse:
            return MakeBool(false);
        case EEntryType::BoolTrue:
            return MakeBool(true);
        case EEntryType::Null:
            return MakeEntity();
        case EEntryType::String:
            return MakeString(cursor.GetString(), valueBuilder);
        case EEntryType::Number:
            return MakeDouble(cursor.GetNumber());
        case EEntryType::Container:
            return ReadContainerToJsonDom(cursor.GetContainer(), valueBuilder);
    }
}

TUnboxedValue ReadContainerToJsonDom(const TContainerCursor& cursor, const NUdf::IValueBuilder* valueBuilder) {
    switch (cursor.GetType()) {
        case EContainerType::TopLevelScalar: {
            return ReadElementToJsonDom(cursor.GetElement(0), valueBuilder);
        }
        case EContainerType::Array: {
            TVector<TUnboxedValue> items;
            items.reserve(cursor.GetSize());

            auto it = cursor.GetArrayIterator();
            while (it.HasNext()) {
                const auto element = ReadElementToJsonDom(it.Next(), valueBuilder);
                items.push_back(element);
            }
            return MakeList(items.data(), items.size(), valueBuilder);
        }
        case EContainerType::Object: {
            TVector<TPair> items;
            items.reserve(cursor.GetSize());

            auto it = cursor.GetObjectIterator();
            while (it.HasNext()) {
                const auto [sourceKey, sourceValue] = it.Next();
                auto key = ReadElementToJsonDom(sourceKey, valueBuilder);
                auto value = ReadElementToJsonDom(sourceValue, valueBuilder);
                items.emplace_back(std::move(key), std::move(value));
            }
            return MakeDict(items.data(), items.size());
        }
    }
}

TUnboxedValue ReadToJsonDom(const TBinaryJson& binaryJson, const NUdf::IValueBuilder* valueBuilder) {
    return ReadToJsonDom(TStringBuf(binaryJson.Data(), binaryJson.Size()), valueBuilder);
}

TUnboxedValue ReadToJsonDom(TStringBuf binaryJson, const NUdf::IValueBuilder* valueBuilder) {
    auto reader = TBinaryJsonReader::Make(binaryJson);
    return ReadContainerToJsonDom(reader->GetRootCursor(), valueBuilder);
}

namespace {

void ReadContainerToJson(const TContainerCursor& cursor, TJsonWriter& writer);

void ReadElementToJson(const TEntryCursor& cursor, TJsonWriter& writer) {
    switch (cursor.GetType()) {
        case EEntryType::BoolFalse:
            writer.Write(false);
            break;
        case EEntryType::BoolTrue:
            writer.Write(true);
            break;
        case EEntryType::Null:
            writer.WriteNull();
            break;
        case EEntryType::String:
            writer.Write(cursor.GetString());
            break;
        case EEntryType::Number:
            writer.Write(cursor.GetNumber());
            break;
        case EEntryType::Container:
            ReadContainerToJson(cursor.GetContainer(), writer);
            break;
    }
}

void ReadContainerToJson(const TContainerCursor& cursor, TJsonWriter& writer) {
    switch (cursor.GetType()) {
        case EContainerType::TopLevelScalar: {
            ReadElementToJson(cursor.GetElement(0), writer);
            break;
        }
        case EContainerType::Array: {
            writer.OpenArray();
            auto it = cursor.GetArrayIterator();
            while (it.HasNext()) {
                ReadElementToJson(it.Next(), writer);
            }
            writer.CloseArray();
            break;
        }
        case EContainerType::Object: {
            writer.OpenMap();
            auto it = cursor.GetObjectIterator();
            while (it.HasNext()) {
                const auto [key, value] = it.Next();
                writer.WriteKey(key.GetString());
                ReadElementToJson(value, writer);
            }
            writer.CloseMap();
            break;
        }
    }
}

}

TString SerializeToJson(const TBinaryJson& binaryJson) {
    return SerializeToJson(TStringBuf(binaryJson.Data(), binaryJson.Size()));
}

TString SerializeToJson(TStringBuf binaryJson) {
    auto reader = TBinaryJsonReader::Make(binaryJson);

    TJsonWriterConfig config;
    config.DoubleNDigits = 16;
    config.FloatNDigits = 8;

    TStringStream output;
    TJsonWriter writer(&output, config);
    ReadContainerToJson(reader->GetRootCursor(), writer);
    writer.Flush();
    return output.Str();
}

namespace {

struct TPODReader {
    TPODReader(TStringBuf buffer)
        : TPODReader(buffer, 0, buffer.Size())
    {
    }

    TPODReader(TStringBuf buffer, ui32 start, ui32 end)
        : Buffer(buffer)
        , Pos(start)
        , End(end)
    {
        Y_DEBUG_ABORT_UNLESS(Pos <= End && End <= Buffer.Size());
    }

    template <typename T>
    TMaybe<T> Read() {
        static_assert(std::is_pod_v<T>, "TPODReader can read only POD values");
        if (Pos + sizeof(T) > End) {
            return Nothing();
        }
        TMaybe<T> result{ReadUnaligned<T>(Buffer.Data() + Pos)};
        Pos += sizeof(T);
        return result;
    }

    template <typename T>
    void Skip(ui32 count) {
        static_assert(std::is_pod_v<T>, "TPODReader can read only POD values");
        Pos += sizeof(T) * count;
    }

    TStringBuf Buffer;
    ui32 Pos;
    ui32 End;
};

struct TBinaryJsonValidator {
    TBinaryJsonValidator(TStringBuf buffer)
        : Buffer(buffer)
    {
    }

    TMaybe<TStringBuf> ValidateWithError() && {
        // Validate Header
        TPODReader reader(Buffer);
        const auto header = reader.Read<THeader>();
        if (!header.Defined()) {
            return "Missing header"sv;
        }
        if (header->Version != CURRENT_VERSION) {
            return "Version does not match current"sv;
        }
        if (header->Version > EVersion::MaxVersion) {
            return "Invalid version"sv;
        }
        if (header->StringOffset >= Buffer.Size()) {
            return "String index offset points outside of buffer"sv;
        }
        StringIndexStart = header->StringOffset;

        // Validate String index
        TPODReader stringReader(Buffer, /* start */ StringIndexStart, /* end */ Buffer.Size());
        const auto stringCount = stringReader.Read<ui32>();
        if (!stringCount.Defined()) {
            return "Missing string index size"sv;
        }
        StringEntryStart = StringIndexStart + sizeof(ui32);
        StringDataStart = StringEntryStart + (*stringCount) * sizeof(TSEntry);

        ui32 totalLength = 0;
        ui32 lastStringOffset = StringDataStart;
        for (ui32 i = 0; i < *stringCount; i++) {
            const auto entry = stringReader.Read<TSEntry>();
            if (!entry.Defined()) {
                return "Missing entry in string index"sv;
            }
            if (entry->Type != EStringType::RawNullTerminated) {
                return "String entry type is invalid"sv;
            }
            if (lastStringOffset >= entry->Value) {
                return "String entry offset points to invalid location"sv;
            }
            totalLength += entry->Value - lastStringOffset;
            lastStringOffset = entry->Value;
        }

        NumberIndexStart = StringDataStart + totalLength;
        if (NumberIndexStart > Buffer.Size()) {
            return "Total length of strings in String index exceeds Buffer size"sv;
        }

        // Validate Number index
        if ((Buffer.Size() - NumberIndexStart) % sizeof(double) != 0) {
            return "Number index cannot be split into doubles"sv;
        }

        TPODReader numberReader(Buffer, /* start */ NumberIndexStart, /* end */ Buffer.Size());
        TMaybe<double> current;
        while (current = numberReader.Read<double>()) {
            if (std::isnan(*current)) {
                return "Number index element is NaN"sv;
            }
            if (std::isinf(*current)) {
                return "Number index element is infinite"sv;
            }
        }

        // Validate Tree
        return IsValidContainer(reader, /* depth */ 0);
    }

private:
    TMaybe<TStringBuf> IsValidStringOffset(ui32 offset) {
        if (offset < StringEntryStart || offset >= StringDataStart) {
            return "String offset is out of String index entries section"sv;
        }
        if ((offset - StringEntryStart) % sizeof(TSEntry) != 0) {
            return "String offset does not point to the start of entry"sv;
        }
        return Nothing();
    }

    TMaybe<TStringBuf> IsValidEntry(TPODReader& reader, ui32 depth, bool containersAllowed = true) {
        const auto entry = reader.Read<TEntry>();
        if (!entry.Defined()) {
            return "Missing entry"sv;
        }

        switch (entry->Type) {
            case EEntryType::BoolFalse:
            case EEntryType::BoolTrue:
            case EEntryType::Null:
                // Nothing is stored in value of such entry, nothing to check
                break;
            case EEntryType::String:
                return IsValidStringOffset(entry->Value);
            case EEntryType::Number: {
                const auto numberOffset = entry->Value;
                if (numberOffset < NumberIndexStart || numberOffset >= Buffer.Size()) {
                    return "Number offset cannot point outside of Number index"sv;
                }
                if ((numberOffset - NumberIndexStart) % sizeof(double) != 0) {
                    return "Number offset does not point to the start of number"sv;
                }
                break;
            }
            case EEntryType::Container: {
                if (!containersAllowed) {
                    return "This entry cannot be a container"sv;
                }
                const auto metaOffset = entry->Value;
                if (metaOffset < reader.Pos) {
                    return "Offset to container cannot point before element"sv;
                }
                if (metaOffset >= StringIndexStart) {
                    return "Offset to container cannot point outside of Tree section"sv;
                }
                TPODReader containerReader(reader.Buffer, metaOffset, StringIndexStart);
                return IsValidContainer(containerReader, depth + 1);
            }
            default:
                return "Invalid entry type"sv;
        }

        return Nothing();
    }

    TMaybe<TStringBuf> IsValidContainer(TPODReader& reader, ui32 depth) {
        const auto meta = reader.Read<TMeta>();
        if (!meta.Defined()) {
            return "Missing Meta for container"sv;
        }

        switch (meta->Type) {
            case EContainerType::TopLevelScalar: {
                if (depth > 0) {
                    return "Top level scalar can be located only at the root of BinaryJson tree"sv;
                }

                return IsValidEntry(reader, depth, /* containersAllowed */ false);
            }
            case EContainerType::Array: {
                for (ui32 i = 0; i < meta->Size; i++) {
                    const auto error = IsValidEntry(reader, depth);
                    if (error.Defined()) {
                        return error;
                    }
                }
                break;
            }
            case EContainerType::Object: {
                TPODReader keyReader(reader);
                reader.Skip<TKeyEntry>(meta->Size);

                for (ui32 i = 0; i < meta->Size; i++) {
                    const auto keyOffset = keyReader.Read<TKeyEntry>();
                    if (!keyOffset.Defined()) {
                        return "Cannot read key offset"sv;
                    }
                    auto error = IsValidStringOffset(*keyOffset);
                    if (error.Defined()) {
                        return error;
                    }

                    error = IsValidEntry(reader, depth);
                    if (error.Defined()) {
                        return error;
                    }
                }
                break;
            }
            default:
                return "Invalid container type"sv;
        }

        return Nothing();
    }

    ui32 StringIndexStart = 0;
    ui32 StringEntryStart = 0;
    ui32 StringDataStart = 0;
    ui32 NumberIndexStart = 0;
    TStringBuf Buffer;
};

}

TMaybe<TStringBuf> IsValidBinaryJsonWithError(TStringBuf buffer) {
    return TBinaryJsonValidator(buffer).ValidateWithError();
}

bool IsValidBinaryJson(TStringBuf buffer) {
    return !IsValidBinaryJsonWithError(buffer).Defined();
}

}
