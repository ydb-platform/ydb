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
    : Reader_(reader)
    , Entry_(entry)
{
}

EEntryType TEntryCursor::GetType() const {
    return Entry_.Type;
}

TContainerCursor TEntryCursor::GetContainer() const {
    Y_DEBUG_ABORT_UNLESS(Entry_.Type == EEntryType::Container, "Expected container type");
    return TContainerCursor(Reader_, Entry_.Value);
}

TStringBuf TEntryCursor::GetString() const {
    Y_DEBUG_ABORT_UNLESS(Entry_.Type == EEntryType::String, "Expected string type");
    return Reader_->ReadString(Entry_.Value);
}

double TEntryCursor::GetNumber() const {
    Y_DEBUG_ABORT_UNLESS(Entry_.Type == EEntryType::Number, "Expected number type");
    return Reader_->ReadNumber(Entry_.Value);
}

TArrayIterator::TArrayIterator(const TBinaryJsonReaderPtr reader, ui32 startOffset, ui32 count)
    : Reader_(reader)
    , Offset_(startOffset)
{
    EndOffset_ = Offset_ + count * sizeof(TEntry);
}

TEntryCursor TArrayIterator::Next() {
    Y_DEBUG_ABORT_UNLESS(HasNext());
    TEntryCursor element(Reader_, Reader_->ReadEntry(Offset_));
    Offset_ += sizeof(TEntry);
    return element;
}

bool TArrayIterator::HasNext() const {
    return Offset_ < EndOffset_;
}

TObjectIterator::TObjectIterator(const TBinaryJsonReaderPtr reader, ui32 startOffset, ui32 count)
    : Reader_(reader)
{
    KeyOffset_ = startOffset;
    ValueOffset_ = KeyOffset_ + count * sizeof(TKeyEntry);
    ValueEndOffset_ = ValueOffset_ + count * sizeof(TEntry);
}

std::pair<TEntryCursor, TEntryCursor> TObjectIterator::Next() {
    Y_DEBUG_ABORT_UNLESS(HasNext());
    // Here we create fake Entry to return Entry cursor
    const auto stringOffset = static_cast<ui32>(Reader_->ReadKeyEntry(KeyOffset_));
    TEntryCursor key(Reader_, TEntry(EEntryType::String, stringOffset));
    TEntryCursor value(Reader_, Reader_->ReadEntry(ValueOffset_));
    KeyOffset_ += sizeof(TKeyEntry);
    ValueOffset_ += sizeof(TEntry);
    return std::make_pair(std::move(key), std::move(value));
}

bool TObjectIterator::HasNext() const {
    return ValueOffset_ < ValueEndOffset_;
}

TContainerCursor::TContainerCursor(const TBinaryJsonReaderPtr reader, ui32 startOffset)
    : Reader_(reader)
    , StartOffset_(startOffset)
{
    Meta_ = Reader_->ReadMeta(StartOffset_);
    StartOffset_ += sizeof(Meta_);
}

EContainerType TContainerCursor::GetType() const {
    return Meta_.Type;
}

ui32 TContainerCursor::GetSize() const {
    return Meta_.Size;
}

TEntryCursor TContainerCursor::GetElement(ui32 index) const {
    Y_DEBUG_ABORT_UNLESS(Meta_.Type == EContainerType::Array || Meta_.Type == EContainerType::TopLevelScalar, "Expected array");
    Y_DEBUG_ABORT_UNLESS(index < GetSize(), "Invalid index");
    const ui32 offset = StartOffset_ + index * sizeof(TEntry);
    return TEntryCursor(Reader_, Reader_->ReadEntry(offset));
}

TArrayIterator TContainerCursor::GetArrayIterator() const {
    Y_DEBUG_ABORT_UNLESS(Meta_.Type == EContainerType::Array || Meta_.Type == EContainerType::TopLevelScalar, "Expected array");
    return TArrayIterator(Reader_, StartOffset_, Meta_.Size);
}

TMaybe<TEntryCursor> TContainerCursor::Lookup(const TStringBuf key) const {
    if (Meta_.Size == 0) {
        return Nothing();
    }

    i32 left = 0;
    i32 right = Meta_.Size - 1;
    while (left <= right) {
        const i32 middle = (left + right) / 2;
        const ui32 keyEntryOffset = StartOffset_ + middle * sizeof(TKeyEntry);
        const auto keyStringOffset = Reader_->ReadKeyEntry(keyEntryOffset);

        const int compare = Reader_->ReadString(keyStringOffset).compare(key);
        if (compare == 0) {
            const ui32 entryOffset = StartOffset_ + Meta_.Size * sizeof(TKeyEntry) + middle * sizeof(TEntry);
            return TEntryCursor(Reader_, Reader_->ReadEntry(entryOffset));
        } else if (compare < 0) {
            left = middle + 1;
        } else {
            right = middle - 1;
        }
    }
    return Nothing();
}

TObjectIterator TContainerCursor::GetObjectIterator() const {
    Y_DEBUG_ABORT_UNLESS(Meta_.Type == EContainerType::Object, "Expected object");
    return TObjectIterator(Reader_, StartOffset_, Meta_.Size);
}

TBinaryJsonReader::TBinaryJsonReader(const TBinaryJson& buffer)
    : TBinaryJsonReader(TStringBuf(buffer.Data(), buffer.Size()))
{
}

TBinaryJsonReader::TBinaryJsonReader(TStringBuf buffer)
    : Buffer_(buffer)
{
    // Header is stored at the beginning of BinaryJson
    Header_ = ReadPOD<THeader>(0);

    Y_ENSURE(
        Header_.Version == CURRENT_VERSION,
        TStringBuilder() << "Version in BinaryJson `" << static_cast<ui64>(Header_.Version) << "` "
        << "does not match current version `" << static_cast<ui64>(CURRENT_VERSION) << "`"
    );

    Y_ENSURE(
        Header_.StringOffset < Buffer_.size(),
        "StringOffset must be inside buffer"
    );

    // Tree starts right after Header
    TreeStart_ = sizeof(Header_);

    // SEntry sequence starts right after count of strings
    StringCount_ = ReadPOD<ui32>(Header_.StringOffset);
    StringSEntryStart_ = Header_.StringOffset + sizeof(ui32);
}

TContainerCursor TBinaryJsonReader::GetRootCursor() {
    return TContainerCursor(TIntrusivePtr(this), TreeStart_);
}

TMeta TBinaryJsonReader::ReadMeta(ui32 offset) const {
    Y_DEBUG_ABORT_UNLESS(TreeStart_ <= offset && offset < Header_.StringOffset, "Offset is not inside Tree section");
    return ReadPOD<TMeta>(offset);
}

TEntry TBinaryJsonReader::ReadEntry(ui32 offset) const {
    Y_DEBUG_ABORT_UNLESS(TreeStart_ <= offset && offset < Header_.StringOffset, "Offset is not inside Tree section");
    return ReadPOD<TEntry>(offset);
}

TKeyEntry TBinaryJsonReader::ReadKeyEntry(ui32 offset) const {
    Y_DEBUG_ABORT_UNLESS(TreeStart_ <= offset && offset < Header_.StringOffset, "Offset is not inside Tree section");
    return ReadPOD<TKeyEntry>(offset);
}

const TStringBuf TBinaryJsonReader::ReadString(ui32 offset) const {
    Y_DEBUG_ABORT_UNLESS(StringSEntryStart_ <= offset && offset < StringSEntryStart_ + StringCount_ * sizeof(TSEntry), "Offset is not inside string index");
    ui32 startOffset = 0;
    if (offset == StringSEntryStart_) {
        startOffset = StringSEntryStart_ + StringCount_ * sizeof(TSEntry);
    } else {
        ui32 previousOffset = offset - sizeof(TSEntry);
        const auto previousEntry = ReadPOD<TSEntry>(previousOffset);
        startOffset = previousEntry.Value;
    }
    const auto entry = ReadPOD<TSEntry>(offset);
    const ui32 endOffset = entry.Value - 1;
    Y_ENSURE(startOffset <= endOffset && startOffset <= Buffer_.size() && endOffset <= Buffer_.size(), "Incorrect string bounds");
    return TStringBuf(Buffer_.data() + startOffset, endOffset - startOffset);
}

double TBinaryJsonReader::ReadNumber(ui32 offset) const {
    double result;
    Y_ENSURE(offset <= Buffer_.size() && offset + sizeof(result) <= Buffer_.size(), "Incorrect number bounds");
    MemCopy(reinterpret_cast<char*>(&result), Buffer_.data() + offset, sizeof(result));
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

TString SerializeToJson(const TContainerCursor& cursor) {
    TJsonWriterConfig config;
    config.DoubleNDigits = 16;
    config.FloatNDigits = 8;
    config.WriteNanAsString = true;

    TStringStream output;
    TJsonWriter writer(&output, config);
    ReadContainerToJson(cursor, writer);
    writer.Flush();
    return output.Str();
}

TString SerializeToJson(TStringBuf binaryJson) {
    auto reader = TBinaryJsonReader::Make(binaryJson);
    return SerializeToJson(reader->GetRootCursor());
}

namespace {

struct TPODReader {
    TPODReader(TStringBuf buffer)
        : TPODReader(buffer, 0, buffer.size())
    {
    }

    TPODReader(TStringBuf buffer, ui32 start, ui32 end)
        : Buffer(buffer)
        , Pos(start)
        , End(end)
    {
        Y_DEBUG_ABORT_UNLESS(Pos <= End && End <= Buffer.size());
    }

    template <typename T>
    TMaybe<T> Read() {
        static_assert(std::is_pod_v<T>, "TPODReader can read only POD values");
        if (Pos + sizeof(T) > End) {
            return Nothing();
        }
        TMaybe<T> result{ReadUnaligned<T>(Buffer.data() + Pos)};
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
        : Buffer_(buffer)
    {
    }

    TMaybe<TStringBuf> ValidateWithError() && {
        // Validate Header
        TPODReader reader(Buffer_);
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
        if (header->StringOffset >= Buffer_.size()) {
            return "String index offset points outside of buffer"sv;
        }
        StringIndexStart_ = header->StringOffset;

        // Validate String index
        TPODReader stringReader(Buffer_, /* start */ StringIndexStart_, /* end */ Buffer_.size());
        const auto stringCount = stringReader.Read<ui32>();
        if (!stringCount.Defined()) {
            return "Missing string index size"sv;
        }
        StringEntryStart_ = StringIndexStart_ + sizeof(ui32);
        StringDataStart_ = StringEntryStart_ + (*stringCount) * sizeof(TSEntry);

        ui32 totalLength = 0;
        ui32 lastStringOffset = StringDataStart_;
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

        NumberIndexStart_ = StringDataStart_ + totalLength;
        if (NumberIndexStart_ > Buffer_.size()) {
            return "Total length of strings in String index exceeds Buffer size"sv;
        }

        // Validate Number index
        if ((Buffer_.size() - NumberIndexStart_) % sizeof(double) != 0) {
            return "Number index cannot be split into doubles"sv;
        }

        TPODReader numberReader(Buffer_, /* start */ NumberIndexStart_, /* end */ Buffer_.size());
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
        if (offset < StringEntryStart_ || offset >= StringDataStart_) {
            return "String offset is out of String index entries section"sv;
        }
        if ((offset - StringEntryStart_) % sizeof(TSEntry) != 0) {
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
                if (numberOffset < NumberIndexStart_ || numberOffset >= Buffer_.size()) {
                    return "Number offset cannot point outside of Number index"sv;
                }
                if ((numberOffset - NumberIndexStart_) % sizeof(double) != 0) {
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
                if (metaOffset >= StringIndexStart_) {
                    return "Offset to container cannot point outside of Tree section"sv;
                }
                TPODReader containerReader(reader.Buffer, metaOffset, StringIndexStart_);
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

    ui32 StringIndexStart_ = 0;
    ui32 StringEntryStart_ = 0;
    ui32 StringDataStart_ = 0;
    ui32 NumberIndexStart_ = 0;
    TStringBuf Buffer_;
};

}

TMaybe<TStringBuf> IsValidBinaryJsonWithError(TStringBuf buffer) {
    return TBinaryJsonValidator(buffer).ValidateWithError();
}

bool IsValidBinaryJson(TStringBuf buffer) {
    return !IsValidBinaryJsonWithError(buffer).Defined();
}

}
