#include "skiff.h"

#include "skiff_validator.h"

#include <library/cpp/yt/coding/varint.h>

#include <library/cpp/yt/exception/exception.h>

#include <util/stream/buffered.h>
#include <util/system/byteorder.h>
#include <util/system/unaligned_mem.h>

namespace NSkiff {

////////////////////////////////////////////////////////////////////////////////

bool operator==(TInt128 lhs, TInt128 rhs)
{
    return lhs.Low == rhs.Low && lhs.High == rhs.High;
}

bool operator!=(TInt128 lhs, TInt128 rhs)
{
    return !(lhs == rhs);
}

bool operator==(TUint128 lhs, TUint128 rhs)
{
    return lhs.Low == rhs.Low && lhs.High == rhs.High;
}

bool operator!=(TUint128 lhs, TUint128 rhs)
{
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

bool operator==(const TInt256& lhs, const TInt256& rhs)
{
    return lhs.Parts == rhs.Parts;
}

bool operator==(const TUint256& lhs, const TUint256& rhs)
{
    return lhs.Parts == rhs.Parts;
}

////////////////////////////////////////////////////////////////////////////////

TUncheckedSkiffParser::TUncheckedSkiffParser(IZeroCopyInput* underlying)
    : Underlying_(underlying)
    , Buffer_(512 * 1024)
{ }

TUncheckedSkiffParser::TUncheckedSkiffParser(const std::shared_ptr<TSkiffSchema>& /*schema*/, IZeroCopyInput* underlying)
    : TUncheckedSkiffParser(underlying)
{ }

i8 TUncheckedSkiffParser::ParseInt8()
{
    return ParseSimple<i8>();
}

i16 TUncheckedSkiffParser::ParseInt16()
{
    return ParseSimple<i16>();
}

i32 TUncheckedSkiffParser::ParseInt32()
{
    return ParseSimple<i32>();
}

i64 TUncheckedSkiffParser::ParseInt64()
{
    return ParseSimple<i64>();
}

ui8 TUncheckedSkiffParser::ParseUint8()
{
    return ParseSimple<ui8>();
}

ui16 TUncheckedSkiffParser::ParseUint16()
{
    return ParseSimple<ui16>();
}

ui32 TUncheckedSkiffParser::ParseUint32()
{
    return ParseSimple<ui32>();
}

ui64 TUncheckedSkiffParser::ParseUint64()
{
    return ParseSimple<ui64>();
}

TInt128 TUncheckedSkiffParser::ParseInt128()
{
    auto low = ParseSimple<ui64>();
    auto high = ParseSimple<i64>();
    return {low, high};
}

TUint128 TUncheckedSkiffParser::ParseUint128()
{
    auto low = ParseSimple<ui64>();
    auto high = ParseSimple<ui64>();
    return {low, high};
}

TInt256 TUncheckedSkiffParser::ParseInt256()
{
    TInt256 result;
    for (auto& part : result.Parts) {
        part = ParseSimple<ui64>();
    }

    return result;
}

TUint256 TUncheckedSkiffParser::ParseUint256()
{
    TUint256 result;
    for (auto& part : result.Parts) {
        part  = ParseSimple<ui64>();
    }

    return result;
}

i32 TUncheckedSkiffParser::ParseVarInt32()
{
    i32 value;
    try {
        if (RemainingBytes() >= NYT::MaxVarInt32Size) {
            // In this branch, GetData (and its Underlying_->Next) are never called.
            auto size = NYT::ReadVarInt32(Position_, End_, &value);
            Advance(size);
        } else {
            NYT::ReadVarInt32(
                [this] {
                    auto* data = GetData(1);
                    return *static_cast<const char*>(data);
                },
                &value);
        }
    } catch (const NYT::TSimpleException& ex) {
        ythrow TSkiffException() << "Error parsing \"" << ToString(EWireType::VarInt32) << "\": " << ex.what();
    }
    return value;
}

i64 TUncheckedSkiffParser::ParseVarInt64()
{
    i64 value;
    try {
        if (RemainingBytes() >= NYT::MaxVarInt64Size) {
            // In this branch, GetData (and its Underlying_->Next) are never called.
            auto size = NYT::ReadVarInt64(Position_, End_, &value);
            Advance(size);
        } else {
            NYT::ReadVarInt64(
                [this] {
                    auto* data = GetData(1);
                    return *static_cast<const char*>(data);
                },
                &value);
        }
    } catch (const NYT::TSimpleException& ex) {
        ythrow TSkiffException() << "Error parsing \"" << ToString(EWireType::VarInt64) << "\": " << ex.what();
    }
    return value;
}

float TUncheckedSkiffParser::ParseFloat()
{
    return ParseSimple<float>();
}

double TUncheckedSkiffParser::ParseDouble()
{
    return ParseSimple<double>();
}

bool TUncheckedSkiffParser::ParseBoolean()
{
    ui8 result = ParseSimple<ui8>();
    if (result > 1) {
        ythrow TSkiffException() << "Invalid boolean value \"" << result << "\"";
    }
    return result;
}

namespace {

void ValidateStringLength(EWireType wireType, i64 length)
{
    if (length < 0 || length > MaxStringLength) {
        ythrow TSkiffException()
            << "\"" << ToString(wireType) << "\" length " << length
            << " is out of range [0, " << MaxStringLength << "]";
    }
}

} // namespace

template <typename TFunction>
TStringBuf TUncheckedSkiffParser::ParseString(EWireType wireType, TFunction&& parseLength)
{
    auto length = std::invoke(parseLength, this);
    ValidateStringLength(wireType, length);
    const void* data = GetData(length);
    return TStringBuf(static_cast<const char*>(data), length);
}

TStringBuf TUncheckedSkiffParser::ParseString32()
{
    return ParseString(EWireType::String32, &TUncheckedSkiffParser::ParseUint32);
}

TStringBuf TUncheckedSkiffParser::ParseStringVar()
{
    return ParseString(EWireType::StringVar, &TUncheckedSkiffParser::ParseVarInt64);
}

TStringBuf TUncheckedSkiffParser::ParseYson32()
{
    return ParseString(EWireType::Yson32, &TUncheckedSkiffParser::ParseUint32);
}

TStringBuf TUncheckedSkiffParser::ParseStringFixed(i64 size)
{
    Y_ABORT_UNLESS(0 <= size && size <= MaxStringLength);
    const void* data = GetData(size);
    return TStringBuf(static_cast<const char*>(data), size);
}

ui8 TUncheckedSkiffParser::ParseVariant8Tag()
{
    return ParseSimple<ui8>();
}

ui16 TUncheckedSkiffParser::ParseVariant16Tag()
{
    return ParseSimple<ui16>();
}

i32 TUncheckedSkiffParser::ParseVariantVarTag()
{
    return ParseVarInt32();
}

TBlockVarHeader TUncheckedSkiffParser::ParseBlockVarHeader()
{
    auto count = ParseVarInt64();
    if (count == std::numeric_limits<i64>::min()) {
        ythrow TSkiffException() << "Invalid BlockVarHeader: count value INT64_MIN is not allowed";
    }

    if (count < 0) {
        return TBlockVarHeader{.Count = -count, .ByteSize = ParseVarInt64()};
    } else {
        return TBlockVarHeader{.Count = count};
    }
}

template <typename T>
T TUncheckedSkiffParser::ParseSimple()
{
    return ReadUnaligned<T>(GetData(sizeof(T)));
}

const void* TUncheckedSkiffParser::GetData(size_t size)
{
    if (RemainingBytes() >= size) {
        const void* result = Position_;
        Advance(size);
        return result;
    }

    return GetDataViaBuffer(size);
}

const void* TUncheckedSkiffParser::GetDataViaBuffer(size_t size)
{
    Buffer_.Clear();
    Buffer_.Reserve(size);
    while (Buffer_.Size() < size) {
        size_t toCopy = Min(size - Buffer_.Size(), RemainingBytes());
        Buffer_.Append(Position_, toCopy);
        Advance(toCopy);

        if (RemainingBytes() == 0) {
            RefillBuffer();
            if (Exhausted_ && Buffer_.Size() < size) {
                ythrow TSkiffException() << "Premature end of stream while parsing Skiff";
            }
        }
    }
    return Buffer_.Data();
}

size_t TUncheckedSkiffParser::RemainingBytes() const
{
    Y_ASSERT(End_ >= Position_);
    return End_ - Position_;
}

void TUncheckedSkiffParser::Advance(size_t size)
{
    Y_ASSERT(size <= RemainingBytes());
    Position_ += size;
    ReadBytesCount_ += size;
}

void TUncheckedSkiffParser::RefillBuffer()
{
    size_t bufferSize = Underlying_->Next(&Position_);
    End_ = Position_ + bufferSize;
    if (bufferSize == 0) {
        Exhausted_ = true;
    }
}

bool TUncheckedSkiffParser::HasMoreData()
{
    if (RemainingBytes() == 0 && !Exhausted_) {
        RefillBuffer();
    }
    return !(RemainingBytes() == 0 && Exhausted_);
}

void TUncheckedSkiffParser::ValidateFinished()
{ }

ui64 TUncheckedSkiffParser::GetReadBytesCount() const
{
    return ReadBytesCount_;
}

////////////////////////////////////////////////////////////////////////////////

TCheckedSkiffParser::TCheckedSkiffParser(const std::shared_ptr<TSkiffSchema>& schema, IZeroCopyInput* stream)
    : Parser_(stream)
    , Validator_(std::make_unique<TSkiffValidator>(schema))
{ }

TCheckedSkiffParser::~TCheckedSkiffParser() = default;

i8 TCheckedSkiffParser::ParseInt8()
{
    Validator_->OnSimpleType(EWireType::Int8);
    return Parser_.ParseInt8();
}

i16 TCheckedSkiffParser::ParseInt16()
{
    Validator_->OnSimpleType(EWireType::Int16);
    return Parser_.ParseInt16();
}

i32 TCheckedSkiffParser::ParseInt32()
{
    Validator_->OnSimpleType(EWireType::Int32);
    return Parser_.ParseInt32();
}

i64 TCheckedSkiffParser::ParseInt64()
{
    Validator_->OnSimpleType(EWireType::Int64);
    return Parser_.ParseInt64();
}

ui8 TCheckedSkiffParser::ParseUint8()
{
    Validator_->OnSimpleType(EWireType::Uint8);
    return Parser_.ParseUint8();
}

ui16 TCheckedSkiffParser::ParseUint16()
{
    Validator_->OnSimpleType(EWireType::Uint16);
    return Parser_.ParseUint16();
}

ui32 TCheckedSkiffParser::ParseUint32()
{
    Validator_->OnSimpleType(EWireType::Uint32);
    return Parser_.ParseUint32();
}

ui64 TCheckedSkiffParser::ParseUint64()
{
    Validator_->OnSimpleType(EWireType::Uint64);
    return Parser_.ParseUint64();
}

TInt128 TCheckedSkiffParser::ParseInt128()
{
    Validator_->OnSimpleType(EWireType::Int128);
    return Parser_.ParseInt128();
}

TUint128 TCheckedSkiffParser::ParseUint128()
{
    Validator_->OnSimpleType(EWireType::Uint128);
    return Parser_.ParseUint128();
}

TInt256 TCheckedSkiffParser::ParseInt256()
{
    Validator_->OnSimpleType(EWireType::Int256);
    return Parser_.ParseInt256();
}

TUint256 TCheckedSkiffParser::ParseUint256()
{
    Validator_->OnSimpleType(EWireType::Uint256);
    return Parser_.ParseUint256();
}

i32 TCheckedSkiffParser::ParseVarInt32()
{
    Validator_->OnSimpleType(EWireType::VarInt32);
    return Parser_.ParseVarInt32();
}

i64 TCheckedSkiffParser::ParseVarInt64()
{
    Validator_->OnSimpleType(EWireType::VarInt64);
    return Parser_.ParseVarInt64();
}

float TCheckedSkiffParser::ParseFloat()
{
    Validator_->OnSimpleType(EWireType::Float);
    return Parser_.ParseFloat();
}

double TCheckedSkiffParser::ParseDouble()
{
    Validator_->OnSimpleType(EWireType::Double);
    return Parser_.ParseDouble();
}

bool TCheckedSkiffParser::ParseBoolean()
{
    Validator_->OnSimpleType(EWireType::Boolean);
    return Parser_.ParseBoolean();
}

TStringBuf TCheckedSkiffParser::ParseString32()
{
    Validator_->OnSimpleType(EWireType::String32);
    return Parser_.ParseString32();
}

TStringBuf TCheckedSkiffParser::ParseStringVar()
{
    Validator_->OnSimpleType(EWireType::StringVar);
    return Parser_.ParseStringVar();
}

TStringBuf TCheckedSkiffParser::ParseStringFixed(i64 size)
{
    Validator_->OnStringFixed(size);
    return Parser_.ParseStringFixed(size);
}

TStringBuf TCheckedSkiffParser::ParseYson32()
{
    Validator_->OnSimpleType(EWireType::Yson32);
    return Parser_.ParseYson32();
}

ui8 TCheckedSkiffParser::ParseVariant8Tag()
{
    Validator_->BeforeVariant8Tag();
    auto result = Parser_.ParseVariant8Tag();
    Validator_->OnVariant8Tag(result);
    return result;
}

ui16 TCheckedSkiffParser::ParseVariant16Tag()
{
    Validator_->BeforeVariant16Tag();
    auto result = Parser_.ParseVariant16Tag();
    Validator_->OnVariant16Tag(result);
    return result;
}

i32 TCheckedSkiffParser::ParseVariantVarTag()
{
    Validator_->BeforeVariantVarTag();
    auto result = Parser_.ParseVariantVarTag();
    Validator_->OnVariantVarTag(result);
    return result;
}

TBlockVarHeader TCheckedSkiffParser::ParseBlockVarHeader()
{
    Validator_->BeforeBlockVarHeader();
    auto blockHeader = Parser_.ParseBlockVarHeader();
    Validator_->OnBlockVarHeader(blockHeader);
    return blockHeader;
}

bool TCheckedSkiffParser::HasMoreData()
{
    return Parser_.HasMoreData();
}

void TCheckedSkiffParser::ValidateFinished()
{
    Validator_->ValidateFinished();
    Parser_.ValidateFinished();
}

ui64 TCheckedSkiffParser::GetReadBytesCount() const
{
    return Parser_.GetReadBytesCount();
}

////////////////////////////////////////////////////////////////////////////////

TUncheckedSkiffWriter::TUncheckedSkiffWriter(IZeroCopyOutput* underlying)
    : UnderlyingOutputWriter_(underlying)
    , CurrentOutputWriter_(&UnderlyingOutputWriter_)
{ }

TUncheckedSkiffWriter::TUncheckedSkiffWriter(IOutputStream* underlying)
    : BufferedOutput_(MakeHolder<TBufferedOutput>(underlying))
    , UnderlyingOutputWriter_(BufferedOutput_.Get())
    , CurrentOutputWriter_(&UnderlyingOutputWriter_)
{ }

TUncheckedSkiffWriter::TUncheckedSkiffWriter(const std::shared_ptr<TSkiffSchema>& /*schema*/, IZeroCopyOutput* underlying)
    : TUncheckedSkiffWriter(underlying)
{ }

TUncheckedSkiffWriter::TUncheckedSkiffWriter(const std::shared_ptr<TSkiffSchema>& /*schema*/, IOutputStream* underlying)
    : TUncheckedSkiffWriter(underlying)
{ }

TUncheckedSkiffWriter::~TUncheckedSkiffWriter()
{
    try {
        Flush();
    } catch (...) {
    }
}

void TUncheckedSkiffWriter::WriteInt8(i8 value)
{
    WriteSimple<i8>(value);
}

void TUncheckedSkiffWriter::WriteInt16(i16 value)
{
    WriteSimple<i16>(value);
}

void TUncheckedSkiffWriter::WriteInt32(i32 value)
{
    WriteSimple<i32>(value);
}

void TUncheckedSkiffWriter::WriteInt64(i64 value)
{
    WriteSimple<i64>(value);
}

void TUncheckedSkiffWriter::WriteInt128(TInt128 value)
{
    WriteSimple<ui64>(value.Low);
    WriteSimple<i64>(value.High);
}

void TUncheckedSkiffWriter::WriteUint128(TUint128 value)
{
    WriteSimple<ui64>(value.Low);
    WriteSimple<ui64>(value.High);
}

void TUncheckedSkiffWriter::WriteInt256(const TInt256& value)
{
    for (auto part : value.Parts) {
        WriteSimple<ui64>(part);
    }
}

void TUncheckedSkiffWriter::WriteUint256(const TUint256& value)
{
    for (auto part : value.Parts) {
        WriteSimple<ui64>(part);
    }
}

void TUncheckedSkiffWriter::WriteVarInt32(i32 value)
{
    std::array<char, NYT::MaxVarInt32Size> buffer;
    auto size = NYT::WriteVarInt32(buffer.data(), value);
    CurrentOutputWriter_->Write(buffer.data(), size);
}

void TUncheckedSkiffWriter::WriteVarInt64(i64 value)
{
    std::array<char, NYT::MaxVarInt64Size> buffer;
    auto size = NYT::WriteVarInt64(buffer.data(), value);
    CurrentOutputWriter_->Write(buffer.data(), size);
}

void TUncheckedSkiffWriter::WriteUint8(ui8 value)
{
    WriteSimple<ui8>(value);
}

void TUncheckedSkiffWriter::WriteUint16(ui16 value)
{
    WriteSimple<ui16>(value);
}

void TUncheckedSkiffWriter::WriteUint32(ui32 value)
{
    WriteSimple<ui32>(value);
}

void TUncheckedSkiffWriter::WriteUint64(ui64 value)
{
    WriteSimple<ui64>(value);
}

void TUncheckedSkiffWriter::WriteFloat(float value)
{
    return WriteSimple<float>(value);
}

void TUncheckedSkiffWriter::WriteDouble(double value)
{
    return WriteSimple<double>(value);
}

void TUncheckedSkiffWriter::WriteBoolean(bool value)
{
    return WriteSimple<ui8>(value ? 1 : 0);
}

void TUncheckedSkiffWriter::WriteString32(TStringBuf value)
{
    WriteSimple<ui32>(value.size());
    CurrentOutputWriter_->Write(value.data(), value.size());
}

void TUncheckedSkiffWriter::WriteStringVar(TStringBuf value)
{
    WriteVarInt64(std::ssize(value));
    CurrentOutputWriter_->Write(value.data(), value.size());
}

void TUncheckedSkiffWriter::WriteYson32(TStringBuf value)
{
    WriteSimple<ui32>(value.size());
    CurrentOutputWriter_->Write(value.data(), value.size());
}

void TUncheckedSkiffWriter::WriteStringFixed(TStringBuf value)
{
    CurrentOutputWriter_->Write(value.data(), value.size());
}

void TUncheckedSkiffWriter::WriteVariant8Tag(ui8 tag)
{
    WriteSimple<ui8>(tag);
}

void TUncheckedSkiffWriter::WriteVariant16Tag(ui16 tag)
{
    WriteSimple<ui16>(tag);
}

void TUncheckedSkiffWriter::WriteVariantVarTag(i32 tag)
{
    WriteVarInt32(tag);
}

void TUncheckedSkiffWriter::WriteBlockVarHeader(const TBlockVarHeader& blockHeader)
{
    if (blockHeader.ByteSize) {
        if (blockHeader.Count == std::numeric_limits<i64>::min()) {
            ythrow TSkiffException() << "Invalid BlockVarHeader: count value INT64_MIN is not allowed";
        }
        WriteVarInt64(-blockHeader.Count);
        WriteVarInt64(*blockHeader.ByteSize);
    } else {
        WriteVarInt64(blockHeader.Count);
    }
}

void TUncheckedSkiffWriter::StartBlob()
{
    if (BlobOutputWriter_) {
        throw TSkiffException() << "Blob start called before previous blob was finished";
    }
    BlobOutput_.emplace(Blob_);
    BlobOutputWriter_.emplace(&*BlobOutput_);

    CurrentOutputWriter_ = &*BlobOutputWriter_;
}

void TUncheckedSkiffWriter::FinishBlob()
{
    if (!BlobOutput_) {
        throw TSkiffException() << "Blob finish called before blob was started";
    }

    BlobOutputWriter_->UndoRemaining();

    BlobOutput_.reset();
    BlobOutputWriter_.reset();

    CurrentOutputWriter_ = &UnderlyingOutputWriter_;

    WriteString32(Blob_);
    Blob_.clear();
}

void TUncheckedSkiffWriter::Flush()
{
    UnderlyingOutputWriter_.UndoRemaining();
    if (BufferedOutput_) {
        BufferedOutput_->Flush();
    }
}

template <typename T>
Y_FORCE_INLINE void TUncheckedSkiffWriter::WriteSimple(T value)
{
    if constexpr (std::is_integral_v<T>) {
        value = HostToLittle(value);
        CurrentOutputWriter_->Write(&value, sizeof(T));
    } else {
        CurrentOutputWriter_->Write(&value, sizeof(T));
    }
}

void TUncheckedSkiffWriter::Finish()
{
    Flush();
}

////////////////////////////////////////////////////////////////////////////////

TCheckedSkiffWriter::TCheckedSkiffWriter(const std::shared_ptr<TSkiffSchema>& schema, IZeroCopyOutput* underlying)
    : Writer_(underlying)
    , Validator_(std::make_unique<TSkiffValidator>(schema))
{ }

TCheckedSkiffWriter::TCheckedSkiffWriter(const std::shared_ptr<TSkiffSchema>& schema, IOutputStream* underlying)
    : Writer_(underlying)
    , Validator_(std::make_unique<TSkiffValidator>(schema))
{ }

TCheckedSkiffWriter::~TCheckedSkiffWriter() = default;

void TCheckedSkiffWriter::WriteDouble(double value)
{
    Validator_->OnSimpleType(EWireType::Double);
    Writer_.WriteDouble(value);
}

void TCheckedSkiffWriter::WriteFloat(float value)
{
    Validator_->OnSimpleType(EWireType::Float);
    Writer_.WriteFloat(value);
}

void TCheckedSkiffWriter::WriteBoolean(bool value)
{
    Validator_->OnSimpleType(EWireType::Boolean);
    Writer_.WriteBoolean(value);
}

void TCheckedSkiffWriter::WriteInt8(i8 value)
{
    Validator_->OnSimpleType(EWireType::Int8);
    Writer_.WriteInt8(value);
}

void TCheckedSkiffWriter::WriteInt16(i16 value)
{
    Validator_->OnSimpleType(EWireType::Int16);
    Writer_.WriteInt16(value);
}

void TCheckedSkiffWriter::WriteInt32(i32 value)
{
    Validator_->OnSimpleType(EWireType::Int32);
    Writer_.WriteInt32(value);
}

void TCheckedSkiffWriter::WriteInt64(i64 value)
{
    Validator_->OnSimpleType(EWireType::Int64);
    Writer_.WriteInt64(value);
}

void TCheckedSkiffWriter::WriteUint8(ui8 value)
{
    Validator_->OnSimpleType(EWireType::Uint8);
    Writer_.WriteUint8(value);
}

void TCheckedSkiffWriter::WriteUint16(ui16 value)
{
    Validator_->OnSimpleType(EWireType::Uint16);
    Writer_.WriteUint16(value);
}

void TCheckedSkiffWriter::WriteUint32(ui32 value)
{
    Validator_->OnSimpleType(EWireType::Uint32);
    Writer_.WriteUint32(value);
}

void TCheckedSkiffWriter::WriteUint64(ui64 value)
{
    Validator_->OnSimpleType(EWireType::Uint64);
    Writer_.WriteUint64(value);
}

void TCheckedSkiffWriter::WriteInt128(TInt128 value)
{
    Validator_->OnSimpleType(EWireType::Int128);
    Writer_.WriteInt128(value);
}

void TCheckedSkiffWriter::WriteUint128(TUint128 value)
{
    Validator_->OnSimpleType(EWireType::Uint128);
    Writer_.WriteUint128(value);
}

void TCheckedSkiffWriter::WriteInt256(const TInt256& value)
{
    Validator_->OnSimpleType(EWireType::Int256);
    Writer_.WriteInt256(value);
}

void TCheckedSkiffWriter::WriteUint256(const TUint256& value)
{
    Validator_->OnSimpleType(EWireType::Uint256);
    Writer_.WriteUint256(value);
}

void TCheckedSkiffWriter::WriteVarInt32(i32 value)
{
    Validator_->OnSimpleType(EWireType::VarInt32);
    Writer_.WriteVarInt32(value);
}

void TCheckedSkiffWriter::WriteVarInt64(i64 value)
{
    Validator_->OnSimpleType(EWireType::VarInt64);
    Writer_.WriteVarInt64(value);
}

void TCheckedSkiffWriter::WriteString32(TStringBuf value)
{
    Validator_->OnSimpleType(EWireType::String32);
    ValidateStringLength(EWireType::String32, std::ssize(value));
    Writer_.WriteString32(value);
}

void TCheckedSkiffWriter::WriteStringVar(TStringBuf value)
{
    Validator_->OnSimpleType(EWireType::StringVar);
    ValidateStringLength(EWireType::StringVar, std::ssize(value));
    Writer_.WriteStringVar(value);
}

void TCheckedSkiffWriter::WriteYson32(TStringBuf value)
{
    Validator_->OnSimpleType(EWireType::Yson32);
    ValidateStringLength(EWireType::Yson32, std::ssize(value));
    Writer_.WriteYson32(value);
}

void TCheckedSkiffWriter::WriteStringFixed(TStringBuf value)
{
    Validator_->OnStringFixed(std::ssize(value));
    Writer_.WriteStringFixed(value);
}

void TCheckedSkiffWriter::WriteVariant8Tag(ui8 tag)
{
    Validator_->OnVariant8Tag(tag);
    Writer_.WriteVariant8Tag(tag);
}

void TCheckedSkiffWriter::WriteVariant16Tag(ui16 tag)
{
    Validator_->OnVariant16Tag(tag);
    Writer_.WriteVariant16Tag(tag);
}

void TCheckedSkiffWriter::WriteVariantVarTag(i32 tag)
{
    Validator_->OnVariantVarTag(tag);
    Writer_.WriteVariantVarTag(tag);
}

void TCheckedSkiffWriter::WriteBlockVarHeader(const TBlockVarHeader& blockHeader)
{
    Validator_->OnBlockVarHeader(blockHeader);
    Writer_.WriteBlockVarHeader(blockHeader);
}

void TCheckedSkiffWriter::StartBlob()
{
    Validator_->OnSimpleType(EWireType::Int32);
    Writer_.StartBlob();
}

void TCheckedSkiffWriter::FinishBlob()
{
    Writer_.FinishBlob();
}

void TCheckedSkiffWriter::Flush()
{
    Writer_.Flush();
}

void TCheckedSkiffWriter::Finish()
{
    Validator_->ValidateFinished();
    Writer_.Finish();
}

////////////////////////////////////////////////////////////////////

} // namespace NSkiff
