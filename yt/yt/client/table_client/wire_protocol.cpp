#include "wire_protocol.h"

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/unversioned_writer.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/row_batch.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/bitmap.h>
#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/compression/codec.h>

#include <library/cpp/yt/misc/variant.h>

#include <library/cpp/yt/memory/chunked_output_stream.h>
#include <library/cpp/yt/memory/chunked_memory_pool.h>

#include <util/system/sanitizers.h>

#include <google/protobuf/io/coded_stream.h>

namespace NYT::NTableClient {

using NYT::ToProto;
using NYT::FromProto;

using NChunkClient::NProto::TDataStatistics;
using NCrypto::TMD5Hash;

////////////////////////////////////////////////////////////////////////////////

const static NLogging::TLogger Logger("WireProtocol");

struct TWireProtocolWriterTag
{ };

struct TWireProtocolReaderTag
{ };

static constexpr size_t ReaderBufferChunkSize = 4096;

static constexpr size_t WriterInitialBufferCapacity = 1024;
static constexpr size_t PreallocateBlockSize = 4096;

static constexpr ui64 MinusOne = static_cast<ui64>(-1);

static_assert(sizeof(i64) == SerializationAlignment, "Wrong serialization alignment");
static_assert(sizeof(double) == SerializationAlignment, "Wrong serialization alignment");
static_assert(sizeof(TUnversionedValue) == 16, "sizeof(TUnversionedValue) != 16");
static_assert(sizeof(TUnversionedValueData) == 8, "sizeof(TUnversionedValueData) == 8");
static_assert(sizeof(TUnversionedRowHeader) == 8, "sizeof(TUnversionedRowHeader) != 8");
static_assert(sizeof(TVersionedValue) == 24, "sizeof(TVersionedValue) != 24");
static_assert(sizeof(TVersionedRowHeader) == 16, "sizeof(TVersionedRowHeader) != 16");

////////////////////////////////////////////////////////////////////////////////

EWireProtocolCommand GetWireProtocolCommand(const TWireProtocolWriteCommand& command)
{
    EWireProtocolCommand result = EWireProtocolCommand::LookupRows;
    Visit(command,
        [&] (const TWriteRowCommand&) { result = EWireProtocolCommand::WriteRow; },
        [&] (const TDeleteRowCommand&) { result = EWireProtocolCommand::DeleteRow; },
        [&] (const TVersionedWriteRowCommand&) { result = EWireProtocolCommand::VersionedWriteRow; },
        [&] (const TWriteAndLockRowCommand&) { result = EWireProtocolCommand::WriteAndLockRow; },
        [&] (auto) { YT_ABORT(); });

    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TWireProtocolWriter
    : public IWireProtocolWriter
{
public:
    TWireProtocolWriter()
    {
        EnsureCapacity(WriterInitialBufferCapacity);
    }

    size_t GetByteSize() const override
    {
        return Stream_.GetSize() + (Current_ - BeginPreallocated_);
    }

    void WriteCommand(EWireProtocolCommand command) override
    {
        WriteUint64(static_cast<unsigned int>(command));
    }

    void WriteLegacyLockBitmap(TLegacyLockBitmap lockBitmap) override
    {
        WriteUint64(lockBitmap);
    }

    void WriteLockMask(TLockMask lockMask) override
    {
        auto size = lockMask.GetSize();
        YT_VERIFY(size <= TLockMask::MaxSize);
        auto wordCount = DivCeil(size, TLockMask::LocksPerWord);

        WriteUint16(size);

        auto bitmap = lockMask.GetBitmap();
        YT_VERIFY(std::ssize(bitmap) >= wordCount);
        for (int index = 0; index < wordCount; ++index) {
            WriteUint64(bitmap[index]);
        }
    }

    void WriteTableSchema(const TTableSchema& schema) override
    {
        WriteMessage(ToProto<NTableClient::NProto::TTableSchemaExt>(schema));
    }

    void WriteMessage(const ::google::protobuf::MessageLite& message) override
    {
        size_t size = static_cast<size_t>(message.ByteSizeLong());
        WriteUint64(size);
        EnsureAlignedUpCapacity(size);
        YT_VERIFY(message.SerializePartialToArray(Current_, size));
        memset(Current_ + size, 0, AlignUpSpace(size, SerializationAlignment));

        NSan::CheckMemIsInitialized(Current_, AlignUp<size_t>(size, SerializationAlignment));
        Current_ += AlignUp<size_t>(size, SerializationAlignment);
    }

    void WriteInt64(i64 value) override
    {
        WriteUint64(static_cast<ui64>(value));
    }

    size_t WriteSchemafulRow(
        TUnversionedRow row,
        const TNameTableToSchemaIdMapping* idMapping) override
    {
        size_t bytes = EstimateSchemafulRowByteSize(row);
        EnsureCapacity(bytes);

        if (!row) {
            UnsafeWriteUint64(MinusOne);
            return bytes;
        }

        UnsafeWriteUint64(row.GetCount());
        UnsafeWriteSchemafulValueRange(row.Elements(), idMapping);
        return bytes;
    }

    size_t WriteUnversionedRow(
        TUnversionedRow row,
        const TNameTableToSchemaIdMapping* idMapping) override
    {
        size_t bytes = EstimateUnversionedRowByteSize(row);
        EnsureCapacity(bytes);

        if (!row) {
            UnsafeWriteUint64(MinusOne);
            return bytes;
        }

        UnsafeWriteUint64(row.GetCount());
        UnsafeWriteUnversionedValueRange(row.Elements(), idMapping);
        return bytes;
    }

    size_t WriteVersionedRow(TVersionedRow row) override
    {
        size_t bytes = EstimateVersionedRowByteSize(row);
        EnsureCapacity(bytes);

        if (!row) {
            UnsafeWriteUint64(MinusOne);
            return bytes;
        }

        UnsafeWriteRaw(row.GetHeader(), sizeof(TVersionedRowHeader));
        UnsafeWriteRaw(row.BeginWriteTimestamps(), sizeof(TTimestamp) * row.GetWriteTimestampCount());
        UnsafeWriteRaw(row.BeginDeleteTimestamps(), sizeof(TTimestamp) * row.GetDeleteTimestampCount());

        UnsafeWriteSchemafulValueRange(row.Keys(), nullptr);
        UnsafeWriteVersionedValueRange(row.Values());
        return bytes;
    }

    void WriteUnversionedValueRange(
        TUnversionedValueRange valueRange,
        const TNameTableToSchemaIdMapping* idMapping) override
    {
        size_t bytes = AlignUp<size_t>(8, SerializationAlignment); // -1 or value count
        bytes += EstimateUnversionedValueRangeByteSize(valueRange);
        EnsureCapacity(bytes);

        UnsafeWriteUint64(valueRange.Size());
        UnsafeWriteUnversionedValueRange(valueRange, idMapping);
    }

    void WriteUnversionedRowset(
        TRange<TUnversionedRow> rowset,
        const TNameTableToSchemaIdMapping* idMapping) override
    {
        WriteRowCount(rowset.Size());
        for (auto row : rowset) {
            WriteUnversionedRow(row, idMapping);
        }
    }

    void WriteSerializedRowset(
        size_t rowCount,
        const std::vector<TSharedRef>& serializedRowset) override
    {
        WriteRowCount(rowCount);

        for (const auto& item : serializedRowset) {
            EnsureCapacity(item.Size());
            UnsafeWriteRaw(item.Data(), item.Size());
        }
    }

    void WriteSchemafulRowset(
        TRange<TUnversionedRow> rowset,
        const TNameTableToSchemaIdMapping* idMapping) override
    {
        WriteRowCount(rowset.Size());
        for (auto row : rowset) {
            WriteSchemafulRow(row, idMapping);
        }
    }

    void WriteVersionedRowset(TRange<TVersionedRow> rowset) override
    {
        WriteRowCount(rowset.Size());
        for (auto row : rowset) {
            WriteVersionedRow(row);
        }
    }

    std::vector<TSharedRef> Finish() override
    {
        FlushPreallocated();
        return Stream_.Finish();
    }

private:
    TChunkedOutputStream Stream_{GetRefCountedTypeCookie<TWireProtocolWriterTag>()};

    char EmptyBuf_[0];
    char* BeginPreallocated_ = EmptyBuf_;
    char* EndPreallocated_ = EmptyBuf_;
    char* Current_ = EmptyBuf_;

    std::vector<TUnversionedValue> PooledValues_;

    void FlushPreallocated()
    {
        if (!Current_) {
            return;
        }

        YT_VERIFY(Current_ <= EndPreallocated_);
        Stream_.Advance(Current_ - BeginPreallocated_);
        BeginPreallocated_ = EndPreallocated_ = Current_ = EmptyBuf_;
    }

    void EnsureCapacity(size_t more)
    {
        if (Y_LIKELY(Current_ + more < EndPreallocated_)) {
            return;
        }

        FlushPreallocated();

        size_t size = std::max(PreallocateBlockSize, more);
        Current_ = BeginPreallocated_ = Stream_.Preallocate(size);
        EndPreallocated_ = BeginPreallocated_ + size;
    }

    void EnsureAlignedUpCapacity(size_t more)
    {
        EnsureCapacity(AlignUp<size_t>(more, SerializationAlignment));
    }

    void UnsafeWriteRaw(const void* buffer, size_t size)
    {
        if (!buffer) {
            YT_VERIFY(size == 0);
            return;
        }

        NSan::CheckMemIsInitialized(buffer, size);

        memcpy(Current_, buffer, size);
        memset(Current_ + size, 0, AlignUp<size_t>(size, SerializationAlignment) - size);

        NSan::CheckMemIsInitialized(Current_, AlignUp<size_t>(size, SerializationAlignment));
        Current_ += AlignUp<size_t>(size, SerializationAlignment);
        YT_ASSERT(Current_ <= EndPreallocated_);
    }

    template <class T>
    void UnsafeWritePod(const T& value)
    {
        NSan::CheckMemIsInitialized(&value, sizeof(T));

        static_assert(!std::is_reference<T>::value, "T must not be a reference");
        static_assert(!std::is_pointer<T>::value, "T must not be a pointer");
        // Do not use #UnsafeWriteRaw here to allow compiler to optimize memcpy & AlignUp.
        // Both of them are constexprs.
        memcpy(Current_, &value, sizeof(T));
        memset(Current_ + sizeof(T), 0, AlignUpSpace(sizeof(T), SerializationAlignment));

        NSan::CheckMemIsInitialized(Current_, AlignUp<size_t>(sizeof(T), SerializationAlignment));
        Current_ += AlignUp<size_t>(sizeof(T), SerializationAlignment);
        YT_ASSERT(Current_ <= EndPreallocated_);
    }

    void WriteUint16(ui16 value)
    {
        EnsureCapacity(AlignUp<size_t>(sizeof(ui16), SerializationAlignment));
        UnsafeWritePod(value);
    }

    void WriteUint64(ui64 value)
    {
        EnsureCapacity(AlignUp<size_t>(sizeof(ui64), SerializationAlignment));
        UnsafeWritePod(value);
    }

    void UnsafeWriteUint64(ui64 value)
    {
        UnsafeWritePod(value);
    }

    void WriteRowCount(size_t rowCount)
    {
        ValidateRowCount(rowCount);
        WriteUint64(rowCount);
    }

    void UnsafeWriteSchemafulValue(const TUnversionedValue& value)
    {
        // Write data in-place.
        if (IsStringLikeType(value.Type)) {
            UnsafeWritePod<ui64>(value.Length);
            UnsafeWriteRaw(value.Data.String, value.Length);
        } else if (IsValueType(value.Type)) {
            UnsafeWritePod(value.Data);
        }
    }

    void UnsafeWriteUnversionedValue(const TUnversionedValue& value)
    {
        // Write header (id, type, flags, length).
        UnsafeWritePod(*reinterpret_cast<const ui64*>(&value));
        // Write data in-place.
        if (IsStringLikeType(value.Type)) {
            UnsafeWriteRaw(value.Data.String, value.Length);
        } else if (IsValueType(value.Type)) {
            UnsafeWritePod(value.Data);
        }
    }

    void UnsafeWriteVersionedValue(const TVersionedValue& value)
    {
        // Write header (id, type, flags, length).
        const ui64* rawValue = reinterpret_cast<const ui64*>(&value);
        UnsafeWritePod<ui64>(rawValue[0]);
        // Write data in-place.
        if (IsStringLikeType(value.Type)) {
            UnsafeWriteRaw(value.Data.String, value.Length);
        } else if (IsValueType(value.Type)) {
            UnsafeWritePod(value.Data);
        }
        // Write timestamp.
        UnsafeWritePod<ui64>(value.Timestamp);
    }

    TUnversionedValueRange RemapValues(
        TUnversionedValueRange values,
        const TNameTableToSchemaIdMapping* idMapping)
    {
        auto valueCount = values.Size();
        PooledValues_.resize(valueCount);
        for (size_t index = 0; index < valueCount; ++index){
            const auto& srcValue = values[index];
            auto& dstValue = PooledValues_[index];
            dstValue = srcValue;
            dstValue.Id = static_cast<ui16>((*idMapping)[srcValue.Id]);
        }

        std::sort(
            PooledValues_.begin(),
            PooledValues_.end(),
            [] (const TUnversionedValue& lhs, const TUnversionedValue& rhs) -> bool {
                return lhs.Id < rhs.Id;
            });

        return TRange(PooledValues_);
    }

    void UnsafeWriteNullBitmap(TUnversionedValueRange values)
    {
        // TODO(lukyan): Allocate space and write directly.
        auto nullBitmap = TBitmapOutput(values.Size());
        for (int index = 0; index < std::ssize(values); ++index) {
            nullBitmap.Append(values[index].Type == EValueType::Null);
        }
        UnsafeWriteRaw(nullBitmap.GetData(), nullBitmap.GetByteSize());
    }

    void UnsafeWriteSchemafulValueRange(
        TUnversionedValueRange values,
        const TNameTableToSchemaIdMapping* idMapping)
    {
        if (idMapping) {
            values = RemapValues(values, idMapping);
        }
        UnsafeWriteNullBitmap(values);
        for (const auto& value : values) {
            UnsafeWriteSchemafulValue(value);
        }
    }

    void UnsafeWriteUnversionedValueRange(
        TUnversionedValueRange values,
        const TNameTableToSchemaIdMapping* idMapping)
    {
        if (idMapping) {
            values = RemapValues(values, idMapping);
        }
        for (const auto& value : values) {
            UnsafeWriteUnversionedValue(value);
        }
    }

    void UnsafeWriteVersionedValueRange(
        TRange<TVersionedValue> values)
    {
        for (const auto& value : values) {
            UnsafeWriteVersionedValue(value);
        }
    }

    size_t EstimateSchemafulValueRangeByteSize(TUnversionedValueRange values)
    {
        size_t bytes = 0;
        bytes += AlignUp<size_t>(NBitmapDetail::GetByteSize(values.Size()), SerializationAlignment); // null bitmap
        for (const auto& value : values) {
            if (IsStringLikeType(value.Type)) {
                bytes += AlignUp<size_t>(8 + value.Length, SerializationAlignment);
            } else if (value.Type != EValueType::Null) {
                bytes += AlignUp<size_t>(8, SerializationAlignment);
            }
        }
        return bytes;
    }

    size_t EstimateUnversionedValueRangeByteSize(TUnversionedValueRange values)
    {
        size_t bytes = 0;
        for (const auto& value : values) {
            bytes += AlignUp<size_t>(8, SerializationAlignment);
            if (IsStringLikeType(value.Type)) {
                bytes += AlignUp<size_t>(value.Length, SerializationAlignment);
            } else if (value.Type != EValueType::Null) {
                bytes += AlignUp<size_t>(8, SerializationAlignment);
            }
        }
        return bytes;
    }

    size_t EstimateVersionedValueRangeByteSize(TRange<TVersionedValue> values)
    {
        size_t bytes = 0;
        for (const auto& value : values) {
            bytes += AlignUp<size_t>(16, SerializationAlignment);
            if (IsStringLikeType(value.Type)) {
                bytes += AlignUp<size_t>(value.Length, SerializationAlignment);
            } else if (value.Type != EValueType::Null) {
                bytes += AlignUp<size_t>(8, SerializationAlignment);
            }
        }
        return bytes;
    }

    size_t EstimateSchemafulRowByteSize(TUnversionedRow row)
    {
        size_t bytes = AlignUp<size_t>(8, SerializationAlignment); // -1 or value count
        if (row) {
            bytes += EstimateSchemafulValueRangeByteSize(row.Elements());
        }
        return bytes;
    }

    size_t EstimateUnversionedRowByteSize(TUnversionedRow row)
    {
        size_t bytes = AlignUp<size_t>(8, SerializationAlignment); // -1 or value count
        if (row) {
            bytes += EstimateUnversionedValueRangeByteSize(row.Elements());
        }
        return bytes;
    }

    size_t EstimateVersionedRowByteSize(TVersionedRow row)
    {
        size_t bytes = AlignUp<size_t>(8, SerializationAlignment); // -1 or value count
        if (row) {
            bytes += AlignUp<size_t>(8, SerializationAlignment); // -1 or value count
            bytes += AlignUp<size_t>(sizeof(TTimestamp) * (
                row.GetWriteTimestampCount() +
                row.GetDeleteTimestampCount()), // timestamps
                SerializationAlignment);
            bytes += EstimateSchemafulValueRangeByteSize(row.Keys());
            bytes += EstimateVersionedValueRangeByteSize(row.Values());
        }
        return bytes;
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IWireProtocolWriter> CreateWireProtocolWriter()
{
    return std::make_unique<TWireProtocolWriter>();
}

////////////////////////////////////////////////////////////////////////////////

class TWireProtocolReader
    : public IWireProtocolReader
{
public:
    explicit TWireProtocolReader(TSharedRef data, TRowBufferPtr rowBuffer)
        : RowBuffer_(rowBuffer ? std::move(rowBuffer) : New<TRowBuffer>(TWireProtocolReaderTag(), ReaderBufferChunkSize))
        , Data_(std::move(data))
        , Current_(Data_.Begin())
    { }

    const TRowBufferPtr& GetRowBuffer() const override
    {
        return RowBuffer_;
    }

    bool IsFinished() const override
    {
        return Current_ == Data_.End();
    }

    TIterator GetBegin() const override
    {
        return Data_.Begin();
    }

    TIterator GetEnd() const override
    {
        return Data_.End();
    }

    TIterator GetCurrent() const override
    {
        return Current_;
    }

    void SetCurrent(TIterator current) override
    {
        Current_ = current;
    }

    TSharedRef Slice(TIterator begin, TIterator end) override
    {
        return Data_.Slice(begin, end);
    }

    EWireProtocolCommand ReadCommand() override
    {
        return EWireProtocolCommand(ReadUint64());
    }

    TLegacyLockBitmap ReadLegacyLockBitmap() override
    {
        return ReadUint64();
    }

    TLockMask ReadLockMask() override
    {
        auto size = ReadUint16();
        auto wordCount = DivCeil<int>(size, TLockMask::LocksPerWord);

        TLockBitmap bitmap;
        bitmap.reserve(wordCount);
        for (int index = 0; index < wordCount; ++index) {
            bitmap.push_back(ReadUint64());
        }

        return TLockMask(bitmap, size);
    }

    TTableSchema ReadTableSchema() override
    {
        NTableClient::NProto::TTableSchemaExt protoSchema;
        ReadMessage(&protoSchema);
        return FromProto<TTableSchema>(protoSchema);
    }

    void ReadMessage(::google::protobuf::MessageLite* message) override
    {
        size_t size = ReadUint64();
        ::google::protobuf::io::CodedInputStream chunkStream(
            reinterpret_cast<const ui8*>(Current_),
            static_cast<int>(size));
        Y_PROTOBUF_SUPPRESS_NODISCARD message->ParsePartialFromCodedStream(&chunkStream);
        Current_ += AlignUp<size_t>(size, SerializationAlignment);
    }

    i64 ReadInt64() override
    {
        return static_cast<i64>(ReadUint64());
    }

    TUnversionedRow ReadSchemafulRow(const TSchemaData& schemaData, bool captureValues) override
    {
        auto valueCount = ReadUint64();
        if (valueCount == MinusOne) {
            return TUnversionedRow();
        }
        ValidateRowValueCount(valueCount);
        auto row = RowBuffer_->AllocateUnversioned(valueCount);
        DoReadSchemafulValueRange(schemaData, captureValues, row.Begin(), valueCount);
        return row;
    }

    TUnversionedRow ReadUnversionedRow(bool captureValues, const TIdMapping* idMapping) override
    {
        auto valueCount = ReadUint64();
        if (valueCount == MinusOne) {
            return TUnversionedRow();
        }
        ValidateRowValueCount(valueCount);
        auto row = RowBuffer_->AllocateUnversioned(valueCount);
        DoReadUnversionedValueRange(captureValues, row.Begin(), valueCount, idMapping);
        return row;
    }

    TVersionedRow ReadVersionedRow(const TSchemaData& schemaData, bool captureValues, const TIdMapping* valueIdMapping) override
    {
        union
        {
            ui64 Parts[2];
            TVersionedRowHeader Value;
        } header;

        header.Parts[0] = ReadUint64();
        if (header.Parts[0] == MinusOne) {
            return TVersionedRow();
        }
        header.Parts[1] = ReadUint64();

        ValidateKeyColumnCount(header.Value.KeyCount);
        ValidateVersionedRowTimestampCount(header.Value);

        auto row = TMutableVersionedRow::Allocate(
            RowBuffer_->GetPool(),
            header.Value.KeyCount,
            header.Value.ValueCount,
            header.Value.WriteTimestampCount,
            header.Value.DeleteTimestampCount);

        ReadRaw(row.BeginWriteTimestamps(), sizeof(TTimestamp) * row.GetWriteTimestampCount());
        ReadRaw(row.BeginDeleteTimestamps(), sizeof(TTimestamp) * row.GetDeleteTimestampCount());

        DoReadSchemafulValueRange(schemaData, captureValues, row.BeginKeys(), header.Value.KeyCount);
        DoReadVersionedValueRange(captureValues, row.BeginValues(), header.Value.ValueCount, valueIdMapping);

        ValidateVersionedRowDataWeight(row);

        return row;
    }

    TSharedRange<TUnversionedRow> ReadSchemafulRowset(const TSchemaData& schemaData, bool captureValues) override
    {
        int rowCount = DoReadRowCount();
        auto* rows = RowBuffer_->GetPool()->AllocateUninitialized<TUnversionedRow>(rowCount);
        for (int index = 0; index < rowCount; ++index) {
            rows[index] = ReadSchemafulRow(schemaData, captureValues);
        }

        auto range = TRange<TUnversionedRow>(rows, rows + rowCount);
        return captureValues ? MakeSharedRange(range, RowBuffer_) : MakeSharedRange(range, RowBuffer_, Data_);
    }

    TSharedRange<TUnversionedRow> ReadUnversionedRowset(bool captureValues, const TIdMapping* idMapping) override
    {
        int rowCount = DoReadRowCount();
        auto* rows = RowBuffer_->GetPool()->AllocateUninitialized<TUnversionedRow>(rowCount);
        for (int index = 0; index < rowCount; ++index) {
            rows[index] = ReadUnversionedRow(captureValues, idMapping);
        }
        auto range = TRange<TUnversionedRow>(rows, rows + rowCount);
        return captureValues ? MakeSharedRange(range, RowBuffer_) : MakeSharedRange(range, RowBuffer_, Data_);
    }

    TSharedRange<TVersionedRow> ReadVersionedRowset(
        const TSchemaData& schemaData,
        bool captureValues,
        const TIdMapping* valueIdMapping) override
    {
        int rowCount = DoReadRowCount();
        auto* rows = RowBuffer_->GetPool()->AllocateUninitialized<TVersionedRow>(rowCount);
        for (int index = 0; index < rowCount; ++index) {
            rows[index] = ReadVersionedRow(schemaData, captureValues, valueIdMapping);
        }
        auto range = TRange<TVersionedRow>(rows, rows + rowCount);
        return captureValues ? MakeSharedRange(range, RowBuffer_) : MakeSharedRange(range, RowBuffer_, Data_);
    }

    TWireProtocolWriteCommand ReadWriteCommand(
        const TSchemaData& schemaData,
        bool captureValues,
        bool versionedWriteIsUnversioned) override
    {
        auto command = ReadCommand();
        switch (command) {
            case EWireProtocolCommand::WriteRow: {
                auto row = ReadUnversionedRow(captureValues, /*idMapping*/ nullptr);
                return TWriteRowCommand{
                    .Row = row
                };
            }
            case EWireProtocolCommand::DeleteRow: {
                auto row = ReadUnversionedRow(captureValues, /*idMapping*/ nullptr);
                return TDeleteRowCommand{
                    .Row = row
                };
            }
            case EWireProtocolCommand::VersionedWriteRow: {
                if (versionedWriteIsUnversioned) {
                    auto unversionedRow = ReadUnversionedRow(captureValues, /*idMapping*/ nullptr);
                    return TVersionedWriteRowCommand{
                        .UnversionedRow = unversionedRow
                    };
                } else {
                    auto versionedRow = ReadVersionedRow(schemaData, captureValues, /*valueIdMapping*/ nullptr);
                    return TVersionedWriteRowCommand{
                        .VersionedRow = versionedRow
                    };
                }
            }
            // COMPAT(gritukan)
            case EWireProtocolCommand::ReadLockWriteRow: {
                TLockMask lockMask;
                TLegacyLockMask legacyLocks(ReadLegacyLockBitmap());
                for (int index = 0; index < TLegacyLockMask::MaxCount; ++index) {
                    lockMask.Set(index, legacyLocks.Get(index));
                }

                auto row = ReadUnversionedRow(captureValues, /*idMapping*/ nullptr);
                return TWriteAndLockRowCommand{
                    .Row = row,
                    .LockMask = std::move(lockMask)
                };
            }
            case EWireProtocolCommand::WriteAndLockRow: {
                auto row = ReadUnversionedRow(captureValues, /*idMapping*/ nullptr);
                auto lockMask = ReadLockMask();
                return TWriteAndLockRowCommand{
                    .Row = row,
                    .LockMask = std::move(lockMask)
                };
            }
            default:
                YT_LOG_FATAL("Unknown write command (Command: %v)",
                    command);
        }
    }

private:
    const TRowBufferPtr RowBuffer_;

    TSharedRef Data_;
    TIterator Current_;

    void ValidateSizeAvailable(size_t size)
    {
        if (Current_ + size > Data_.End()) {
            THROW_ERROR_EXCEPTION("Premature end of stream while reading %v bytes", size);
        }
    }

    void ReadRaw(void* buffer, size_t size)
    {
        ValidateSizeAvailable(size);

        memcpy(buffer, Current_, size);
        Current_ += size;
        Current_ += AlignUpSpace(size, SerializationAlignment);
    }

    const char* PeekRaw(size_t size)
    {
        ValidateSizeAvailable(size);

        auto result = Current_;
        Current_ += size;
        Current_ += AlignUpSpace(size, SerializationAlignment);
        return result;
    }

    template <class T>
    void ReadPod(T* value)
    {
        ValidateSizeAvailable(sizeof(T));

        memcpy(value, Current_, sizeof(T));
        Current_ += sizeof(T);
        Current_ += AlignUpSpace(sizeof(T), SerializationAlignment);
    }

    ui64 ReadUint64()
    {
        ui64 value;
        ReadPod(&value);
        return value;
    }

    ui32 ReadUint32()
    {
        ui64 result = ReadUint64();
        if (result > std::numeric_limits<ui32>::max()) {
            THROW_ERROR_EXCEPTION("Value is out of range to fit into uint32");
        }
        return static_cast<ui32>(result);
    }

    ui16 ReadUint16()
    {
        ui64 result = ReadUint64();
        if (result > std::numeric_limits<ui16>::max()) {
            THROW_ERROR_EXCEPTION("Value is out of range to fit into uint16");
        }
        return static_cast<ui16>(result);
    }

    i32 ReadInt32()
    {
        i64 result = ReadInt64();
        if (result < std::numeric_limits<i32>::min() || result > std::numeric_limits<i32>::max()) {
            THROW_ERROR_EXCEPTION("Value is out of range to fit into int32");
        }
        return static_cast<i32>(result);
    }

    void DoReadStringData(EValueType type, ui32 length, const char** result, bool captureValues)
    {
        ui32 limit = 0;
        if (type == EValueType::String) {
            limit = MaxStringValueLength;
        }
        if (type == EValueType::Any) {
            limit = MaxAnyValueLength;
        }
        if (type == EValueType::Composite) {
            limit = MaxCompositeValueLength;
        }
        if (length > limit) {
            THROW_ERROR_EXCEPTION("Value of type %Qlv is too long: length %v, limit %v",
                type,
                length,
                limit);
        }
        if (captureValues) {
            char* tmp = RowBuffer_->GetPool()->AllocateUnaligned(length);
            ReadRaw(tmp, length);
            *result = tmp;
        } else {
            *result = PeekRaw(length);
        }
    }

    int DoReadRowCount()
    {
        int rowCount = ReadInt32();
        ValidateRowCount(rowCount);
        return rowCount;
    }

    void DoReadSchemafulValue(
        ui32 schemaData,
        bool null,
        bool captureValues,
        TUnversionedValue* value)
    {
        ui64* rawValue = reinterpret_cast<ui64*>(value);
        rawValue[0] = schemaData;
        if (null) {
            value->Type = EValueType::Null;
            // NB: Don't leave the second half uninitialized.
            rawValue[1] = 0;
        } else if (IsStringLikeType(value->Type)) {
            value->Length = ReadUint32();
            DoReadStringData(value->Type, value->Length, &value->Data.String, captureValues);
        } else if (IsValueType(value->Type)) {
            rawValue[1] = ReadUint64();
        }
    }

    void DoReadUnversionedValue(bool captureValues, TUnversionedValue* value)
    {
        *value = TUnversionedValue{};
        ui64* rawValue = reinterpret_cast<ui64*>(value);
        rawValue[0] = ReadUint64();
        if (IsStringLikeType(value->Type)) {
            DoReadStringData(value->Type, value->Length, &value->Data.String, captureValues);
        } else if (IsValueType(value->Type)) {
            rawValue[1] = ReadUint64();
        }
    }

    void DoReadVersionedValue(bool captureValues, TVersionedValue* value)
    {
        *value = TVersionedValue{};
        ui64* rawValue = reinterpret_cast<ui64*>(value);
        rawValue[0] = ReadUint64();
        if (IsStringLikeType(value->Type)) {
            DoReadStringData(value->Type, value->Length, &value->Data.String, captureValues);
        } else if (IsValueType(value->Type)) {
            rawValue[1] = ReadUint64();
        }
        value->Timestamp = ReadUint64();
    }

    void DoReadSchemafulValueRange(
        const TSchemaData& schemaData,
        bool captureValues,
        TUnversionedValue* values,
        ui32 valueCount)
    {
        auto bitmapPtr = PeekRaw(NBitmapDetail::GetByteSize(valueCount));
        TBitmap nullBitmap(bitmapPtr);
        for (size_t index = 0; index < valueCount; ++index) {
            DoReadSchemafulValue(schemaData[index], nullBitmap[index], captureValues, &values[index]);
        }
    }

    void DoApplyIdMapping(ui16* id, int index, const TIdMapping* idMapping)
    {
        if (*id >= idMapping->size()) {
            THROW_ERROR_EXCEPTION("Value with index %v has id %v which is out of range [0, %v)",
                index,
                *id,
                idMapping->size());
        }
        int mappedId = (*idMapping)[*id];
        if (mappedId == -1) {
            THROW_ERROR_EXCEPTION("Id mapping for value with index %v contains unexpected value %Qv",
                index,
                -1);
        }
        *id = mappedId;
    }

    void DoReadUnversionedValueRange(
        bool captureValues,
        TUnversionedValue* values,
        ui32 valueCount,
        const TIdMapping* idMapping)
    {
        for (size_t index = 0; index < valueCount; ++index) {
            DoReadUnversionedValue(captureValues, &values[index]);
            if (idMapping) {
                DoApplyIdMapping(&values[index].Id, index, idMapping);
            }
        }
    }

    void DoReadVersionedValueRange(
        bool captureValues,
        TVersionedValue* values,
        ui32 valueCount,
        const TIdMapping* valueIdMapping)
    {
        for (size_t index = 0; index < valueCount; ++index) {
            DoReadVersionedValue(captureValues, &values[index]);
            if (valueIdMapping) {
                DoApplyIdMapping(&values[index].Id, index, valueIdMapping);
            }
        }
    }

    void ValidateVersionedRowTimestampCount(const TVersionedRowHeader& rowHeader)
    {
        if (rowHeader.WriteTimestampCount > MaxTimestampCountPerRow) {
            THROW_ERROR_EXCEPTION("Too many write timestamps in a versioned row");
        }
        if (rowHeader.DeleteTimestampCount > MaxTimestampCountPerRow) {
            THROW_ERROR_EXCEPTION("Too many delete timestamps in a versioned row");
        }
    }

    void ValidateVersionedRowDataWeight(TVersionedRow row)
    {
        auto dataWeight = GetDataWeight(row);
        if (dataWeight > MaxServerVersionedRowDataWeight) {
            THROW_ERROR_EXCEPTION("Versioned row data weight is too large: %v > %v",
                dataWeight,
                MaxServerVersionedRowDataWeight)
                << TErrorAttribute("key", ToOwningKey(row));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

auto IWireProtocolReader::GetSchemaData(
    const TTableSchema& schema,
    const TColumnFilter& filter) -> TSchemaData
{
    TSchemaData schemaData;
    auto addColumn = [&] (int id) {
        auto value = MakeUnversionedValueHeader(schema.Columns()[id].GetWireType(), id);
        schemaData.push_back(*reinterpret_cast<ui32*>(&value));
    };
    if (!filter.IsUniversal()) {
        schemaData.reserve(std::ssize(filter.GetIndexes()));
        for (int id : filter.GetIndexes()) {
            addColumn(id);
        }
    } else {
        schemaData.reserve(schema.GetColumnCount());
        for (int id = 0; id < schema.GetColumnCount(); ++id) {
            addColumn(id);
        }
    }
    return schemaData;
}

auto IWireProtocolReader::GetSchemaData(const TTableSchema& schema) -> TSchemaData
{
    TSchemaData schemaData;
    schemaData.reserve(schema.GetColumnCount());
    for (int id = 0; id < schema.GetColumnCount(); ++id) {
        auto value = MakeUnversionedValueHeader(schema.Columns()[id].GetWireType(), id);
        schemaData.push_back(*reinterpret_cast<ui32*>(&value));
    }
    return schemaData;
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IWireProtocolReader> CreateWireProtocolReader(TSharedRef data, TRowBufferPtr rowBuffer)
{
    return std::make_unique<TWireProtocolReader>(std::move(data), std::move(rowBuffer));
}

////////////////////////////////////////////////////////////////////////////////

class TWireProtocolRowsetReader
    : public IWireProtocolRowsetReader
{
public:
    TWireProtocolRowsetReader(
        const std::vector<TSharedRef>& compressedBlocks,
        NCompression::ECodec codecId,
        TTableSchemaPtr schema,
        bool schemaful,
        const NLogging::TLogger& logger)
        : CompressedBlocks_(compressedBlocks)
        , Codec_(NCompression::GetCodec(codecId))
        , Schema_(std::move(schema))
        , Schemaful_(schemaful)
        , Logger(logger.WithTag("ReaderId: %v", TGuid::Create()))
    {
        YT_LOG_DEBUG("Wire protocol rowset reader created (BlockCount: %v, TotalCompressedSize: %v)",
            CompressedBlocks_.size(),
            GetByteSize(CompressedBlocks_));
    }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& /*options*/) override
    {
        if (Finished_) {
            return nullptr;
        }

        if (BlockIndex_ >= std::ssize(CompressedBlocks_)) {
            Finished_ = true;
            YT_LOG_DEBUG("Wire protocol rowset reader finished");
            return nullptr;
        }

        const auto& compressedBlock = CompressedBlocks_[BlockIndex_];
        YT_LOG_DEBUG("Started decompressing rowset reader block (BlockIndex: %v, CompressedSize: %v)",
            BlockIndex_,
            compressedBlock.Size());
        auto uncompressedBlock = Codec_->Decompress(compressedBlock);
        YT_LOG_DEBUG("Finished decompressing rowset reader block (BlockIndex: %v, UncompressedSize: %v)",
            BlockIndex_,
            uncompressedBlock.Size());

        auto rowBuffer = New<TRowBuffer>(TWireProtocolReaderTag(), ReaderBufferChunkSize);
        WireReader_ = CreateWireProtocolReader(uncompressedBlock, std::move(rowBuffer));

        if (!SchemaChecked_) {
            auto actualSchema = WireReader_->ReadTableSchema();

            //
            // NB this comparison is compat for YT-10668
            // This could be replaced with simple `operator==', once all nodes and proxies are updated to have new schema
            // representation introduced in cec93e9435fc3bbecc02ee5b8fd9ffa0eafc1672
            //
            // Guess it will be surely the case after after 01.11.2019
            if (!IsEqualIgnoringRequiredness(*Schema_, actualSchema)) {
                THROW_ERROR_EXCEPTION("Schema mismatch while parsing wire protocol");
            }
            SchemaChecked_ = true;
        }

        auto schemaData = WireReader_->GetSchemaData(*Schema_, TColumnFilter());

        std::vector<TUnversionedRow> rows;
        while (!WireReader_->IsFinished()) {
            auto row = Schemaful_
                ? WireReader_->ReadSchemafulRow(schemaData, false)
                : WireReader_->ReadUnversionedRow(false);
            rows.push_back(row);
        }
        ++BlockIndex_;

        return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
    }

    TFuture<void> GetReadyEvent() const override
    {
        return VoidFuture;
    }

    TDataStatistics GetDataStatistics() const override
    {
        YT_ABORT();
    }

    NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        YT_ABORT();
    }

    bool IsFetchingCompleted() const override
    {
        return false;
    }

    std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override
    {
        return {};
    }

private:
    const std::vector<TSharedRef> CompressedBlocks_;
    NCompression::ICodec* const Codec_;
    const TTableSchemaPtr Schema_;
    bool Schemaful_;
    const NLogging::TLogger Logger;

    int BlockIndex_ = 0;
    std::unique_ptr<IWireProtocolReader> WireReader_;
    bool Finished_ = false;
    bool SchemaChecked_ = false;

};

IWireProtocolRowsetReaderPtr CreateWireProtocolRowsetReader(
    const std::vector<TSharedRef>& compressedBlocks,
    NCompression::ECodec codecId,
    TTableSchemaPtr schema,
    bool schemaful,
    const NLogging::TLogger& logger)
{
    return New<TWireProtocolRowsetReader>(
        compressedBlocks,
        codecId,
        std::move(schema),
        schemaful,
        logger);
}

////////////////////////////////////////////////////////////////////////////////

class TWireProtocolRowsetWriter
    : public IWireProtocolRowsetWriter
{
public:
    TWireProtocolRowsetWriter(
        NCompression::ECodec codecId,
        size_t desiredUncompressedBlockSize,
        TTableSchemaPtr schema,
        bool schemaful,
        const NLogging::TLogger& logger)
        : Codec_(NCompression::GetCodec(codecId))
        , DesiredUncompressedBlockSize_(desiredUncompressedBlockSize)
        , Schema_(std::move(schema))
        , Schemaful_(schemaful)
        , Logger(logger.WithTag("WriterId: %v", TGuid::Create()))
    {
        YT_LOG_DEBUG("Wire protocol rowset writer created (Codec: %v, DesiredUncompressedBlockSize: %v)",
            codecId,
            DesiredUncompressedBlockSize_);
    }

    TFuture<void> Close() override
    {
        if (!Closed_) {
            YT_LOG_DEBUG("Wire protocol rowset writer closed");
            FlushBlock();
            Closed_ = true;
        }
        return VoidFuture;
    }

    bool Write(TRange<TUnversionedRow> rows) override
    {
        YT_VERIFY(!Closed_);
        for (auto row : rows) {
            if (!WireWriter_) {
                WireWriter_ = CreateWireProtocolWriter();
                if (!SchemaWritten_) {
                    WireWriter_->WriteTableSchema(*Schema_);
                    SchemaWritten_ = true;
                }
            }
            if (Schemaful_) {
                WireWriter_->WriteSchemafulRow(row);
            } else {
                WireWriter_->WriteUnversionedRow(row);
            }
            if (WireWriter_->GetByteSize() >= DesiredUncompressedBlockSize_) {
                FlushBlock();
            }
        }
        return true;
    }

    TFuture<void> GetReadyEvent() override
    {
        return VoidFuture;
    }

    std::vector<TSharedRef> GetCompressedBlocks() override
    {
        YT_VERIFY(Closed_);
        return CompressedBlocks_;
    }

    std::optional<TMD5Hash> GetDigest() const override
    {
        return std::nullopt;
    }

private:
    NCompression::ICodec* const Codec_;
    const size_t DesiredUncompressedBlockSize_;
    const TTableSchemaPtr Schema_;
    const bool Schemaful_;
    const NLogging::TLogger Logger;

    std::vector<TSharedRef> CompressedBlocks_;
    std::unique_ptr<IWireProtocolWriter> WireWriter_;
    bool Closed_ = false;
    bool SchemaWritten_ = false;


    void FlushBlock()
    {
        if (!WireWriter_) {
            return;
        }

        auto uncompressedBlocks = WireWriter_->Finish();

        YT_LOG_DEBUG("Started compressing rowset writer block (BlockIndex: %v, UncompressedSize: %v)",
            CompressedBlocks_.size(),
            GetByteSize(uncompressedBlocks));
        auto compressedBlock = Codec_->Compress(uncompressedBlocks);
        YT_LOG_DEBUG("Finished compressing rowset writer block (BlockIndex: %v, CompressedSize: %v)",
            CompressedBlocks_.size(),
            compressedBlock.Size());

        CompressedBlocks_.push_back(compressedBlock);
        WireWriter_.reset();
    }
};

IWireProtocolRowsetWriterPtr CreateWireProtocolRowsetWriter(
    NCompression::ECodec codecId,
    size_t desiredUncompressedBlockSize,
    TTableSchemaPtr schema,
    bool schemaful,
    const NLogging::TLogger& logger)
{
    return New<TWireProtocolRowsetWriter>(
        codecId,
        desiredUncompressedBlockSize,
        std::move(schema),
        schemaful,
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
