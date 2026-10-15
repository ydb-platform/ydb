#include "read_stream.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/arrow/accessor.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/public/udf/arrow/util.h>
#include <yql/essentials/utils/yql_panic.h>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/dictionary.h>
#include <arrow/ipc/metadata_internal.h>
#include <arrow/util/bitmap_ops.h>

#include <util/generic/hash_set.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <mutex>
#include <cstring>
#include <optional>
#include <utility>

namespace NYql::NYdbExternal {
namespace {

constexpr ui64 MaxSchemaBytes = 64 * 1024;
constexpr ui64 MaxColumns = 1024;

std::shared_ptr<arrow::DataType> ArrowType(const Ydb::Type& type) {
    const auto& item = type.has_optional_type() ? type.optional_type().item() : type;
    YQL_ENSURE(item.has_type_id(), "YdbExternal only supports primitive and optional primitive columns");
    switch (item.type_id()) {
        case Ydb::Type::BOOL: return arrow::uint8();
        case Ydb::Type::INT8: return arrow::int8();
        case Ydb::Type::INT16: return arrow::int16();
        case Ydb::Type::INT32: return arrow::int32();
        case Ydb::Type::INT64: return arrow::int64();
        case Ydb::Type::UINT8: return arrow::uint8();
        case Ydb::Type::UINT16: return arrow::uint16();
        case Ydb::Type::UINT32: return arrow::uint32();
        case Ydb::Type::UINT64: return arrow::uint64();
        case Ydb::Type::FLOAT: return arrow::float32();
        case Ydb::Type::DOUBLE: return arrow::float64();
        case Ydb::Type::STRING: return arrow::binary();
        case Ydb::Type::UTF8: return arrow::utf8();
        default: ythrow yexception() << "YdbExternal column type is unsupported";
    }
}

bool IsBool(const Ydb::Type& type) {
    const auto& item = type.has_optional_type() ? type.optional_type().item() : type;
    return item.has_type_id() && item.type_id() == Ydb::Type::BOOL;
}

TString QuoteIdentifier(TStringBuf value) {
    YQL_ENSURE(!value.empty() && value.size() <= 4096, "YdbExternal invalid identifier length");
    TString result("`");
    for (unsigned char c : value) {
        YQL_ENSURE(c >= 32 && c != 127, "YdbExternal control character in identifier");
        if (c == '`' || c == '\\') {
            result += '\\';
        }
        result += c;
    }
    result += '`';
    return result;
}

template <class T>
T Checked(arrow::Result<T> result) {
    YQL_ENSURE(result.ok(), "YdbExternal invalid or unsupported Arrow payload");
    return std::move(result).ValueOrDie();
}

// TResultSet copies share the immutable SDK implementation. Keep it alive while
// Arrow views the IPC body; output compaction copies only rows actually delivered.
class TResultBuffer final : public arrow::Buffer {
public:
    TResultBuffer(const std::string& bytes, const NYdb::TResultSet& owner)
        : arrow::Buffer(reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size())
        , Owner_(owner) {}
private:
    NYdb::TResultSet Owner_;
};

class TResponseError : public yexception {};
class TResponseLimitError : public TResponseError {};

void CheckResponse(bool accepted, const char* category) {
    if (!accepted) {
        ythrow TResponseError() << "YdbExternal response validation failed: " << category;
    }
}

void CheckLimit(bool accepted, const char* category, ui64 limit) {
    if (!accepted) {
        ythrow TResponseLimitError() << "YdbExternal response limit exceeded: " << category
            << " (limit " << limit << " bytes)";
    }
}

// Arrow Message::Open copies custom metadata into std::strings. Check the raw
// Flatbuffer first: repeated offsets can expand a small wire message arbitrarily.
std::unique_ptr<arrow::ipc::Message> ReadIpcMessage(const std::string& bytes, const NYdb::TResultSet& owner) {
    YQL_ENSURE(bytes.size() >= 8, "YdbExternal incomplete Arrow IPC message");
    auto word = [&](size_t offset) {
        const auto* p = reinterpret_cast<const ui8*>(bytes.data() + offset);
        return ui32(p[0]) | (ui32(p[1]) << 8) | (ui32(p[2]) << 16) | (ui32(p[3]) << 24);
    };
    YQL_ENSURE(word(0) == 0xffffffffu, "YdbExternal requires the Arrow IPC continuation prefix");
    constexpr size_t offset = 8;
    const ui32 metadataSize = word(4);
    YQL_ENSURE(metadataSize % 8 == 0, "YdbExternal invalid Arrow IPC alignment");
    YQL_ENSURE(metadataSize && metadataSize <= bytes.size() - offset, "YdbExternal invalid Arrow IPC framing");
    const arrow::flatbuf::Message* flatMessage = nullptr;
    YQL_ENSURE(arrow::ipc::internal::VerifyMessage(
        reinterpret_cast<const ui8*>(bytes.data() + offset), metadataSize, &flatMessage).ok(),
        "YdbExternal invalid Arrow IPC metadata");
    YQL_ENSURE(!flatMessage->custom_metadata() || flatMessage->custom_metadata()->size() == 0,
               "YdbExternal Arrow custom metadata is unsupported");
    YQL_ENSURE(flatMessage->version() == arrow::flatbuf::MetadataVersion::V5,
               "YdbExternal requires Arrow IPC metadata version 5");
    const auto bodyOffset = offset + metadataSize;
    YQL_ENSURE(flatMessage->bodyLength() >= 0 &&
               static_cast<ui64>(flatMessage->bodyLength()) <= bytes.size() - bodyOffset,
               "YdbExternal invalid Arrow IPC body size");
    auto buffer = std::make_shared<TResultBuffer>(bytes, owner);
    return Checked(arrow::ipc::Message::Open(arrow::SliceBuffer(buffer, offset, metadataSize),
        arrow::SliceBuffer(buffer, bodyOffset, flatMessage->bodyLength())));
}

bool IsRetryable(NYdb::EStatus status) {
    // Baseline SDK does not distinguish message-size rejection from other
    // RESOURCE_EXHAUSTED causes. Retrying can repeat the same oversized result.
    switch (status) {
        case NYdb::EStatus::ABORTED:
        case NYdb::EStatus::UNAVAILABLE:
        case NYdb::EStatus::OVERLOADED:
        case NYdb::EStatus::TRANSPORT_UNAVAILABLE:
            return true;
        default:
            return false;
    }
}

NNative::TReadResult Error(const NYdb::TStatus& status) {
    // Do not propagate server issues: they can contain query text or authentication data.
    TStringBuilder message;
    message << "YdbExternal query failed with status " << ToString(status.GetStatus());
    if (status.GetStatus() == NYdb::EStatus::BAD_REQUEST || status.GetStatus() == NYdb::EStatus::UNSUPPORTED) {
        message << "; verify the source schema and remote Query Service support for Arrow results";
    }
    return {.Error = message, .Retryable = IsRetryable(status.GetStatus())};
}

class TYdbReadStream final : public NNative::IReadStream, public std::enable_shared_from_this<TYdbReadStream> {
public:
    TYdbReadStream(std::shared_ptr<NYdb::NQuery::TQueryClient> client, TSource source, NNative::TReadContext context)
        : Client_(std::move(client)), Source_(std::move(source)), Context_(context) {
    }

    void SubscribeCancellation() {
        if (Context_.Cancellation.Future().StateId() != NThreading::TCancellationToken::Default().Future().StateId()) {
            Context_.Cancellation.Future().Subscribe([weak = weak_from_this()](const auto&) {
                if (auto self = weak.lock()) {
                    self->Cancel();
                }
            });
        }
    }

    ~TYdbReadStream() override {
        Cancel();
    }

    NThreading::TFuture<NNative::TReadResult> Next() override {
        auto promise = NThreading::NewPromise<NNative::TReadResult>();
        std::shared_ptr<NYdb::NQuery::TExecuteQueryIterator> iterator;
        std::shared_ptr<arrow::RecordBatch> buffered;
        int64_t offset = 0;
        {
            std::lock_guard lock(Mutex_);
            if (Cancelled_ || Context_.Cancellation.IsCancellationRequested()) {
                promise.SetValue({.Error = "YdbExternal read cancelled"});
                return promise.GetFuture();
            }
            if (TInstant::Now() >= Context_.Deadline) {
                promise.SetValue({.Error = "YdbExternal read deadline exceeded"});
                return promise.GetFuture();
            }
            if (Pending_) {
                promise.SetValue({.Error = "YdbExternal concurrent reads are unsupported"});
                return promise.GetFuture();
            }
            Pending_ = promise;
            iterator = Iterator_;
            buffered = Buffered_;
            offset = BufferedOffset_;
        }
        try {
            if (buffered) {
                Deliver(std::move(buffered), offset, promise);
            } else if (iterator) {
                Read(iterator, promise);
            } else {
                const auto now = TInstant::Now();
                if (now >= Context_.Deadline) {
                    Complete(promise, {.Error = "YdbExternal read deadline exceeded"});
                    return promise.GetFuture();
                }
                NYdb::NQuery::TExecuteQuerySettings settings;
                settings.ClientTimeout(Context_.Deadline - now);
                settings.Deadline(NYdb::TDeadline::AfterDuration(Context_.Deadline - now));
                settings.OutputChunkMaxSize(Context_.MaxBatchBytes);
                settings.Format(NYdb::TResultSet::EFormat::Arrow);
                settings.SchemaInclusionMode(NYdb::NQuery::ESchemaInclusionMode::Always);
                settings.ConcurrentResultSets(false);
                settings.ArrowFormatSettings(NYdb::NQuery::TArrowFormatSettings().CompressionCodec(
                    NYdb::NQuery::TArrowFormatSettings::TCompressionCodec().Type(
                        NYdb::NQuery::TArrowFormatSettings::TCompressionCodec::EType::None)));
                {
                    std::lock_guard lock(Mutex_);
                    if (Cancelled_ || Context_.Cancellation.IsCancellationRequested()) {
                        return promise.GetFuture();
                    }
                }
                // Dispatch is accepted at the check above. A concurrent Cancel
                // can complete locally before the SDK call returns; its callback
                // must still discard that result. Do not call SDK credential code
                // under Mutex_: a credentials provider may cancel reentrantly.
                auto future = Client_->StreamExecuteQuery(std::string(BuildReadQuery(Source_)),
                    NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SnapshotRO()).CommitTx(), settings);
                // Keep the stream alive until this callback returns, even if
                // local cancellation has already completed the caller's future.
                future.Subscribe([self = shared_from_this(), promise](const auto& result) mutable {
                    try {
                        auto iterator = std::make_shared<NYdb::NQuery::TExecuteQueryIterator>(result.GetValue());
                        {
                            std::lock_guard lock(self->Mutex_);
                            if (self->Cancelled_ || self->Context_.Cancellation.IsCancellationRequested()) {
                                return; // Dropping a late iterator requests SDK cleanup.
                            }
                            self->Iterator_ = iterator;
                        }
                        if (!iterator->IsSuccess()) {
                            self->Complete(promise, Error(*iterator));
                        } else {
                            self->Read(iterator, promise);
                        }
                    } catch (...) {
                        self->Complete(promise, {.Error = "YdbExternal could not open the query stream"});
                    }
                });
            }
        } catch (const TResponseError& error) {
            Complete(promise, {.Error = error.what()});
        } catch (...) {
            Complete(promise, {.Error = "YdbExternal could not start the query read"});
        }
        return promise.GetFuture();
    }

    void Cancel() override {
        std::optional<NThreading::TPromise<NNative::TReadResult>> pending;
        std::shared_ptr<NYdb::NQuery::TExecuteQueryIterator> iterator;
        {
            std::lock_guard lock(Mutex_);
            Cancelled_ = true;
            pending = std::exchange(Pending_, {});
            iterator = std::move(Iterator_);
            Buffered_.reset();
            BufferedOffset_ = 0;
        }
        if (pending) {
            pending->TrySetValue({.Error = "YdbExternal read cancelled"});
        }
        // The existing SDK cancels on reader destruction. During initial open or
        // ReadNext it still owns the reader: the RPC can outlive local cancellation
        // until its absolute deadline. No transport-quiescence guarantee is made.
    }

private:
    void Deliver(std::shared_ptr<arrow::RecordBatch> batch, int64_t offset,
                 NThreading::TPromise<NNative::TReadResult> promise) {
        auto output = TakeOutputBatch(*batch, offset, Context_.MaxBatchBytes, MaxOutputRowBytes);
        offset += output->num_rows();
        {
            std::lock_guard lock(Mutex_);
            if (!Cancelled_ && !Context_.Cancellation.IsCancellationRequested()) {
                Buffered_ = offset < batch->num_rows() ? std::move(batch) : nullptr;
                BufferedOffset_ = offset;
            }
        }
        const ui64 bytes = NUdf::GetSizeOfArrowBatchInBytes(*output);
        Complete(promise, {.Batch = std::move(output), .Bytes = bytes});
    }

    void Complete(NThreading::TPromise<NNative::TReadResult> promise, NNative::TReadResult result) {
        {
            std::lock_guard lock(Mutex_);
            if (Cancelled_ || Context_.Cancellation.IsCancellationRequested()) {
                result = {.Error = "YdbExternal read cancelled"};
            } else if (TInstant::Now() >= Context_.Deadline) {
                result = {.Error = "YdbExternal read deadline exceeded"};
            }
            Pending_.reset();
        }
        promise.TrySetValue(std::move(result));
    }

    void Read(const std::shared_ptr<NYdb::NQuery::TExecuteQueryIterator>& iterator,
              NThreading::TPromise<NNative::TReadResult> promise) {
        {
            std::lock_guard lock(Mutex_);
            if (Cancelled_ || Context_.Cancellation.IsCancellationRequested()) {
                return;
            }
        }
        iterator->ReadNext().Subscribe([self = shared_from_this(), promise](const auto& future) mutable {
            try {
                {
                    std::lock_guard lock(self->Mutex_);
                    if (self->Cancelled_ || self->Context_.Cancellation.IsCancellationRequested()) {
                        return; // Do not decode a late result after cancellation.
                    }
                }
                const auto& part = future.GetValue();
                if (part.EOS()) {
                    self->Complete(promise, {.Finished = true});
                } else if (!part.IsSuccess()) {
                    self->Complete(promise, Error(part));
                } else if (part.HasResultSet()) {
                    YQL_ENSURE(part.GetResultSetIndex() == 0, "YdbExternal unexpected result set");
                    auto batch = DecodeArrowResult(part.GetResultSet(), self->Source_, MaxDecodedPartBytes);
                    self->Deliver(std::move(batch), 0, promise);
                } else {
                    self->Complete(promise, {});
                }
            } catch (const TResponseError& error) {
                self->Complete(promise, {.Error = error.what()});
            } catch (...) {
                self->Complete(promise, {.Error = "YdbExternal Arrow validation failed: malformed or unsupported response"});
            }
        });
    }

    const std::shared_ptr<NYdb::NQuery::TQueryClient> Client_;
    const TSource Source_;
    const NNative::TReadContext Context_;
    std::mutex Mutex_;
    bool Cancelled_ = false;
    std::optional<NThreading::TPromise<NNative::TReadResult>> Pending_;
    std::shared_ptr<NYdb::NQuery::TExecuteQueryIterator> Iterator_;
    // Retain one validated part while the consumer pulls compact output blocks.
    // No further SDK ReadNext is issued until all of its rows have been consumed.
    std::shared_ptr<arrow::RecordBatch> Buffered_;
    int64_t BufferedOffset_ = 0;
};

} // namespace

void ValidateSource(const TSource& source) {
    YQL_ENSURE(source.GetVersion() == 1, "YdbExternal unsupported source payload version");
    YQL_ENSURE(!source.GetEndpoint().empty() && !source.GetDatabase().empty(), "YdbExternal endpoint and database are required");
    YQL_ENSURE(source.GetReadTimeoutMs() && source.GetReadTimeoutMs() <= 3600000, "YdbExternal invalid read timeout");
    YQL_ENSURE(source.GetMaxBatchBytes() >= 1024 && source.GetMaxBatchBytes() <= 1024 * 1024,
               "YdbExternal invalid output batch target");
    YQL_ENSURE(source.GetMaxRetries() <= 5, "YdbExternal invalid retry limit");
    YQL_ENSURE(source.ColumnsSize() && source.ColumnsSize() <= MaxColumns, "YdbExternal invalid column count");
    THashSet<TString> names;
    ui64 nameBytes = 0;
    for (const auto& column : source.GetColumns()) {
        YQL_ENSURE(names.insert(column.GetName()).second && column.GetName() != BlockLengthColumnName,
                   "YdbExternal invalid or reserved column name");
        nameBytes += column.GetName().size();
        ArrowType(column.GetType());
    }
    YQL_ENSURE(nameBytes <= MaxSchemaBytes / 2, "YdbExternal projection names exceed the schema limit");
    BuildReadQuery(source);
}

TString BuildReadQuery(const TSource& source) {
    TStringBuilder sql;
    sql << "SELECT ";
    bool first = true;
    for (const auto& column : source.GetColumns()) {
        if (!first) {
            sql << ", ";
        }
        first = false;
        sql << QuoteIdentifier(column.GetName());
    }
    sql << " FROM " << QuoteIdentifier(source.GetTable()) << ";";
    return sql;
}

std::shared_ptr<arrow::RecordBatch> DecodeArrowResult(const NYdb::TResultSet& result,
    const TSource& source, ui64 maxDecodedBytes) {
    YQL_ENSURE(!result.Truncated(), "YdbExternal returned a truncated result");
    CheckResponse(NYdb::TArrowAccessor::Format(result) == NYdb::TResultSet::EFormat::Arrow,
                  "remote Query Service did not return the required Arrow format");
    const auto& schemaBytes = NYdb::TArrowAccessor::GetArrowSchema(result);
    const auto& chunks = NYdb::TArrowAccessor::GetArrowBatches(result);
    YQL_ENSURE(schemaBytes.size() <= MaxSchemaBytes && !schemaBytes.empty(), "YdbExternal invalid Arrow schema size");
    YQL_ENSURE(chunks.size() <= 1, "YdbExternal unexpected multiple Arrow chunks");
    CheckLimit(chunks.empty() || chunks.front().size() <= maxDecodedBytes, "Arrow part", maxDecodedBytes);

    // Validate the flat schema before recursive Arrow schema decoding. No dictionaries,
    // nested children or compression are accepted. Aliases and Bool expansion are
    // accounted separately below, before Arrow opens or converts the batch.
    auto schemaMessage = ReadIpcMessage(schemaBytes, result);
    YQL_ENSURE(schemaMessage && schemaMessage->type() == arrow::ipc::MessageType::SCHEMA && schemaMessage->header(), "YdbExternal expected Arrow schema");
    const auto* flatSchema = static_cast<const arrow::flatbuf::Schema*>(schemaMessage->header());
    CheckResponse(flatSchema->fields() && flatSchema->fields()->size() == source.ColumnsSize(),
                  "schema column count changed; recompile the query");
    YQL_ENSURE(!flatSchema->custom_metadata() || flatSchema->custom_metadata()->size() == 0,
               "YdbExternal Arrow schema custom metadata is unsupported");
    YQL_ENSURE(flatSchema->endianness() == arrow::flatbuf::Endianness::Little,
               "YdbExternal big-endian Arrow responses are unsupported");
    int fieldIndex = 0;
    for (const auto* field : *flatSchema->fields()) {
        YQL_ENSURE(field, "YdbExternal missing Arrow schema field");
        YQL_ENSURE(!field->custom_metadata() || field->custom_metadata()->size() == 0,
                   "YdbExternal Arrow field custom metadata is unsupported");
        CheckResponse(field->name() && TStringBuf(field->name()->c_str(), field->name()->size()) ==
                   source.GetColumns(fieldIndex++).GetName(), "schema column name changed; recompile the query");
        YQL_ENSURE(field->type(), "YdbExternal missing Arrow field type");
        switch (field->type_type()) {
            case arrow::flatbuf::Type::Int:
            case arrow::flatbuf::Type::FloatingPoint:
            case arrow::flatbuf::Type::Binary:
            case arrow::flatbuf::Type::Utf8:
            case arrow::flatbuf::Type::Bool:
                break;
            default:
                ythrow TResponseError() << "YdbExternal response validation failed: unsupported Arrow type";
        }
        YQL_ENSURE(!field->dictionary() && (!field->children() || field->children()->size() == 0), "YdbExternal nested Arrow types are unsupported");
    }
    arrow::ipc::DictionaryMemo dictionary;
    auto schema = Checked(arrow::ipc::ReadSchema(*schemaMessage, &dictionary));
    for (int i = 0; i < schema->num_fields(); ++i) {
        const auto& column = source.GetColumns(i);
        const bool compatibleType = schema->field(i)->type()->Equals(ArrowType(column.GetType())) ||
            (IsBool(column.GetType()) && schema->field(i)->type()->id() == arrow::Type::BOOL);
        CheckResponse(schema->field(i)->name() == column.GetName() && compatibleType,
                      "schema changed; recompile the query");
    }

    if (chunks.empty()) {
        std::vector<std::shared_ptr<arrow::Array>> empty;
        for (const auto& field : schema->fields()) {
            empty.push_back(Checked(arrow::MakeArrayOfNull(field->type(), 0)));
        }
        return arrow::RecordBatch::Make(schema, 0, std::move(empty));
    }
    auto message = ReadIpcMessage(chunks.front(), result);
    YQL_ENSURE(message && message->type() == arrow::ipc::MessageType::RECORD_BATCH && message->header(), "YdbExternal expected an Arrow record batch");
    const auto* flatBatch = static_cast<const arrow::flatbuf::RecordBatch*>(message->header());
    YQL_ENSURE(!flatBatch->compression(), "YdbExternal compressed Arrow responses are unsupported");
    YQL_ENSURE(flatBatch->length() >= 0 && static_cast<ui64>(flatBatch->length()) <= maxDecodedBytes,
               "YdbExternal excessive Arrow batch row count");
    YQL_ENSURE(flatBatch->nodes() && flatBatch->nodes()->size() == source.ColumnsSize() &&
               flatBatch->buffers() && flatBatch->buffers()->size() <= MaxColumns * 3,
               "YdbExternal invalid Arrow batch layout");
    for (const auto* node : *flatBatch->nodes()) {
        YQL_ENSURE(node->length() == flatBatch->length() && node->null_count() >= 0 && node->null_count() <= node->length(),
                   "YdbExternal invalid Arrow array length");
    }
    ui64 allocationBudget = 0;
    for (const auto* buffer : *flatBatch->buffers()) {
        YQL_ENSURE(buffer->offset() >= 0 && buffer->length() >= 0 &&
                   buffer->offset() <= message->body_length() &&
                   buffer->length() <= message->body_length() - buffer->offset(),
                   "YdbExternal invalid Arrow buffer bounds");
        const ui64 alignedLength = (static_cast<ui64>(buffer->length()) + 63) & ~ui64(63);
        CheckLimit(alignedLength <= maxDecodedBytes - allocationBudget, "decoded buffers", maxDecodedBytes);
        allocationBudget += alignedLength;
    }
    for (const auto& field : schema->fields()) {
        if (field->type()->id() == arrow::Type::BOOL) {
            // Count each column, even when the wire buffers alias. Reserve the full
            // UInt8 replacement before any conversion can allocate its first column.
            const ui64 expanded = ((flatBatch->length() + 63) & ~ui64(63)) +
                                  (((flatBatch->length() + 7) / 8 + 63) & ~ui64(63));
            CheckLimit(expanded <= maxDecodedBytes - allocationBudget, "Bool expansion", maxDecodedBytes);
            allocationBudget += expanded;
        }
    }
    auto batch = Checked(arrow::ipc::ReadRecordBatch(*message, schema, &dictionary, arrow::ipc::IpcReadOptions::Defaults()));
    YQL_ENSURE(batch->ValidateFull().ok(), "YdbExternal invalid Arrow batch");
    auto columns = batch->columns();
    auto fields = schema->fields();
    for (int i = 0; i < batch->num_columns(); ++i) {
        YQL_ENSURE(source.GetColumns(i).GetType().has_optional_type() || columns[i]->null_count() == 0,
                   "YdbExternal NULL in a non-optional column");
        if (IsBool(source.GetColumns(i).GetType()) && columns[i]->type_id() == arrow::Type::UINT8) {
            // Query Service and MiniKQL represent Bool as UInt8, with only 0 and 1 valid.
            const auto& input = static_cast<const arrow::UInt8Array&>(*columns[i]);
            for (int64_t row = 0; row < input.length(); ++row) {
                YQL_ENSURE(input.IsNull(row) || input.Value(row) <= 1, "YdbExternal invalid Bool value");
            }
        }
        if (columns[i]->type_id() == arrow::Type::BOOL) {
            // Accept bit-packed Arrow Bool as well, converting it to the MiniKQL layout.
            const auto& input = static_cast<const arrow::BooleanArray&>(*columns[i]);
            arrow::UInt8Builder builder;
            YQL_ENSURE(builder.Reserve(input.length()).ok(), "YdbExternal Bool allocation failed");
            for (int64_t row = 0; row < input.length(); ++row) {
                if (input.IsNull(row)) {
                    builder.UnsafeAppendNull();
                } else {
                    builder.UnsafeAppend(input.Value(row));
                }
            }
            columns[i] = Checked(builder.Finish());
            fields[i] = fields[i]->WithType(arrow::uint8());
        }
    }
    auto output = arrow::RecordBatch::Make(arrow::schema(std::move(fields)), batch->num_rows(), std::move(columns));
    CheckLimit(NUdf::GetSizeOfArrowBatchInBytes(*output) <= maxDecodedBytes, "decoded buffers", maxDecodedBytes);
    return output;
}

std::shared_ptr<arrow::RecordBatch> TakeOutputBatch(const arrow::RecordBatch& batch,
    int64_t offset, ui64 targetBytes, ui64 maxRowBytes) {
    YQL_ENSURE(offset >= 0 && offset <= batch.num_rows() && targetBytes && maxRowBytes,
               "YdbExternal invalid output batch bounds");
    if (offset == batch.num_rows()) {
        return batch.Slice(offset, 0);
    }
    const auto aligned = [](ui64 bytes) { return (bytes + 63) & ~ui64(63); };
    const auto isBinary = [](const arrow::Array& array) {
        return array.type_id() == arrow::Type::BINARY || array.type_id() == arrow::Type::STRING;
    };
    // Count precisely the standalone buffers allocated below, including bitmap,
    // offset and allocator padding. IPC part size and row count are not proxies
    // for output size: variable-width rows can differ by many megabytes.
    const auto outputSize = [&](int64_t rows) {
        ui64 bytes = sizeof(batch) + batch.num_columns() * sizeof(void*);
        for (const auto& column : batch.columns()) {
            bytes += sizeof(arrow::ArrayData) + (isBinary(*column) ? 3 : 2) * sizeof(void*);
            if (column->null_count()) {
                bytes += aligned((rows + 7) / 8);
            }
            if (isBinary(*column)) {
                const auto& binary = static_cast<const arrow::BinaryArray&>(*column);
                bytes += aligned((rows + 1) * sizeof(int32_t));
                bytes += aligned(binary.value_offset(offset + rows) - binary.value_offset(offset));
            } else {
                const auto& type = static_cast<const arrow::FixedWidthType&>(*column->type());
                bytes += aligned(rows * (type.bit_width() / 8));
            }
        }
        return bytes;
    };
    CheckLimit(outputSize(1) <= maxRowBytes, "single output row", maxRowBytes);
    int64_t rows = 1;
    if (outputSize(1) <= targetBytes) {
        int64_t end = batch.num_rows() - offset;
        while (rows < end) {
            const auto middle = rows + (end - rows + 1) / 2;
            if (outputSize(middle) <= targetBytes) {
                rows = middle;
            } else {
                end = middle - 1;
            }
        }
    }

    const auto allocate = [&](ui64 bytes) -> std::shared_ptr<arrow::Buffer> {
        auto buffer = Checked(arrow::AllocateBuffer(aligned(bytes)));
        if (static_cast<ui64>(buffer->size()) > bytes) {
            std::memset(buffer->mutable_data() + bytes, 0, buffer->size() - bytes);
        }
        return std::move(buffer);
    };
    const auto copy = [&](const uint8_t* data, ui64 bytes) {
        auto buffer = allocate(bytes);
        if (bytes) {
            std::memcpy(buffer->mutable_data(), data, bytes);
        }
        return buffer;
    };
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(batch.num_columns());
    for (const auto& column : batch.columns()) {
        std::vector<std::shared_ptr<arrow::Buffer>> buffers;
        if (column->null_count()) {
            auto bitmap = allocate((rows + 7) / 8);
            std::memset(bitmap->mutable_data(), 0, (rows + 7) / 8);
            arrow::internal::CopyBitmap(column->null_bitmap_data(), column->offset() + offset,
                rows, bitmap->mutable_data(), 0);
            buffers.push_back(std::move(bitmap));
        } else {
            buffers.push_back(nullptr);
        }
        if (isBinary(*column)) {
            const auto& binary = static_cast<const arrow::BinaryArray&>(*column);
            const auto begin = binary.value_offset(offset);
            const auto bytes = binary.value_offset(offset + rows) - begin;
            auto offsets = allocate((rows + 1) * sizeof(int32_t));
            auto* values = reinterpret_cast<int32_t*>(offsets->mutable_data());
            for (int64_t row = 0; row <= rows; ++row) {
                values[row] = binary.value_offset(offset + row) - begin;
            }
            buffers.push_back(std::move(offsets));
            buffers.push_back(copy(bytes ? binary.raw_data() + begin : nullptr, bytes));
        } else {
            const auto& type = static_cast<const arrow::FixedWidthType&>(*column->type());
            const auto width = type.bit_width() / 8;
            buffers.push_back(copy(column->data()->buffers[1]->data() +
                (column->offset() + offset) * width, rows * width));
        }
        columns.push_back(arrow::MakeArray(arrow::ArrayData::Make(column->type(), rows,
            std::move(buffers), column->null_count() ? arrow::kUnknownNullCount : 0)));
    }
    return arrow::RecordBatch::Make(batch.schema(), rows, std::move(columns));
}

std::shared_ptr<NNative::IReadStream> CreateReadStream(std::shared_ptr<NYdb::NQuery::TQueryClient> client,
    const TSource& source, const NNative::TReadContext& context) {
    auto stream = std::make_shared<TYdbReadStream>(std::move(client), source, context);
    stream->SubscribeCancellation();
    return stream;
}

} // namespace NYql::NYdbExternal
