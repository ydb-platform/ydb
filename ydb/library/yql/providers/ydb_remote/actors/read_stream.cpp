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

#include <util/generic/hash_set.h>
#include <util/string/builder.h>
#include <mutex>
#include <cstring>
#include <optional>
#include <utility>

namespace NYql::NYdbRemote {
namespace {

constexpr ui64 MaxSchemaBytes = 64 * 1024;
constexpr ui64 MaxColumns = 1024;

std::shared_ptr<arrow::DataType> ArrowType(const Ydb::Type& type) {
    const auto& item = type.has_optional_type() ? type.optional_type().item() : type;
    YQL_ENSURE(item.has_type_id(), "YdbRemote only supports primitive and optional primitive columns");
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
        default: ythrow yexception() << "YdbRemote column type is unsupported";
    }
}

bool IsBool(const Ydb::Type& type) {
    const auto& item = type.has_optional_type() ? type.optional_type().item() : type;
    return item.has_type_id() && item.type_id() == Ydb::Type::BOOL;
}

TString QuoteIdentifier(TStringBuf value) {
    YQL_ENSURE(!value.empty() && value.size() <= 4096, "YdbRemote invalid identifier length");
    TString result("`");
    for (unsigned char c : value) {
        YQL_ENSURE(c >= 32 && c != 127, "YdbRemote control character in identifier");
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
    YQL_ENSURE(result.ok(), "YdbRemote invalid or unsupported Arrow payload");
    return std::move(result).ValueOrDie();
}

// Arrow Message::Open copies custom metadata into std::strings. Check the raw
// Flatbuffer first: repeated offsets can expand a small wire message arbitrarily.
std::unique_ptr<arrow::ipc::Message> ReadIpcMessage(const std::string& bytes) {
    YQL_ENSURE(bytes.size() >= 8, "YdbRemote incomplete Arrow IPC message");
    auto word = [&](size_t offset) {
        const auto* p = reinterpret_cast<const ui8*>(bytes.data() + offset);
        return ui32(p[0]) | (ui32(p[1]) << 8) | (ui32(p[2]) << 16) | (ui32(p[3]) << 24);
    };
    YQL_ENSURE(word(0) == 0xffffffffu, "YdbRemote requires the Arrow IPC continuation prefix");
    constexpr size_t offset = 8;
    const ui32 metadataSize = word(4);
    YQL_ENSURE(metadataSize % 8 == 0, "YdbRemote invalid Arrow IPC alignment");
    YQL_ENSURE(metadataSize && metadataSize <= bytes.size() - offset, "YdbRemote invalid Arrow IPC framing");
    const arrow::flatbuf::Message* flatMessage = nullptr;
    YQL_ENSURE(arrow::ipc::internal::VerifyMessage(
        reinterpret_cast<const ui8*>(bytes.data() + offset), metadataSize, &flatMessage).ok(),
        "YdbRemote invalid Arrow IPC metadata");
    YQL_ENSURE(!flatMessage->custom_metadata() || flatMessage->custom_metadata()->size() == 0,
               "YdbRemote Arrow custom metadata is unsupported");
    YQL_ENSURE(flatMessage->version() == arrow::flatbuf::MetadataVersion::V5,
               "YdbRemote requires Arrow IPC metadata version 5");
    const auto bodyOffset = offset + metadataSize;
    YQL_ENSURE(flatMessage->bodyLength() >= 0 &&
               static_cast<ui64>(flatMessage->bodyLength()) <= bytes.size() - bodyOffset,
               "YdbRemote invalid Arrow IPC body size");
    auto buffer = arrow::Buffer::FromString(bytes);
    return Checked(arrow::ipc::Message::Open(arrow::SliceBuffer(buffer, offset, metadataSize),
        arrow::SliceBuffer(buffer, bodyOffset, flatMessage->bodyLength())));
}

bool IsRetryable(NYdb::EStatus status) {
    switch (status) {
        case NYdb::EStatus::ABORTED:
        case NYdb::EStatus::UNAVAILABLE:
        case NYdb::EStatus::OVERLOADED:
        case NYdb::EStatus::CLIENT_RESOURCE_EXHAUSTED:
        case NYdb::EStatus::TRANSPORT_UNAVAILABLE:
            return true;
        default:
            return false;
    }
}

NNative::TReadResult Error(const NYdb::TStatus& status) {
    // Do not propagate server issues: they can contain query text or authentication data.
    return {.Error = TStringBuilder() << "YdbRemote query failed with status " << static_cast<size_t>(status.GetStatus()),
            .Retryable = IsRetryable(status.GetStatus())};
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
        {
            std::lock_guard lock(Mutex_);
            if (Cancelled_ || Context_.Cancellation.IsCancellationRequested()) {
                promise.SetValue({.Error = "YdbRemote read cancelled"});
                return promise.GetFuture();
            }
            if (TInstant::Now() >= Context_.Deadline) {
                promise.SetValue({.Error = "YdbRemote read deadline exceeded"});
                return promise.GetFuture();
            }
            if (Pending_) {
                promise.SetValue({.Error = "YdbRemote concurrent reads are unsupported"});
                return promise.GetFuture();
            }
            Pending_ = promise;
            iterator = Iterator_;
        }
        try {
            if (iterator) {
                Read(iterator, promise);
            } else {
                const auto now = TInstant::Now();
                if (now >= Context_.Deadline) {
                    Complete(promise, {.Error = "YdbRemote read deadline exceeded"});
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
                        self->Complete(promise, {.Error = "YdbRemote could not open the query stream"});
                    }
                });
            }
        } catch (...) {
            Complete(promise, {.Error = "YdbRemote could not start the query read"});
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
        }
        if (pending) {
            pending->TrySetValue({.Error = "YdbRemote read cancelled"});
        }
        // The existing SDK cancels on reader destruction. During initial open or
        // ReadNext it still owns the reader: the RPC can outlive local cancellation
        // until its absolute deadline. No transport-quiescence guarantee is made.
    }

private:
    void Complete(NThreading::TPromise<NNative::TReadResult> promise, NNative::TReadResult result) {
        {
            std::lock_guard lock(Mutex_);
            if (Cancelled_ || Context_.Cancellation.IsCancellationRequested()) {
                result = {.Error = "YdbRemote read cancelled"};
            } else if (TInstant::Now() >= Context_.Deadline) {
                result = {.Error = "YdbRemote read deadline exceeded"};
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
                    YQL_ENSURE(part.GetResultSetIndex() == 0, "YdbRemote unexpected result set");
                    auto batch = DecodeArrowResult(part.GetResultSet(), self->Source_, self->Context_.MaxBatchBytes);
                    const ui64 bytes = NUdf::GetSizeOfArrowBatchInBytes(*batch);
                    self->Complete(promise, {.Batch = std::move(batch), .Bytes = bytes});
                } else {
                    self->Complete(promise, {});
                }
            } catch (...) {
                self->Complete(promise, {.Error = "YdbRemote invalid, unsupported or oversized response"});
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
};

} // namespace

void ValidateSource(const TSource& source) {
    YQL_ENSURE(source.GetVersion() == 1, "YdbRemote unsupported source payload version");
    YQL_ENSURE(!source.GetEndpoint().empty() && !source.GetDatabase().empty(), "YdbRemote endpoint and database are required");
    YQL_ENSURE(source.GetReadTimeoutMs() && source.GetReadTimeoutMs() <= 3600000, "YdbRemote invalid read timeout");
    YQL_ENSURE(source.GetMaxBatchBytes() >= 1024 && source.GetMaxBatchBytes() <= 1024 * 1024,
               "YdbRemote invalid batch memory limit");
    YQL_ENSURE(source.GetMaxRetries() <= 5, "YdbRemote invalid retry limit");
    YQL_ENSURE(source.ColumnsSize() && source.ColumnsSize() <= MaxColumns, "YdbRemote invalid column count");
    THashSet<TString> names;
    ui64 nameBytes = 0;
    for (const auto& column : source.GetColumns()) {
        YQL_ENSURE(names.insert(column.GetName()).second && column.GetName() != BlockLengthColumnName,
                   "YdbRemote invalid or reserved column name");
        nameBytes += column.GetName().size();
        ArrowType(column.GetType());
    }
    YQL_ENSURE(nameBytes <= MaxSchemaBytes / 2, "YdbRemote projection names exceed the schema limit");
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
    const TSource& source, ui64 maxBatchBytes) {
    YQL_ENSURE(!result.Truncated(), "YdbRemote returned a truncated result");
    YQL_ENSURE(NYdb::TArrowAccessor::Format(result) == NYdb::TResultSet::EFormat::Arrow,
               "YdbRemote requires the Arrow query result format");
    const auto& schemaBytes = NYdb::TArrowAccessor::GetArrowSchema(result);
    const auto& chunks = NYdb::TArrowAccessor::GetArrowBatches(result);
    YQL_ENSURE(schemaBytes.size() <= MaxSchemaBytes && !schemaBytes.empty(), "YdbRemote invalid Arrow schema size");
    YQL_ENSURE(chunks.size() <= 1 && (chunks.empty() || chunks.front().size() <= maxBatchBytes), "YdbRemote oversized Arrow part");

    // Validate the flat schema before recursive Arrow schema decoding. No dictionaries,
    // nested children or compression are accepted, so decoded memory cannot expand remotely.
    auto schemaMessage = ReadIpcMessage(schemaBytes);
    YQL_ENSURE(schemaMessage && schemaMessage->type() == arrow::ipc::MessageType::SCHEMA && schemaMessage->header(), "YdbRemote expected Arrow schema");
    const auto* flatSchema = static_cast<const arrow::flatbuf::Schema*>(schemaMessage->header());
    YQL_ENSURE(flatSchema->fields() && flatSchema->fields()->size() == source.ColumnsSize(), "YdbRemote schema column count changed");
    YQL_ENSURE(!flatSchema->custom_metadata() || flatSchema->custom_metadata()->size() == 0,
               "YdbRemote Arrow schema custom metadata is unsupported");
    YQL_ENSURE(flatSchema->endianness() == arrow::flatbuf::Endianness::Little,
               "YdbRemote big-endian Arrow responses are unsupported");
    int fieldIndex = 0;
    for (const auto* field : *flatSchema->fields()) {
        YQL_ENSURE(field, "YdbRemote missing Arrow schema field");
        YQL_ENSURE(!field->custom_metadata() || field->custom_metadata()->size() == 0,
                   "YdbRemote Arrow field custom metadata is unsupported");
        YQL_ENSURE(field->name() && TStringBuf(field->name()->c_str(), field->name()->size()) ==
                   source.GetColumns(fieldIndex++).GetName(), "YdbRemote result column name changed");
        YQL_ENSURE(field->type(), "YdbRemote missing Arrow field type");
        switch (field->type_type()) {
            case arrow::flatbuf::Type::Int:
            case arrow::flatbuf::Type::FloatingPoint:
            case arrow::flatbuf::Type::Binary:
            case arrow::flatbuf::Type::Utf8:
            case arrow::flatbuf::Type::Bool:
                break;
            default:
                ythrow yexception() << "YdbRemote Arrow type is unsupported";
        }
        YQL_ENSURE(!field->dictionary() && (!field->children() || field->children()->size() == 0), "YdbRemote nested Arrow types are unsupported");
    }
    arrow::ipc::DictionaryMemo dictionary;
    auto schema = Checked(arrow::ipc::ReadSchema(*schemaMessage, &dictionary));
    for (int i = 0; i < schema->num_fields(); ++i) {
        const auto& column = source.GetColumns(i);
        const bool compatibleType = schema->field(i)->type()->Equals(ArrowType(column.GetType())) ||
            (IsBool(column.GetType()) && schema->field(i)->type()->id() == arrow::Type::BOOL);
        YQL_ENSURE(schema->field(i)->name() == column.GetName() && compatibleType,
                   "YdbRemote result schema changed; recompile the query");
    }

    if (chunks.empty()) {
        std::vector<std::shared_ptr<arrow::Array>> empty;
        for (const auto& field : schema->fields()) {
            empty.push_back(Checked(arrow::MakeArrayOfNull(field->type(), 0)));
        }
        return arrow::RecordBatch::Make(schema, 0, std::move(empty));
    }
    auto message = ReadIpcMessage(chunks.front());
    YQL_ENSURE(message && message->type() == arrow::ipc::MessageType::RECORD_BATCH && message->header(), "YdbRemote expected an Arrow record batch");
    const auto* flatBatch = static_cast<const arrow::flatbuf::RecordBatch*>(message->header());
    YQL_ENSURE(!flatBatch->compression(), "YdbRemote compressed Arrow responses are unsupported");
    YQL_ENSURE(flatBatch->length() >= 0 && static_cast<ui64>(flatBatch->length()) <= maxBatchBytes,
               "YdbRemote excessive Arrow batch row count");
    YQL_ENSURE(flatBatch->nodes() && flatBatch->nodes()->size() == source.ColumnsSize() &&
               flatBatch->buffers() && flatBatch->buffers()->size() <= MaxColumns * 3,
               "YdbRemote invalid Arrow batch layout");
    for (const auto* node : *flatBatch->nodes()) {
        YQL_ENSURE(node->length() == flatBatch->length() && node->null_count() >= 0 && node->null_count() <= node->length(),
                   "YdbRemote invalid Arrow array length");
    }
    ui64 allocationBudget = 0;
    for (const auto* buffer : *flatBatch->buffers()) {
        YQL_ENSURE(buffer->offset() >= 0 && buffer->length() >= 0 &&
                   buffer->offset() <= message->body_length() &&
                   buffer->length() <= message->body_length() - buffer->offset(),
                   "YdbRemote invalid Arrow buffer bounds");
        const ui64 alignedLength = (static_cast<ui64>(buffer->length()) + 63) & ~ui64(63);
        YQL_ENSURE(alignedLength <= maxBatchBytes - allocationBudget,
                   "YdbRemote Arrow buffers exceed the decoded memory limit");
        allocationBudget += alignedLength;
    }
    for (const auto& field : schema->fields()) {
        if (field->type()->id() == arrow::Type::BOOL) {
            // Count each column, even when the wire buffers alias. Reserve the full
            // UInt8 replacement before any conversion can allocate its first column.
            const ui64 expanded = ((flatBatch->length() + 63) & ~ui64(63)) +
                                  (((flatBatch->length() + 7) / 8 + 63) & ~ui64(63));
            YQL_ENSURE(expanded <= maxBatchBytes - allocationBudget,
                       "YdbRemote Bool expansion exceeds the decoded memory limit");
            allocationBudget += expanded;
        }
    }
    auto batch = Checked(arrow::ipc::ReadRecordBatch(*message, schema, &dictionary, arrow::ipc::IpcReadOptions::Defaults()));
    YQL_ENSURE(batch->ValidateFull().ok(), "YdbRemote invalid Arrow batch");
    auto columns = batch->columns();
    auto fields = schema->fields();
    for (int i = 0; i < batch->num_columns(); ++i) {
        YQL_ENSURE(source.GetColumns(i).GetType().has_optional_type() || columns[i]->null_count() == 0,
                   "YdbRemote NULL in a non-optional column");
        if (IsBool(source.GetColumns(i).GetType()) && columns[i]->type_id() == arrow::Type::UINT8) {
            // Query Service and MiniKQL represent Bool as UInt8, with only 0 and 1 valid.
            const auto& input = static_cast<const arrow::UInt8Array&>(*columns[i]);
            for (int64_t row = 0; row < input.length(); ++row) {
                YQL_ENSURE(input.IsNull(row) || input.Value(row) <= 1, "YdbRemote invalid Bool value");
            }
        }
        if (columns[i]->type_id() == arrow::Type::BOOL) {
            // Accept bit-packed Arrow Bool as well, converting it to the MiniKQL layout.
            const auto& input = static_cast<const arrow::BooleanArray&>(*columns[i]);
            arrow::UInt8Builder builder;
            YQL_ENSURE(builder.Reserve(input.length()).ok(), "YdbRemote Bool allocation failed");
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
        // DQ charges the visible buffer sizes, not the parent IPC blob. Detach every
        // buffer so padding/unused wire data cannot remain alive after delivery.
        auto compact = columns[i]->data()->Copy();
        for (auto& buffer : compact->buffers) {
            if (!buffer) {
                continue;
            }
            const ui64 bytes = (static_cast<ui64>(buffer->size()) + 63) & ~ui64(63);
            auto copy = Checked(arrow::AllocateBuffer(bytes));
            if (buffer->size()) {
                std::memcpy(copy->mutable_data(), buffer->data(), buffer->size());
                std::memset(copy->mutable_data() + buffer->size(), 0, bytes - buffer->size());
            }
            buffer = std::move(copy);
        }
        columns[i] = arrow::MakeArray(std::move(compact));
    }
    auto output = arrow::RecordBatch::Make(arrow::schema(std::move(fields)), batch->num_rows(), std::move(columns));
    YQL_ENSURE(NUdf::GetSizeOfArrowBatchInBytes(*output) <= maxBatchBytes, "YdbRemote decoded batch exceeds the memory limit");
    return output;
}

std::shared_ptr<NNative::IReadStream> CreateReadStream(std::shared_ptr<NYdb::NQuery::TQueryClient> client,
    const TSource& source, const NNative::TReadContext& context) {
    auto stream = std::make_shared<TYdbReadStream>(std::move(client), source, context);
    stream->SubscribeCancellation();
    return stream;
}

} // namespace NYql::NYdbRemote
