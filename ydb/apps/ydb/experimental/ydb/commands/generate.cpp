#include "generate.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <ydb/public/api/protos/ydb_formats.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/lib/ydb_cli/common/recursive_list.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <ydb/public/lib/ydb_cli/common/progress_bar.h>
#include <ydb/public/lib/ydb_cli/dump/util/util.h>
#include <ydb/public/lib/ydb_cli/import/cli_arrow_helpers.h>

#include <library/cpp/threading/future/async.h>

#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/system/thread.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>


namespace NYdb::NConsoleClient {
namespace {

std::optional<NYdb::EPrimitiveType> ExtractPrimitiveType(const NYdb::TType& ydbType) {
    TTypeParser parser(ydbType);
    if (parser.GetKind() == TTypeParser::ETypeKind::Optional) {
        parser.OpenOptional();
    }
    if (parser.GetKind() != TTypeParser::ETypeKind::Primitive) {
        return {};
    }
    return parser.GetPrimitive();
}

class TColumnGenerator {
public:
    TColumnGenerator(ui64 seed, ui64 numRows)
        : Seed(seed)
        , NumRows(numRows)
    {}

    std::shared_ptr<arrow::Array> AppendGenerated(arrow::DataType& arrowType, const NYdb::TType& ydbType) {
        auto type = ExtractPrimitiveType(ydbType);
        if (!type) {
            return {};
        }

        switch(*type) {
            case NYdb::EPrimitiveType::Int8:
                return AppendPrimitives<arrow::Int8Type>(arrowType, *type);
            case NYdb::EPrimitiveType::Uint8:
                return AppendPrimitives<arrow::UInt8Type>(arrowType, *type);
            case NYdb::EPrimitiveType::Int16:
                return AppendPrimitives<arrow::Int16Type>(arrowType, *type);
            case NYdb::EPrimitiveType::Uint16:
                return AppendPrimitives<arrow::UInt16Type>(arrowType, *type);
            case NYdb::EPrimitiveType::Int32:
                return AppendPrimitives<arrow::Int32Type>(arrowType, *type);
            case NYdb::EPrimitiveType::Uint32:
                return AppendPrimitives<arrow::UInt32Type>(arrowType, *type);
            case NYdb::EPrimitiveType::Int64:
                return AppendPrimitives<arrow::Int64Type>(arrowType, *type);
            case NYdb::EPrimitiveType::Uint64:
                return AppendPrimitives<arrow::UInt64Type>(arrowType, *type);
            case NYdb::EPrimitiveType::Float:
                return AppendPrimitives<arrow::FloatType>(arrowType, *type);
            case NYdb::EPrimitiveType::Double:
                return AppendPrimitives<arrow::DoubleType>(arrowType, *type);
            case NYdb::EPrimitiveType::Date:
                return AppendPrimitives<arrow::UInt16Type>(arrowType, *type);
            case NYdb::EPrimitiveType::Datetime:
                return AppendPrimitives<arrow::UInt32Type>(arrowType, *type);
            //case NYdb::EPrimitiveType::Timestamp:
            //    return AppendPrimitives<arrow::UInt64Type>(arrowType, *type);
            case NYdb::EPrimitiveType::String:
                return AppendStrings<arrow::BinaryType>(arrowType, *type);
            case NYdb::EPrimitiveType::Utf8:
                return AppendStrings<arrow::StringType>(arrowType, *type);
            case NYdb::EPrimitiveType::Json:
                return AppendJsons(arrowType, *type);

            case NYdb::EPrimitiveType::Timestamp:
            case NYdb::EPrimitiveType::Bool:
            case NYdb::EPrimitiveType::Interval:
            case NYdb::EPrimitiveType::Date32:
            case NYdb::EPrimitiveType::Datetime64:
            case NYdb::EPrimitiveType::Timestamp64:
            case NYdb::EPrimitiveType::Interval64:
            case NYdb::EPrimitiveType::TzDate:
            case NYdb::EPrimitiveType::TzDatetime:
            case NYdb::EPrimitiveType::TzTimestamp:
            case NYdb::EPrimitiveType::Yson:
            case NYdb::EPrimitiveType::JsonDocument:
            case NYdb::EPrimitiveType::Uuid:
            case NYdb::EPrimitiveType::DyNumber:
                break;
        }
        return {};
    }

    template <typename ArrowType>
    std::shared_ptr<arrow::Array> AppendPrimitives(arrow::DataType& type, NYdb::EPrimitiveType primType) {
        using CType = typename arrow::TypeTraits<ArrowType>::CType;
        using TBuilder = typename arrow::TypeTraits<ArrowType>::BuilderType;

        if (type.id() != ArrowType().id()) {
            return {};
        }

        if (Cache.contains(primType)) {
            return Cache[primType];
        }

        std::vector<CType> vec(NumRows);
        for (size_t i = 0; i < NumRows; ++i) {
            vec[i] = Seed + i;
        }

        TBuilder typedBuilder;
        if (!typedBuilder.Reserve(NumRows).ok()
            || !typedBuilder.AppendValues(vec).ok()) {
            return {};
        }
        std::shared_ptr<arrow::Array> array;
        if (!typedBuilder.Finish(&array).ok()) {
            return {};
        }
        Cache[primType] = array;
        return array;
    }

    template <typename ArrowType>
    std::shared_ptr<arrow::Array> AppendStrings(arrow::DataType& type, NYdb::EPrimitiveType primType) {
        using TBuilder = typename arrow::TypeTraits<ArrowType>::BuilderType;

        if (type.id() != ArrowType().id()) {
            return {};
        }

        if (Cache.contains(primType)) {
            return Cache[primType];
        }

        ui64 numBytes = 0;
        std::vector<std::string> vec;
        vec.reserve(NumRows);
        for (size_t i = 0; i < NumRows; ++i) {
            TString s = ToString(Seed + i);
            vec.push_back(s);
            numBytes += vec.back().size();
        }

        TBuilder typedBuilder;
        if (!typedBuilder.Reserve(NumRows).ok()
            || !typedBuilder.ReserveData(numBytes).ok()
            || !typedBuilder.AppendValues(vec).ok()) {
            return {};
        }
        std::shared_ptr<arrow::Array> array;
        if (!typedBuilder.Finish(&array).ok()) {
            return {};
        }
        Cache[primType] = array;
        return array;
    }

    std::shared_ptr<arrow::Array> AppendJsons(arrow::DataType& type, NYdb::EPrimitiveType primType) {
        using TBuilder = typename arrow::TypeTraits<arrow::StringType>::BuilderType;

        if (type.id() != arrow::Type::STRING) {
            return {};
        }

        if (Cache.contains(primType)) {
            return Cache[primType];
        }

        ui64 numBytes = 0;
        std::vector<std::string> vec;
        vec.reserve(NumRows);
        for (size_t i = 0; i < NumRows; ++i) {
            TString s = ToString(Seed + i);
            vec.push_back("{\"a\":\"" + std::string(s.data(), s.size()) + "\"}");
            numBytes += vec.back().size();
        }

        TBuilder typedBuilder;
        if (!typedBuilder.Reserve(NumRows).ok()
            || !typedBuilder.ReserveData(numBytes).ok()
            || !typedBuilder.AppendValues(vec).ok()) {
            return {};
        }
        std::shared_ptr<arrow::Array> array;
        if (!typedBuilder.Finish(&array).ok()) {
            return {};
        }
        Cache[primType] = array;
        return array;
    }

    static bool IsAllowedType(const NYdb::TType& ydbType) {
        auto type = ExtractPrimitiveType(ydbType);
        if (!type) {
            return false;
        }

        switch(*type) {
            case NYdb::EPrimitiveType::Int8:
            case NYdb::EPrimitiveType::Uint8:
            case NYdb::EPrimitiveType::Int16:
            case NYdb::EPrimitiveType::Uint16:
            case NYdb::EPrimitiveType::Int32:
            case NYdb::EPrimitiveType::Uint32:
            case NYdb::EPrimitiveType::Int64:
            case NYdb::EPrimitiveType::Uint64:
            case NYdb::EPrimitiveType::Float:
            case NYdb::EPrimitiveType::Double:
            case NYdb::EPrimitiveType::Date:
            case NYdb::EPrimitiveType::Datetime:
            //case NYdb::EPrimitiveType::Timestamp:
            case NYdb::EPrimitiveType::String:
            case NYdb::EPrimitiveType::Utf8:
            case NYdb::EPrimitiveType::Json:
                return true;
            // TODO: others
            default:
                break;
        }
        return false;
    }

    static std::shared_ptr<arrow::DataType> ArrowType(const NYdb::TType& ydbType) {
        auto type = ExtractPrimitiveType(ydbType);
        if (!type) {
            return {};
        }

        switch(*type) {
            case NYdb::EPrimitiveType::Int8:
                return arrow::int8();
            case NYdb::EPrimitiveType::Uint8:
                return arrow::uint8();
            case NYdb::EPrimitiveType::Int16:
                return arrow::int16();
            case NYdb::EPrimitiveType::Uint16:
                return arrow::uint16();
            case NYdb::EPrimitiveType::Int32:
                return arrow::int32();
            case NYdb::EPrimitiveType::Uint32:
                return arrow::uint32();
            case NYdb::EPrimitiveType::Int64:
                return arrow::int64();
            case NYdb::EPrimitiveType::Uint64:
                return arrow::uint64();
            case NYdb::EPrimitiveType::Float:
                return arrow::float32();
            case NYdb::EPrimitiveType::Double:
                return arrow::float64();
            case NYdb::EPrimitiveType::Date:
                return arrow::uint16();
            case NYdb::EPrimitiveType::Datetime:
                return arrow::uint32();
            //case NYdb::EPrimitiveType::Timestamp:
            //    return arrow::uint64();
            case NYdb::EPrimitiveType::String:
                return arrow::binary();
            case NYdb::EPrimitiveType::Utf8:
                return arrow::utf8();
            case NYdb::EPrimitiveType::Json:
                return arrow::utf8();
            default:
                break;
        }
        return {};
    }

private:
    ui64 Seed;
    ui64 NumRows;

    THashMap<NYdb::EPrimitiveType, std::shared_ptr<arrow::Array>> Cache;
};

std::vector<std::shared_ptr<arrow::Field>> MakeArrowFields(const std::vector<NYdb::NTable::TTableColumn>& columns) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(columns.size());
    for (auto& col : columns) {
        std::string colName(col.Name.data(), col.Name.size());
        fields.emplace_back(std::make_shared<arrow::Field>(colName, TColumnGenerator::ArrowType(col.Type)));
    }
    return fields;
}

std::shared_ptr<arrow::Schema> ToArrowSchema(const std::vector<NYdb::NTable::TTableColumn>& columns) {
    return std::make_shared<arrow::Schema>(MakeArrowFields(columns));
}

inline
TStatus MakeStatus(EStatus code = EStatus::SUCCESS, const TString& error = {}) {
    NYdb::NIssue::TIssues issues;
    if (error) {
        issues.AddIssue(NYdb::NIssue::TIssue(error));
    }
    return TStatus(code, std::move(issues));
}

TStatus WaitForQueue(const size_t maxQueueSize, std::vector<TAsyncStatus>& inFlightRequests) {
    while (!inFlightRequests.empty() && inFlightRequests.size() >= maxQueueSize) {
        NThreading::WaitAny(inFlightRequests).Wait();
        ui32 delta = 0;
        for (ui32 i = 0; i + delta < inFlightRequests.size();) {
            if (inFlightRequests[i].HasValue() || inFlightRequests[i].HasException()) {
                auto status = inFlightRequests[i].ExtractValueSync();
                if (!status.IsSuccess()) {
                    return status;
                }
                ++delta;
                inFlightRequests[i] = inFlightRequests[inFlightRequests.size() - delta];
            } else {
                ++i;
            }
        }
        inFlightRequests.resize(inFlightRequests.size() - delta);
    }

    return MakeStatus();
}

} // namespace


TGenerateClient::TGenerateClient(const TDriver& driver, const TClientCommand::TConfig& rootConfig)
    : OperationClient(std::make_shared<NOperation::TOperationClient>(driver))
    , SchemeClient(std::make_shared<NScheme::TSchemeClient>(driver))
    , TableClient(std::make_shared<NTable::TTableClient>(driver))
{
    RetrySettings
        .MaxRetries(TGenerateSettings::MaxRetries)
        .Idempotent(true)
        .Verbose(rootConfig.IsVerbose());
}

TStatus TGenerateClient::Generate(const TString& dbPath, const TGenerateSettings& settings) {
    TMaybe<NTable::TTableDescription> tableDescription;
    auto resultStatus = NDump::DescribeTable(*TableClient, dbPath, tableDescription);
    if (!resultStatus.IsSuccess() || !tableDescription) {
        return MakeStatus(EStatus::SCHEME_ERROR,
            TStringBuilder() << "Cannot describe table " << dbPath);
    }

    std::vector<NYdb::NTable::TTableColumn> tableColumns = tableDescription->GetTableColumns();
    for (auto& col : tableColumns) {
        if (!TColumnGenerator::IsAllowedType(col.Type)) {
            return MakeStatus(EStatus::SCHEME_ERROR,
                TStringBuilder() << "Generator does not support type: " << NYdb::FormatType(col.Type));
        }
    }

    UpsertSettings
        .OperationTimeout(settings.OperationTimeout_)
        .ClientTimeout(settings.ClientTimeout_);

    std::mutex progressWriteLock;
    std::atomic<ui64> globalProgress{0};

    TProgressBar progressBar(100);

    auto writeProgress = [&](ui64 addition) {
        globalProgress.fetch_add(addition);
        ui64 globalProgressValue = globalProgress.load();

        std::lock_guard<std::mutex> lock(progressWriteLock);
        progressBar.SetProgress(static_cast<double>(globalProgressValue) * 100 / settings.RowsToGenerate_);
    };

    // Default to a no-op so the callback is always safely invokable;
    // calling an empty std::function would throw std::bad_function_call.
    ProgressCallbackFunc progressCallback = [](ui64, ui64) {};

    if (IsStdoutInteractive()) {
        progressCallback = [&](ui64 rows, ui64) mutable {
            writeProgress(rows);
        };
    }

    auto start = TInstant::Now();

    auto result = Upsert(dbPath, tableColumns, settings, progressCallback);
    if (!result.IsSuccess()) {
        return result;
    }

    auto finish = TInstant::Now();
    auto duration = finish - start;
    Cout << "Elapsed: " << duration.SecondsFloat() << " sec\n";

    return MakeStatus(EStatus::SUCCESS);
}

TStatus TGenerateClient::Upsert(const TString& dbPath,
                                const std::vector<NYdb::NTable::TTableColumn>& tableColumns,
                                const TGenerateSettings& settings,
                                ProgressCallbackFunc& progressCallback) {
    THolder<IThreadPool> pool = CreateThreadPool(settings.Threads_);

    const ui64 numRows = settings.RowsPerRequest_;
    std::vector<TAsyncStatus> inFlightRequests;
    ui64 seed = 0;

    std::shared_ptr<arrow::Schema> schema = ToArrowSchema(tableColumns);
    const TString strSchema = NYdb_cli::NArrow::SerializeSchema(*schema);

    while (seed < settings.RowsToGenerate_) {
        TString errorString;
        auto batch = GenerateBatch(tableColumns, schema, settings.MaxBytesPerRequest, numRows, seed, errorString);
        if (!batch) {
            return MakeStatus(EStatus::SCHEME_ERROR, TStringBuilder() << "Cannot generate batch: " << errorString);
        }

        auto upsertBatch = [&, batch = std::move(batch)]() {
            auto value = UpsertArrowBuffer(dbPath, NYdb_cli::NArrow::SerializeBatchNoCompression(batch), strSchema);
            auto status = value.ExtractValueSync();
            if (!status.IsSuccess()) {
                return status;
            }

            progressCallback(batch->num_rows(), settings.RowsToGenerate_);
            return MakeStatus();
        };

        inFlightRequests.push_back(NThreading::Async(upsertBatch, *pool));

        auto status = WaitForQueue(settings.MaxInFlightRequests_, inFlightRequests);
        if (!status.IsSuccess()) {
            return status;
        }
    }

    return WaitForQueue(0, inFlightRequests);
}

inline
TAsyncStatus TGenerateClient::UpsertArrowBuffer(const TString& dbPath, const TString& buffer, const TString& strSchema) {
    auto upsert = [this, dbPath, buffer, strSchema](NYdb::NTable::TTableClient& tableClient) -> TAsyncStatus {
        return tableClient.BulkUpsert(dbPath, NTable::EDataFormat::ApacheArrow, buffer, strSchema, UpsertSettings)
            .Apply([](const NYdb::NTable::TAsyncBulkUpsertResult& bulkUpsertResult) {
                NYdb::TStatus status = bulkUpsertResult.GetValueSync();
                return NThreading::MakeFuture(status);
            });
        };
    return TableClient->RetryOperation(upsert, RetrySettings);
}

std::shared_ptr<arrow::RecordBatch> TGenerateClient::GenerateBatch(const std::vector<NYdb::NTable::TTableColumn>& tableColumns,
        std::shared_ptr<arrow::Schema>& schema, ui64 bytesPerRequest, ui64 numRows, ui64& seed, TString& errorString) {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(schema->num_fields());

    TColumnGenerator columnsGenerator(seed, numRows);
    for (i64 i = 0; i < schema->num_fields(); ++i) {
        auto column = columnsGenerator.AppendGenerated(*schema->field(i)->type(), tableColumns[i].Type);
        if (!column) {
            errorString = "Cannot generate column";
            return {};
        }
        columns.push_back(column);
    }

    std::shared_ptr<arrow::RecordBatch> batch = arrow::RecordBatch::Make(schema, numRows, columns);
    if (!batch) {
        errorString = "Cannot make batch";
        return {};
    }

    // TODO: reuse batch tail
    while (batch->num_rows() > 1 && NYdb_cli::NArrow::GetBatchDataSize(batch) > bytesPerRequest) {
        batch = batch->Slice(0, batch->num_rows() / 2);
    }

    seed += batch->num_rows();

    if (!batch->Validate().ok()) {
        errorString = "Invalid batch generated";
        return {};
    }

    return batch;
}

}
