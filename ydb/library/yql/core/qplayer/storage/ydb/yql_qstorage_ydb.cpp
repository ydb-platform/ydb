#include "yql_qstorage_ydb.h"

#include <ydb/library/yql/core/qplayer/storage/memory/yql_qstorage_memory.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <library/cpp/digest/old_crc/crc.h>

#include <util/string/printf.h>

namespace NYql {

namespace {

constexpr ui32 DefaultPartBytes = 100000;
constexpr ui32 DefaultMaxBatchSize = 1000000;
constexpr ui32 DefaultRetriesCount = 20;

NYdb::TDriver MakeDriver(const TYdbQStorageSettings& settings) {
    auto driverConfig = NYdb::TDriverConfig()
        .SetEndpoint(settings.Endpoint)
        .SetDatabase(settings.Database)
        .SetAuthToken(settings.Token);
    NYdb::TDriver driver(driverConfig);
    return driver;
}

void ThrowOnError(const NYdb::TStatus& status) {
    if (!status.IsSuccess()) {
        throw yexception() << status;
    }
}

void ProcessString(const TString& str, ui64& totalBytes, ui64& checksum) {
    ui32 length = str.Size();
    checksum = crc64(&length, sizeof(length), checksum);
    checksum = crc64(str.Data(), length, checksum);
    totalBytes += length;
}

class TWriter : public IQWriter {
public:
    TWriter(const TYdbQStorageSettings& settings, const TString& operationId, const TQWriterSettings& writerSettings)
        : Settings_(settings)
        , FullOperationId_(settings.OperationIdPrefix + operationId)
        , Storage_(MakeMemoryQStorage())
        , Writer_(Storage_->MakeWriter("", writerSettings))
        , WrittenAt_(writerSettings.WrittenAt.GetOrElse(Now()))
    {}

    NThreading::TFuture<void> Put(const TQItemKey& key, const TString& value) final {
        return Writer_->Put(key, value);
    }

    NThreading::TFuture<void> Commit() final {
        Writer_->Commit().GetValueSync();
        SaveTable(Storage_->MakeIterator("", {}));
        return NThreading::MakeFuture();
    }

private:
    void SaveTable(const IQIteratorPtr& iterator) {
        auto driver = MakeDriver(Settings_);
        NYdb::NTable::TTableClient tableClient(driver);

        TMaybe<NYdb::TValueBuilder> rows;
        rows.ConstructInPlace();
        rows->BeginList();
        const auto partBytes = GetPartBytes();
        const auto maxBatchSize = GetMaxBatchSize();
        ui64 currentBatchSize = 0;
        ui32 currentPart = 0;
        ui32 valueOffset = 0;
        TQItem currentItem;
        bool hasMoreParts = false;

        ui64 totalItems = 0;
        ui64 totalBytes = 0;
        ui64 checksum = 0;

        for (;;) {
            TString data;
            if (hasMoreParts) {
                ++currentPart;
                if (currentItem.Value.size() - valueOffset > partBytes) {
                    data = currentItem.Value.substr(valueOffset, partBytes);
                    valueOffset += partBytes;
                } else {
                    data = currentItem.Value.substr(valueOffset);
                    hasMoreParts = false;
                }
            } else {
                auto item = iterator->Next().GetValueSync();
                if (!item) {
                    break;
                }

                currentItem = *item;
                ++totalItems;
                ProcessString(currentItem.Key.Component, totalBytes, checksum);
                ProcessString(currentItem.Key.Label, totalBytes, checksum);
                ProcessString(currentItem.Value, totalBytes, checksum);
                valueOffset = 0;
                currentPart = 0;
                if (currentItem.Value.size() > partBytes) {
                    hasMoreParts = true;
                    data = currentItem.Value.substr(0, partBytes);
                    valueOffset = partBytes;
                } else {
                    data = currentItem.Value;
                }
            }

            rows->AddListItem()
                .BeginStruct()
                .AddMember("operation_id").OptionalString(FullOperationId_)
                .AddMember("written_at").OptionalTimestamp(WrittenAt_)
                .AddMember("item_index").OptionalUint32(totalItems - 1)
                .AddMember("part").OptionalUint32(currentPart)
                .AddMember("component").String(currentPart ? "" : currentItem.Key.Component)
                .AddMember("label").String(currentPart ? "" : currentItem.Key.Label)
                .AddMember("data").String(data)
                .EndStruct();

            currentBatchSize += FullOperationId_.size() + sizeof(ui64) +
                sizeof(ui32) + currentItem.Key.Component.size() +
                currentItem.Key.Label.size() + data.size();
            if (currentBatchSize >= maxBatchSize) {
                FlushBatch(rows, tableClient);
                currentBatchSize = 0;
            }
        }

        if (currentBatchSize > 0) {
            FlushBatch(rows, tableClient);
        }

        auto table = Settings_.TablesPrefix + "operations";
        NYdb::TParamsBuilder paramsBuilder;
        paramsBuilder.AddParam("$operation_id").String(FullOperationId_).Build();
        paramsBuilder.AddParam("$total_items").Uint64(totalItems).Build();
        paramsBuilder.AddParam("$total_bytes").Uint64(totalBytes).Build();
        paramsBuilder.AddParam("$checksum").Uint64(checksum).Build();
        paramsBuilder.AddParam("$written_at").Timestamp(WrittenAt_).Build();

        NYdb::NTable::TRetryOperationSettings writeRetrySettings;
        writeRetrySettings
                .Idempotent(true)
                .MaxRetries(GetRetriesCount());

        ThrowOnError(tableClient.RetryOperationSync([table, params = paramsBuilder.Build()](NYdb::NTable::TSession session) {
            auto query = Sprintf(R"(
                --!syntax_v1
                DECLARE $operation_id AS String;
                DECLARE $total_items AS Uint64;
                DECLARE $total_bytes AS Uint64;
                DECLARE $checksum AS Uint64;
                DECLARE $written_at AS Timestamp;

                UPSERT INTO `%s` (operation_id, total_items, total_bytes, checksum, written_at)
                VALUES ($operation_id, $total_items, $total_bytes, $checksum, $written_at)
            )", table.c_str());

            return session.ExecuteDataQuery(query,
                NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW())
                    .CommitTx(), params).GetValueSync();
        }, writeRetrySettings));

        driver.Stop(true);
    }

    void FlushBatch(TMaybe<NYdb::TValueBuilder>& rows, NYdb::NTable::TTableClient& tableClient) {
        rows->EndList();

        auto table = Settings_.Database + "/" + Settings_.TablesPrefix + "blobs";
        auto bulkUpsertOperation = [table, rowsValue = rows->Build()](NYdb::NTable::TTableClient& tableClient) {
            NYdb::TValue r = rowsValue;
            auto status = tableClient.BulkUpsert(table, std::move(r));
            return status.GetValueSync();
        };

        NYdb::NTable::TRetryOperationSettings writeRetrySettings;
        writeRetrySettings
            .Idempotent(true)
            .MaxRetries(GetRetriesCount());

        ThrowOnError(tableClient.RetryOperationSync(bulkUpsertOperation, writeRetrySettings));
        rows.Clear();
        rows.ConstructInPlace();
        rows->BeginList();
    }

    ui64 GetPartBytes() const {
        return Settings_.PartBytes.GetOrElse(DefaultPartBytes);
    }

    ui32 GetRetriesCount() const {
        return Settings_.MaxRetries.GetOrElse(DefaultRetriesCount);
    }

    ui64 GetMaxBatchSize() const {
        return Settings_.MaxBatchSize.GetOrElse(DefaultMaxBatchSize);
    }

private:
    const TYdbQStorageSettings Settings_;
    const TString FullOperationId_;
    const IQStoragePtr Storage_;
    const IQWriterPtr Writer_;
    const TInstant WrittenAt_;
};

class TStorage : public IQStorage {
public:
    TStorage(const TYdbQStorageSettings& settings)
        : Settings_(settings)
    {
    }

    IQWriterPtr MakeWriter(const TString& operationId, const TQWriterSettings& settings) const final {
        return std::make_shared<TWriter>(Settings_, operationId, settings);
    }

    IQReaderPtr MakeReader(const TString& operationId, const TQReaderSettings& settings) const final {
        Y_UNUSED(settings);
        auto memory = MakeMemoryQStorage();
        LoadTable(operationId, memory);
        return memory->MakeReader("", {});
    }

    IQIteratorPtr MakeIterator(const TString& operationId, const TQIteratorSettings& settings) const final {
        auto memory = MakeMemoryQStorage();
        LoadTable(operationId, memory);
        return memory->MakeIterator("", settings);
    }

private:
    void LoadTable(const TString& operationId, const IQStoragePtr& memory) const {
        auto driver = MakeDriver(Settings_);
        NYdb::NTable::TTableClient tableClient(driver);
        
        auto operationsTable  = Settings_.TablesPrefix + "operations";
        auto fullOperationId = Settings_.OperationIdPrefix + operationId;

        NYdb::NTable::TRetryOperationSettings readRetrySettings;
        readRetrySettings
            .Idempotent(true)
            .MaxRetries(GetRetriesCount());

        NYdb::TParamsBuilder paramsBuilder;
        paramsBuilder.AddParam("$operation_id").String(fullOperationId).Build();

        TMaybe<NYdb::TResultSet> res;
        ThrowOnError(tableClient.RetryOperationSync([&res, operationsTable, params = paramsBuilder.Build()](NYdb::NTable::TSession session) {
            auto query = Sprintf(R"(
                --!syntax_v1
                DECLARE $operation_id AS String;

                SELECT operation_id, total_items, total_bytes, checksum, written_at
                FROM `%s` WHERE operation_id = $operation_id
            )", operationsTable.c_str());

            auto r = session.ExecuteDataQuery(query,
                NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW())
                    .CommitTx(), params).GetValueSync();
            if (r.IsSuccess()) {
                res = r.GetResultSet(0);
            }

            return r;
        }, readRetrySettings));

        if (res->RowsCount() == 0) {
            return;
        }

        NYdb::TResultSetParser parser(*res);
        parser.TryNextRow();
        auto loadedOperationId = parser.ColumnParser("operation_id").GetString();
        Y_ENSURE(loadedOperationId == fullOperationId);
        ui64 loadedTotalItems = parser.ColumnParser("total_items").GetUint64();
        ui64 loadedTotalBytes = parser.ColumnParser("total_bytes").GetUint64();
        ui64 loadedChecksum = parser.ColumnParser("checksum").GetUint64();
        TInstant writtenAt = parser.ColumnParser("written_at").GetTimestamp();

        TMaybe<NYdb::NTable::TTablePartIterator> tableIter;
        TString blobTable = Settings_.Database + "/" + Settings_.TablesPrefix + "blobs";

        const auto maxBatchSize = Settings_.MaxBatchSize.GetOrElse(DefaultMaxBatchSize);
        auto rtResult = tableClient.RetryOperationSync([&tableIter, maxBatchSize, blobTable, 
            fullOperationId, writtenAt, loadedTotalItems](NYdb::NTable::TSession session) {
            auto key1 = NYdb::TValueBuilder()
                .BeginTuple()
                    .AddElement().OptionalString(fullOperationId)
                    .AddElement().OptionalTimestamp(writtenAt)
                    .AddElement().OptionalUint32(0)
                .EndTuple()
                .Build();
            auto key2 = NYdb::TValueBuilder()
                .BeginTuple()
                    .AddElement().OptionalString(fullOperationId)
                    .AddElement().OptionalTimestamp(writtenAt)
                    .AddElement().OptionalUint32(loadedTotalItems)
                .EndTuple()
                .Build();
            auto from = NYdb::NTable::TKeyBound::Inclusive(key1);
            auto to = NYdb::NTable::TKeyBound::Exclusive(key2);
            auto settings = NYdb::NTable::TReadTableSettings()
                .BatchLimitBytes(maxBatchSize)
                .Ordered(true)
                .From(from)
                .To(to)
                .AppendColumns("operation_id")
                .AppendColumns("written_at")
                .AppendColumns("item_index")
                .AppendColumns("part")
                .AppendColumns("component")
                .AppendColumns("label")
                .AppendColumns("data");
            auto res = session.ReadTable(blobTable, settings).GetValueSync();
            if (res.IsSuccess()) {
                tableIter = res;
            }
            
            return res;
        }, readRetrySettings);
        ThrowOnError(rtResult);

        auto writer = memory->MakeWriter("", {});
        ui64 totalBytes = 0, totalItems = 0, checksum = 0;
        ui32 currentIndex = Max<ui32>(), currentPart = Max<ui32>();
        TQItemKey currentKey;
        TString currentValue;
        auto flushItem = [&]() {
            ++totalItems;
            ProcessString(currentKey.Component, totalBytes, checksum);
            ProcessString(currentKey.Label, totalBytes, checksum);
            ProcessString(currentValue, totalBytes, checksum);
            writer->Put(currentKey, currentValue).GetValueSync();
        };

        while (true) {
            auto tablePart = tableIter->ReadNext().GetValueSync();
            if (!tablePart.IsSuccess()) {
                if (tablePart.EOS()) {
                    break;
                }

                throw yexception() << NYdb::TStatus(tablePart);
            }

            auto parser = NYdb::TResultSetParser(tablePart.ExtractPart());
            while (parser.TryNextRow()) {
                auto index = *parser.ColumnParser("item_index").GetOptionalUint32();
                auto part = *parser.ColumnParser("part").GetOptionalUint32();
                if (index != currentIndex) {
                    if (currentIndex != Max<ui32>()) {
                        flushItem();
                    }

                    Y_ENSURE(part == 0);
                    ++currentIndex;
                    Y_ENSURE(index == currentIndex);
                    currentPart = 0;
                    currentKey.Component = *parser.ColumnParser("component").GetOptionalString();
                    currentKey.Label = *parser.ColumnParser("label").GetOptionalString();
                    currentValue = *parser.ColumnParser("data").GetOptionalString();
                } else {
                    ++currentPart;
                    Y_ENSURE(part == currentPart);
                    currentValue += *parser.ColumnParser("data").GetOptionalString();
                }
            }
        }

        if (currentIndex != Max<ui32>()) {
            flushItem();
        }

        Y_ENSURE(totalItems == loadedTotalItems);
        Y_ENSURE(totalBytes == loadedTotalBytes);
        Y_ENSURE(checksum == loadedChecksum);
        writer->Commit().GetValueSync();
    }

    ui32 GetRetriesCount() const {
        return Settings_.MaxRetries.GetOrElse(DefaultRetriesCount);
    }

private:
    const TYdbQStorageSettings Settings_;
};

}

IQStoragePtr MakeYdbQStorage(const TYdbQStorageSettings& settings) {
    return std::make_shared<TStorage>(settings);
}

}
