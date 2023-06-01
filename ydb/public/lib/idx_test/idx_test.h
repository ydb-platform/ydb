#pragma once

#include <util/system/types.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <memory>

#include <library/cpp/json/json_value.h>

namespace NIdxTest {

class IDataProvider {
public:
    using TPtr = std::unique_ptr<IDataProvider>;
public:
    virtual ~IDataProvider() = default;

    virtual NYdb::NTable::TTableDescription GetTableSchema() = 0;
    virtual TMaybe<TVector<NYdb::TValue>> GetBatch(ui32 upload_id) = 0;
    virtual NYdb::TType GetRowType() = 0;
};

class IUploader {
public:
    using TPtr = std::unique_ptr<IUploader>;
public:
    virtual ~IUploader() = default;

    virtual void Run(IDataProvider::TPtr dataProvider) = 0;
};

class IWorkLoader {
public:
    using TPtr = std::unique_ptr<IWorkLoader>;
    enum ELoadCommand {
        LC_UPSERT = 1,
        LC_INSERT = 2,
        LC_UPDATE = 4,
        LC_UPDATE_ON = 8,
        LC_REPLACE = 16,
        LC_DELETE = 32,
        LC_DELETE_ON = 64,
        LC_SELECT = 128,
        LC_UPSERT_IF_UNIQ = 256,
        LC_ALTER_ADD_INDEX = 512,
        LC_ALTER_ADD_INDEX_WITH_DATA_COLUMN = 1024
    };
public:
    virtual ~IWorkLoader() = default;

    struct TRunSettings {
        size_t RunLimit;
        size_t Infly;
        size_t OpsPerTx;
    };

    virtual NJson::TJsonValue Run(const TString& tableName, ui32 loadCommands, const TRunSettings& settings) = 0;
};

class IProgressTracker {
public:
    using TPtr = std::unique_ptr<IProgressTracker>;
public:
    virtual ~IProgressTracker() = default;
    virtual void Start(const TString& progressString, const TString& freeMessage) = 0;
    virtual void Update(size_t progress) = 0;
    virtual void Finish(const TString& freeMessage) = 0;
};

class IChecker {
public:
    using TPtr = std::unique_ptr<IChecker>;
public:
    virtual ~IChecker() = default;

    virtual void Run(const TString& tableName) = 0;
};

struct TUploaderParams {
    ui32 ShardsCount;
};

IUploader::TPtr CreateUploader(NYdb::TDriver& driver, const TString& table, const TUploaderParams& params);
IDataProvider::TPtr CreateDataProvider(ui32 rowsCount, ui32 shardsCount, NYdb::NTable::TTableDescription tableDesc);
IWorkLoader::TPtr CreateWorkLoader(NYdb::TDriver& driver, IProgressTracker::TPtr&& progressTracker = IProgressTracker::TPtr(nullptr));
IChecker::TPtr CreateChecker(NYdb::TDriver& driver, IProgressTracker::TPtr&& progressTracker = IProgressTracker::TPtr(nullptr));

IProgressTracker::TPtr CreateStderrProgressTracker(size_t subsampling, const TString& title = "");

} // namespace NIdxTest
