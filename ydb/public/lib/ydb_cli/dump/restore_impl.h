#pragma once

#include "dump.h"

#include <ydb/public/sdk/cpp/client/ydb_import/import.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_query/client.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

#include <util/folder/path.h>
#include <util/generic/hash_set.h>
#include <util/stream/fwd.h>
#include <util/string/builder.h>

namespace NYdb {
namespace NDump {

extern const char DOC_API_TABLE_VERSION_ATTR[23];
extern const char DOC_API_REQUEST_TYPE[22];

namespace NPrivate {

class TBatch;

class TLocation {
    friend class TBatch;
    TStringBuf File;
    ui64 LineNo;

public:
    explicit TLocation(TStringBuf file, ui64 lineNo);
    void Out(IOutputStream& out) const;
};

class TLine {
    TString Data;
    TLocation Location;

public:
    explicit TLine(TString&& data, TStringBuf file, ui64 lineNo);
    explicit TLine(TString&& data, const TLocation& location);

    inline const TString& GetData() const {
        return Data;
    }

    inline const TLocation& GetLocation() const {
        return Location;
    }

    inline operator TString() const {
        return Data;
    }

    inline operator TStringBuf() const {
        return Data;
    }

    inline auto size() const {
        return Data.size();
    }
};

class IDataAccumulator;

class TBatch {
    TStringBuilder Data;
    TVector<TLocation> Locations;
    IDataAccumulator* OriginAccumulator;

public:
    void Add(const TLine& line);
    TString GetLocation() const;

    inline const TString& GetData() const {
        return Data;
    }

    inline const TLocation& GetLocation(ui32 index) const {
        return Locations.at(index);
    }

    inline operator TString() const {
        return Data;
    }

    inline auto size() const {
        return Data.size();
    }

    inline void SetOriginAccumulator(IDataAccumulator* originAccumulator) {
        OriginAccumulator = originAccumulator;
    }

    inline IDataAccumulator* GetOriginAccumulator() const {
        return OriginAccumulator;
    }
};

class IDataAccumulator {
public:
    enum EStatus {
        OK,
        FULL,
        ERROR,
    };

    virtual ~IDataAccumulator() = default;
    virtual EStatus Check(const TLine& line) const = 0;
    virtual void Feed(TLine&& line) = 0;
    virtual bool Ready(bool force = false) const = 0;
    virtual TBatch GetData(bool force = false) = 0;
};

class IDataWriter {
public:
    virtual ~IDataWriter() = default;
    virtual bool Push(TBatch&& data) = 0;
    virtual void Wait() = 0;
};

} // NPrivate

class TRestoreClient {
    TRestoreResult RestoreFolder(const TFsPath& fsPath, const TString& dbRestoreRoot, const TString& dbPathRelativeToRestoreRoot, const TRestoreSettings& settings, const THashSet<TString>& oldEntries);
    TRestoreResult RestoreEmptyDir(const TFsPath& fsPath, const TString &dbPath, const TRestoreSettings& settings, bool isAlreadyExisting);
    TRestoreResult RestoreTable(const TFsPath& fsPath, const TString& dbPath, const TRestoreSettings& settings, bool isAlreadyExisting);
    TRestoreResult RestoreView(const TFsPath& fsPath, const TString& dbRestoreRoot, const TString& dbPathRelativeToRestoreRoot, const TRestoreSettings& settings, bool isAlreadyExisting);

    TRestoreResult CheckSchema(const TString& dbPath, const NTable::TTableDescription& desc);
    TRestoreResult RestoreData(const TFsPath& fsPath, const TString& dbPath, const TRestoreSettings& settings, const NTable::TTableDescription& desc);
    TRestoreResult RestoreIndexes(const TString& dbPath, const NTable::TTableDescription& desc);
    TRestoreResult RestoreChangefeeds(const TFsPath& path, const TString& dbPath);
    TRestoreResult RestorePermissions(const TFsPath& fsPath, const TString& dbPath, const TRestoreSettings& settings, bool isAlreadyExisting);
    TRestoreResult RestoreConsumers(const TString& topicPath, const TVector<NTopic::TConsumer>& consumers);

    THolder<NPrivate::IDataWriter> CreateDataWriter(const TString& dbPath, const TRestoreSettings& settings,
        const NTable::TTableDescription& desc, const TVector<THolder<NPrivate::IDataAccumulator>>& accumulators);
    TRestoreResult CreateDataAccumulators(TVector<THolder<NPrivate::IDataAccumulator>>& outAccumulators,
        const TString& dbPath, const TRestoreSettings& settings, const NTable::TTableDescription& desc,
        ui32 dataFilesCount);

public:
    explicit TRestoreClient(const TDriver& driver, const std::shared_ptr<TLog>& log);

    TRestoreResult Restore(const TString& fsPath, const TString& dbPath, const TRestoreSettings& settings = {});

private:
    NImport::TImportClient ImportClient;
    NOperation::TOperationClient OperationClient;
    NScheme::TSchemeClient SchemeClient;
    NTable::TTableClient TableClient;
    NTopic::TTopicClient TopicClient;
    NQuery::TQueryClient QueryClient;
    std::shared_ptr<TLog> Log;

    struct TRestoreViewCall {
        TFsPath FsPath;
        TString DbRestoreRoot;
        TString DbPathRelativeToRestoreRoot;
        TRestoreSettings Settings;
        bool IsAlreadyExisting;
    };
    // Views usually depend on other objects.
    // If the dependency is not created yet, then the view restoration will fail.
    // We retry failed view creation attempts until either all views are created, or the errors are persistent.
    TVector<TRestoreViewCall> ViewRestorationCalls;

}; // TRestoreClient

} // NDump
} // NYdb
