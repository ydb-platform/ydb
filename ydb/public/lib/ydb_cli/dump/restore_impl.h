#pragma once

#include "dump.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/cms/cms.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/coordination.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/import/import.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/rate_limiter/rate_limiter.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/folder/path.h>
#include <util/generic/hash_set.h>
#include <util/stream/fwd.h>
#include <util/string/builder.h>

namespace NYdb::NDump {

extern const char DOC_API_TABLE_VERSION_ATTR[23];
extern const char DOC_API_REQUEST_TYPE[22];

class TRestoreClient;

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
    IDataAccumulator* OriginAccumulator = nullptr;

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

struct TDelayedRestoreCall {
    using TSimplePath = TString;

    struct TTwoComponentPath {
        TString RestoreRoot;
        TString RelativeToRestoreRoot;
    };

    NScheme::ESchemeEntryType Type;
    TFsPath FsPath;
    std::variant<TSimplePath, TTwoComponentPath> DbPath;
    TRestoreSettings Settings;
    bool IsAlreadyExisting;

    TDelayedRestoreCall(
        NScheme::ESchemeEntryType type,
        TFsPath fsPath,
        TString dbPath,
        TRestoreSettings settings,
        bool isAlreadyExisting
    );

    TDelayedRestoreCall(
        NScheme::ESchemeEntryType type,
        TFsPath fsPath,
        TString dbRestoreRoot,
        TString dbPathRelativeToRestoreRoot,
        TRestoreSettings settings,
        bool isAlreadyExisting
    );

    int GetOrder() const;
};

class TDelayedRestoreManager {
    TVector<TDelayedRestoreCall> Calls;
    TRestoreClient* Client = nullptr;

    TRestoreResult Restore(const TDelayedRestoreCall& call);
    static bool ShouldRetry(const TRestoreResult& result, NScheme::ESchemeEntryType type);
    TRestoreResult RestoreWithRetries(TVector<TDelayedRestoreCall>&& calls);

public:
    void SetClient(TRestoreClient& client);
    TRestoreResult RestoreDelayed();

    template <typename... Args>
    void Add(NScheme::ESchemeEntryType type, Args&&... args) {
        Calls.emplace_back(type, std::forward<Args>(args)...);
    }
};

} // NPrivate

class TRestoreClient {
    TRestoreResult RestoreFolder(const TFsPath& fsPath, const TString& dbRestoreRoot, const TString& dbPathRelativeToRestoreRoot, const TRestoreSettings& settings, const THashSet<TString>& oldEntries);
    TRestoreResult RestoreEmptyDir(const TFsPath& fsPath, const TString& dbPath, const TRestoreSettings& settings, bool isAlreadyExisting);
    TRestoreResult RestoreTable(const TFsPath& fsPath, const TString& dbPath, const TRestoreSettings& settings, bool isAlreadyExisting);
    TRestoreResult RestoreView(const TFsPath& fsPath, const TString& dbRestoreRoot, const TString& dbPathRelativeToRestoreRoot, const TRestoreSettings& settings, bool isAlreadyExisting);
    TRestoreResult RestoreTopic(const TFsPath& fsPath, const TString& dbPath, const TRestoreSettings& settings, bool isAlreadyExisting);
    TRestoreResult RestoreReplication(const TFsPath& fsPath, const TString& dbRestoreRoot, const TString& dbPathRelativeToRestoreRoot, const TRestoreSettings& settings, bool isAlreadyExisting);
    TRestoreResult RestoreCoordinationNode(const TFsPath& fsPath, const TString& dbPath, const TRestoreSettings& settings, bool isAlreadyExisting);
    TRestoreResult RestoreDependentResources(const TFsPath& fsPath, const TString& dbPath);
    TRestoreResult RestoreRateLimiter(const TFsPath& fsPath, const TString& coordinationNodePath, const TString& resourcePath);
    TRestoreResult RestoreExternalDataSource(const TFsPath& fsPath, const TString& dbPath, const TRestoreSettings& settings, bool isAlreadyExisting);
    TRestoreResult RestoreExternalTable(const TFsPath& fsPath, const TString& dbPath, const TRestoreSettings& settings, bool isAlreadyExisting);

    TRestoreResult CheckSchema(const TString& dbPath, const NTable::TTableDescription& desc);
    TRestoreResult RestoreData(const TFsPath& fsPath, const TString& dbPath, const TRestoreSettings& settings, const NTable::TTableDescription& desc, ui32 partitionCount);
    TRestoreResult RestoreIndexes(const TString& dbPath, const NTable::TTableDescription& desc);
    TRestoreResult RestoreChangefeeds(const TFsPath& path, const TString& dbPath);
    TRestoreResult RestorePermissions(const TFsPath& fsPath, const TString& dbPath, const TRestoreSettings& settings, bool isAlreadyExisting);
    TRestoreResult RestoreConsumers(const TString& topicPath, const std::vector<NTopic::TConsumer>& consumers);

    TRestoreResult FindClusterRootPath();
    TRestoreResult ReplaceClusterRoot(TString& outPath);
    TRestoreResult WaitForAvailableNodes(const TString& database, TDuration waitDuration);

    TRestoreResult RestoreClusterRoot(const TFsPath& fsPath);
    TRestoreResult RestoreDatabases(const TFsPath& fsPath, const TRestoreClusterSettings& settings);
    TRestoreResult RestoreUsers(NTable::TTableClient& client, const TFsPath& fsPath, const TString& dbPath);
    TRestoreResult RestoreGroups(NTable::TTableClient& client, const TFsPath& fsPath, const TString& dbPath);
    TRestoreResult RestoreGroupMembers(NTable::TTableClient& client, const TFsPath& fsPath, const TString& dbPath);

    TRestoreResult RestoreClusterImpl(const TString& fsPath, const TRestoreClusterSettings& settings);
    TRestoreResult RestoreDatabaseImpl(const TString& fsPath, const TRestoreDatabaseSettings& settings);
    TRestoreResult RestorePermissionsImpl(NScheme::TSchemeClient& client, const TFsPath& fsPath, const TString& dbPath);

    THolder<NPrivate::IDataWriter> CreateDataWriter(const TString& dbPath, const TRestoreSettings& settings,
        const NTable::TTableDescription& desc, ui32 partitionCount, const TVector<THolder<NPrivate::IDataAccumulator>>& accumulators);
    TRestoreResult CreateDataAccumulators(TVector<THolder<NPrivate::IDataAccumulator>>& outAccumulators,
        const TString& dbPath, const TRestoreSettings& settings, const NTable::TTableDescription& desc,
        ui32 dataFilesCount);

    TRestoreResult CheckSecretExistence(const TString& secretName);

public:
    explicit TRestoreClient(const TDriver& driver, const std::shared_ptr<TLog>& log);

    TRestoreResult Restore(const TString& fsPath, const TString& dbPath, const TRestoreSettings& settings = {});
    TRestoreResult RestoreCluster(const TString& fsPath, const TRestoreClusterSettings& settings = {});
    TRestoreResult RestoreDatabase(const TString& fsPath, const TRestoreDatabaseSettings& settings = {});

private:
    NImport::TImportClient ImportClient;
    NOperation::TOperationClient OperationClient;
    NScheme::TSchemeClient SchemeClient;
    NTable::TTableClient TableClient;
    NTopic::TTopicClient TopicClient;
    NCoordination::TClient CoordinationNodeClient;
    NRateLimiter::TRateLimiterClient RateLimiterClient;
    NQuery::TQueryClient QueryClient;
    NCms::TCmsClient CmsClient;
    std::shared_ptr<TLog> Log;
    // Used to creating child drivers with different database settings.
    TDriverConfig DriverConfig;
    TString ClusterRootPath;
    NPrivate::TDelayedRestoreManager DelayedRestoreManager;

    friend class NPrivate::TDelayedRestoreManager;
}; // TRestoreClient

} // NYdb::NDump
