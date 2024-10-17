#pragma once
#include "test_client.h"
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <library/cpp/yson/writer.h>

namespace NKikimr::NKqp {
class TKikimrRunner;
}

namespace NKikimr::Tests::NCommon {

class TLoggerInit {
public:
    static const std::vector<NKikimrServices::EServiceKikimr> KqpServices;
    static const std::vector<NKikimrServices::EServiceKikimr> CSServices;
private:
    NActors::TTestActorRuntime* Runtime;
    NActors::NLog::EPriority Priority = NActors::NLog::EPriority::PRI_DEBUG;
    THashMap<TString, std::vector<NKikimrServices::EServiceKikimr>> Services;
public:
    TLoggerInit(NActors::TTestActorRuntime* runtime)
        : Runtime(runtime) {
        Services.emplace("KQP", KqpServices);
        Services.emplace("CS", CSServices);
    }
    TLoggerInit(NActors::TTestActorRuntime& runtime)
        : Runtime(&runtime) {
    }
    TLoggerInit(NKqp::TKikimrRunner& kikimr);
    void Initialize();
    ~TLoggerInit() {
        Initialize();
    }
    TLoggerInit& Clear() {
        Services.clear();
        return *this;
    }
    TLoggerInit& SetComponents(const std::vector<NKikimrServices::EServiceKikimr> services, const TString& name) {
        Services[name] = services;
        return *this;
    }
    TLoggerInit& AddComponents(const std::vector<NKikimrServices::EServiceKikimr> services, const TString& name) {
        AFL_VERIFY(Services.emplace(name, services).second);
        return *this;
    }
    TLoggerInit& RemoveComponents(const TString& name) {
        Services.erase(name);
        return *this;
    }
    TLoggerInit& SetPriority(const NActors::NLog::EPriority priority) {
        Priority = priority;
        return *this;
    }
};

class THelper {
protected:
    void WaitForSchemeOperation(TActorId sender, ui64 txId);
    void PrintResultSet(const NYdb::TResultSet& resultSet, NYson::TYsonWriter& writer) const;

    void StartSchemaRequestTableServiceImpl(
        const TString& request, const bool expectSuccess, const bool waiting, const TString& authToken = "root@builtin") const;
    void StartSchemaRequestQueryServiceImpl(
        const TString& request, const bool expectSuccess, const bool waiting, const TString& authToken = "root@builtin") const;

    Tests::TServer& Server;
    bool UseQueryService = false;
public:
    THelper(TServer& server)
        : Server(server) {

    }

    void SetUseQueryService(bool use = true) {
        UseQueryService = use;
    }

    void DropTable(const TString& tablePath);

    void StartScanRequest(const TString& request, const bool expectSuccess, TVector<THashMap<TString, NYdb::TValue>>* result,
        const TString& authToken = "root@builtin") const;
    void StartDataRequest(
        const TString& request, const bool expectSuccess = true, TString* result = nullptr, const TString& authToken = "root@builtin") const;
    void StartSchemaRequest(
        const TString& request, const bool expectSuccess = true, const bool waiting = true, const TString& authToken = "root@builtin") const;
};
}
