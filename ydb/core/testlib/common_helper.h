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
    std::vector<std::vector<NKikimrServices::EServiceKikimr>> Services = {KqpServices, CSServices};
public:
    TLoggerInit(NActors::TTestActorRuntime* runtime)
        : Runtime(runtime) {
    }
    TLoggerInit(NActors::TTestActorRuntime& runtime)
        : Runtime(&runtime) {
    }
    TLoggerInit(NKqp::TKikimrRunner& kikimr);
    void Initialize();
    ~TLoggerInit() {
        Initialize();
    }
    TLoggerInit& SetComponents(const std::vector<NKikimrServices::EServiceKikimr> services) {
        Services = { services };
        return *this;
    }
    TLoggerInit& AddComponents(const std::vector<NKikimrServices::EServiceKikimr> services) {
        Services.emplace_back(services);
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

    Tests::TServer& Server;
public:
    THelper(TServer& server)
        : Server(server) {

    }

    void DropTable(const TString& tablePath);

    void StartScanRequest(const TString& request, const bool expectSuccess, TVector<THashMap<TString, NYdb::TValue>>* result) const;
    void StartDataRequest(const TString& request, const bool expectSuccess = true, TString* result = nullptr) const;
    void StartSchemaRequest(const TString& request, const bool expectSuccess = true, const bool waiting = true) const;
};
}
