#pragma once
#include "test_client.h"
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <library/cpp/yson/writer.h>

namespace NKikimr::Tests::NCommon {

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
