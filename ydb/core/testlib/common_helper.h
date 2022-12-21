#pragma once
#include "test_client.h"

namespace NKikimr::Tests::NCommon {

class THelper {
protected:
    void WaitForSchemeOperation(TActorId sender, ui64 txId);

    Tests::TServer& Server;
public:
    THelper(TServer& server)
        : Server(server) {

    }

    void DropTable(const TString& tablePath);

    void StartDataRequest(const TString& request, const bool expectSuccess = true) const;
    void StartSchemaRequest(const TString& request, const bool expectSuccess = true, const bool waiting = true) const;
};
}
