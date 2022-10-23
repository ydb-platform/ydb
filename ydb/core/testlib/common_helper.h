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
};
}
