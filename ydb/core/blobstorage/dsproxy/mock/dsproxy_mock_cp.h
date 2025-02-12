#pragma once

#include "defs.h"
#include <ydb/core/base/blobstorage.h>

namespace NKikimr::NFake {

    class TProxyDS;

    class TProxyDSCP {
        TMutex Mutex;
        TMap<TGroupId, TProxyDS*> Models;
    public:
        TProxyDSCP();

        void AddMock(TGroupId groupId, TProxyDS* model);
        void RemoveMock(TGroupId groupId);

        TProxyDS* GetMock(TGroupId groupId);

        TMap<TGroupId, TProxyDS*> GetMocks();
    };

    std::unique_ptr<TProxyDSCP> CreateProxyDSCP();
}
