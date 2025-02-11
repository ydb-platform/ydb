#pragma once

#include "defs.h"
#include <ydb/core/base/blobstorage.h>

namespace NKikimr::NFake {

    class TProxyDS;

    class TProxyDSCP {
        TMutex Mutex;
        TMap<TGroupId, TIntrusivePtr<TProxyDS>> Models;
    public:
        TProxyDSCP();

        void AddMock(TGroupId groupId, TIntrusivePtr<TProxyDS> model);

        TIntrusivePtr<TProxyDS> GetMock(TGroupId groupId);

        TMap<TGroupId, TIntrusivePtr<TProxyDS>> GetMocks();
    };

    std::unique_ptr<TProxyDSCP> CreateProxyDSCP();
}
