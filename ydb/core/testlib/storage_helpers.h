#pragma once

#include <ydb/core/blobstorage/dsproxy/mock/dsproxy_mock.h>

namespace NKikimr {
    int CountBlobsWithSubstring(ui64 tabletId, const TVector<TIntrusivePtr<NFake::TProxyDS>>& proxyDSs, const TString& substring);
    bool BlobStorageContains(const TVector<TIntrusivePtr<NFake::TProxyDS>>& proxyDSs, const TString& value);
} // namespace NKikimr
