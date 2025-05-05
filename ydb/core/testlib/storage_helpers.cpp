#include "storage_helpers.h"

#include <ydb/core/blobstorage/dsproxy/mock/model.h>

namespace NKikimr {
    int CountBlobsWithSubstring(ui64 tabletId, const TVector<TIntrusivePtr<NFake::TProxyDS>>& proxyDSs, const TString& substring) {
        int res = 0;
        for (const auto& proxyDS : proxyDSs) {
            for (const auto& [id, blob] : proxyDS->AllMyBlobs()) {
                if (id.TabletID() == tabletId && !blob.DoNotKeep && blob.Buffer.ConvertToString().Contains(substring)) {
                    ++res;
                }
            }
        }
        return res;
    }

    bool BlobStorageContains(const TVector<TIntrusivePtr<NFake::TProxyDS>>& proxyDSs, const TString& value) {
        for (const auto& proxyDS : proxyDSs) {
            for (const auto& [id, blob] : proxyDS->AllMyBlobs()) {
                if (!blob.DoNotKeep && blob.Buffer.ConvertToString().Contains(value)) {
                    return true;
                }
            }
        }
        return false;
    }
} // namespace NKikimr
