#pragma once
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/shared_blobs.h>
#include <ydb/core/tx/columnshard/common/tablet_id.h>

namespace NKikimr::NColumnShard {
class TColumnShard;
}

namespace NKikimr::NOlap {

class TStoragesManager: public IStoragesManager {
private:
    using TBase = IStoragesManager;
    NColumnShard::TColumnShard& Shard;
    std::shared_ptr<NDataSharing::TSharedBlobsManager> SharedBlobsManager;
protected:
    virtual std::shared_ptr<NOlap::IBlobsStorageOperator> DoBuildOperator(const TString& storageId) override;
    virtual bool DoLoadIdempotency(NTable::TDatabase& database) override;

    virtual const std::shared_ptr<NDataSharing::TSharedBlobsManager>& DoGetSharedBlobsManager() const override {
        AFL_VERIFY(SharedBlobsManager);
        return SharedBlobsManager;
    }

public:
    TStoragesManager(NColumnShard::TColumnShard& shard);
};


}