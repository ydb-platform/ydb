#pragma once

#include "snapshot.h"
#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/abstract/manager.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NMetadataInitializer {

class TFetcher: public NMetadataProvider::TSnapshotsFetcher<TSnapshot> {
protected:
    virtual std::vector<NMetadata::IOperationsManager::TPtr> DoGetManagers() const override;
public:
};

}
