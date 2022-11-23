#pragma once

#include "snapshot.h"
#include <ydb/services/metadata/abstract/common.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NMetadata::NSecret {

class TManager: public NMetadataProvider::TSnapshotsManager<TSnapshot> {
protected:
    virtual std::vector<IOperationsManager::TPtr> DoGetManagers() const override;
public:
};

}
