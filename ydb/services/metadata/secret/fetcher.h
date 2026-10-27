#pragma once

#include "snapshot.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/services/metadata/abstract/common.h>

namespace NKikimr::NMetadata::NSecret {

class TSnapshotsFetcher: public NFetcher::TSnapshotsFetcher<TSnapshot> {
protected:
    virtual std::vector<IClassBehaviour::TPtr> DoGetManagers() const override;

public:
};

}   // namespace NKikimr::NMetadata::NSecret
