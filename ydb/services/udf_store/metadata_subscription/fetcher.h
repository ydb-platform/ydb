#pragma once

#include "snapshot.h"
#include <ydb/services/metadata/abstract/common.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NUdfStore {

class TSnapshotsFetcher: public NMetadata::NFetcher::TSnapshotsFetcher<TSnapshot> {
    virtual std::vector<NMetadata::IClassBehaviour::TPtr> DoGetManagers() const override {
        return  {
            TUdfMeta::GetBehaviour()
        };
    }
};

} // namespace NKikimr::NUdfStore
