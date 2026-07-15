#pragma once

#include "snapshot.h"
#include "udf_behaviour.h"
#include "library_behaviour.h"
#include <ydb/services/metadata/abstract/common.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NUdfStore {

class TSnapshotsFetcher: public NMetadata::NFetcher::TSnapshotsFetcher<TSnapshot> {
    virtual std::vector<NMetadata::IClassBehaviour::TPtr> DoGetManagers() const override {
        return  {
            TUdfMeta::GetBehaviour(),
            TUdfLibrarySource::GetBehaviour(),
        };
    }
};

} // namespace NKikimr::NUdfStore
