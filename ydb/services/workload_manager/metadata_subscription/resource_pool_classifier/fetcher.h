#pragma once

#include "snapshot.h"


namespace NKikimr::NWorkloadManager {

class TResourcePoolClassifierSnapshotsFetcher : public NMetadata::NFetcher::TSnapshotsFetcher<TResourcePoolClassifierSnapshot> {
protected:
    virtual std::vector<NMetadata::IClassBehaviour::TPtr> DoGetManagers() const override;
};

}  // namespace NKikimr::NWorkloadManager
