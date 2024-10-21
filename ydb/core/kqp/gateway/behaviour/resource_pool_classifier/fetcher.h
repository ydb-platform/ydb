#pragma once

#include "snapshot.h"


namespace NKikimr::NKqp {

class TResourcePoolClassifierSnapshotsFetcher : public NMetadata::NFetcher::TSnapshotsFetcher<TResourcePoolClassifierSnapshot> {
protected:
    virtual std::vector<NMetadata::IClassBehaviour::TPtr> DoGetManagers() const override;
};

}  // namespace NKikimr::NKqp
