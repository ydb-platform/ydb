#pragma once
#include "snapshot.h"
#include <ydb/services/metadata/abstract/fetcher.h>
#include <ydb/services/metadata/abstract/kqp_common.h>

namespace NKikimr::NMetadata::NInitializer {

class TFetcher: public NFetcher::TSnapshotsFetcher<TSnapshot> {
protected:
    virtual std::vector<IClassBehaviour::TPtr> DoGetManagers() const override;
public:
};

}
