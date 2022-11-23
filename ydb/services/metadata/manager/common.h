#pragma once
#include <ydb/core/base/events.h>
#include <library/cpp/actors/core/events.h>

namespace NKikimr::NMetadataManager {

class IAlterController {
public:
    using TPtr = std::shared_ptr<IAlterController>;
    virtual ~IAlterController() = default;

    virtual void AlterProblem(const TString& errorMessage) = 0;
    virtual void AlterFinished() = 0;

};

enum EEvents {
    EvRestoreFinished = EventSpaceBegin(TKikimrEvents::ES_METADATA_MANAGER),
    EvRestoreProblem,
    EvModificationFinished,
    EvModificationProblem,
    EvAlterFinished,
    EvAlterProblem,
    EvAlterPreparationFinished,
    EvAlterPreparationProblem,
    EvEnd
};

static_assert(EEvents::EvEnd < EventSpaceEnd(TKikimrEvents::ES_METADATA_MANAGER), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_METADATA_MANAGER)");
}
