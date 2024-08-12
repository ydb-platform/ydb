#pragma once
#include <ydb/core/base/events.h>
#include <ydb/library/actors/core/events.h>

namespace Ydb {

class Value;

};

namespace NKikimr::NMetadata {

namespace NModifications {

namespace NColumnMerger {

using TMerger = std::function<bool(Ydb::Value& self, const Ydb::Value& other)>;
using TMergerFactory = std::function<TMerger(const TString& columnName)>;

}

template <class TObject>
class IAlterPreparationController {
public:
    using TPtr = std::shared_ptr<IAlterPreparationController>;
    virtual ~IAlterPreparationController() = default;

    virtual void OnPreparationFinished(std::vector<TObject>&& objects) = 0;
    virtual void OnPreparationProblem(const TString& errorMessage) = 0;
};

class IAlterController {
public:
    using TPtr = std::shared_ptr<IAlterController>;
    virtual ~IAlterController() = default;

    virtual void OnAlteringProblem(const TString& errorMessage) = 0;
    virtual void OnAlteringFinished() = 0;

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

}
