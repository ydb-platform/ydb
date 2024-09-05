#pragma once
#include <ydb/core/base/events.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/conclusion/status.h>

namespace Ydb {

class Value;

};

namespace NKikimr::NMetadata {

namespace NInternal {

class TTableRecord;

};

namespace NModifications {

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

class IColumnValuesMerger {
public:
    using TPtr = std::shared_ptr<IColumnValuesMerger>;
    virtual ~IColumnValuesMerger() = default;

    virtual TConclusionStatus Merge(Ydb::Value& value, const Ydb::Value& patch) const = 0;
};

class IRecordsMerger {
protected:
    virtual IColumnValuesMerger::TPtr BuildMerger(const TString& columnName) const = 0;

public:
    virtual ~IRecordsMerger() = default;
    virtual TConclusionStatus MergeRecords(NInternal::TTableRecord& value, const NInternal::TTableRecord& patch) const = 0;
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
