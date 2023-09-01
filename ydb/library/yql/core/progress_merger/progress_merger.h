#include <ydb/library/yql/core/yql_execution.h>

#include <util/generic/hash.h>
#include <util/system/spinlock.h>


namespace NYql::NProgressMerger {

//////////////////////////////////////////////////////////////////////////////
// TNodeProgressBase
//////////////////////////////////////////////////////////////////////////////
class TNodeProgressBase {
public:
    using EState = TOperationProgress::EState;

    TNodeProgressBase(const TOperationProgress& p);
    TNodeProgressBase(
        const TOperationProgress& p,
        TInstant startedAt,
        TInstant finishedAt,
        const TVector<TOperationProgress::TStage>& stages);

    bool MergeWith(const TOperationProgress& p);
    void Abort();
    bool IsUnfinished() const;
    bool IsDirty() const;
    void SetDirty(bool dirty);

protected:
    TOperationProgress Progress_;
    TInstant StartedAt_;
    TInstant FinishedAt_;
    TVector<TOperationProgress::TStage> Stages_;

private:
    bool Dirty_;
};

//////////////////////////////////////////////////////////////////////////////
// ITaskProgressMerger
//////////////////////////////////////////////////////////////////////////////
struct ITaskProgressMerger {
    virtual ~ITaskProgressMerger() = default;
    virtual void MergeWith(const TOperationProgress& progress) = 0;
    virtual void AbortAllUnfinishedNodes() = 0;
};

} // namespace NProgressMerger 
