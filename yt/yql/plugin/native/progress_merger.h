#pragma once

#include <ydb/library/yql/core/yql_execution.h>
#include <ydb/library/yql/core/progress_merger/progress_merger.h>

#include <library/cpp/yson/writer.h>

namespace NYT::NYqlPlugin {

using namespace NYql::NProgressMerger;
using namespace NYson;

//////////////////////////////////////////////////////////////////////////////

class TNodeProgress : public TNodeProgressBase {
public:
    TNodeProgress(const NYql::TOperationProgress& p);
    void Serialize(::NYson::TYsonWriter& yson) const;
};

//////////////////////////////////////////////////////////////////////////////

class TProgressMerger : public ITaskProgressMerger {
public:
    void MergeWith(const NYql::TOperationProgress& progress) override;
    void AbortAllUnfinishedNodes() override;

    bool HasChangesSinceLastFlush() const;
    TString ToYsonString();

private:
    bool HasChanges_ = false;
    THashMap<ui32, TNodeProgress> NodesMap_;
};

//////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
