#include "progress_merger.h"

namespace NYT::NYqlPlugin {

//////////////////////////////////////////////////////////////////////////////

TNodeProgress::TNodeProgress(const NYql::TOperationProgress& p)
    : TNodeProgressBase(p)
{}

void TNodeProgress::Serialize(::NYson::TYsonWriter& writer) const
{
    writer.OnBeginMap();
    {
        writer.OnKeyedItem("category");
        writer.OnStringScalar(Progress_.Category);

        writer.OnKeyedItem("state");
        writer.OnStringScalar(ToString(Progress_.State));

        writer.OnKeyedItem("remoteId");
        writer.OnStringScalar(Progress_.RemoteId);

        writer.OnKeyedItem("remoteData");
        writer.OnBeginMap();
        for (const auto& it : Progress_.RemoteData) {
            writer.OnKeyedItem(it.first);
            writer.OnStringScalar(it.second);
        }
        writer.OnEndMap();

        writer.OnKeyedItem("stages");
        writer.OnBeginMap();
        for (size_t index = 0; index < Stages_.size(); index++) {
            writer.OnKeyedItem(ToString(index));
            writer.OnBeginMap();
            {
                writer.OnKeyedItem(Stages_[index].first);
                writer.OnStringScalar(Stages_[index].second.ToString());
            }
            writer.OnEndMap();
        }
        writer.OnEndMap();

        if (Progress_.Counters) {
            writer.OnKeyedItem("completed");
            writer.OnUint64Scalar(Progress_.Counters->Completed);

            writer.OnKeyedItem("running");
            writer.OnUint64Scalar(Progress_.Counters->Running);

            writer.OnKeyedItem("total");
            writer.OnUint64Scalar(Progress_.Counters->Total);

            writer.OnKeyedItem("aborted");
            writer.OnUint64Scalar(Progress_.Counters->Aborted);

            writer.OnKeyedItem("failed");
            writer.OnUint64Scalar(Progress_.Counters->Failed);

            writer.OnKeyedItem("lost");
            writer.OnUint64Scalar(Progress_.Counters->Lost);

            writer.OnKeyedItem("pending");
            writer.OnUint64Scalar(Progress_.Counters->Pending);
        }

        writer.OnKeyedItem("startedAt");
        writer.OnStringScalar(StartedAt_.ToString());

        if (FinishedAt_ != TInstant::Max()) {
            writer.OnKeyedItem("finishedAt");
            writer.OnStringScalar(FinishedAt_.ToString());
        }
    }
    writer.OnEndMap();
}

//////////////////////////////////////////////////////////////////////////////

void TProgressMerger::MergeWith(const NYql::TOperationProgress& progress)
{
    auto in = NodesMap_.emplace(progress.Id, progress);
    if (!in.second) {
        in.first->second.MergeWith(progress);
    }
    HasChanges_ = true;
}

void TProgressMerger::AbortAllUnfinishedNodes()
{
    for (auto& node: NodesMap_) {
        if (node.second.IsUnfinished()) {
            node.second.Abort();
            HasChanges_ = true;
        }
    }
}

TString TProgressMerger::ToYsonString()
{
    TStringStream yson;
    ::NYson::TYsonWriter writer(&yson);

    writer.OnBeginMap();
    for (auto& node: NodesMap_) {
        writer.OnKeyedItem(ToString(node.first));
        node.second.Serialize(writer);
    }
    writer.OnEndMap();
    HasChanges_ = false;

    return yson.Str();
}

bool TProgressMerger::HasChangesSinceLastFlush() const
{
    return HasChanges_;
}

//////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
