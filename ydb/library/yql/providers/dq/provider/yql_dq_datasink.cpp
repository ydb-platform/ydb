#include "yql_dq_datasink.h"
#include "yql_dq_state.h"
#include "yql_dq_datasink_constraints.h"
#include "yql_dq_datasink_type_ann.h"
#include "yql_dq_recapture.h"

#include <ydb/library/yql/providers/dq/opt/logical_optimize.h>
#include <ydb/library/yql/providers/dq/opt/physical_optimize.h>
#include <ydb/library/yql/providers/dq/opt/dqs_opt.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_common.h>
#include <ydb/library/yql/dq/opt/dq_opt_build.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/transform/yql_lazy_init.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>

#include <ydb/library/yql/core/services/yql_transform_pipeline.h>
#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

class TDqDataProviderSink: public TDataProviderBase {
public:
    TDqDataProviderSink(const TDqState::TPtr& state)
        : State(state)
        , LogOptTransformer([state] () { return CreateDqsLogOptTransformer(state->TypeCtx, state->Settings); })
        , PhyOptTransformer([state] () { return CreateDqsPhyOptTransformer(/*TODO*/nullptr, state->Settings); })
        , PhysicalFinalizingTransformer([] () { return CreateDqsFinalizingOptTransformer(); })
        , TypeAnnotationTransformer([state] () {
            return CreateDqsDataSinkTypeAnnotationTransformer(
                state->TypeCtx, state->Settings->EnableDqReplicate.Get().GetOrElse(TDqSettings::TDefault::EnableDqReplicate));
        })
        , ConstraintsTransformer([] () { return CreateDqDataSinkConstraintTransformer(); })
        , RecaptureTransformer([state] () { return CreateDqsRecaptureTransformer(state); })
    { }

    bool CollectStatistics(NYson::TYsonWriter& writer, bool totalOnly) override {
        auto statistics = [&]() {
            TGuard<TMutex> lock(State->Mutex);
            return State->Statistics;
        } ();

        if (statistics.empty()) {
            return false;
        }

        TOperationStatistics taskRunner;
        THashMap<TString, std::tuple<i64, i64, i64, TMaybe<i64>>> total; // sum, count, max, min

        for (auto& opStatistics : statistics) {
            TOperationStatistics newStat;
            for (auto& el : opStatistics.second.Entries) {

                if (el.Value) {
                    continue;
                }

                if (el.Name.StartsWith("TaskRunner")) {
                    taskRunner.Entries.push_back(el);
                } else {
                    newStat.Entries.push_back(el);
                }

                auto& totalEntry = total[el.Name];
                if (auto val = el.Sum) {
                    std::get<0>(totalEntry) += *val;
                }
                if (auto val = el.Count) {
                    std::get<1>(totalEntry) += *val;
                }
                if (auto val = el.Max) {
                    std::get<2>(totalEntry) = Max<i64>(*val, std::get<2>(totalEntry));
                }
                if (auto val = el.Min) {
                    std::get<3>(totalEntry) = Min<i64>(*val, std::get<3>(totalEntry).GetOrElse(Max<i64>()));
                }
            }
            opStatistics.second = newStat;
        }

        // NCommon::WriteStatistics(writer, totalOnly, statistics);

        writer.OnBeginMap();
        writer.OnKeyedItem("All");
        NCommon::WriteStatistics(writer, totalOnly, statistics);

        THashMap<TString, TOperationStatistics> taskRunnerStage;
        THashMap<TString, TOperationStatistics> taskRunnerInput;
        THashMap<TString, TOperationStatistics> taskRunnerOutput;

        for (const auto& entry : taskRunner.Entries) {
            TString prefix, name;
            std::map<TString, TString> labels;
            if (!NCommon::ParseCounterName(&prefix, &labels, &name, entry.Name)) {
                continue;
            }
            auto maybeInput = labels.find("Input");
            auto maybeOutput = labels.find("Output");
            auto maybeStage = labels.find("Stage");
            if (maybeStage == labels.end()) {
                maybeStage = labels.find("Task");
            }
            if (maybeStage == labels.end()) {
                continue;
            }

            if (maybeInput != labels.end()) {
                auto newEntry = entry; newEntry.Name = name;
                taskRunnerInput[maybeStage->second].Entries.push_back(newEntry);
            }
            if (maybeOutput != labels.end()) {
                auto newEntry = entry; newEntry.Name = name;
                taskRunnerOutput[maybeStage->second].Entries.push_back(newEntry);
            }
            if (maybeInput == labels.end() && maybeOutput == labels.end()) {
                auto newEntry = entry; newEntry.Name = name;
                taskRunnerStage[maybeStage->second].Entries.push_back(newEntry);
            }
        }

        writer.OnKeyedItem("TaskRunner");

        {
            writer.OnBeginMap();
            for (const auto& [stageId, stat] : taskRunnerStage) {
                auto& inputStat = taskRunnerInput[stageId];
                auto& outputStat = taskRunnerInput[stageId];

                writer.OnKeyedItem("Stage=" + stageId);
                {
                    writer.OnBeginMap();

                    writer.OnKeyedItem("Input");
                    NCommon::WriteStatistics(writer, totalOnly, {{0, inputStat}});

                    writer.OnKeyedItem("Output");
                    NCommon::WriteStatistics(writer, totalOnly, {{0, outputStat}});

                    writer.OnKeyedItem("Task");
                    NCommon::WriteStatistics(writer, totalOnly, {{0, stat}});

                    writer.OnEndMap();
                }
            }

            writer.OnEndMap();
        }

        writer.OnEndMap();

        return true;
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        if (node.IsCallable(TCoDataSink::CallableName())) {
            if (!EnsureMinMaxArgsCount(node, 1, 2, ctx)) {
                return false;
            }

            if (node.Child(0)->Content() == DqProviderName) {
                if (node.ChildrenSize() == 2) {
                    if (!EnsureAtom(*node.Child(1), ctx)) {
                        return false;
                    }

                    if (node.Child(1)->Content() != "$all") {
                        ctx.AddError(TIssue(ctx.GetPosition(node.Child(1)->Pos()), TStringBuilder() << "Unexpected cluster name: " << node.Child(1)->Content()));
                        return false;
                    }
                }
                cluster = Nothing();
                return true;
            }
        }

        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Invalid DQ DataSource parameters"));
        return false;
    }

    bool CanParse(const TExprNode& node) override {
        return TypeAnnotationTransformer->CanParse(node);
    }

    IGraphTransformer& GetTypeAnnotationTransformer(bool instantOnly) override {
        Y_UNUSED(instantOnly);
        return *TypeAnnotationTransformer;
    }

    IGraphTransformer& GetConstraintTransformer(bool instantOnly, bool subGraph) override {
        Y_UNUSED(instantOnly && subGraph);
        return *ConstraintsTransformer;
    }

    IGraphTransformer& GetRecaptureOptProposalTransformer() override {
        return *RecaptureTransformer;
    }

    IGraphTransformer& GetLogicalOptProposalTransformer() override {
        return *LogOptTransformer;
    }

    IGraphTransformer& GetPhysicalOptProposalTransformer() override {
        return *PhyOptTransformer;
    }

    IGraphTransformer& GetPhysicalFinalizingTransformer() override {
        return *PhysicalFinalizingTransformer;
    }

    TStringBuf GetName() const override {
        return DqProviderName;
    }

    bool GetDependencies(const TExprNode& node, TExprNode::TListType& children, bool compact) override {
        Y_UNUSED(compact);

        if (TDqConnection::Match(&node)) {
            children.push_back(node.ChildPtr(TDqConnection::idx_Output)->ChildPtr(TDqOutput::idx_Stage));
            return false;
        }

        if (TDqStageBase::Match(&node)) {
            auto inputs = node.ChildPtr(TDqStageBase::idx_Inputs);
            for (size_t i = 0; i < inputs->ChildrenSize(); ++i) {
                children.push_back(inputs->ChildPtr(i));
            }
            ScanPlanDependencies(node.ChildPtr(TDqStageBase::idx_Program), children);
            return true;
        }

        if (TDqQuery::Match(&node)) {
            auto stagesList = node.ChildPtr(TDqQuery::idx_SinkStages);
            for (size_t i = 0; i < stagesList->ChildrenSize(); ++i) {
                children.push_back(stagesList->ChildPtr(i));
            }
            return true;
        }

        return false;
    }

    void ScanPlanDependencies(const TExprNode::TPtr& input, TExprNode::TListType& children) {
        VisitExpr(input, [&children](const TExprNode::TPtr& node) {
            if (TMaybeNode<TDqReadWrapBase>(node)) {
                children.push_back(node->ChildPtr(TDqReadWrapBase::idx_Input));
                return false;
            }
            return true;
        });
    }

    TString GetOperationDisplayName(const TExprNode& node) override {
        if (auto maybeStage = TMaybeNode<TDqStageBase>(&node)) {
            TStringBuilder builder;
            builder << TPlanFormatterBase::GetOperationDisplayName(node);
            if (auto publicId = State->TypeCtx->TranslateOperationId(maybeStage.Raw()->UniqueId())) {
                builder << " #" << publicId;
            }
            return builder;
        }
        return TPlanFormatterBase::GetOperationDisplayName(node);
    }

    void WritePlanDetails(const TExprNode& node, NYson::TYsonWriter& writer) override {
        if (auto maybeStage = TMaybeNode<TDqStageBase>(&node)) {
            writer.OnKeyedItem("Streams");
            writer.OnBeginMap();
            NCommon::WriteStreams(writer, "Program", maybeStage.Cast().Program());
            writer.OnEndMap();
        }
    }

    const TDqState::TPtr State;

    TLazyInitHolder<IGraphTransformer> LogOptTransformer;
    TLazyInitHolder<IGraphTransformer> PhyOptTransformer;
    TLazyInitHolder<IGraphTransformer> PhysicalFinalizingTransformer;
    TLazyInitHolder<TVisitorTransformerBase> TypeAnnotationTransformer;
    TLazyInitHolder<IGraphTransformer> ConstraintsTransformer;
    TLazyInitHolder<IGraphTransformer> RecaptureTransformer;
};

}

TIntrusivePtr<IDataProvider> CreateDqDataSink(const TDqState::TPtr& state) {
    return new TDqDataProviderSink(state);
}

} // namespace NYql
