#include "yql_yt_touch_stage_operation_manager.h"

namespace NYql::NFmr {

namespace {

// Touch has no job/lambda and produces no tasks: the coordinator registers its
// output tables synchronously inside StartOperation, so this stage manager's
// task-generation methods are never actually invoked — only GetExpectedOutputTableIds
// is used, by GetOperation, once the (already-completed) operation is polled.
class TTouchStageOperationManager: public TFmrStageOperationManagerBase {
public:
    TTouchStageOperationManager(TIntrusivePtr<IRandomProvider> randomProvider)
        : TFmrStageOperationManagerBase(randomProvider)
    {
    }

    TPartitionResult PartitionOperationImpl(const TPrepareOperationStageContext& /* context */) final {
        return {};
    }

    TGenerateTasksResult GenerateTasksImpl(const TGenerateTasksContext& /* context */) final {
        return {};
    }

    std::vector<TString> GetExpectedOutputTableIds(const TOperationParams& params) const override {
        const auto& touchParams = std::get<TTouchOperationParams>(params);
        std::vector<TString> ids;
        for (const auto& output : touchParams.Output) {
            ids.emplace_back(output.FmrTableId.Id);
        }
        return ids;
    }

    std::vector<TPartIdInfo> GetPartIdsForTask(const GetPartIdsForTaskContext& /* context */) override {
        return {};
    }
};

} // namespace

IFmrStageOperationManager::TPtr MakeTouchStageOperationManager(TIntrusivePtr<IRandomProvider> randomProvider) {
    return MakeIntrusive<TTouchStageOperationManager>(randomProvider);
}

} // namespace NYql::NFmr
