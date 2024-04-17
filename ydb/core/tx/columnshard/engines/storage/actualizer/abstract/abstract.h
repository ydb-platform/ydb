#pragma once
#include "context.h"

#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NOlap::NActualizer {

class IActualizer {
protected:
    virtual void DoAddPortion(const TPortionInfo& info, const TAddExternalContext& context) = 0;
    virtual void DoRemovePortion(const ui64 portionId) = 0;
    virtual void DoExtractTasks(TTieringProcessContext& tasksContext, const TExternalTasksContext& externalContext, TInternalTasksContext& internalContext) = 0;
public:
    virtual ~IActualizer() = default;
    void ExtractTasks(TTieringProcessContext& tasksContext, const TExternalTasksContext& externalContext, TInternalTasksContext& internalContext) {
        return DoExtractTasks(tasksContext, externalContext, internalContext);
    }
    void AddPortion(const std::shared_ptr<TPortionInfo>& info, const TAddExternalContext& context) {
        AFL_VERIFY(info);
        if (info->HasRemoveSnapshot()) {
            return;
        }
        return DoAddPortion(*info, context);
    }
    void RemovePortion(const ui64 portionId) {
        return DoRemovePortion(portionId);
    }
};

}