#pragma once
#include "context.h"

#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NOlap::NActualizer {

class IActualizer {
protected:
    virtual void DoAddPortion(const TPortionInfo& info, const TAddExternalContext& context) = 0;
    virtual void DoRemovePortion(const TPortionInfo& info) = 0;
    virtual void DoBuildTasks(TTieringProcessContext& tasksContext, const TExternalTasksContext& externalContext, TInternalTasksContext& internalContext) const = 0;
public:
    virtual ~IActualizer() = default;
    void BuildTasks(TTieringProcessContext& tasksContext, const TExternalTasksContext& externalContext, TInternalTasksContext& internalContext) {
        return DoBuildTasks(tasksContext, externalContext, internalContext);
    }
    void AddPortion(const std::shared_ptr<TPortionInfo>& info, const TAddExternalContext& context) {
        AFL_VERIFY(info);
        if (info->HasRemoveSnapshot()) {
            return;
        }
        return DoAddPortion(*info, context);
    }
    void RemovePortion(const std::shared_ptr<TPortionInfo>& info) {
        AFL_VERIFY(info);
        if (info->HasRemoveSnapshot()) {
            return;
        }
        return DoRemovePortion(*info);
    }
};

}