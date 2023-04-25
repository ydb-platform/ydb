#include "conveyor_task.h"
#include <ydb/core/tx/columnshard/engines/indexed_read_data.h>

namespace NKikimr::NColumnShard {

bool IDataTasksProcessor::ITask::DoExecute() {
    if (OwnerOperator && OwnerOperator->IsStopped()) {
        return true;
    } else {
        DataProcessedFlag = true;
        return DoExecuteImpl();
    }
}

bool IDataTasksProcessor::ITask::Apply(NOlap::TIndexedReadData& indexedDataRead) const {
    if (OwnerOperator) {
        OwnerOperator->ReplyReceived();
    }
    return DoApply(indexedDataRead);
}

bool IDataTasksProcessor::Add(ITask::TPtr task) {
    if (IsStopped()) {
        return false;
    }
    if (DoAdd(task)) {
        DataProcessorAddDataCounter.Inc();
        return true;
    }
    return false;
}


void TDataTasksProcessorContainer::Add(NOlap::TIndexedReadData& indexedDataRead, IDataTasksProcessor::ITask::TPtr task) {
    if (Object) {
        Object->Add(task);
    } else {
        task->Execute();
        task->Apply(indexedDataRead);
    }
}

}
