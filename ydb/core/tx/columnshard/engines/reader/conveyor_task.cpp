#include "conveyor_task.h"
#include "read_context.h"

namespace NKikimr::NColumnShard {

bool IDataTasksProcessor::ITask::Apply(NOlap::IDataReader& indexedDataRead) const {
    return DoApply(indexedDataRead);
}

}
