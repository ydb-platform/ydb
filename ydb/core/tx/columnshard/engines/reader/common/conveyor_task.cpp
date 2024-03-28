#include "conveyor_task.h"

namespace NKikimr::NOlap::NReader {

bool IDataTasksProcessor::ITask::Apply(IDataReader& indexedDataRead) const {
    return DoApply(indexedDataRead);
}

}
