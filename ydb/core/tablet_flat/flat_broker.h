#pragma once

#include <util/system/types.h>
#include <util/generic/string.h>

namespace NKikimr {
namespace NTable {

    using TTaskId = ui64;

    struct TResourceParams {
        TString Type;
        ui64 CPU = 0;
        ui64 Memory = 0;
        ui64 Priority = 0;

        explicit TResourceParams(TString type)
            : Type(std::move(type))
        { }

        TResourceParams&& WithCPU(ui64 cpu) && {
            CPU = cpu;
            return std::move(*this);
        }

        TResourceParams&& WithMemory(ui64 memory) && {
            Memory = memory;
            return std::move(*this);
        }

        TResourceParams&& WithPriority(ui64 priority) && {
            Priority = priority;
            return std::move(*this);
        }
    };

    enum class EResourceStatus {
        Finished,
        Cancelled,
    };

    class IResourceBroker {
    public:
        virtual ~IResourceBroker() = default;

        using TResourceConsumer = std::function<void(TTaskId)>;

        /**
         * SubmitTask allocates task id and schedules a new task with the
         * specified parameters. The consumer interface will be used to report
         * the allocation status.
         */
        virtual TTaskId SubmitTask(TString name, TResourceParams params, TResourceConsumer consumer) = 0;

        /**
         * UpdateTask may be used to update scheduled task, unless resources have
         * already been allocated. Will be ignored for cancelled or running tasks.
         */
        virtual void UpdateTask(TTaskId taskId, TResourceParams params) = 0;

        /**
         * FinishTask signals task resources are no longer needed. May only be
         * called after ResourceAllocated callback. If status is Cancelled then
         * task will not contribute towards average stats.
         */
        virtual void FinishTask(TTaskId taskId, EResourceStatus status = EResourceStatus::Finished) = 0;

        /**
         * CancelTask may be called to cancel a scheduled task. When the result
         * is true the task is cancelled and ResourceAllocated will not be called.
         * The false return status may mean the task is currently running or has
         * already been cancelled or finished.
         */
        virtual bool CancelTask(TTaskId taskId) = 0;
    };

}
}
