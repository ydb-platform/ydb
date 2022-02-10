#pragma once

#include "coro_stack.h"
#include "task.h"

#include <util/generic/ptr.h>
#include <util/memory/alloc.h>
#include <util/system/align.h>
#include <util/system/context.h>
#include <util/system/valgrind.h>

namespace NRainCheck {
    class ICoroTask;

    class TCoroTaskRunner: public TTaskRunnerBase, private ITrampoLine {
        friend class ICoroTask;

    private:
        NPrivate::TCoroStack Stack;
        TContMachineContext ContMachineContext;
        bool CoroDone;

    public:
        TCoroTaskRunner(IEnv* env, ISubtaskListener* parent, TAutoPtr<ICoroTask> impl);
        ~TCoroTaskRunner() override;

    private:
        static TContClosure ContClosure(TCoroTaskRunner* runner, TArrayRef<char> memRegion);

        bool ReplyReceived() override /* override */;

        void DoRun() override /* override */;

        ICoroTask* GetImpl() {
            return (ICoroTask*)GetImplBase();
        }
    };

    class ICoroTask: public ITaskBase {
        friend class TCoroTaskRunner;

    private:
        size_t StackSize;

    public:
        typedef TCoroTaskRunner TTaskRunner;
        typedef ICoroTask ITask;

        ICoroTask(size_t stackSize = 0x2000)
            : StackSize(stackSize)
        {
        }

        virtual void Run() = 0;
        static void WaitForSubtasks();
    };

}
