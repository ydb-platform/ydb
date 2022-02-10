#pragma once

#include "task.h"

namespace NRainCheck {
    class ISimpleTask;

    // Function called on continue
    class TContinueFunc {
        friend class TSimpleTaskRunner;

        typedef TContinueFunc (ISimpleTask::*TFunc)();
        TFunc Func;

    public:
        TContinueFunc()
            : Func(nullptr)
        {
        }

        TContinueFunc(void*)
            : Func(nullptr)
        {
        }

        template <typename TTask>
        TContinueFunc(TContinueFunc (TTask::*func)())
            : Func((TFunc)func)
        {
            static_assert((std::is_base_of<ISimpleTask, TTask>::value), "expect (std::is_base_of<ISimpleTask, TTask>::value)");
        }

        bool operator!() const {
            return !Func;
        }
    };

    class TSimpleTaskRunner: public TTaskRunnerBase {
    public:
        TSimpleTaskRunner(IEnv* env, ISubtaskListener* parentTask, TAutoPtr<ISimpleTask>);
        ~TSimpleTaskRunner() override;

    private:
        // Function to be called on completion of all pending tasks.
        TContinueFunc ContinueFunc;

        bool ReplyReceived() override /* override */;

        ISimpleTask* GetImpl() {
            return (ISimpleTask*)GetImplBase();
        }
    };

    class ISimpleTask: public ITaskBase {
    public:
        typedef TSimpleTaskRunner TTaskRunner;
        typedef ISimpleTask ITask;

        virtual TContinueFunc Start() = 0;
    };

}
