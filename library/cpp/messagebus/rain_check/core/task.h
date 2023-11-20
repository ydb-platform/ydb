#pragma once

#include "fwd.h"

#include <library/cpp/messagebus/actor/actor.h>
#include <library/cpp/messagebus/misc/atomic_box.h>

#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>

#include <util/generic/noncopyable.h>
#include <util/generic/ptr.h>
#include <util/thread/lfstack.h>

namespace NRainCheck {
    struct ISubtaskListener {
        virtual void SetDone() = 0;
        virtual ~ISubtaskListener() {
        }
    };

    struct TNopSubtaskListener: public ISubtaskListener {
        void SetDone() override;

        static TNopSubtaskListener Instance;
    };

    class TSubtaskCompletionFunc {
        friend class TSubtaskCompletion;

        typedef void (ITaskBase::*TFunc)(TSubtaskCompletion*);
        TFunc Func;

    public:
        TSubtaskCompletionFunc()
            : Func(nullptr)
        {
        }

        TSubtaskCompletionFunc(void*)
            : Func(nullptr)
        {
        }

        template <typename TTask>
        TSubtaskCompletionFunc(void (TTask::*func)(TSubtaskCompletion*))
            : Func((TFunc)func)
        {
            static_assert((std::is_base_of<ITaskBase, TTask>::value), "expect (std::is_base_of<ITaskBase, TTask>::value)");
        }

        bool operator!() const {
            return !Func;
        }
    };

    template <typename T>
    class TTaskFuture;

#define SUBTASK_STATE_MAP(XX)                                     \
    XX(CREATED, "Initial")                                        \
    XX(RUNNING, "Running")                                        \
    XX(DONE, "Completed")                                         \
    XX(CANCEL_REQUESTED, "Cancel requested, but still executing") \
    XX(CANCELED, "Canceled")                                      \
    /**/

    enum ESubtaskState {
        SUBTASK_STATE_MAP(ENUM_VALUE_GEN_NO_VALUE)
    };

    ENUM_TO_STRING(ESubtaskState, SUBTASK_STATE_MAP)

    class TSubtaskCompletion : TNonCopyable, public ISubtaskListener {
        friend struct TTaskAccessor;

    private:
        TAtomicBox<ESubtaskState> State;
        TTaskRunnerBase* volatile TaskRunner;
        TSubtaskCompletionFunc CompletionFunc;

    public:
        TSubtaskCompletion()
            : State(CREATED)
            , TaskRunner()
        {
        }
        ~TSubtaskCompletion() override;

        // Either done or cancel requested or cancelled
        bool IsComplete() const {
            ESubtaskState state = State.Get();
            switch (state) {
                case RUNNING:
                    return false;
                case DONE:
                    return true;
                case CANCEL_REQUESTED:
                    return false;
                case CANCELED:
                    return true;
                case CREATED:
                    Y_ABORT("not started");
                default:
                    Y_ABORT("unknown value: %u", (unsigned)state);
            }
        }

        void FireCompletionCallback(ITaskBase*);

        void SetCompletionCallback(TSubtaskCompletionFunc func) {
            CompletionFunc = func;
        }

        // Completed, but not cancelled
        bool IsDone() const {
            return State.Get() == DONE;
        }

        // Request cancel by actor
        // Does nothing but marks task cancelled,
        // and allows proceeding to next callback
        void Cancel();

        // called by service provider implementations
        // must not be called by actor
        void SetRunning(TTaskRunnerBase* parent);
        void SetDone() override;
    };

    // See ISimpleTask, ICoroTask
    class TTaskRunnerBase: public TAtomicRefCount<TTaskRunnerBase>, public NActor::TActor<TTaskRunnerBase> {
        friend class NActor::TActor<TTaskRunnerBase>;
        friend class TContinueFunc;
        friend struct TTaskAccessor;
        friend class TSubtaskCompletion;

    private:
        THolder<ITaskBase> Impl;

        ISubtaskListener* const ParentTask;
        // While task is running, it holds extra reference to self.
        //bool HoldsSelfReference;
        bool Done;
        bool SetDoneCalled;

        // Subtasks currently executed.
        TVector<TSubtaskCompletion*> Pending;

        void Act(NActor::TDefaultTag);

    public:
        // Construct task. Task is not automatically started.
        TTaskRunnerBase(IEnv*, ISubtaskListener* parent, TAutoPtr<ITaskBase> impl);
        ~TTaskRunnerBase() override;

        bool IsRunningInThisThread() const;
        void AssertInThisThread() const;
        static TTaskRunnerBase* CurrentTask();
        static ITaskBase* CurrentTaskImpl();

        TString GetStatusSingleLine();

    protected:
        //void RetainRef();
        //void ReleaseRef();
        ITaskBase* GetImplBase() {
            return Impl.Get();
        }

    private:
        // true if need to call again
        virtual bool ReplyReceived() = 0;
    };

    class ITaskBase {
    public:
        virtual ~ITaskBase() {
        }
    };

    // Check that current method executed inside some task.
    bool AreWeInsideTask();

}
