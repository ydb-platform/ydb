#pragma once

#include "util_fmt_line.h"
#include "flat_boot_iface.h"
#include <util/system/yassert.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NBoot {

    class TRoot : public IEnv, private IStep {
    public:
        TRoot(TLogic *logic, TBack *state, TAutoPtr<NUtil::ILogger> logger)
            : IStep(logic, state)
            , Logger_(logger)
        {
            Ref(); /* Prevent deletion on child IStep-s destruction */
        }

        ~TRoot()
        {
            Y_ABORT_UNLESS(RefCount() == 1, "Boot env shouldn't be deleted by TIntrusivePtr");
        }

        const NUtil::ILogger* Logger() const noexcept override
        {
            return Logger_.Get();
        }

        void Describe(IOutputStream &out) const noexcept override
        {
            out
                << "Boot{ " << Queue.size() << " que"
                << ", " << RefCount() << " refs }";
        }

        bool Alone() const
        {
            return RefCount() <= 1 && !Queue;
        }

        template<typename TStep, typename ... TArgs>
        void Spawn(TArgs&& ... args)
        {
            Start(new TStep(this, std::forward<TArgs>(args)...));
        }

        void Execute() noexcept
        {
            for (; Queue; Queue.pop_front()) {
                auto order = std::move(Queue.front());

                switch (order.Op) {
                    case EOp::Start: {
                        *const_cast<IEnv**>(&order.Step->Env) = this;

                        order.Step->Start();
                        break;
                    }

                    case EOp::Finish: {
                        TIntrusivePtr<IStep> owner = order.Step->Owner;

                        owner->HandleStep(std::move(order.Step));
                        break;
                    }
                }
            }
        }

    protected:
        void Start() noexcept override { }

        void Start(TIntrusivePtr<IStep> step) noexcept override
        {
            Y_ABORT_UNLESS(step->Env == nullptr, "IStep is already fired");
            Y_ABORT_UNLESS(step->Owner, "Start called on step without an owner");

            Queue.emplace_back(EOp::Start, std::move(step));
        }

        void Finish(TIntrusivePtr<IStep> step) noexcept override
        {
            Y_ABORT_UNLESS(step, "Finish called without a step");
            Y_ABORT_UNLESS(step->Owner, "Finish called on step without an owner");

            Queue.emplace_back(EOp::Finish, std::move(step));
        }

    private:
        enum class EOp {
            Start,
            Finish,
        };

        struct TOrder {
            TOrder(EOp op, TIntrusivePtr<IStep> step)
                : Op(op)
                , Step(std::move(step))
            {

            }

            const EOp Op;
            TIntrusivePtr<IStep> Step;
        };

        TAutoPtr<NUtil::ILogger> Logger_;
        TDeque<TOrder> Queue;
    };
}
}
}
