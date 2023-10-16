#pragma once

#include "flat_boot_util.h"
#include "flat_bio_events.h"
#include "shared_cache_events.h"
#include "util_fmt_logger.h"
#include "util_fmt_desc.h"
#include "util_basics.h"
#include "util_fmt_abort.h"
#include <util/system/yassert.h>

#include <typeinfo>

namespace NKikimr {
namespace NTabletFlatExecutor {
    class TExecutorBootLogic;

namespace NBoot {
    struct TBack;
    class IStep;
    class TRoot;

    using ELnLev = NUtil::ELnLev;

    enum class EStep {
        None    = 0,
        Blobs   = 1,    // Generic blobs loader tool (like NBlockIO)
        Stages  = 2,
        Bundle  = 3,    // DB bundles (parts) loader and merger
        Redo    = 4,    // DB Redo log queue loader and applier
        MemTable= 5,    // Loader of external TMemTable blobs to cache
        Alter   = 6,    // Local db scheme alteration (TAlter) log
        Loans   = 7,    // Executor borrow and loans logic log
        GCELog  = 8,    // Extended garbage collector log
        Turns   = 9,    // Bundles switch (turns) log processing
        Snap    = 10,   // Log snapshot and deps graph processor
        TxStatus = 11,  // TxStatus data loader and merger
    };

    struct IEnv {
        virtual const NUtil::ILogger* Logger() const noexcept = 0;
        virtual void Describe(IOutputStream&) const noexcept = 0;
        virtual void Start(TIntrusivePtr<IStep>) noexcept = 0;
        virtual void Finish(TIntrusivePtr<IStep>) noexcept = 0;
    };

    class IStep : public TSimpleRefCount<IStep> {
        friend class TRoot;

    public:
        using TLogic = TExecutorBootLogic;

        IStep() = default;

        IStep(TLogic *logic, TBack *state)
            : Logic(logic)
            , Back(state)
        {

        }

        IStep(IStep *owner, EStep kind)
            : Kind__(kind)
            , Owner(owner)
            , Logic(owner->Logic)
            , Back(owner->Back)
        {
            Y_ABORT_UNLESS(Owner != this, "Boot IStep Cannot be on its own");
        }

        virtual ~IStep() = default;

        virtual void Start() noexcept = 0;

        virtual bool HandleBio(NSharedCache::TEvResult&) noexcept
        {
            Y_ABORT("Boot IStep got an unhandled NSharedCache::TEvResult event");
        }

        virtual void HandleStep(TIntrusivePtr<IStep>) noexcept
        {
            Y_ABORT("Boot IStep got an unhandled child step result");
        }

        template<typename TStep, typename ... TArgs>
        TSpawned Spawn(TArgs&& ... args)
        {
            Env->Start(new TStep(this, std::forward<TArgs>(args)...));

            return TSpawned(true);
        }

        template<typename TStep>
        TStep* ConsumeAs(TLeft &left)
        {
            left -= 1; /* may be it is better to track TLeft in step */

            return FinalCast<TStep>(true);
        }

        template<typename TStep>
        TStep* FinalCast(bool require = true)
        {
            if (typeid(*this) == typeid(TStep)) {
                return static_cast<TStep*>(this);
            } else if (require) {
                Y_ABORT("Cannot cast IStep to particular unit");
            } else {
                return nullptr;
            }
        }

    protected:
        const EStep Kind__ = EStep::None;
        const TIntrusivePtr<IStep> Owner;
        TLogic * const Logic = nullptr;
        TBack * const Back = nullptr;
        IEnv * const Env = nullptr;
    };

}
}
}
