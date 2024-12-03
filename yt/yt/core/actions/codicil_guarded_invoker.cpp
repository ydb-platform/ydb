#include "codicil_guarded_invoker.h"

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/current_invoker.h>
#include <yt/yt/core/actions/invoker_detail.h>

#include <yt/yt/core/misc/codicil.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TCodicilGuardedInvoker
    : public TInvokerWrapper<false>
{
public:
    TCodicilGuardedInvoker(IInvokerPtr invoker, std::string codicil)
        : TInvokerWrapper(std::move(invoker))
        , Codicil_(std::move(codicil))
    { }

    using TInvokerWrapper::Invoke;

    void Invoke(TClosure callback) override
    {
        UnderlyingInvoker_->Invoke(BIND_NO_PROPAGATE(
            &TCodicilGuardedInvoker::RunCallback,
            MakeStrong(this),
            Passed(std::move(callback))));
    }

private:
    const std::string Codicil_;

    void RunCallback(TClosure callback)
    {
        auto currentInvokerGuard = TCurrentInvokerGuard(this);
        auto codicilGuard = TCodicilGuard(MakeNonOwningCodicilBuilder(Codicil_));
        callback();
    }
};

IInvokerPtr CreateCodicilGuardedInvoker(IInvokerPtr underlyingInvoker, std::string codicil)
{
    return New<TCodicilGuardedInvoker>(std::move(underlyingInvoker), std::move(codicil));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
