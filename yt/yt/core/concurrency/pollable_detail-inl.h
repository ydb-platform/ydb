#ifndef POLLABLE_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include pollable_detail.h"
// For the sake of sane code completion.
#include "pollable_detail.h"
#endif
#undef POLLABLE_DETAIL_INL_H_

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <std::invocable<NConcurrency::IPollable&, NConcurrency::EPollControl> T>
NConcurrency::IPollablePtr MakeSimplePollable(T body, std::string loggingTag)
{
    class TSimplePollable
        : public NConcurrency::TPollableBase
    {
    public:
        TSimplePollable(T body, std::string loggingTag)
            : Body_(std::move(body))
            , LoggingTag_(std::move(loggingTag))
        { }

        const std::string& GetLoggingTag() const override
        {
            return LoggingTag_;
        }

        void OnEvent(NConcurrency::EPollControl control) override
        {
            Body_(*this, control);
        }

        void OnShutdown() override
        { }

    private:
        T Body_;
        const std::string LoggingTag_;
    };

    return New<TSimplePollable>(std::move(body), std::move(loggingTag));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
