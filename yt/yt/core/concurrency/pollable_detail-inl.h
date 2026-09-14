#ifndef POLLABLE_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include pollable_detail.h"
// For the sake of sane code completion.
#include "pollable_detail.h"
#endif

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <std::invocable<NConcurrency::IPollable&, NConcurrency::EPollControl> T>
NConcurrency::IPollablePtr MakeSimplePollable(T body, NLogging::TLoggingTagList loggingTags)
{
    class TSimplePollable
        : public NConcurrency::TPollableBase
    {
    public:
        TSimplePollable(T body, NLogging::TLoggingTagList loggingTags)
            : Body_(std::move(body))
            , LoggingTags_(std::move(loggingTags))
        { }

        const NLogging::TLoggingTagList& GetLoggingTags() const override
        {
            return LoggingTags_;
        }

        void OnEvent(NConcurrency::EPollControl control) override
        {
            Body_(*this, control);
        }

        void OnShutdown() override
        { }

    private:
        T Body_;
        const NLogging::TLoggingTagList LoggingTags_;
    };

    return New<TSimplePollable>(std::move(body), std::move(loggingTags));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
