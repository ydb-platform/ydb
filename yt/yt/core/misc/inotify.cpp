#include "inotify.h"

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/error/error.h>

#include <library/cpp/yt/system/handle_eintr.h>

#include <errno.h>

#ifdef _linux_
    #include <sys/inotify.h>
#endif

namespace NYT::NFS {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "Inotify");

////////////////////////////////////////////////////////////////////////////////

TInotifyHandle::TInotifyHandle()
{
#ifdef _linux_
    FD_ = inotify_init1(IN_NONBLOCK | IN_CLOEXEC);
    if (FD_ < 0) {
        THROW_ERROR_EXCEPTION("Error creating inotify descriptor")
            << TError::FromSystem();
    }
#endif
}

TInotifyHandle::~TInotifyHandle()
{
#ifdef _linux_
    if (FD_ >= 0) {
        ::close(FD_);
    }
#endif
}

std::optional<TInotifyHandle::TPollResult> TInotifyHandle::Poll()
{
#ifdef _linux_
    YT_VERIFY(FD_ >= 0);
    std::array<char, sizeof(struct inotify_event) + NAME_MAX + 1> buffer;
    ssize_t rv = HandleEintr(::read, FD_, buffer.data(), buffer.size());
    if (rv < 0) {
        if (errno != EAGAIN) {
            YT_LOG_ERROR(
                TError::FromSystem(errno),
                "Error polling inotify descriptor (FD: %v)",
                FD_);
        }
    } else if (rv > 0) {
        YT_VERIFY(rv >= static_cast<ssize_t>(sizeof(struct inotify_event)));
        auto* event = reinterpret_cast<struct inotify_event*>(buffer.data());
        TPollResult result{
            .WD = event->wd,
            .Events = static_cast<EInotifyWatchEvents>(event->mask),
        };
        YT_LOG_DEBUG(
            "Watch was triggered (WD: %v, Events: %v)",
            result.WD,
            result.Events);
        return result;
    } else {
        // Do nothing.
    }
#endif
    return {};
}

////////////////////////////////////////////////////////////////////////////////

TInotifyWatch::TInotifyWatch(
    TInotifyHandle* handle,
    std::string path,
    EInotifyWatchEvents mask)
    : FD_(handle->GetFD())
    , Path_(std::move(path))
    , Mask_(mask)
{
#ifdef _linux_
    YT_VERIFY(FD_ >= 0);
    WD_ = inotify_add_watch(
        FD_,
        Path_.c_str(),
        ToUnderlying(Mask_));
    if (WD_ < 0) {
        WD_ = -1;
        THROW_ERROR_EXCEPTION("Error registering watch for %v",
            Path_)
            << TError::FromSystem();
    }
    YT_LOG_DEBUG("Watch registered (WD: %v, Path: %v)",
        WD_,
        Path_);
#endif
}

TInotifyWatch::~TInotifyWatch()
{
#ifdef _linux_
    if (WD_ > 0) {
        YT_LOG_DEBUG("Watch unregistered (WD: %v, Path: %v)",
            WD_,
            Path_);
        inotify_rm_watch(FD_, WD_);
    }
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFS
