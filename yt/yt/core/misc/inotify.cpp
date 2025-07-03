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
    EInotifyWatchEvents mask,
    TClosure callback)
    : FD_(handle->GetFD())
    , Path_(std::move(path))
    , Mask_(mask)
    , Callback_(std::move(callback))
{
    YT_VERIFY(FD_ >= 0);
    CreateWatch();
}

TInotifyWatch::~TInotifyWatch()
{
    DropWatch();
}

bool TInotifyWatch::IsValid() const
{
    return WD_ >= 0;
}

void TInotifyWatch::Run()
{
    // Unregister before create a new file.
    DropWatch();
    Callback_();
    // Register the newly created file.
    CreateWatch();
}

void TInotifyWatch::CreateWatch()
{
    YT_VERIFY(WD_ < 0);
#ifdef _linux_
    WD_ = inotify_add_watch(
        FD_,
        Path_.c_str(),
        ToUnderlying(Mask_));

    if (WD_ < 0) {
        YT_LOG_ERROR(
            TError::FromSystem(errno),
            "Error registering watch (Path: %v)",
            Path_);
        WD_ = -1;
    } else if (WD_ > 0) {
        YT_LOG_DEBUG("Watch registered (WD: %v, Path: %v)",
            WD_,
            Path_);
    } else {
        YT_ABORT();
    }
#else
    WD_ = -1;
#endif
}

void TInotifyWatch::DropWatch()
{
#ifdef _linux_
    if (WD_ > 0) {
        YT_LOG_DEBUG("Watch unregistered (WD: %v, Path: %v)",
            WD_,
            Path_);
        inotify_rm_watch(FD_, WD_);
    }
#endif
    WD_ = -1;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFS
