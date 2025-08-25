#include "interconnect_zc_processor.h"
#include "logging.h"

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <variant>

#if defined (__linux__)

#define YDB_MSG_ZEROCOPY_SUPPORTED 1

#endif

#ifdef YDB_MSG_ZEROCOPY_SUPPORTED

#include <linux/errqueue.h>
#include <linux/netlink.h>
#include <linux/socket.h>

#ifndef MSG_ZEROCOPY
#define MSG_ZEROCOPY 0x4000000
#endif

#ifndef SO_ZEROCOPY
#define SO_ZEROCOPY 60
#endif

static bool CmsgIsIpLevel(const cmsghdr& cmsg) {
    return (cmsg.cmsg_level == SOL_IPV6 && cmsg.cmsg_type == IPV6_RECVERR) ||
       (cmsg.cmsg_level == SOL_IP && cmsg.cmsg_type == IP_RECVERR);
}

static bool CmsgIsZeroCopy(const cmsghdr& cmsg) {
    if (!CmsgIsIpLevel(cmsg)) {
        return false;
    }
    auto serr = reinterpret_cast<const sock_extended_err*> CMSG_DATA(&cmsg);
    return serr->ee_errno == 0 && serr->ee_origin == SO_EE_ORIGIN_ZEROCOPY;
}

#endif

namespace NInterconnect {
using NActors::TEvents;

#ifdef YDB_MSG_ZEROCOPY_SUPPORTED

struct TErr {
    explicit TErr(const TString& err)
        : Reason(err)
    {} 
    TString Reason;
};

struct TAgain {
};

struct TZcSendResult {
    ui64 SendNum = 0;
    ui64 SendWithCopyNum = 0;
};

using TProcessErrQueueResult = std::variant<TErr, TAgain, TZcSendResult>;

static TProcessErrQueueResult DoProcessErrQueue(NInterconnect::TStreamSocket& socket) {
    // Mostly copy-paste from grpc ERRQUEUE handling
    struct msghdr msg;
    msg.msg_name = nullptr;
    msg.msg_namelen = 0;
    msg.msg_iov = nullptr;
    msg.msg_iovlen = 0;
    msg.msg_flags = 0;

    constexpr size_t cmsg_alloc_space =
        CMSG_SPACE(sizeof(sock_extended_err) + sizeof(sockaddr_in)) +
        CMSG_SPACE(32 * NLA_ALIGN(NLA_HDRLEN + sizeof(uint64_t)));

    union {
        char rbuf[cmsg_alloc_space];
        struct cmsghdr align;
    } aligned_buf;
    msg.msg_control = aligned_buf.rbuf;

    TZcSendResult result;

    while (true) {
        ssize_t r;
        msg.msg_controllen = sizeof(aligned_buf.rbuf);

        do {
            r = socket.RecvErrQueue(&msg);
        } while (r == -EINTR);

        if (r < 0) {
            if (result.SendNum == 0) {
                if (r == -EAGAIN || r == -EWOULDBLOCK) {
                    return TAgain();
                } else {
                    return TErr("unexpected error during errqueue read, err: " + ToString(r) + ", " + TString(strerror(r)));
                }
            } else {
                break;
            }
        }

        // Unlikly, but grpc handle it
        if ((msg.msg_flags & MSG_CTRUNC) != 0) {
            if (result.SendNum == 0) {
                return TErr("errqueue message was truncated");
            } else {
                break;
            }
        }

        for (auto cmsg = CMSG_FIRSTHDR(&msg); cmsg && cmsg->cmsg_len;
            cmsg = CMSG_NXTHDR(&msg, cmsg)) {
            if (CmsgIsZeroCopy(*cmsg)) {
                auto serr = reinterpret_cast<struct sock_extended_err*>(CMSG_DATA(cmsg));
                if (serr->ee_data < serr->ee_info) {
                    // Incorrect data inside kernel
                    continue;
                }
                ui64 sends = serr->ee_data - serr->ee_info + 1;
                result.SendNum += sends;
                if (serr->ee_code == SO_EE_CODE_ZEROCOPY_COPIED) {
                    result.SendWithCopyNum += sends;
                }
            }
        }
    }
    return result;
}

#endif

// returns number of buffers which should be sent to find ZC ready buffer on the first place
size_t AdjustLen(std::span<const TConstIoVec> wbuf, std::span<const TOutgoingStream::TBufController> ctrl, ui64 threshold)
{
    size_t l = wbuf.size();
    for (size_t i = 0; i < wbuf.size(); i++) {
        if (ctrl[i].ZcReady() && wbuf[i].Size > threshold) {
            l = i;
            break;
        }
    }

    return l;
}

void TInterconnectZcProcessor::DoProcessNotification(NInterconnect::TStreamSocket& socket) {
#ifdef YDB_MSG_ZEROCOPY_SUPPORTED
    const TProcessErrQueueResult res = DoProcessErrQueue(socket);

    std::visit(TOverloaded{
        [this](const TErr& err) {
            ZcState = ZC_DISABLED_ERR;
            AddErr(err.Reason);
        },
        [](const TAgain&) {
            // Noting here. Probably read during next iteration
        },
        [this](const TZcSendResult& res) {
            Confirmed += res.SendNum;
            ConfirmedWithCopy += res.SendWithCopyNum;
        }}, res);
    

    if (ZcState == ZC_CONGESTED && Confirmed == SendAsZc) {
        ZcState = ZC_OK;
    }

    // There is no reliable way to check if both side of tcp connection
    // are on the same host (consider different namespaces as the same host too).
    // So we check that each transfer has hidden copy during some period.
    if (ZcState == ZC_OK && ConfirmedWithCopy == Confirmed && Confirmed > 10) {
        AddErr("Hidden copy during ZC operation");
        ZcState = ZC_DISABLED_HIDDEN_COPY;
    }
#else
    Y_UNUSED(socket);
#endif
}

void TInterconnectZcProcessor::ApplySocketOption(NInterconnect::TStreamSocket& socket)
{
#ifdef YDB_MSG_ZEROCOPY_SUPPORTED
    if (ZcState == ZC_OK) {
        const int enable = 1;
        if (setsockopt((int)socket, SOL_SOCKET, SO_ZEROCOPY, &enable, sizeof(enable)) < 0) {
            AddErr("Unable to set SO_ZEROCOPY option");
            ZcState = ZC_DISABLED_ERR;
        } else {
            ResetState();
        }
    }
#else
    Y_UNUSED(socket);
#endif
}

void TInterconnectZcProcessor::ResetState() {
    if (ZcState == ZC_DISABLED) {
        return;
    }

    ZcState = ZC_OK;
    Confirmed = 0;
    ConfirmedWithCopy = 0;
    SendAsZc = 0;
    SendAsZcBytes = 0;
    LastErr.clear();
}

ssize_t TInterconnectZcProcessor::ProcessSend(std::span<TConstIoVec> wbuf, TStreamSocket& socket,
    std::span<TOutgoingStream::TBufController> ctrl)
{
    Y_DEBUG_ABORT_UNLESS(wbuf.size() == ctrl.size());
    size_t len = wbuf.size();

    if (ZcStateIsOk()) {
        len = AdjustLen(wbuf, ctrl, ZcThreshold);
    }

    ssize_t r = 0;
    int flags = 0;

    do {
        switch (len) {
            case 0:
#ifdef YDB_MSG_ZEROCOPY_SUPPORTED
                if (ZcStateIsOk()) {
                    flags |= MSG_ZEROCOPY;
                }
#endif
            case 1:
                r = socket.SendWithFlags(wbuf.front().Data, wbuf.front().Size, flags);
                break;
            default:
                r = socket.WriteV(reinterpret_cast<const iovec*>(wbuf.data()), len);
        }
    } while (r == -EINTR);

#ifdef YDB_MSG_ZEROCOPY_SUPPORTED
    if (flags & MSG_ZEROCOPY) {
        if (r > 0) {
            // Successful enqueued in to the kernel - increment counter to track dequeue progress
            SendAsZc++;
            SendAsZcBytes += wbuf.front().Size;
            ctrl.front().Update(SendAsZc);
        } else if (r == -ENOBUFS) {
                if (SendAsZc == Confirmed) {
                    // Got ENOBUFS just for first not completed zero copy transfer
                    // it looks like misconfiguration (unable to lock page or net.core.optmem_max extremely small)
                    // It is better just to stop trying using ZC
                    ZcState = ZC_DISABLED_ERR;
                    AddErr("Got ENOBUF just for first transfer");
                } else {
                    // Got ENOBUFS after some successful send calls. Probably net.core.optmem_max still is not enought
                    // Just disable temporary ZC until we dequeue notifications
                    ZcState = ZC_CONGESTED;
                    AddErr("Got ENOBUF after some transfers, consider to increase net.core.optmem_max");
                }
                // The state changed. Trigger retry
                r = -EAGAIN;
        }
    }
#endif
    return r;
}

TString TInterconnectZcProcessor::GetCurrentStateName() const {
    switch (ZcState) {
        case ZC_DISABLED:
            return "Disabled";
        case ZC_DISABLED_ERR:
            return "DisabledErr";
        case ZC_DISABLED_HIDDEN_COPY:
            return "DisabledHiddenCopy";
        case ZC_OK:
            return "Ok";
        case ZC_CONGESTED:
            return "Congested";
    }
}

TString TInterconnectZcProcessor::ExtractErrText() {
    if (LastErr) {
        TString err;
        err.swap(LastErr);
        return err;
    } else {
        return {};
    }
}

void TInterconnectZcProcessor::AddErr(const TString& err) {
    if (LastErr) {
        LastErr.reserve(err.size() + 2);
        LastErr += ", ";
        LastErr += err;
    } else {
        LastErr = err;
    }
}

///////////////////////////////////////////////////////////////////////////////

#ifdef YDB_MSG_ZEROCOPY_SUPPORTED
TInterconnectZcProcessor::TInterconnectZcProcessor(bool enabled)
    : ZcState(enabled ? ZC_OK : ZC_DISABLED)
{}

// Guard part.
// We must guarantee liveness of buffers used for zc
// until enqueued zc operation completed by kernel

class TGuardActor
    : public NActors::TActorBootstrapped<TGuardActor>
    , public NActors::TInterconnectLoggingBase {
public:
    TGuardActor(ui64 uncompleted, ui64 confirmed, std::list<TEventHolder>&& queue,
        TIntrusivePtr<NInterconnect::TStreamSocket> socket,
        std::unique_ptr<NActors::TEventHolderPool>&& pool);
    void Bootstrap();
    STATEFN(StateFunc);
private:
    void DoGc();
    const ui64 Uncompleted;
    ui64 Confirmed;
    std::list<TEventHolder> Delayed;
    TIntrusivePtr<NInterconnect::TStreamSocket> Socket;
    std::unique_ptr<NActors::TEventHolderPool> Pool;
};

TGuardActor::TGuardActor(ui64 uncompleted, ui64 confirmed, std::list<TEventHolder>&& queue,
    TIntrusivePtr<NInterconnect::TStreamSocket> socket,
    std::unique_ptr<NActors::TEventHolderPool>&& pool)
    : TInterconnectLoggingBase(Sprintf("TGuardActor, socket %lu", socket ? i64(*socket) : -1))
    , Uncompleted(uncompleted)
    , Confirmed(confirmed)
    , Delayed(std::move(queue))
    , Socket(socket)
    , Pool(std::move(pool))
{}

void TGuardActor::Bootstrap() {
    Become(&TThis::StateFunc);
    DoGc();
}

void TGuardActor::DoGc()
{
    Y_DEBUG_ABORT_UNLESS(Pool);
    Y_DEBUG_ABORT_UNLESS(Socket);

    const TProcessErrQueueResult res = DoProcessErrQueue(*Socket);

    std::visit(TOverloaded{
        [this](const TErr& err) {
            // Nothing can do here (( VERIFY, or just drop buffer probably unsafe from network perspective
            LOG_ERROR_IC_SESSION("ICZC01", "error during ERRQUEUE processing: %s",
                err.Reason.data());
            Pool->Release(Delayed);
            Pool->Trim();
            TActor::PassAway();
        },
        [this](const TAgain&) {
            Schedule(TDuration::MilliSeconds(100), new TEvents::TEvWakeup());
        },
        [this](const TZcSendResult& res) {
            Confirmed += res.SendNum;
            if (Confirmed >= Uncompleted) {
                Pool->Release(Delayed);
                Pool->Trim();
                TActor::PassAway();
            } else {
                Schedule(TDuration::MilliSeconds(100), new TEvents::TEvWakeup());
            }
        }}, res);
}

STFUNC(TGuardActor::StateFunc) {
    STRICT_STFUNC_BODY(
        cFunc(TEvents::TEvWakeup::EventType, DoGc)
    )
}

class TGuardRunner : public IZcGuard {
public:
    TGuardRunner(ui64 uncompleted, ui64 confirmed)
        : Uncompleted(uncompleted)
        , Confirmed(confirmed)
    {}

    void ExtractToSafeTermination(std::list<TEventHolder>& queue) noexcept override {
        for (std::list<TEventHolder>::iterator event = queue.begin(); event != queue.end();) {
            if (event->ZcTransferId > Confirmed) {
                Delayed.splice(Delayed.end(), queue, event++);
            } else {
                event++;
            }
        }
    }

    void Terminate(std::unique_ptr<NActors::TEventHolderPool>&& pool, TIntrusivePtr<NInterconnect::TStreamSocket> socket, const NActors::TActorContext &ctx) override {
        if (!Delayed.empty()) {
            // must be registered on the same mailbox!
            ctx.RegisterWithSameMailbox(new TGuardActor(Uncompleted, Confirmed, std::move(Delayed), socket, std::move(pool)));
        }
    }
private:
    const ui64 Uncompleted;
    const ui64 Confirmed;
    std::list<TEventHolder> Delayed;
};

std::unique_ptr<IZcGuard> TInterconnectZcProcessor::GetGuard()
{
    return std::make_unique<TGuardRunner>(SendAsZc, Confirmed);
}

#else
TInterconnectZcProcessor::TInterconnectZcProcessor(bool)
    : ZcState(ZC_DISABLED)
{}

class TDummyGuardRunner : public IZcGuard {
public:
    TDummyGuardRunner(ui64 uncompleted, ui64 confirmed)
    {
        Y_UNUSED(uncompleted);
        Y_UNUSED(confirmed);
    }

    void ExtractToSafeTermination(std::list<TEventHolder>&) noexcept override {}
    void Terminate(std::unique_ptr<NActors::TEventHolderPool>&&, TIntrusivePtr<NInterconnect::TStreamSocket>, const NActors::TActorContext&) override {}
};

std::unique_ptr<IZcGuard> TInterconnectZcProcessor::GetGuard()
{
    return std::make_unique<TDummyGuardRunner>(SendAsZc, Confirmed);
}

#endif

}
