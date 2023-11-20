#include "read_data_protocol.h"

#include <ydb/core/base/appdata.h>
#include <util/system/error.h>
#include <util/system/yassert.h>

namespace NActors {

void TReadDataProtocolImpl::ProtocolFunc(
        TAutoPtr<NActors::IEventHandle>& ev) noexcept
{
    if (Cancelled) {
        return;
    }

    switch (ev->GetTypeRewrite()) {
    case TEvSocketReadyRead::EventType:
        TryAgain(TlsActivationContext->AsActorContext());
        break;

    default:
        Y_ABORT("Unknown message type dispatched");
    }
}


static TDelegate NotifyReadyRead(
        const TIntrusivePtr<TSharedDescriptor>& FDPtr,
        const TActorContext& ctx)
{
    Y_UNUSED(FDPtr);
    return [=]() { ctx.Send(ctx.SelfID, new TEvSocketReadyRead); };
}


void TReadDataProtocolImpl::TryAgain(const TActorContext& ctx) noexcept {
    int recvResult;
    for (;;) {
        recvResult = Socket->Recv(Data, Len);

        if (recvResult > 0) {
            Y_ABORT_UNLESS(Len >= (size_t)recvResult);
            Data += recvResult;
            Len -= recvResult;
            Filled += recvResult;
            if (Len == 0) {
                /* ResetReadBuf may change Data, Len and Filled here */
                if (Catcher->CatchReadDataComplete(ctx, Filled)) {
                    return;
                }

                if (Cancelled) {
                    return;
                }

                continue;
            }
        } else {
            if (-recvResult == EAGAIN || -recvResult == EWOULDBLOCK) {
                if (Filled > 0) {
                    if (Catcher->CatchReadDataComplete(ctx, Filled)) {
                        return;
                    }

                    if (Cancelled) {
                        return;
                    }

                    break;
                }
            } else if (-recvResult == EINTR) {
                continue;
            } else if (!recvResult) {
                if (Filled > 0 && Catcher->CatchReadDataComplete(ctx, Filled)) {
                    return;
                }
                Catcher->CatchReadDataClosed();
                return;
            }
            break;
        }
    }

    if (-recvResult == EAGAIN || -recvResult == EWOULDBLOCK) {
        IPoller* poller = NKikimr::AppData(ctx)->PollerThreads.Get();
        poller->StartRead(Socket,
            std::bind(NotifyReadyRead, std::placeholders::_1, ctx));
        return;
    }

    switch (-recvResult) {
    case ECONNRESET:
        Catcher->CatchReadDataError("Connection reset by peer");
        return;

    case EPIPE:
        Catcher->CatchReadDataError("Connection is closed");
        return;

    default:
        {
            char buf[1024];
            LastSystemErrorText(buf, 1024, -recvResult);
            Catcher->CatchReadDataError(TString("Socker error: ") + buf);
            return;
        }

    case EBADF:
    case EFAULT:
    case EINVAL:
    case ENOTCONN:
    case ENOTSOCK:
    case EOPNOTSUPP:
        {
            Y_ABORT("Very bad socket error");
        }
    }
}

void TReadDataProtocolImpl::CancelReadData(const TActorContext& /*ctx*/) noexcept {
    Cancelled = true;
//    IPoller* poller = NKikimr::AppData(ctx)->PollerThreads.Get();
//    poller->CancelRead(Socket);
}

}
