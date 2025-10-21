#include "flat_bio_actor.h"
#include "flat_bio_events.h"
#include "flat_bio_stats.h"
#include "util_fmt_abort.h"
#include "util_fmt_logger.h"

#include <ydb/core/base/blobstorage.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NBlockIO {

using TEvGet = TEvBlobStorage::TEvGet;

struct TBlockIO::TLoaded : public TEvBlobStorage::TEvGetResult::TResponse{ };

TBlockIO::TBlockIO(TActorId statActorId, ui64 eventCookie)
    : ::NActors::IActorCallback(static_cast<TReceiveFunc>(&TBlockIO::Inbox), NKikimrServices::TActivity::SAUSAGE_BIO_A)
    , StatActorId(statActorId)
    , EventCookie(eventCookie)
{
}

TBlockIO::~TBlockIO()
{

}

void TBlockIO::Registered(TActorSystem *sys, const TActorId&)
{
    Logger = new NUtil::TLogger(sys, NKikimrServices::SAUSAGE_BIO);
}

void TBlockIO::Inbox(TEventHandlePtr &eh)
{
    if (auto *ev = eh->CastAsLocal<TEvBlobStorage::TEvGetResult>()) {
        if (ev->Status != NKikimrProto::OK)
            Terminate(ev->Status);
        else {
            auto *ptr = reinterpret_cast<TLoaded*>(ev->Responses.Get());

            Handle(eh->Cookie, { ptr, size_t(ev->ResponseSz) });
        }
    } else if (auto *ev = eh->CastAsLocal<NBlockIO::TEvFetch>()) {
        Y_ENSURE(!Sender, "TBlockIO actor now can handle only one request");
        Sender = eh->Sender;

        Priority = ev->Priority;
        TraceId = std::move(ev->TraceId);
        RequestCookie = ev->Cookie;

        PageCollection = std::move(ev->PageCollection);
        Pages = std::move(ev->Pages);
        Y_ENSURE(Pages, "Got TFetch request without pages list");
        PagesToBlobsConverter = new TPagesToBlobsConverter(*PageCollection, Pages);
        BlockStates.reserve(Pages.size());
        for (auto page: Pages) {
            ui64 size = PageCollection->Page(page).Size;
            BlockStates.emplace_back(size);
        }

        Dispatch();
    } else if (eh->CastAsLocal<TEvents::TEvUndelivered>()) {
        Terminate(NKikimrProto::UNKNOWN);
    } else if (eh->CastAsLocal<TEvents::TEvPoison>()) {
        PassAway();
    } else {
        Y_TABLET_ERROR("Page collection blocks IO actor got an unexpected event");
    }
}

void TBlockIO::Dispatch()
{
    const auto ctx = ActorContext();

    NKikimrBlobStorage::EGetHandleClass klass;
    switch (Priority) {
        case NBlockIO::EPriority::None:
        case NBlockIO::EPriority::Fast:
            klass = NKikimrBlobStorage::FastRead;
            break;
        case NBlockIO::EPriority::Bulk:
        case NBlockIO::EPriority::Bkgr: /* FIXME: switch to LowRead in the future */
            klass = NKikimrBlobStorage::AsyncRead;
            break;
        case NBlockIO::EPriority::Low:
            klass = NKikimrBlobStorage::LowRead;
            break;
    }

    while (auto more = PagesToBlobsConverter->Grow(NBlockIO::BlockSize)) {
        auto group = NPageCollection::TLargeGlobId::InvalidGroup;

        TArrayHolder<TEvGet::TQuery> query(new TEvGet::TQuery[+more]);

        ui32 lastBlob = Max<ui32>();
        for (const auto on : xrange(+more)) {
            auto &brick = PagesToBlobsConverter->Queue[more.From + on];
            auto glob = PageCollection->Glob(brick.Blob);

            if ((group = (on ? group : glob.Group)) != glob.Group) {
                Y_TABLET_ERROR("Cannot handle different groups in one request");
            }

            query[on].Id = glob.Logo;
            query[on].Shift = brick.Skip;
            query[on].Size = brick.Size;

            {
                auto skey = std::make_pair(query[on].Id.Channel(), group);

                GroupBytes[skey] += query[on].Size;
                if (lastBlob != brick.Blob) {
                    lastBlob = brick.Blob;
                    GroupOps[skey] += 1;
                    TotalOps++;
                }
            }
        }

        Pending++;

        auto *ev = new TEvGet(query, +more, TInstant::Max(), klass, false);

        SendToBSProxy(ctx, group, ev, more.From /* cookie, request offset */, std::move(TraceId));
    }

    if (auto logl = Logger->Log(ELnLev::Debug)) {
        logl
            << "NBlockIO pageCollection " << PageCollection->Label() << " cooked flow "
            << PagesToBlobsConverter->OnHold << "b " << PagesToBlobsConverter->Tail << "p" << " " << PagesToBlobsConverter->Queue.size()
            << " bricks in " << Pending << " reads, " << BlockStates.size() <<  "p req";
    }

    Y_ENSURE(PagesToBlobsConverter->Complete(), "NPageCollection::TPagesToBlobsConverter cooked incomplete loads");
}

void TBlockIO::Handle(ui32 base, TArrayRef<TLoaded> items)
{
    if (auto logl = Logger->Log(ELnLev::Debug)) {
        logl
            << "NBlockIO pageCollection " << PageCollection->Label() << " got base"
            << " " << items.size() << " bricks, left " << Pending;
    }

    for (auto &piece: items) {
        if (piece.Status != NKikimrProto::OK) {
            if (auto logl = Logger->Log(ELnLev::Warn)) {
                logl
                    << "NBlockIO pageCollection " << PageCollection->Label() << " get failed"
                    << ", " << piece.Id << " status " << piece.Status;
            }

            return Terminate(piece.Status);
        }

        const auto &brick = PagesToBlobsConverter->Queue[base + (&piece - &items[0])];

        auto& state = BlockStates.at(brick.Slot);
        Y_ENSURE(state.Data.size() - state.Offset >= piece.Buffer.size());
        piece.Buffer.begin().ExtractPlainDataAndAdvance(state.Data.mutable_data() + state.Offset, piece.Buffer.size());
        state.Offset += piece.Buffer.size();
    }

    if (--Pending > 0)
        return;

    size_t index = 0;
    for (auto pageId : Pages) {
        auto& state = BlockStates.at(index++);
        Y_ENSURE(state.Offset == state.Data.size());
        if (PageCollection->Verify(pageId, state.Data)) {
            continue;
        } else if (auto logl = Logger->Log(ELnLev::Crit)) {
            const auto bnd = PageCollection->Bounds(pageId);

            logl
                << "NBlockIO pageCollection " << PageCollection->Label() << " verify failed"
                << ", page " << pageId << " " << state.Data.size() << "b"
                << " spans over {";

            for (auto one: xrange(bnd.Lo.Blob, bnd.Up.Blob + 1)) {
                const auto glob = PageCollection->Glob(one);

                logl << " " << glob.Group << " " << glob.Logo;
            }

            logl << " }";
        }

        return Terminate(NKikimrProto::CORRUPTED);
    }

    Terminate(NKikimrProto::OK);
}

void TBlockIO::Terminate(EStatus code)
{
    if (auto logl = Logger->Log(code ? ELnLev::Warn : ELnLev::Debug)) {
        logl
            << "NBlockIO pageCollection " << PageCollection->Label() << " end, status " << code
            << ", cookie {req " << RequestCookie << " ev " << EventCookie << "}"
            << ", " << BlockStates.size() << " pages";
    }

    auto *ev = new TEvData(code, PageCollection, RequestCookie);

    ev->Pages.resize(Pages.size());
    for (auto index : xrange(Pages.size())) {
        auto& page = ev->Pages[index];
        page.PageId = Pages[index];
        if (code == NKikimrProto::OK) {
            page.Data = std::move(BlockStates.at(index++).Data);
        }
    }

    if (StatActorId)
        Send(StatActorId, new TEvStat(EDir::Read, Priority, PagesToBlobsConverter->OnHold, TotalOps, std::move(GroupBytes), std::move(GroupOps)));

    Send(Sender, ev, 0, EventCookie);

    return PassAway();
}

}
}
}
