#pragma once

#include <ydb/core/tablet_flat/flat_sausage_solid.h>
#include <ydb/core/blobstorage/dsproxy/mock/model.h>
#include <ydb/core/base/blobstorage.h>

namespace NKikimr {
namespace NFake {

    class TStorage : public ::NActors::IActorCallback {
    public:
        using TEventHandlePtr = TAutoPtr<::NActors::IEventHandle>;
        using ELnLev = NUtil::ELnLev;
        using NStore = TEvBlobStorage;

        TStorage(ui32 group)
            : ::NActors::IActorCallback(static_cast<TReceiveFunc>(&TStorage::Inbox), NKikimrServices::TActivity::FAKE_ENV_A)
            , Group(group)
            , Model(new NFake::TProxyDS)
        {
             Y_UNUSED(Group);
        }

    private:
        void Registered(TActorSystem *sys, const TActorId &owner) override
        {
            Owner = owner;

            Logger = new NUtil::TLogger(sys, NKikimrServices::FAKE_ENV);
        }

        void Inbox(TEventHandlePtr &eh)
        {
            if (auto *ev = eh->CastAsLocal<NStore::TEvPut>()) {

                if (ev->Buffer.size() == ev->Id.BlobSize()) {

                } else if (auto logl = Logger->Log(ELnLev::Abort)) {
                    logl
                        << "DS." << Group << " got TEvPut { " << ev->Id
                        << " blob " << ev->Buffer.size() << "b" << "}"
                        << " with missmatched data size";
                }

                ++PutItems, PutBytes += ev->Buffer.size();

                Reply(eh, Model->Handle(ev));

            } else if (auto *ev = eh->CastAsLocal<NStore::TEvGet>()) {
                Reply(eh, Model->Handle(ev));
            } else if (auto *ev = eh->CastAsLocal<NStore::TEvBlock>()) {
                Reply(eh, Model->Handle(ev));
            } else if (auto *ev = eh->CastAsLocal<NStore::TEvDiscover>()) {
                Reply(eh, Model->Handle(ev));
            } else if (auto *ev = eh->CastAsLocal<NStore::TEvRange>()) {
                Reply(eh, Model->Handle(ev));
            } else if (auto *ev = eh->CastAsLocal<NStore::TEvCollectGarbage>()) {
                Reply(eh, Model->Handle(ev));
            } else if (eh->CastAsLocal<NStore::TEvStatus>()) {
                auto flg = Model->GetStorageStatusFlags();

                Reply(eh, new NStore::TEvStatusResult(NKikimrProto::OK, flg));

            } else if (eh->CastAsLocal<TEvents::TEvPoison>()) {
                ReportUsage();

                Send(std::exchange(Owner, { }), new TEvents::TEvGone, 0, Group);

                PassAway();
            } else {
                 Y_ABORT("DS proxy model got an unexpected event");
            }
        }

        void Reply(TEventHandlePtr &eh, IEventBase *ev)
        {
            Send(eh->Sender, ev, 0, eh->Cookie);
        }

        void ReportUsage() const noexcept
        {
            if (auto logl = Logger->Log(ELnLev::Info)) {

                auto &blobs = Model->AllMyBlobs();
                size_t bytes = 0;

                for (auto &one: blobs) bytes += one.first.BlobSize();

                logl
                    << "DS." << Group << " gone"
                    << ", left {" << bytes << "b, " << blobs.size() << "}"
                    << ", put {" << PutBytes << "b, " << PutItems << "}";
            }
        }

    private:
        const ui32 Group = NPageCollection::TLargeGlobId::InvalidGroup;

        TActorId Owner;
        TAutoPtr<NUtil::ILogger> Logger;
        TAutoPtr<NFake::TProxyDS> Model;

        ui64 PutItems = 0;
        ui64 PutBytes = 0;
    };

}
}
