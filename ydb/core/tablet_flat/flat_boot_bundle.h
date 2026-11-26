#pragma once

#include "flat_boot_iface.h"
#include "flat_boot_back.h"
#include "flat_boot_blobs.h"
#include "flat_bio_events.h"
#include "flat_sausage_packet.h"
#include "flat_part_loader.h"
#include "flat_dbase_naked.h"
#include "util_fmt_abort.h"

#include <util/generic/xrange.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NBoot {

    class TBundleLoadStep final: public NBoot::IStep {
        friend class NTable::TMetadataLoadContext;

    public:
        static constexpr NBoot::EStep StepKind = NBoot::EStep::Bundle;

        TBundleLoadStep() = delete;

        TBundleLoadStep(IStep *owner, ui32 table, TSwitch::TBundle &bundle)
            : IStep(owner, NBoot::EStep::Bundle)
            , Table(table)
            , LargeGlobIds(std::move(bundle.LargeGlobIds))
            , Legacy(std::move(bundle.Legacy))
            , Opaque(std::move(bundle.Opaque))
            , Deltas(std::move(bundle.Deltas))
            , Epoch(bundle.Epoch)
        {

        }

    private: /* IStep, boot logic DSL actor interface   */
        void Start() override
        {
            auto environment = Logic->PageCache.MakeLoaderEnvironment(LargeGlobIds, Back->PageCollections.Get());
            TVector<NTable::TPageCollectionComponents> components(LargeGlobIds.size());
            for (ui32 slot = 0; slot < LargeGlobIds.size(); ++slot) {
                components[slot].LargeGlobId = LargeGlobIds[slot];
            }
            NTable::TPartComponents parts{
                .PageCollectionComponents = std::move(components),
                .Legacy = std::move(Legacy),
                .Opaque = std::move(Opaque),
                .Deltas = std::move(Deltas),
                .Epoch = Epoch,
            };
            Loader = new NTable::TLoader(std::move(environment), std::move(parts));
            Loader->BeginMetadata(MetadataContext());
            TryFinalize();
        }

        bool HandleBio(const TPageCacheResult& msg, NActors::IEventBase& event) override
        {
            Y_ENSURE(Loader, "PageCollections loader got un unexpected pages fetch");

            LeftReads -= 1;

            if (msg.Status == NKikimrProto::OK) {
                Loader->Save(event);

                TryFinalize();

            } else if (auto logl = Env->Logger()->Log(ELnLev::Error)) {
                logl
                    << NFmt::Do(*Back)
                    << " Page collection load failed, " << NFmt::Do(msg);
            }

            return msg.Status == NKikimrProto::OK;
        }

        void HandleStep(TIntrusivePtr<IStep> step) override
        {
            auto* load = step->FinalCast<TLoadBlobs>();
            const auto result = Loader->SaveMetadata(load->Cookie, load->PlainData());
            if (result == NTable::EMetadataLoadResult::Error) {
                Y_TABLET_ERROR("Got empty page collection metadata");
            }
            if (result != NTable::EMetadataLoadResult::Ignored) {
                TryFinalize();
            }
        }

        NTable::EMetadataLoadResult HandleMetadata(
            ui32 slot, const TPageCacheMetadataResult& metadata, NActors::IEventBase& event) override {
            const auto result = Loader->AcceptMetadata(slot, metadata, event, MetadataContext());
            if (result == NTable::EMetadataLoadResult::Error) {
                if (auto logl = Env->Logger()->Log(ELnLev::Error)) {
                    logl << NFmt::Do(*Back) << " Collection metadata load failed for " << LargeGlobIds[slot].Lead
                         << ": " << metadata.Error;
                }
                return result;
            }
            if (result != NTable::EMetadataLoadResult::Ignored) {
                TryFinalize();
            }
            return result;
        }

        void CancelMetadata() noexcept override {
            Loader->CancelMetadata();
        }

    private:
        NTable::TMetadataLoadContext MetadataContext()
        {
            return { *this, TActivationContext::ActorSystem(), Logic->SelfId, Logic->BootAttempt,
                NBlockIO::EPriority::Fast };
        }

        void LoadRawMetadata(ui32 slot, const NPageCollection::TLargeGlobId& id)
        {
            Spawn<TLoadBlobs>(id, slot);
        }

        bool SendMetadataRequest(ui32 slot, const void* request, THolder<NActors::IEventBase> event)
        {
            return Logic->LoadMetadata(this, slot, LargeGlobIds[slot], request, std::move(event));
        }

        void SendMetadataUpdate(THolder<NActors::IEventBase> event)
        {
            Logic->UpdateMetadata(std::move(event));
        }

        void TryFinalize()
        {
            if (!Loader->MetadataReady()) {
                return;
            }
            if (!LeftReads) {
                if (auto fetch = Loader->Run({.PreloadIndex = true, .PreloadData = false})) {
                    LeftReads += Logic->LoadPages(this, *Loader, std::move(fetch));
                }
            }

            if (!LeftReads) {
                NTable::TPartView partView = Loader->Result();
                const auto* partStore = partView.As<NTable::TPartStore>();

                if (auto logl = Env->Logger()->Log(ELnLev::Debug)) {
                    logl
                        << NFmt::Do(*Back) << " table " << Table
                        << " part loaded, page collections [";

                    for (ui32 slot : xrange(partStore->GetPageCollectionCount()))
                        logl << " " << partStore->GetPageCollectionRef(slot)->Label();

                    logl << " ]";
                }

                Logic->PageCache.CaptureForRestart(*partStore, Logic->Result().PageCollections);
                Back->DatabaseImpl->Merge(Table, std::move(partView));

                Env->Finish(this); /* return self to owner */
            }
        }

    private:
        const ui32 Table = Max<ui32>();

        TAutoPtr<NTable::TLoader> Loader;
        TVector<NPageCollection::TLargeGlobId> LargeGlobIds;
        TString Legacy;
        TString Opaque;
        TVector<TString> Deltas;
        NTable::TEpoch Epoch;

        TLeft LeftReads;
    };
}
}
}
