#pragma once

#include "flat_boot_iface.h"
#include "flat_boot_back.h"
#include "flat_boot_blobs.h"
#include "flat_bio_events.h"
#include "flat_sausage_packet.h"
#include "flat_part_loader.h"
#include "flat_dbase_naked.h"

#include <util/generic/xrange.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NBoot {

    class TBundleLoadStep final: public NBoot::IStep {
    public:
        using TCache = TPrivatePageCache::TInfo;

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
        void Start() noexcept override
        {
            PageCollections.resize(LargeGlobIds.size());

            for (auto slot: xrange(LargeGlobIds.size())) {
                if (auto *info = Back->PageCaches.FindPtr(LargeGlobIds[slot].Lead)) {
                    PageCollections[slot] = *info;
                } else {
                    LeftMetas += Spawn<TLoadBlobs>(LargeGlobIds[slot], slot);
                }
            }

            TryLoad();
        }

        bool HandleBio(NSharedCache::TEvResult &msg) noexcept override
        {
            Y_ABORT_UNLESS(Loader, "PageCollections loader got un unexpected pages fetch");

            LeftReads -= 1;

            if (msg.Status == NKikimrProto::OK) {
                Loader->Save(msg.Cookie, msg.Loaded);

                TryFinalize();

            } else if (auto logl = Env->Logger()->Log(ELnLev::Error)) {
                logl
                    << NFmt::Do(*Back)
                    << " Page collection load failed, " << NFmt::Do(msg);
            }

            return msg.Status == NKikimrProto::OK;
        }

        void HandleStep(TIntrusivePtr<IStep> step) noexcept override
        {
            auto *load = step->ConsumeAs<TLoadBlobs>(LeftMetas);

            if (Loader) {
                Y_ABORT("Got an unexpected load blobs result");
            } else if (load->Cookie >= PageCollections.size()) {
                Y_ABORT("Got blobs load step with an invalid cookie");
            } else if (PageCollections[load->Cookie]) {
                Y_ABORT("Page collection is already loaded at room %zu", load->Cookie);
            } else {
                auto *pack = new NPageCollection::TPageCollection(load->LargeGlobId, load->PlainData());

                PageCollections[load->Cookie] = new TPrivatePageCache::TInfo(pack);
            }

            TryLoad();
        }

    private:
        void TryLoad()
        {
            if (!LeftMetas) {
                Loader = new NTable::TLoader(
                    std::move(PageCollections),
                    std::move(Legacy),
                    std::move(Opaque),
                    std::move(Deltas),
                    Epoch);

                TryFinalize();
            }
        }

        void TryFinalize()
        {
            if (!LeftReads) {
                for (auto req : Loader->Run(false)) {
                    LeftReads += Logic->LoadPages(this, req);
                }
            }

            if (!LeftReads) {
                NTable::TPartView partView = Loader->Result();

                if (auto logl = Env->Logger()->Log(ELnLev::Debug)) {
                    logl
                        << NFmt::Do(*Back) << " table " << Table
                        << " part loaded, page collections [";

                    for (auto &cache : partView.As<NTable::TPartStore>()->PageCollections)
                        logl << " " << cache->Id;

                    logl << " ]";
                }

                PropagateSideEffects(partView);
                Back->DatabaseImpl->Merge(Table, std::move(partView));

                Env->Finish(this); /* return self to owner */
            }
        }

        void PropagateSideEffects(const NTable::TPartView &partView)
        {
            for (auto &cache : partView.As<NTable::TPartStore>()->PageCollections)
                Logic->Result().PageCaches.push_back(cache);

            if (auto &cache = partView.As<NTable::TPartStore>()->Pseudo)
                Logic->Result().PageCaches.push_back(cache);
        }

    private:
        const ui32 Table = Max<ui32>();

        TAutoPtr<NTable::TLoader> Loader;
        TVector<NPageCollection::TLargeGlobId> LargeGlobIds;
        TVector<TIntrusivePtr<TCache>> PageCollections;
        TString Legacy;
        TString Opaque;
        TVector<TString> Deltas;
        NTable::TEpoch Epoch;

        TLeft LeftMetas;
        TLeft LeftReads;
    };
}
}
}
