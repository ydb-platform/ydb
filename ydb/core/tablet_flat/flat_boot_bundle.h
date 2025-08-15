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
    public:
        using TPageCollection = TPrivatePageCache::TPageCollection;

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
            PageCollections.resize(LargeGlobIds.size());

            for (auto slot: xrange(LargeGlobIds.size())) {
                if (auto *pageCollection = Back->PageCollections.FindPtr(LargeGlobIds[slot].Lead)) {
                    PageCollections[slot] = *pageCollection;
                } else {
                    LeftMetas += Spawn<TLoadBlobs>(LargeGlobIds[slot], slot);
                }
            }

            TryLoad();
        }

        bool HandleBio(NSharedCache::TEvResult &msg) override
        {
            Y_ENSURE(Loader, "PageCollections loader got un unexpected pages fetch");

            LeftReads -= 1;

            if (msg.Status == NKikimrProto::OK) {
                Y_ENSURE(msg.Cookie == 0);
                Loader->Save(std::move(msg.Pages));

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
            auto *load = step->ConsumeAs<TLoadBlobs>(LeftMetas);

            if (Loader) {
                Y_TABLET_ERROR("Got an unexpected load blobs result");
            } else if (load->Cookie >= PageCollections.size()) {
                Y_TABLET_ERROR("Got blobs load step with an invalid cookie");
            } else if (PageCollections[load->Cookie]) {
                Y_TABLET_ERROR("Page collection is already loaded at room " << load->Cookie);
            } else {
                auto *pack = new NPageCollection::TPageCollection(load->LargeGlobId, load->PlainData());

                PageCollections[load->Cookie] = new TPageCollection(pack);
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
                if (auto fetch = Loader->Run({.PreloadIndex = true, .PreloadData = false})) {
                    LeftReads += Logic->LoadPages(this, std::move(fetch));
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
            for (auto &pageCollection : partView.As<NTable::TPartStore>()->PageCollections)
                Logic->Result().PageCollections.push_back(pageCollection);

            if (auto &pageCollection = partView.As<NTable::TPartStore>()->Pseudo)
                Logic->Result().PageCollections.push_back(pageCollection);
        }

    private:
        const ui32 Table = Max<ui32>();

        TAutoPtr<NTable::TLoader> Loader;
        TVector<NPageCollection::TLargeGlobId> LargeGlobIds;
        TVector<TIntrusivePtr<TPrivatePageCache::TPageCollection>> PageCollections;
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
