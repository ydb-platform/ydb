#pragma once
#include "defs.h"
#include "flat_part_store.h"
#include "flat_sausagecache.h"
#include "shared_cache_events.h"
#include "util_fmt_abort.h"
#include "util_basics.h"
#include <ydb/core/tablet_flat/protos/flat_table_part.pb.h>
#include <ydb/core/util/pb.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/stream/mem.h>

namespace NKikimr {
namespace NTable {

    class TKeysEnv;

    class TLoader {
    public:
        enum class EStage : ui8 {
            Meta,
            PartView,
            Slice,
            Deltas,
            Result,
        };

        using TCache = NTabletFlatExecutor::TPrivatePageCache::TInfo;

        TLoader(TPartComponents ou)
            : TLoader(TPartStore::Construct(std::move(ou.PageCollectionComponents)),
                    std::move(ou.Legacy),
                    std::move(ou.Opaque),
                    /* no deltas */ { },
                    ou.Epoch)
        {

        }

        TLoader(TVector<TIntrusivePtr<TCache>>, TString legacy, TString opaque,
                TVector<TString> deltas = { },
                TEpoch epoch = NTable::TEpoch::Max());
        ~TLoader();

        TVector<TAutoPtr<NPageCollection::TFetch>> Run()
        {
            while (Stage < EStage::Result) {
                TAutoPtr<NPageCollection::TFetch> fetch;

                switch (Stage) {
                    case EStage::Meta:
                        StageParseMeta();
                        break;
                    case EStage::PartView:
                        fetch = StageCreatePartView();
                        break;
                    case EStage::Slice:
                        fetch = StageSliceBounds();
                        break;
                    case EStage::Deltas:
                        StageDeltas();
                        break;
                    default:
                        break;
                }

                if (fetch) {
                    if (!fetch->Pages) {
                        Y_Fail("TLoader is trying to fetch 0 pages");
                    }
                    return { fetch };
                }

                Stage = EStage(ui8(Stage) + 1);
            }

            return { };
        }

        void Save(ui64 cookie, TArrayRef<NSharedCache::TEvResult::TLoaded>) noexcept;

        constexpr static bool NeedIn(EPage page) noexcept
        {
            return
                page == EPage::Scheme || page == EPage::Index
                || page == EPage::Frames || page == EPage::Globs
                || page == EPage::Schem2 || page == EPage::Bloom
                || page == EPage::GarbageStats
                || page == EPage::TxIdStats;
        }

        TPartView Result() noexcept
        {
            Y_VERIFY(Stage == EStage::Result);
            Y_VERIFY(PartView, "Result may only be grabbed once");
            Y_VERIFY(PartView.Slices, "Missing slices in Result stage");
            return std::move(PartView);
        }

        static TEpoch GrabEpoch(const TPartComponents &pc)
        {
            Y_VERIFY(pc.PageCollectionComponents, "PartComponents should have at least one pageCollectionComponent");
            Y_VERIFY(pc.PageCollectionComponents[0].Packet, "PartComponents should have a parsed meta pageCollectionComponent");

            const auto &meta = pc.PageCollectionComponents[0].Packet->Meta;

            for (ui32 page = meta.TotalPages(); page--;) {
                if (meta.GetPageType(page) == ui32(EPage::Schem2)
                    || meta.GetPageType(page) == ui32(EPage::Scheme))
                {
                    TProtoBox<NProto::TRoot> root(meta.GetPageInplaceData(page));

                    Y_VERIFY(root.HasEpoch());

                    return TEpoch(root.GetEpoch());
                }
            }

            Y_FAIL("Cannot locate part metadata in page collections of PartComponents");
        }

        static TLogoBlobID BlobsLabelFor(const TLogoBlobID &base) noexcept
        {
            /* By convention IPageCollection blobs label for page collection has the same logo
                as the meta logo but Cookie + 1. Blocks writer always make a
                gap between two subsequent meta TLargeGlobIds thus this value does
                not overlap any real TLargeGlobId leader blob.
             */

            return
                TLogoBlobID(
                    base.TabletID(), base.Generation(), base.Step(),
                    base.Channel(), 0 /* size */, base.Cookie() + 1);
        }

    private:
        bool HasBasics() const noexcept
        {
            return SchemeId != Max<TPageId>() && IndexId != Max<TPageId>();
        }

        const TSharedData* GetPage(TPageId page) noexcept
        {
            return page == Max<TPageId>() ? nullptr : Packs[0]->Lookup(page);
        }

        void ParseMeta(TArrayRef<const char> plain) noexcept
        {
            TMemoryInput stream(plain.data(), plain.size());
            bool parsed = Root.ParseFromArcadiaStream(&stream);
            Y_VERIFY(parsed && stream.Skip(1) == 0, "Cannot parse TPart meta");
            Y_VERIFY(Root.HasEpoch(), "TPart meta has no epoch info");
        }

        void StageParseMeta() noexcept;
        TAutoPtr<NPageCollection::TFetch> StageCreatePartView() noexcept;
        TAutoPtr<NPageCollection::TFetch> StageSliceBounds() noexcept;
        void StageDeltas() noexcept;

    private:
        TVector<TIntrusivePtr<TCache>> Packs;
        const TString Legacy;
        const TString Opaque;
        const TVector<TString> Deltas;
        const TEpoch Epoch;
        EStage Stage = EStage::Meta;
        bool Rooted = false; /* Has full topology metablob */
        TPageId SchemeId = Max<TPageId>();
        TPageId IndexId = Max<TPageId>();
        TPageId GlobsId = Max<TPageId>();
        TPageId LargeId = Max<TPageId>();
        TPageId SmallId = Max<TPageId>();
        TPageId ByKeyId = Max<TPageId>();
        TPageId GarbageStatsId = Max<TPageId>();
        TPageId TxIdStatsId = Max<TPageId>();
        TVector<TPageId> GroupIndexesIds;
        TVector<TPageId> HistoricIndexesIds;
        TRowVersion MinRowVersion;
        TRowVersion MaxRowVersion;
        NProto::TRoot Root;
        TPartView PartView;
        TAutoPtr<TKeysEnv> KeysEnv;
    };
}}
