#pragma once

#include "test_part.h"

#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/rows/tool.h>
#include <ydb/core/tablet_flat/test/libs/rows/mass.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>

#include <ydb/core/tablet_flat/flat_sausage_grind.h>
#include <ydb/core/tablet_flat/flat_abi_check.h>
#include <ydb/core/tablet_flat/flat_table_part.h>
#include <ydb/core/tablet_flat/flat_part_iface.h>
#include <ydb/core/tablet_flat/flat_part_scheme.h>
#include <ydb/core/tablet_flat/flat_part_writer.h>
#include <ydb/core/tablet_flat/protos/flat_table_part.pb.h>

#include <util/generic/cast.h>
#include <util/stream/mem.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    class TLoader { /* Test env clone for NTable::TLoader */
    public:
        TLoader(TIntrusiveConstPtr<TStore> store, TString overlay)
            : Store(std::move(store))
            , Overlay(std::move(overlay))
        {

        }

        TIntrusiveConstPtr<TPartStore> Load(const TLogoBlobID token) noexcept
        {
            using NPage::TFrames;
            using NPage::TExtBlobs;
            using NPage::TBloom;

            Y_ABORT_UNLESS(Store, "Cannot load from an empty store");

            if (Store->PageCollectionPagesCount(0 /* primary room */) == 0) {
                return nullptr;
            }

            NProto::TRoot root;

            if (auto *raw = Store->GetMeta()) {
                TMemoryInput stream(raw->data(), raw->size());
                Y_ABORT_UNLESS(root.ParseFromArcadiaStream(&stream));
            } else {
                root.SetEpoch(0); /* for loading from abi blobs */
            }

            if (auto *abi = root.HasEvol() ? &root.GetEvol() : nullptr)
                TAbi().Check(abi->GetTail(), abi->GetHead(), "part");

            TStore::TEggs eggs;

            if (auto *lay = (root.HasLayout() ? &root.GetLayout() : nullptr)) {
                eggs = RootedEggs(*lay);
            } else {
                eggs = Store->LegacyEggs();
            }

            auto run = TOverlay::Decode({ }, Overlay).Slices;

            const auto &stat = root.GetStat();

            const auto &minRowVersion = root.GetMinRowVersion();
            const auto &maxRowVersion = root.GetMaxRowVersion();

            TEpoch epoch = TEpoch(root.GetEpoch());

            size_t indexesRawSize = 0;
            if (eggs.BTreeGroupIndexes) {
                for (const auto &meta : eggs.BTreeGroupIndexes) {
                    indexesRawSize += meta.IndexSize;
                }
                for (const auto &meta : eggs.BTreeHistoricIndexes) {
                    indexesRawSize += meta.IndexSize;
                }
            } else {
                for (auto indexPage : eggs.FlatGroupIndexes) {
                    indexesRawSize += Store->GetPageSize(0, indexPage);
                }
                for (auto indexPage : eggs.FlatHistoricIndexes) {
                    indexesRawSize += Store->GetPageSize(0, indexPage);
                }
            }

            return
                new TPartStore(
                    std::move(Store),
                    token,
                    {
                        epoch,
                        TPartScheme::Parse(*eggs.Scheme, eggs.Rooted),
                        { eggs.FlatGroupIndexes, eggs.FlatHistoricIndexes, eggs.BTreeGroupIndexes, eggs.BTreeHistoricIndexes },
                        eggs.Blobs ? new TExtBlobs(*eggs.Blobs, { }) : nullptr,
                        eggs.ByKey ? new TBloom(*eggs.ByKey) : nullptr,
                        eggs.Large ? new TFrames(*eggs.Large) : nullptr,
                        eggs.Small ? new TFrames(*eggs.Small) : nullptr,
                        indexesRawSize,
                        TRowVersion(minRowVersion.GetStep(), minRowVersion.GetTxId()),
                        TRowVersion(maxRowVersion.GetStep(), maxRowVersion.GetTxId()),
                        eggs.GarbageStats ? new NPage::TGarbageStats(*eggs.GarbageStats) : nullptr,
                        eggs.TxIdStats ? new NPage::TTxIdStatsPage(*eggs.TxIdStats) : nullptr,
                    },
                    {
                        stat.HasBytes() ? stat.GetBytes() : 0,
                        stat.HasCoded() ? stat.GetCoded() : 0,
                        stat.HasDrops() ? stat.GetDrops() : 0,
                        stat.HasRows() ? stat.GetRows() : 0,
                        stat.HasHiddenRows() ? stat.GetHiddenRows() : 0,
                        stat.HasHiddenDrops() ? stat.GetHiddenDrops() : 0,
                    },
                    run ? std::move(run) : TSlices::All()
                );
        }

    private:
        TStore::TEggs RootedEggs(const NProto::TLayout &lay) const noexcept
        {
            const auto undef = Max<NPage::TPageId>();

            TVector<TPageId> indexGroupsPages, indexHistoricPages;
            if (lay.HasIndex()) {
                indexGroupsPages.push_back(lay.GetIndex());
            }
            for (ui32 pageId : lay.GetGroupIndexes()) {
                indexGroupsPages.push_back(pageId);
            }
            for (ui32 pageId : lay.GetHistoricIndexes()) {
                indexHistoricPages.push_back(pageId);
            }

            TVector<NPage::TBtreeIndexMeta> BTreeGroupIndexes, BTreeHistoricIndexes;
            for (bool history : {false, true}) {
                for (const auto &meta : history ? lay.GetBTreeHistoricIndexes() : lay.GetBTreeGroupIndexes()) {
                    NPage::TBtreeIndexMeta converted{{
                        meta.GetRootPageId(),
                        meta.GetRowCount(),
                        meta.GetDataSize(),
                        meta.GetGroupDataSize(),
                        meta.GetErasedRowCount()}, 
                        meta.GetLevelCount(), 
                        meta.GetIndexSize()};
                    (history ? BTreeHistoricIndexes : BTreeGroupIndexes).push_back(converted);
                }
            }

            return {
                true /* rooted page collection */,
                std::move(indexGroupsPages), 
                std::move(indexHistoricPages),
                BTreeGroupIndexes,
                BTreeHistoricIndexes,
                Store->GetPage(0, lay.HasScheme() ? lay.GetScheme() : undef),
                Store->GetPage(0, lay.HasGlobs() ? lay.GetGlobs() : undef),
                Store->GetPage(0, lay.HasByKey() ? lay.GetByKey() : undef),
                Store->GetPage(0, lay.HasLarge() ? lay.GetLarge() : undef),
                Store->GetPage(0, lay.HasSmall() ? lay.GetSmall() : undef),
                Store->GetPage(0, lay.HasGarbageStats() ? lay.GetGarbageStats() : undef),
                Store->GetPage(0, lay.HasTxIdStats() ? lay.GetTxIdStats() : undef),
            };
        }

    private:
        TIntrusiveConstPtr<TStore> Store;
        const TString Overlay;
    };

    class TWriterBundle : public IPageWriter {
    public:
        TWriterBundle(size_t groups, const TLogoBlobID token)
            : Groups(groups)
            , CookieAllocator(new NPageCollection::TCookieAllocator(token.TabletID(),
                        (ui64(token.Generation()) << 32) | token.Step(),
                        { token.Cookie(), token.Cookie() + 1000 },
                        {{ ui8(0) /* channel */, ui32(0) /* grpup*/ }} ))
        {

        }

        TPartEggs Flush(TIntrusiveConstPtr<TRowScheme> scheme, const TWriteStats &written)
        {
            Y_ABORT_UNLESS(!Store, "Writer has not been flushed");
            Y_ABORT_UNLESS(written.Parts == Parts.size());

            return
                { new TWriteStats(written), std::move(scheme), std::move(Parts) };
        }

        TStore& Back() noexcept
        {
            return Store ? *Store : *(Store = new TStore(Groups, NextGlobOffset));
        }

    private:
        TPageId WriteOuter(TSharedData blob) noexcept override
        {
            return Back().WriteOuter(blob);
        }

        TPageId Write(TSharedData page, EPage type, ui32 group) noexcept override
        {
            return Back().Write(page, type, group);
        }

        void WriteInplace(TPageId page, TArrayRef<const char> body) noexcept override
        {
            Back().WriteInplace(page, body);
        }

        NPageCollection::TGlobId WriteLarge(TString blob, ui64 ref) noexcept override
        {
            Growth->Pass(ref);
            return Back().WriteLarge(TSharedData::Copy(blob));
        }

        void Finish(TString overlay) noexcept override
        {
            Y_ABORT_UNLESS(Store, "Finish called without any writes");

            Growth->Unwrap();
            Store->Finish();
            NextGlobOffset = Store->NextGlobOffset();

            NTest::TLoader loader(std::exchange(Store,{ }), std::move(overlay));

            Parts.emplace_back(loader.Load(CookieAllocator->Do(0 /* channel */, 0).Logo));
        }

    private:
        const size_t Groups;
        ui32 NextGlobOffset = 0;
        TIntrusivePtr<TStore> Store;
        TAutoPtr<NPageCollection::TCookieAllocator> CookieAllocator;
        TAutoPtr<TScreen::TCook> Growth = new TScreen::TCook;
        TVector<TIntrusiveConstPtr<TPartStore>> Parts;
    };

    class TPartCook {
    public:
        TPartCook() = delete;

        TPartCook(
                const TLayoutCook &lay,
                const NPage::TConf &opts,
                const TLogoBlobID &token = TLogoBlobID(1, 2, 3, 1, 0, 0),
                TEpoch epoch = TEpoch::Zero())
            : TPartCook(lay.RowScheme(), opts, token, epoch)
        {

        }

        NPageCollection::TGlobId PutBlob(TString data, ui64 ref)
        {
            return static_cast<IPageWriter&>(Pages).WriteLarge(std::move(data), ref);
        }

        TPartCook(
                TIntrusiveConstPtr<TRowScheme> rows,
                const NPage::TConf &opts,
                const TLogoBlobID &token = TLogoBlobID(1, 2, 3, 1, 0, 0),
                TEpoch epoch = TEpoch::Zero())
            : Scheme(rows)
            , Pages(Scheme->Families.size(), token)
        {
            auto tags = Scheme->Tags();
            auto *scheme = new TPartScheme(Scheme->Cols);

            Writer = new TPartWriter(scheme, tags, Pages, opts, epoch);

            for (TPos on = 0; on < tags.size(); on++)
                Remap[tags[on]] = on;
        }

        static TPartEggs Make(
                const TMass &mass,
                NPage::TConf conf = { },
                const TLogoBlobID &token = TLogoBlobID(1, 2, 3, 1, 0, 0),
                TEpoch epoch = TEpoch::Zero(),
                TRowVersion rowVersion = TRowVersion::Min(),
                ERowOp op = ERowOp::Upsert)
        {
            TPartCook cook(mass.Model->Scheme, conf, token, epoch);

            auto eggs = cook.Ver(rowVersion).AddOp(op, mass.Saved.begin(), mass.Saved.end()).Finish();

            if (const auto *written = eggs.Written.Get()) {
                mass.Model->Check({ &written->Rows, 1 });
            } else {
                Y_ABORT("Got part eggs without TWriteStats result");
            }

            return eggs;
        }

        inline TPartCook& Ver(TRowVersion rowVersion = TRowVersion::Min())
        {
            NextVersion = rowVersion;
            NextTxId = 0;

            return *this;
        }

        inline TPartCook& Delta(ui64 txId)
        {
            NextVersion = TRowVersion::Min();
            NextTxId = txId;

            return *this;
        }

        template<typename ...TArgs>
        inline TPartCook& AddOpN(ERowOp op, TArgs&&...args)
        {
            auto row = *TSchemedCookRow(*Scheme).Col(std::forward<TArgs>(args)...);

            return Add(std::move(row), op);
        }

        template<typename ...TArgs>
        inline TPartCook& AddN(TArgs&&...args)
        {
            auto row = *TSchemedCookRow(*Scheme).Col(std::forward<TArgs>(args)...);

            return Add(std::move(row), ERowOp::Upsert);
        }

        template<typename TIter>
        TPartCook& AddOp(ERowOp op, TIter it, const TIter end)
        {
            while (it != end) Add(*it++, op);

            return *this;
        }

        template<typename TIter>
        TPartCook& Add(TIter it, const TIter end)
        {
            while (it != end) Add(*it++, ERowOp::Upsert);

            return *this;
        }

        TPartCook& Add(const TRow &tagged, ERowOp rop = ERowOp::Upsert)
        {
            TVector<TCell> key(Scheme->Keys->Types.size());
            TRowState row(Remap.size());
            row.Touch(rop);

            for (auto &value: *tagged) {
                auto *info = TRowTool(*Scheme).ColFor(value);

                if (info->IsKey()) {
                    key[info->Key] = value.Cell;
                } else {
                    row.Set(Remap.at(value.Tag), value.Op, value.Cell);
                }
            }

            if (LastKey &&
                CompareTypedCellVectors(
                    LastKey.data(), key.data(),
                    Scheme->Keys->Types.data(), Scheme->Keys->Types.size()) != 0)
            {
                // Key changed, end the last key
                Writer->EndKey();
                LastKey = { };
            }

            if (!LastKey) {
                // Start a new key. It must be valid until EndKey, so we make a copy
                LastKey = TOwnedCellVec::Make(key);
                Writer->BeginKey(LastKey);
                CurrentDeltas = 0;
                CurrentVersions = 0;
            }

            if (NextTxId != 0) {
                Y_ABORT_UNLESS(CurrentVersions == 0, "Cannot write deltas after committed versions");
                Writer->AddKeyDelta(row, NextTxId);
                ++CurrentDeltas;
            } else {
                Writer->AddKeyVersion(row, NextVersion);
                ++CurrentVersions;
            }

            return *this;
        }

        TPartEggs Finish()
        {
            if (LastKey) {
                // End the last key before finishing
                Writer->EndKey();
                LastKey = { };
            }

            return Pages.Flush(std::move(Scheme), Writer->Finish());
        }

        ui64 GetDataBytes(ui32 room) noexcept
        {
            return Pages.Back().GetDataBytes(room);
        }

    private:
        TIntrusiveConstPtr<TRowScheme> Scheme;
        TWriterBundle Pages;
        TMap<TTag, TPos> Remap;
        TAutoPtr<TPartWriter> Writer;
        TOwnedCellVec LastKey;
        TRowVersion NextVersion = TRowVersion::Min();
        ui64 NextTxId = 0;
        ui64 CurrentDeltas = 0;
        ui64 CurrentVersions = 0;
    };
}}}
