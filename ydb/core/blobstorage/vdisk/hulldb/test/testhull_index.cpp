#include "testhull_index.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>

namespace NKikimr {
namespace NTest {

    //////////////////////////////////////////////////////////////////////////////////////////////
    // TAllTabletMocks - merges streams from all tablet mocks based on their weights
    //////////////////////////////////////////////////////////////////////////////////////////////
    class TAllTabletMocks {
    public:
        using TTabletMockAndWeight = std::pair<std::shared_ptr<ITabletMock>, ui64>;

        // initialize with a vector of pairs -- tablet mock pointer and tablet weight
        TAllTabletMocks(TVector<TTabletMockAndWeight> &&allMocks)
            : AllMocks(std::move(allMocks))
        {
            GenerateSearchVector();
        }

        TAllTabletMocks(const TVector<TTabletMockAndWeight> &allMocks)
            : AllMocks(allMocks)
        {
            GenerateSearchVector();
        }

        const TTabletCommand &Next() {
            const size_t idx = RandomNumber<size_t>(SearchVector.size());
            return SearchVector[idx]->Next();
        }

    private:
        void GenerateSearchVector() {
            size_t reserve = 0;
            auto reserveCountFunc = [&reserve] (const std::pair<std::shared_ptr<ITabletMock>, ui64> &elem) {
                reserve += elem.second;
            };
            std::for_each(AllMocks.begin(), AllMocks.end(), reserveCountFunc);

            SearchVector.reserve(reserve);

            for (const auto &x : AllMocks) {
                for (ui64 i = 0; i < x.second; ++i) {
                    SearchVector.push_back(x.first);
                }
            }
        }

        TVector<TTabletMockAndWeight> AllMocks;
        TVector<std::shared_ptr<ITabletMock>> SearchVector;
    };


    //////////////////////////////////////////////////////////////////////////////////////////////
    // TSstGenerator -- generates levels
    //////////////////////////////////////////////////////////////////////////////////////////////
    class TSstGenerator {
    public:
        using TLogoBlobSst = TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>;
        using TLogoBlobSstPtr = TIntrusivePtr<TLogoBlobSst>;

        struct TLogoBlobsPolicy {
            size_t SstsNum;
        };

        TSstGenerator(std::shared_ptr<TTestContexts> testCtx, TAllTabletMocks &&allMocks)
            : TestCtx(std::move(testCtx))
            , AllMocks(std::move(allMocks))
        {}

        TSstGenerator(std::shared_ptr<TTestContexts> testCtx, const TAllTabletMocks &allMocks)
            : TestCtx(std::move(testCtx))
            , AllMocks(allMocks)
        {}

        // generate ordered level of LogoBlobs
        TVector<TLogoBlobSstPtr> GenerateOrderedLogoBlobsSsts(const TLogoBlobsPolicy &policy) {
            ui64 logoBlobsTotalBytes = 0;
            const ui64 logoBlobsRequiredBytes = policy.SstsNum * ChunkSize;

            // accumulate generated commands in LogoBlobs and Barriers buffers
            while (logoBlobsTotalBytes < logoBlobsRequiredBytes) {
                const TTabletCommand &cmd = AllMocks.Next();
                if (cmd.LogoBlobsCmd) {
                    LogoBlobs.push_back(*cmd.LogoBlobsCmd);
                    logoBlobsTotalBytes += LogoBlobRecSizeof + cmd.LogoBlobsCmd->Key.LogoBlobID().BlobSize();
                }
                if (cmd.BarriersCmd) {
                    Barriers.push_back(*cmd.BarriersCmd);
                }
            }

            return GenerateOrderedSsts<TKeyLogoBlob, TMemRecLogoBlob, TLogoBlobsCmd>(LogoBlobs);
        }

        TVector<TBarriersSstPtr> ReturnAllBarriersAsOrderedSsts() {
            return GenerateOrderedSsts<TKeyBarrier, TMemRecBarrier, TBarriersCmd>(Barriers);
        }

    private:
        std::shared_ptr<TTestContexts> TestCtx;
        TAllTabletMocks AllMocks;
        TVector<TLogoBlobsCmd> LogoBlobs;
        TVector<TBarriersCmd> Barriers;

        static const ui64 ChunkSize = 128u << 20u;
        static const ui64 LogoBlobRecSizeof = sizeof(TKeyLogoBlob) + sizeof(TMemRecLogoBlob);
        static const ui64 BarrierRecSizeof = sizeof(TKeyBarrier) + sizeof(TMemRecBarrier);

        static size_t DataSize(const TKeyLogoBlob &key) {
            return key.LogoBlobID().BlobSize();
        }

        static size_t DataSize(const TKeyBarrier &) {
            return 0;
        }

        template <class TKey, class TMemRec, class TCmd>
        TIntrusivePtr<TLevelSegment<TKey, TMemRec>> GenerateSst(
                typename TVector<TCmd>::iterator &it,
                const typename TVector<TCmd>::iterator &end)
        {
            using TSst = TLevelSegment<TKey, TMemRec>;
            const ui64 requiredBytes = ChunkSize;
            ui64 totalBytes = 0;
            TIntrusivePtr<TSst> sst(new TSst(TestCtx->GetVCtx()));
            sst->Info.CTime = TAppData::TimeProvider->Now();

            while (it != end && totalBytes < requiredBytes) {
                sst->LoadedIndex.emplace_back(it->Key, it->MemRec);
                totalBytes += sizeof(TKey) + sizeof(TMemRec) + DataSize(it->Key);
                ++it;
            }

            return sst;
        }

        template <class TKey, class TMemRec, class TCmd>
        TVector<TIntrusivePtr<TLevelSegment<TKey, TMemRec>>> GenerateOrderedSsts(TVector<TCmd> &cmds) {
            std::sort(cmds.begin(), cmds.end());

            using TSst = TLevelSegment<TKey, TMemRec>;
            using TSstPtr = TIntrusivePtr<TSst>;
            TVector<TSstPtr> result;
            auto it = cmds.begin();
            while (it != cmds.end()) {
                TSstPtr sst = GenerateSst<TKey, TMemRec, TCmd>(it, cmds.end());
                result.push_back(std::move(sst));
            }

            cmds.clear();
            return result;
        }
    };

    //////////////////////////////////////////////////////////////////////////////////////////////
    // TOneLevelHullDsGenerator -- generate THullDs from one level
    //////////////////////////////////////////////////////////////////////////////////////////////
    class TOneLevelHullDsGenerator : public IHullDsIndexMock {
    public:
        TOneLevelHullDsGenerator(TAllTabletMocks &&allMocks)
            : AllMocks(std::move(allMocks))
        {}

        TIntrusivePtr<THullDs> GenerateHullDs() override {
            auto testCtx = std::make_shared<TTestContexts>(ChunkSize, CompWorthReadSize);
            std::shared_ptr<TRopeArena> arena = std::make_shared<TRopeArena>(&TRopeArenaBackend::Allocate);

            TSstGenerator gen(testCtx, std::move(AllMocks));

            TVector<TLogoBlobsSstPtr> logoBlobsLevel = gen.GenerateOrderedLogoBlobsSsts(
                    TSstGenerator::TLogoBlobsPolicy {8ul});
            TVector<TBarriersSstPtr> barriersLevel = gen.ReturnAllBarriersAsOrderedSsts();

            auto hullDs = MakeIntrusive<THullDs>(testCtx->GetHullCtx());
            Allocate17Levels<TKeyLogoBlob, TMemRecLogoBlob, TLogoBlobsDs>(testCtx, arena, hullDs->LogoBlobs);
            PutAtLastLevel(logoBlobsLevel, hullDs->LogoBlobs);

            Allocate17Levels<TKeyBarrier, TMemRecBarrier, TBarriersDs>(testCtx, arena, hullDs->Barriers);
            PutAtLastLevel(barriersLevel, hullDs->Barriers);

            hullDs->Blocks = MakeIntrusive<TBlocksDs>(testCtx->GetLevelIndexSettings(), arena);

            return hullDs;
        }

    private:
        TAllTabletMocks AllMocks;
        static constexpr ui32 ChunkSize = 128u << 20u;
        static constexpr ui64 CompWorthReadSize = 2u << 20u;

        template <class TKey, class TMemRec, class TDs>
        void Allocate17Levels(
                const std::shared_ptr<TTestContexts> &testCtx,
                const std::shared_ptr<TRopeArena> &arena,
                TIntrusivePtr<TDs> &ds)
        {
            ds = MakeIntrusive<TDs>(testCtx->GetLevelIndexSettings(), arena);
            for (int i = 0; i < 17; ++i) {
                ds->CurSlice->SortedLevels.push_back(TSortedLevel<TKey, TMemRec>(TKey()));
            }
        }

        template <class TKey, class TMemRec, class TDs>
        void PutAtLastLevel(TVector<TIntrusivePtr<TLevelSegment<TKey, TMemRec>>> &sorted, TIntrusivePtr<TDs> &ds) {
            size_t idx = ds->CurSlice->SortedLevels.size() - 1;
            for (auto &x : sorted) {
                ds->CurSlice->SortedLevels[idx].Put(x);
            }
        }

    };


    //////////////////////////////////////////////////////////////////////////////////////////////
    // TTabletMockLog -- tablet write log and periodically set up barrier to keep only recsToKeep
    // number of records +20%
    //////////////////////////////////////////////////////////////////////////////////////////////
    class TTabletMockLog : public ITabletMock {
    public:
        TTabletMockLog(std::shared_ptr<TTestContexts> testCtx, ui64 tabletId, ui64 recsToKeep)
            : TabletId(tabletId)
            , RecsToKeep(recsToKeep)
        {
            RecsToKeepLimit = RecsToKeep + Max(RecsToKeep / 5ul, ui64(100));

            ui32 totalVDisks = testCtx->GetHullCtx()->IngressCache->TotalVDisks;
            for (ui32 i = 0; i < totalVDisks; ++i) {
                TBarrierIngress::Merge(FullySyncedBarrierIngress, TBarrierIngress(ui8(i)));
            }
        }

        virtual const TTabletCommand &Next() override {
            Cmd.LogoBlobsCmd.Clear();
            Cmd.BarriersCmd.Clear();

            if (CurRecsToKeep < RecsToKeepLimit) {
                ++CurRecsToKeep;
                TLogoBlobID id(TabletId, Gen, ++Step, Channel, BlobSize, Cookie);
                TMemRecLogoBlob memRec; // FIXME: put ingress here
                memRec.SetDiskBlob(NextDiskPart());
                Cmd.LogoBlobsCmd = TLogoBlobsCmd {TLogoBlobID(id), memRec};
                return Cmd;
            } else {
                ui32 collectStep = Step - (CurRecsToKeep - RecsToKeep);
                CurRecsToKeep = RecsToKeep;

                TKeyBarrier barrierKey(TabletId, Channel, Gen, ++BarrierGenCounter, false);
                TMemRecBarrier barrierMemRec(Gen, collectStep, FullySyncedBarrierIngress);

                Cmd.BarriersCmd = TBarriersCmd {barrierKey, barrierMemRec};
                return Cmd;
            }
        }

    private:
        const ui64 TabletId;
        const ui64 RecsToKeep;
        const ui8 Channel = 0;
        ui64 RecsToKeepLimit = 0;
        ui32 BlobSize = 4u << 10u;
        ui64 CurRecsToKeep = 0;
        ui32 Cookie = 0;
        TTabletCommand Cmd;
        TBarrierIngress FullySyncedBarrierIngress;

        ui32 Gen = 2;
        ui32 Step = 0;
        ui32 BarrierGenCounter = 0;
        ui32 ChunkId = 845u;
        ui32 ChunkOffset = 64u << 10u;

        TDiskPart NextDiskPart() {
            const ui32 offset = ChunkOffset;
            ChunkOffset += BlobSize;
            return TDiskPart(ChunkId, offset, BlobSize);
        }
    };

    //////////////////////////////////////////////////////////////////////////////////////////////
    // GenerateDs_18Level_Logs
    //////////////////////////////////////////////////////////////////////////////////////////////
    TIntrusivePtr<THullDs> GenerateDs_17Level_Logs() {
        auto testCtx = std::make_shared<TTestContexts>();
        TVector<TAllTabletMocks::TTabletMockAndWeight> vec;
        vec.reserve(10);
        vec.emplace_back(std::make_shared<TTabletMockLog>(testCtx, 1ul, 10000ul), 64ul);
        vec.emplace_back(std::make_shared<TTabletMockLog>(testCtx, 2ul, 10000ul), 64ul);
        vec.emplace_back(std::make_shared<TTabletMockLog>(testCtx, 3ul, 10000ul), 64ul);
        vec.emplace_back(std::make_shared<TTabletMockLog>(testCtx, 4ul, 10000ul), 64ul);
        vec.emplace_back(std::make_shared<TTabletMockLog>(testCtx, 5ul, 10000ul), 4ul);

        TAllTabletMocks allTabletMocks(std::move(vec));


        TOneLevelHullDsGenerator gen(std::move(allTabletMocks));
        auto ret = gen.GenerateHullDs();
        ret->LogoBlobs->LoadCompleted();
        ret->Blocks->LoadCompleted();
        ret->Barriers->LoadCompleted();
        return ret;
    }

} // NTest
} // NKikimr

