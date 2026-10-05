#include "blobstorage_pdisk_abstract.h"
#include "blobstorage_pdisk_chunk_tracker.h"
#include "blobstorage_pdisk_color_limits.h"
#include "blobstorage_pdisk_impl.h"
#include "blobstorage_pdisk_quota_record.h"

#include "blobstorage_pdisk_ut.h"
#include "blobstorage_pdisk_ut_actions.h"
#include "blobstorage_pdisk_ut_helpers.h"
#include "blobstorage_pdisk_ut_run.h"

#include <ydb/core/blobstorage/crypto/default.h>

#include <ydb/core/testlib/actors/test_runtime.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TColorLimitsTest) {
    // Define color codes
    enum class TDiskColor {
        Black,
        Red,
        Orange,
        PreOrange,
        LightOrange,
        Yellow,
        LightYellow,
        Cyan,
        Default
    };

    // Function to set text color based on TDiskColor enum
    void SetColor(NKikimrBlobStorage::TPDiskSpaceColor_E color) {
        switch (color) {
            case NKikimrBlobStorage::TPDiskSpaceColor::BLACK:
                Cout << "\033[30m"; // Black
                break;
            case NKikimrBlobStorage::TPDiskSpaceColor::RED:
                Cout << "\033[31m"; // Red
                break;
            case NKikimrBlobStorage::TPDiskSpaceColor::GREEN:
                Cout << "\033[32m"; // Green
                break;
            case NKikimrBlobStorage::TPDiskSpaceColor::ORANGE:
                Cout << "\033[33m"; // Orange
                break;
            case NKikimrBlobStorage::TPDiskSpaceColor::PRE_ORANGE:
                Cout << "\033[38;5;208m"; // PreOrange (closest approximation)
                break;
            case NKikimrBlobStorage::TPDiskSpaceColor::LIGHT_ORANGE:
                Cout << "\033[38;5;215m"; // LightOrange (closest approximation)
                break;
            case NKikimrBlobStorage::TPDiskSpaceColor::YELLOW:
                Cout << "\033[93m"; // Yellow
                break;
            case NKikimrBlobStorage::TPDiskSpaceColor::LIGHT_YELLOW:
                Cout << "\033[38;5;229m"; // LightYellow (closest approximation)
                break;
            case NKikimrBlobStorage::TPDiskSpaceColor::CYAN:
                Cout << "\033[36m"; // Cyan
                break;
            default:
                // Default color (reset)
                break;
        }
    }

    void ClearColor() {
        Cout << "\033[0m";
    }

    Y_UNIT_TEST(Colors) {
        NKikimrBlobStorage::TPDiskSpaceColor_E colors[] = {
            NKikimrBlobStorage::TPDiskSpaceColor::BLACK,
            NKikimrBlobStorage::TPDiskSpaceColor::RED,
            NKikimrBlobStorage::TPDiskSpaceColor::ORANGE,
            NKikimrBlobStorage::TPDiskSpaceColor::PRE_ORANGE,
            NKikimrBlobStorage::TPDiskSpaceColor::LIGHT_ORANGE,
            NKikimrBlobStorage::TPDiskSpaceColor::YELLOW,
            NKikimrBlobStorage::TPDiskSpaceColor::LIGHT_YELLOW,
            NKikimrBlobStorage::TPDiskSpaceColor::CYAN
        };

        auto printLimitsFn = [&colors](int percent) {
            NPDisk::TColorLimits limits = NPDisk::TColorLimits::MakeChunkLimits(percent);

            Cout << "Print for " << (percent / 10.0) << "%" << Endl;

            i64 chunks = 1000;

            i64 cur = 0;
            i64 all = 0;

            std::map<NKikimrBlobStorage::TPDiskSpaceColor_E, i64> sizeByColor;

            for (auto color : colors) {
                i64 curChunks = limits.GetQuotaForColor(color, chunks);

                SetColor(color);

                i64 sz = curChunks - all;

                sizeByColor[color] = sz;

                for (i64 i = 0; i < sz; i++) {
                    Cout << "#";

                    if ((++cur % 100) == 0) {
                        cur = 0;
                        Cout << Endl;
                    }
                }

                all = curChunks;
            }

            SetColor(NKikimrBlobStorage::TPDiskSpaceColor::GREEN);
            for (i64 i = 0; i < (chunks - all); i++) {
                Cout << "#";

                if ((++cur % 100) == 0) {
                    cur = 0;
                    Cout << Endl;
                }
            }

            ClearColor();

            Cout << Endl;

            for (auto color : colors) {
                Cout << color << ": " << sizeByColor[color] << Endl;
            }

            Cout << Endl;
        };

        printLimitsFn(130);
        printLimitsFn(100);
        printLimitsFn(65);
        printLimitsFn(13);
    }

    NKikimrBlobStorage::TPDiskSpaceColor_E AllColors[] = {
        NKikimrBlobStorage::TPDiskSpaceColor::GREEN,
        NKikimrBlobStorage::TPDiskSpaceColor::CYAN,
        NKikimrBlobStorage::TPDiskSpaceColor::LIGHT_YELLOW,
        NKikimrBlobStorage::TPDiskSpaceColor::YELLOW,
        NKikimrBlobStorage::TPDiskSpaceColor::LIGHT_ORANGE,
        NKikimrBlobStorage::TPDiskSpaceColor::PRE_ORANGE,
        NKikimrBlobStorage::TPDiskSpaceColor::ORANGE,
        NKikimrBlobStorage::TPDiskSpaceColor::RED,
        NKikimrBlobStorage::TPDiskSpaceColor::BLACK,
    };

    Y_UNIT_TEST(OwnerFreeSpaceShare) {
        using namespace NPDisk;

        double prevOccupancy = 0;

        for (auto borderColor : AllColors) {
            TChunkTracker chunkTracker;
            TKeeperParams params {
                .TotalChunks = 1000,
                .ExpectedOwnerCount = 2,
                .SysLogSize = 0,
                .CommonLogSize = 0,
                .MaxCommonLogChunks = 0,
                .SpaceColorBorder = borderColor,
                .SeparateCommonLog = true,
            };
            auto limits = NPDisk::TColorLimits::MakeChunkLimits(params.ChunkBaseLimit);
            TString errorReason;
            bool ok = chunkTracker.Reset(params, limits, errorReason);
            UNIT_ASSERT(ok);

            TOwner owner1 = NPDisk::EOwner::OwnerBeginUser + 1;
            TOwner owner2 = NPDisk::EOwner::OwnerBeginUser + 2;

            TVDiskID vdiskId1(TGroupID(EGroupConfigurationType::Dynamic, 1, 1).GetRaw(), 1, TVDiskIdShort(0, 0, 0));
            TVDiskID vdiskId2(TGroupID(EGroupConfigurationType::Dynamic, 1, 2).GetRaw(), 1, TVDiskIdShort(0, 0, 0));
            chunkTracker.AddOwner(owner1, vdiskId1);
            chunkTracker.AddOwner(owner2, vdiskId2);

            double occupancy;
            // consume 100% of personal quota and 50% of common quota
            auto color = chunkTracker.EstimateSpaceColor(owner1, params.TotalChunks / 2, &occupancy);
            double borderOccupancy = limits.GetOccupancyForColor(color, params.TotalChunks);
            Cerr << color << " \t" << occupancy << " \t" << borderOccupancy << Endl;

            UNIT_ASSERT_C(color == borderColor, "Because owner consumed all his quota his color should be equal to border");
            if (color != NKikimrBlobStorage::TPDiskSpaceColor::GREEN) {
                UNIT_ASSERT_C(std::fabs(occupancy - borderOccupancy) <= 0.001,
                    "Because owner consumed all his quota his occupancy should be equal to occupancy border");
            }
            UNIT_ASSERT_C(occupancy >= prevOccupancy, "check that with Border is increasing fair occupancy is increasing too");
        }
    }

    Y_UNIT_TEST(TightLargeDiskUsesPercents) {
        using namespace NPDisk;
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

        const i64 total = 10'000;
        auto limits = TColorLimits::MakeChunkLimits(TColorLimits::TightCyanPermille, true);

        UNIT_ASSERT_VALUES_EQUAL(limits.GetQuotaForColor(TColor::CYAN, total), 300);
        UNIT_ASSERT_VALUES_EQUAL(limits.GetQuotaForColor(TColor::LIGHT_YELLOW, total), 230);
        UNIT_ASSERT_VALUES_EQUAL(limits.GetQuotaForColor(TColor::YELLOW, total), 180);
        UNIT_ASSERT_VALUES_EQUAL(limits.GetQuotaForColor(TColor::LIGHT_ORANGE, total), 150);
        UNIT_ASSERT_VALUES_EQUAL(limits.GetQuotaForColor(TColor::PRE_ORANGE, total), 110);
        UNIT_ASSERT_VALUES_EQUAL(limits.GetQuotaForColor(TColor::ORANGE, total), 60);
        UNIT_ASSERT_VALUES_EQUAL(limits.GetQuotaForColor(TColor::RED, total), 20);
        UNIT_ASSERT_VALUES_EQUAL(limits.GetQuotaForColor(TColor::BLACK, total), 10);
    }

    Y_UNIT_TEST(TightSmallDiskUsesChunkFloors) {
        using namespace NPDisk;
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

        const i64 total = 200;
        auto limits = TColorLimits::MakeChunkLimits(TColorLimits::TightCyanPermille, true);

        UNIT_ASSERT_VALUES_EQUAL(limits.GetQuotaForColor(TColor::CYAN, total), TColorLimits::TightMinChunksCyan);
        UNIT_ASSERT_VALUES_EQUAL(limits.GetQuotaForColor(TColor::LIGHT_YELLOW, total), TColorLimits::TightMinChunksLightYellow);
        UNIT_ASSERT_VALUES_EQUAL(limits.GetQuotaForColor(TColor::YELLOW, total), TColorLimits::TightMinChunksYellow);
        UNIT_ASSERT_VALUES_EQUAL(limits.GetQuotaForColor(TColor::LIGHT_ORANGE, total), TColorLimits::TightMinChunksLightOrange);
        UNIT_ASSERT_VALUES_EQUAL(limits.GetQuotaForColor(TColor::PRE_ORANGE, total), TColorLimits::TightMinChunksPreOrange);
        UNIT_ASSERT_VALUES_EQUAL(limits.GetQuotaForColor(TColor::ORANGE, total), TColorLimits::TightMinChunksOrange);
        UNIT_ASSERT_VALUES_EQUAL(limits.GetQuotaForColor(TColor::RED, total), TColorLimits::TightMinChunksRed);
        UNIT_ASSERT_VALUES_EQUAL(limits.GetQuotaForColor(TColor::BLACK, total), TColorLimits::TightMinChunksBlack);

        UNIT_ASSERT_LT(total * TColorLimits::TightCyanPermille / 1000, TColorLimits::TightMinChunksCyan);
    }

    Y_UNIT_TEST(TightIcbCyanPermilleKeepsFloors) {
        using namespace NPDisk;
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

        // ICB may raise cyan above 30; floors still bind on a small pool.
        auto limits = TColorLimits::MakeChunkLimits(50, true);

        UNIT_ASSERT_VALUES_EQUAL(limits.GetQuotaForColor(TColor::CYAN, 10'000), 500);
        UNIT_ASSERT_VALUES_EQUAL(limits.GetQuotaForColor(TColor::CYAN, 200), TColorLimits::TightMinChunksCyan);
    }

    Y_UNIT_TEST(TightQuotasAreNested) {
        using namespace NPDisk;
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

        auto limits = TColorLimits::MakeChunkLimits(TColorLimits::TightCyanPermille, true);
        const TColor::E colors[] = {
            TColor::BLACK,
            TColor::RED,
            TColor::ORANGE,
            TColor::PRE_ORANGE,
            TColor::LIGHT_ORANGE,
            TColor::YELLOW,
            TColor::LIGHT_YELLOW,
            TColor::CYAN,
        };

        for (i64 total : {50, 200, 10'000}) {
            i64 prev = -1;
            for (auto color : colors) {
                const i64 quota = limits.GetQuotaForColor(color, total);
                UNIT_ASSERT_C(quota > prev, "total# " << total << " color# " << color
                    << " quota# " << quota << " prev# " << prev);
                prev = quota;
            }
            const i64 runway = limits.GetQuotaForColor(TColor::PRE_ORANGE, total)
                - limits.GetQuotaForColor(TColor::BLACK, total);
            UNIT_ASSERT_C(runway >= 8, "total# " << total << " runway# " << runway);
        }

        TQuotaRecord rec;
        rec.ForceHardLimit(20, limits);
        double occupancy = 0;
        NKikimrBlobStorage::TPDiskSpaceColor::E prevColor = TColor::GREEN;
        for (i64 used = 0; used <= 20; ++used) {
            const auto color = rec.EstimateSpaceColor(used, &occupancy);
            UNIT_ASSERT_C(color >= prevColor, "used# " << used
                << " color# " << color << " prev# " << prevColor);
            prevColor = color;
        }
    }

    Y_UNIT_TEST(LegacyCyanThirtyKeepsAddends) {
        using namespace NPDisk;
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

        const i64 total = 200;
        auto limits = TColorLimits::MakeChunkLimits(TColorLimits::TightCyanPermille, false);
        UNIT_ASSERT_VALUES_EQUAL(limits.GetQuotaForColor(TColor::CYAN, total), total * 30 / 1000 + 8);
        UNIT_ASSERT_LT(limits.GetQuotaForColor(TColor::CYAN, total), TColorLimits::TightMinChunksCyan);
    }
}
} // namespace NKikimr
