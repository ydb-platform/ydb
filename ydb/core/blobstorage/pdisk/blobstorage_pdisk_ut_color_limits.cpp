#include "blobstorage_pdisk_abstract.h"
#include "blobstorage_pdisk_chunk_tracker.h"
#include "blobstorage_pdisk_color_limits.h"
#include "blobstorage_pdisk_impl.h"

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
                .HasStaticGroups = false,
                .SpaceColorBorder = borderColor,
                .SeparateCommonLog = true,
            };
            auto limits = NPDisk::TColorLimits::MakeChunkLimits(params.ChunkBaseLimit);
            TString errorReason;
            bool ok = chunkTracker.Reset(params, limits, errorReason);
            UNIT_ASSERT(ok);

            TOwner owner1 = NPDisk::EOwner::OwnerBeginUser + 1;
            TOwner owner2 = NPDisk::EOwner::OwnerBeginUser + 2;

            chunkTracker.AddOwner(owner1, TVDiskID());
            chunkTracker.AddOwner(owner2, TVDiskID());

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
}
} // namespace NKikimr
