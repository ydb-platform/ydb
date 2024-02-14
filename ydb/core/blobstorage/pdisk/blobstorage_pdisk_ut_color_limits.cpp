#include "blobstorage_pdisk_abstract.h"
#include "blobstorage_pdisk_impl.h"

#include "blobstorage_pdisk_ut.h"
#include "blobstorage_pdisk_ut_actions.h"
#include "blobstorage_pdisk_ut_helpers.h"
#include "blobstorage_pdisk_ut_run.h"
#include "blobstorage_pdisk_color_limits.h"

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
        Cout << Endl;
        i64 chunks = 1000;

        NPDisk::TColorLimits limits = NPDisk::TColorLimits::MakeChunkLimits();

        //limits.GetOccupancyForColor(NKikimrBlobStorage::TPDiskSpaceColor::E color, i64 total)

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

        i64 cur = 0;
        i64 all = 0;

        for (auto color : colors) {
            i64 curChunks = limits.GetQuotaForColor(color, chunks);

            SetColor(color);

            for (i64 i = 0; i < (curChunks - all); i++) {
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
    }
}
} // namespace NKikimr
