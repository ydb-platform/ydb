#pragma once
#include "defs.h"

#include <util/stream/null.h>
#include <util/system/valgrind.h>

#include <cassert>

namespace NKikimr {

#define ENABLE_SPEED_TESTS 0
#define ENABLE_UNSTABLE_TESTS 0
#define ENABLE_FORKED_TESTS 0

constexpr ui32 TEST_TIMEOUT = NSan::PlainOrUnderSanitizer(
    NValgrind::PlainOrUnderValgrind(600000, 1200000),
    1200000
);

constexpr ui32 MIN_CHUNK_SIZE = 1620 << 10;
#ifdef NDEBUG
constexpr bool IsLowVerbose = false;
#else
constexpr bool IsLowVerbose = true;
#endif
constexpr bool IsVerbose = false;
constexpr bool IsMonitoringEnabled = false;
constexpr bool IsRealBlockDevice = false;
static const char *RealBlockDevicePath = "/dev/sda5";

#ifndef NDEBUG
#   define IS_SLOW_MACHINE 1
#else
#   define IS_SLOW_MACHINE 0
#endif

#if ENABLE_FORKED_TESTS
#    define YARD_UNIT_TEST(a) SIMPLE_UNIT_FORKED_TEST(a)
#else
#    define YARD_UNIT_TEST(a) Y_UNIT_TEST(a)
#endif //ENABLE_FORKED_TESTS

#define ASSERT_YTHROW(expr, str) \
do { \
    if (!(expr)) { \
        ythrow TWithBackTrace<yexception>() << str; \
    } \
} while(false)

#define TEST_RESPONSE(msg, st) \
do { \
    ui32 eventSpace = (int)LastResponse.EventType >> 16; \
    ui32 eventGroup = ((int)LastResponse.EventType & 0xffff) >> 9; \
    ui32 eventId = ((int)LastResponse.EventType & 0x1ff); \
    ASSERT_YTHROW(LastResponse.EventType == TEvBlobStorage::msg, \
        "Unexpected message in space " << eventSpace << ": 512 * " << eventGroup << " + " << eventId << "\n"); \
    ASSERT_YTHROW(LastResponse.Status == NKikimrProto::st, \
        "Unexpected status, got# " << StatusToString(LastResponse.Status) << \
            " expect# " << StatusToString(NKikimrProto::st) << "\n"); \
} while(false)

#define Ctest (IsVerbose ? Cerr : Cnull)

#define VERBOSE_COUT(str) \
do { \
    if (IsVerbose) { \
        Cerr << str << Endl; \
    } \
} while(false)

#define LOW_VERBOSE_COUT(str) \
do { \
    if (IsLowVerbose) { \
        Cerr << str << Endl; \
    } \
} while(false)

#define TEST_DATA_EQUALS(a, b) \
do { \
    if ((a).size() > 255 || (b).size() > 255) { \
        ASSERT_YTHROW((a).size() == (b).size(), "Unexpected data size, got size# " << (a).size() << " expected size# " << (b).size()); \
    } else { \
        ASSERT_YTHROW((a).size() == (b).size(), "Unexpected data size, got size# " << (a).size() << " expected size# " << (b).size() << \
            "\n got \n'" << a.data() << "'\n expected \n'" << (b).data() << "'\n" ); \
    } \
    for (ui32 i = 0; i < (a).size(); ++i) { \
        if ((a).data()[i] != (b).data()[i]) { \
            if ((a).size() > 255) { \
                ASSERT_YTHROW(false, \
                    "Unexpected data at position " << i << " got " << (ui32)(a)[i] << \
                    " expected " << (ui32)(b)[i]); \
            } else { \
                ASSERT_YTHROW(false, \
                    "Unexpected data at position " << i << " got " << (ui32)(a)[i] << \
                    " expected " << (ui32)(b)[i] << \
                    "\n got \n'" << (a) << \
                    "'\n expected \n'" << (b) << "'\n" ); \
            } \
        } \
    } \
} while (false)

#define TEST_LOG_RECORD(record, lsn, signature, data) \
do { \
    ASSERT_YTHROW((record).Lsn == (lsn), "Unexpected Lsn == " << (record).Lsn << " instead of: " << (lsn)); \
    ASSERT_YTHROW((record).Signature == (signature), "Unexpected Signature == " << (ui32)(record).Signature); \
    TEST_DATA_EQUALS(TRcBuf((record).Data).ExtractUnderlyingContainerOrCopy<TString>(), (data)); \
} while(false)

#define TEST_PDISK_STATUS(s) \
do { \
    ASSERT_YTHROW((LastResponse.StatusFlags & ui32(NKikimrBlobStorage::StatusIsValid)), "Invalid PDisk status"); \
    ASSERT_YTHROW(LastResponse.StatusFlags == (s), \
            "Unexpected PDisk status flags:" \
        << (ui32)LastResponse.StatusFlags << " (" \
        << (LastResponse.StatusFlags & ui32(NKikimrBlobStorage::StatusIsValid) ? "IsValid " : "") \
        << (LastResponse.StatusFlags & ui32(NKikimrBlobStorage::StatusDiskSpaceCyan) ? "SpaceCyan " : "") \
        << (LastResponse.StatusFlags & ui32(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove) ? "SpaceLightYellow " : "") \
        << (LastResponse.StatusFlags & ui32(NKikimrBlobStorage::StatusDiskSpaceYellowStop) ? "SpaceYellow " : "") \
        << (LastResponse.StatusFlags & ui32(NKikimrBlobStorage::StatusDiskSpaceOrange) ? "SpaceOrange " : "") \
        << (LastResponse.StatusFlags & ui32(NKikimrBlobStorage::StatusDiskSpacePreOrange) ? "SpacePreOrange " : "") \
        << (LastResponse.StatusFlags & ui32(NKikimrBlobStorage::StatusDiskSpaceLightOrange) ? "SpaceLightOrange " : "") \
        << (LastResponse.StatusFlags & ui32(NKikimrBlobStorage::StatusDiskSpaceRed) ? "SpaceRed " : "") \
        << (LastResponse.StatusFlags & ui32(NKikimrBlobStorage::StatusDiskSpaceBlack) ? "SpaceBlack " : "") \
        << (LastResponse.StatusFlags & ui32(NKikimrBlobStorage::StatusNewOwner) ? "NewOwner " : "") \
        << (LastResponse.StatusFlags & ui32(NKikimrBlobStorage::StatusNotEnoughDiskSpaceForOperation) ? \
            "NotEnoughDiskSpaceForOperation " : "") \
        << "expected:" \
        << (ui32)(s) << " (" \
        << ((s) & ui32(NKikimrBlobStorage::StatusIsValid) ? "IsValid " : "") \
        << ((s) & ui32(NKikimrBlobStorage::StatusDiskSpaceCyan) ? "SpaceCyan " : "") \
        << ((s) & ui32(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove) ? "SpaceLightYellow " : "") \
        << ((s) & ui32(NKikimrBlobStorage::StatusDiskSpaceYellowStop) ? "SpaceYellow " : "") \
        << ((s) & ui32(NKikimrBlobStorage::StatusDiskSpaceOrange) ? "SpaceOrange " : "") \
        << ((s) & ui32(NKikimrBlobStorage::StatusDiskSpacePreOrange) ? "SpacePreOrange " : "") \
        << ((s) & ui32(NKikimrBlobStorage::StatusDiskSpaceLightOrange) ? "SpaceLightOrange " : "") \
        << ((s) & ui32(NKikimrBlobStorage::StatusDiskSpaceRed) ? "SpaceRed " : "") \
        << ((s) & ui32(NKikimrBlobStorage::StatusDiskSpaceBlack) ? "SpaceBlack " : "") \
        << ((s) & ui32(NKikimrBlobStorage::StatusNewOwner) ? "NewOwner " : "") \
        << ((s) & ui32(NKikimrBlobStorage::StatusNotEnoughDiskSpaceForOperation) ? \
            "NotEnoughDiskSpaceForOperation " : "") \
        << ")"); \
} while (false)


}
