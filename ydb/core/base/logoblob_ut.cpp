#include "defs.h"
#include "logoblob.h"
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/ptr.h>

#include <array>
#include <bit>

namespace NKikimr {
    namespace {
        // Mirrors the legacy bitfield member without using its union overlay.
        // This is a migration oracle for the compiler and target ABI used by YDB.
        struct TLegacyLogoBlobIDFields {
            ui64 TabletID;

            ui64 StepR1 : 24;
            ui64 Generation : 32;
            ui64 Channel : 8;

            ui64 PartId : 4;
            ui64 BlobSize : 26;
            ui64 CrcMode : 2;

            ui64 Cookie : 24;
            ui64 StepR2 : 8;
        };

        static_assert(sizeof(TLegacyLogoBlobIDFields) == TLogoBlobID::BinarySize);

        std::array<ui64, 3> MakeLegacyRaw(
            ui64 tabletId,
            ui32 generation,
            ui32 step,
            ui32 channel,
            ui32 blobSize,
            ui32 cookie,
            ui32 partId,
            ui32 crcMode)
        {
            TLegacyLogoBlobIDFields fields{};
            fields.TabletID = tabletId;
            fields.Generation = generation;
            fields.StepR1 = step >> 8;
            fields.StepR2 = step & 0xFF;
            fields.Channel = channel;
            fields.Cookie = cookie;
            fields.BlobSize = blobSize;
            fields.CrcMode = crcMode;
            fields.PartId = partId;
            return std::bit_cast<std::array<ui64, 3>>(fields);
        }

        constexpr TLogoBlobID ConstexprId(
            0x0123456789ABCDEFull,
            0x30002C2D,
            0x50005F6F,
            1,
            0x3333,
            0x01A01B,
            7,
            2);
        constexpr TLogoBlobID ConstexprPartId(ConstexprId, 3);
        constexpr TLogoBlobID ConstexprRaw(ConstexprId.GetRaw());
        constexpr TLogoBlobID ConstexprMade = TLogoBlobID::Make(42, 1, 2, 3, 100, 15, 2);
        constexpr TLogoBlobID ConstexprPrevious = TLogoBlobID::PrevFull(ConstexprMade, 100);

        static_assert(ConstexprId.GetRaw()[0] == 0x0123456789ABCDEFull);
        static_assert(ConstexprId.GetRaw()[1] == 0x0130002C2D50005Full);
        static_assert(ConstexprId.GetRaw()[2] == 0x6F01A01B80033337ull);
        static_assert(ConstexprId.TabletID() == 0x0123456789ABCDEFull);
        static_assert(ConstexprId.Generation() == 0x30002C2D);
        static_assert(ConstexprId.Step() == 0x50005F6F);
        static_assert(ConstexprId.Channel() == 1);
        static_assert(ConstexprId.BlobSize() == 0x3333);
        static_assert(ConstexprId.Cookie() == 0x01A01B);
        static_assert(ConstexprId.PartId() == 7);
        static_assert(ConstexprId.CrcMode() == 2);
        static_assert(ConstexprPartId.PartId() == 3);
        static_assert(ConstexprPartId.FullID().PartId() == 0);
        static_assert(ConstexprPartId.IsSameBlob(ConstexprId));
        static_assert(ConstexprRaw == ConstexprId);
        static_assert(ConstexprMade.Hash() == TLogoBlobID::THash()(ConstexprMade));
        static_assert(ConstexprPrevious.Cookie() == ConstexprMade.Cookie() - 1);
        static_assert(ConstexprPrevious < ConstexprMade);
        static_assert(ConstexprMade > ConstexprPrevious);
        static_assert(ConstexprPrevious <= ConstexprMade);
        static_assert(ConstexprMade >= ConstexprPrevious);
        static_assert(ConstexprMade != ConstexprPrevious);
        static_assert(ConstexprMade.Compare(ConstexprPrevious) > 0);
        static_assert(ConstexprMade.IsValid());
        static_assert(static_cast<bool>(ConstexprMade));
        static_assert(!TLogoBlobID().IsValid());
    }

    Y_UNIT_TEST_SUITE(TLogoBlobMigrationTest) {
        Y_UNIT_TEST(MatchesLegacyBitfieldLayout) {
            constexpr std::array<ui64, 4> tabletIds = {
                0,
                1,
                0x0123456789ABCDEFull,
                Max<ui64>(),
            };
            constexpr std::array<ui32, 5> generations = {
                0,
                1,
                0x00FFFFFF,
                0x01000000,
                Max<ui32>(),
            };
            constexpr std::array<ui32, 5> steps = {
                0,
                1,
                0x000000FF,
                0x00000100,
                Max<ui32>(),
            };
            constexpr std::array<ui32, 4> channels = {0, 1, 127, TLogoBlobID::MaxChannel};
            constexpr std::array<ui32, 4> blobSizes = {0, 1, 1 << 25, TLogoBlobID::MaxBlobSize};
            constexpr std::array<ui32, 4> cookies = {0, 1, 1 << 23, TLogoBlobID::MaxCookie};
            constexpr std::array<ui32, 4> partIds = {0, 1, 7, TLogoBlobID::MaxPartId};
            constexpr std::array<ui32, 4> crcModes = {0, 1, 2, TLogoBlobID::MaxCrcMode};

            for (ui64 tabletId : tabletIds) {
                for (ui32 generation : generations) {
                    for (ui32 step : steps) {
                        for (ui32 channel : channels) {
                            for (ui32 blobSize : blobSizes) {
                                for (ui32 cookie : cookies) {
                                    for (ui32 partId : partIds) {
                                        for (ui32 crcMode : crcModes) {
                                            const auto legacy = MakeLegacyRaw(
                                                tabletId,
                                                generation,
                                                step,
                                                channel,
                                                blobSize,
                                                cookie,
                                                partId,
                                                crcMode);
                                            const TLogoBlobID current(
                                                tabletId,
                                                generation,
                                                step,
                                                channel,
                                                blobSize,
                                                cookie,
                                                partId,
                                                crcMode);

                                            UNIT_ASSERT_VALUES_EQUAL(legacy[0], current.GetRaw()[0]);
                                            UNIT_ASSERT_VALUES_EQUAL(legacy[1], current.GetRaw()[1]);
                                            UNIT_ASSERT_VALUES_EQUAL(legacy[2], current.GetRaw()[2]);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Y_UNIT_TEST(MatchesLegacyBinaryFormat) {
            const TLogoBlobID id(
                0x0123456789ABCDEFull,
                0x30002C2D,
                0x50005F6F,
                1,
                0x3333,
                0x01A01B,
                7,
                2);
            constexpr std::array<ui8, TLogoBlobID::BinarySize> expected = {
                0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF,
                0x01, 0x30, 0x00, 0x2C, 0x2D, 0x50, 0x00, 0x5F,
                0x6F, 0x01, 0xA0, 0x1B, 0x80, 0x03, 0x33, 0x37,
            };
            std::array<ui8, TLogoBlobID::BinarySize> actual;

            id.ToBinary(actual.data());
            UNIT_ASSERT_EQUAL(expected, actual);
            UNIT_ASSERT_VALUES_EQUAL(id, TLogoBlobID::FromBinary(expected.data()));
        }
    }

    Y_UNIT_TEST_SUITE(TLogoBlobTest) {

        Y_UNIT_TEST(LogoBlobParse) {
            TLogoBlobID id;
            TString explanation;
            bool res = false;

            res = TLogoBlobID::Parse(id, "[               0:1:2:0:0:0:0]", explanation);
            UNIT_ASSERT(res && id == TLogoBlobID(0, 1, 2, 0, 0, 0));

            res = TLogoBlobID::Parse(id, "[               0:1:2:0:0:0:0", explanation);
            UNIT_ASSERT(!res && explanation == "Can't find trailing ']' after part id");

            res = TLogoBlobID::Parse(id, "[               0:1:2:0:0 v:0:0", explanation);
            UNIT_ASSERT(!res && explanation == "Can't find trailing ':' after cookie");

            res = TLogoBlobID::Parse(id, "[               0:1:2:0:0  :0:0]", explanation);
            UNIT_ASSERT(res);
        }

        Y_UNIT_TEST(LogoBlobCompare) {
            bool res = false;

            const TLogoBlobID left(1, 0x30002C2D, 0x50005F6F, 1, 0x3333, 0x0001A01B);

            UNIT_ASSERT(left.TabletID() == 1);
            UNIT_ASSERT(left.Channel() == 1);
            UNIT_ASSERT(left.Generation() == 0x30002C2D);
            UNIT_ASSERT(left.Step() == 0x50005F6F);
            UNIT_ASSERT(left.Cookie() == 0x0001A01B);
            UNIT_ASSERT(left.BlobSize() == 0x3333);
            UNIT_ASSERT(left.PartId() == 0);

            res = left < TLogoBlobID(2, 0x30002C2D, 0x50005F6F, 1, 0x3333, 0x0001A01B);
            UNIT_ASSERT(res);

            res = left < TLogoBlobID(2, 0x80008C8D, 0x80008F8F, 8, 0x8333, 0x0001801B);
            UNIT_ASSERT(res);

            res = left < TLogoBlobID(1, 0x00002C2D, 0x00005F6F, 2, 0x3333, 0x0001A01B);
            UNIT_ASSERT(res);

            res = left < TLogoBlobID(1, 0x40002C2D, 0x20005F6F, 1, 0x3333, 0x0001A01B);
            UNIT_ASSERT(res);

            res = left < TLogoBlobID(1, 0x30002C2D, 0x60005F6F, 1, 0x5333, 0x0005A01B);
            UNIT_ASSERT(res);

            res = left < TLogoBlobID(1, 0x30002C2D, 0x50005F6F, 1, 0x3333, 0x0001A01C);
            UNIT_ASSERT(res);

            res = left == TLogoBlobID(1, 0x30002C2D, 0x50005F6F, 1, 0x3333, 0x0001A01C);
            UNIT_ASSERT(!res);

            res = left == TLogoBlobID(1, 0x30002C2D, 0x50005F6F, 1, 0x3333, 0x0001A01B);
            UNIT_ASSERT(res);

            res = left < TLogoBlobID(1, 0x30002C2D, 0x50005F6F, 1, 0x3333, 0x0001A01B);
            UNIT_ASSERT(!res);

            res = left <= TLogoBlobID(1, 0x30002C2D, 0x50005F6F, 1, 0x3333, 0x0001A01B);
            UNIT_ASSERT(res);

            res = left == TLogoBlobID(1, 0x30002C2D, 0x50005F6F, 1, 0x3333, 0x0001A01B, 1).FullID();
            UNIT_ASSERT(res);

            UNIT_ASSERT(left.IsSameBlob(TLogoBlobID(1, 0x30002C2D, 0x50005F6F, 1, 0x3333, 0x0001A01B, 1)));
        }

        Y_UNIT_TEST(LogoBlobMaximumValues) {
            const TLogoBlobID id(
                Max<ui64>(),
                Max<ui32>(),
                Max<ui32>(),
                TLogoBlobID::MaxChannel,
                TLogoBlobID::MaxBlobSize,
                TLogoBlobID::MaxCookie,
                TLogoBlobID::MaxPartId,
                TLogoBlobID::MaxCrcMode);

            UNIT_ASSERT_VALUES_EQUAL(Max<ui64>(), id.TabletID());
            UNIT_ASSERT_VALUES_EQUAL(Max<ui32>(), id.Generation());
            UNIT_ASSERT_VALUES_EQUAL(Max<ui32>(), id.Step());
            UNIT_ASSERT_VALUES_EQUAL(TLogoBlobID::MaxChannel, id.Channel());
            UNIT_ASSERT_VALUES_EQUAL(TLogoBlobID::MaxBlobSize, id.BlobSize());
            UNIT_ASSERT_VALUES_EQUAL(TLogoBlobID::MaxCookie, id.Cookie());
            UNIT_ASSERT_VALUES_EQUAL(TLogoBlobID::MaxPartId, id.PartId());
            UNIT_ASSERT_VALUES_EQUAL(TLogoBlobID::MaxCrcMode, id.CrcMode());
            UNIT_ASSERT_VALUES_EQUAL(Max<ui64>(), id.GetRaw()[0]);
            UNIT_ASSERT_VALUES_EQUAL(Max<ui64>(), id.GetRaw()[1]);
            UNIT_ASSERT_VALUES_EQUAL(Max<ui64>(), id.GetRaw()[2]);
        }

        Y_UNIT_TEST(LogoBlobBinaryRoundTripWithUnalignedBuffer) {
            const TLogoBlobID id(42, 1, 2, 3, 100, 15, 4, 2);
            char buffer[TLogoBlobID::BinarySize + 1];

            id.ToBinary(buffer + 1);
            UNIT_ASSERT_VALUES_EQUAL(id, TLogoBlobID::FromBinary(buffer + 1));
        }

        Y_UNIT_TEST(LogoBlobSort) {
            TVector<TLogoBlobID> vec;
            vec.emplace_back(TLogoBlobID(66, 1, 0, 0, 110, 20));
            vec.emplace_back(TLogoBlobID(66, 1, 0, 0, 109, 21));
            vec.emplace_back(TLogoBlobID(66, 1, 0, 0, 108, 22));
            vec.emplace_back(TLogoBlobID(66, 1, 0, 0, 107, 23));
            vec.emplace_back(TLogoBlobID(66, 1, 0, 0, 106, 24));

            vec.emplace_back(TLogoBlobID(42, 1, 1, 0, 100, 15));
            vec.emplace_back(TLogoBlobID(42, 1, 2, 0, 100, 19));
            vec.emplace_back(TLogoBlobID(42, 1, 3, 0, 100, 16));
            vec.emplace_back(TLogoBlobID(42, 1, 1, 3, 100, 17));
            vec.emplace_back(TLogoBlobID(42, 1, 2, 3, 100, 18));
            vec.emplace_back(TLogoBlobID(42, 2, 0, 0, 100, 20));

            Sort(vec.begin(), vec.end());

            TStringStream str;
            for (const auto &x : vec) {
                str << x.ToString() << "\n";
            }

            // sorted by: TabletId, Channel, Generation, Step, Cookie, BlobSize
            TString result =
            "[42:1:1:0:15:100:0]\n"
            "[42:1:2:0:19:100:0]\n"
            "[42:1:3:0:16:100:0]\n"
            "[42:2:0:0:20:100:0]\n"
            "[42:1:1:3:17:100:0]\n"
            "[42:1:2:3:18:100:0]\n"
            "[66:1:0:0:20:110:0]\n"
            "[66:1:0:0:21:109:0]\n"
            "[66:1:0:0:22:108:0]\n"
            "[66:1:0:0:23:107:0]\n"
            "[66:1:0:0:24:106:0]\n";

            UNIT_ASSERT_STRINGS_EQUAL(result, str.Str());
        }
    }

    Y_UNIT_TEST_SUITE(TLogoBlobIdHashTest) {
        Y_UNIT_TEST(SimpleTest) {
    //      ui64 tabletId, ui32 generation, ui32 step, ui32 channel, ui32 blobSize, ui32 cookie
            UNIT_ASSERT_VALUES_EQUAL(0x13150f70, TLogoBlobID(42, 1, 1, 0, 100, 15).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0xc05a9a80, TLogoBlobID(42, 2, 1, 0, 100, 15).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x4061a4ef, TLogoBlobID(42, 1, 2, 0, 100, 15).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x6dbe758a, TLogoBlobID(42, 1, 1, 1, 100, 15).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x13150f70, TLogoBlobID(42, 1, 1, 0, 101, 15).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x6b67039d, TLogoBlobID(42, 1, 1, 0, 100, 16).Hash());
        }

        Y_UNIT_TEST(SimpleTestPartIdDoesNotMatter) {
    //      ui64 tabletId, ui32 generation, ui32 step, ui32 channel, ui32 blobSize, ui32 cookie, ui32 partId
            ui32 partId = 1;
            UNIT_ASSERT_VALUES_EQUAL(0x13150f70, TLogoBlobID(42, 1, 1, 0, 100, 15, partId).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0xc05a9a80, TLogoBlobID(42, 2, 1, 0, 100, 15, partId).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x4061a4ef, TLogoBlobID(42, 1, 2, 0, 100, 15, partId).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x6dbe758a, TLogoBlobID(42, 1, 1, 1, 100, 15, partId).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x13150f70, TLogoBlobID(42, 1, 1, 0, 101, 15, partId).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x6b67039d, TLogoBlobID(42, 1, 1, 0, 100, 16, partId).Hash());

            partId = 2;
            UNIT_ASSERT_VALUES_EQUAL(0x13150f70, TLogoBlobID(42, 1, 1, 0, 100, 15, partId).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0xc05a9a80, TLogoBlobID(42, 2, 1, 0, 100, 15, partId).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x4061a4ef, TLogoBlobID(42, 1, 2, 0, 100, 15, partId).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x6dbe758a, TLogoBlobID(42, 1, 1, 1, 100, 15, partId).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x13150f70, TLogoBlobID(42, 1, 1, 0, 101, 15, partId).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x6b67039d, TLogoBlobID(42, 1, 1, 0, 100, 16, partId).Hash());

            partId = 3;
            UNIT_ASSERT_VALUES_EQUAL(0x13150f70, TLogoBlobID(42, 1, 1, 0, 100, 15, partId).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0xc05a9a80, TLogoBlobID(42, 2, 1, 0, 100, 15, partId).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x4061a4ef, TLogoBlobID(42, 1, 2, 0, 100, 15, partId).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x6dbe758a, TLogoBlobID(42, 1, 1, 1, 100, 15, partId).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x13150f70, TLogoBlobID(42, 1, 1, 0, 101, 15, partId).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x6b67039d, TLogoBlobID(42, 1, 1, 0, 100, 16, partId).Hash());
        }

         Y_UNIT_TEST(SimpleTestBlobSizeDoesNotMatter) {
    //      ui64 tabletId, ui32 generation, ui32 step, ui32 channel, ui32 blobSize, ui32 cookie, ui32 partId
            ui32 partId = 1;
            UNIT_ASSERT_VALUES_EQUAL(0x13150f70, TLogoBlobID(42, 1, 1, 0, 32423523, 15, partId).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0xc05a9a80, TLogoBlobID(42, 2, 1, 0, 43, 15, partId).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x4061a4ef, TLogoBlobID(42, 1, 2, 0, 54645, 15, partId).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x6dbe758a, TLogoBlobID(42, 1, 1, 1, 56650, 15, partId).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x13150f70, TLogoBlobID(42, 1, 1, 0, 0, 15, partId).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x6b67039d, TLogoBlobID(42, 1, 1, 0, 58435455, 16, partId).Hash());
        }

        Y_UNIT_TEST(SimpleTestWithDifferentTabletId) {
    //      ui64 tabletId, ui32 generation, ui32 step, ui32 channel, ui32 blobSize, ui32 cookie
            UNIT_ASSERT_VALUES_EQUAL(0x13150f70, TLogoBlobID(42, 1, 1, 0, 100, 15).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x13330eae, TLogoBlobID(43, 1, 1, 0, 100, 15).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x13510deb, TLogoBlobID(44, 1, 1, 0, 100, 15).Hash());

            UNIT_ASSERT_VALUES_EQUAL(0x136f0d29, TLogoBlobID(45, 1, 1, 0, 100, 15).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x138d0c66, TLogoBlobID(46, 1, 1, 0, 100, 15).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x13ab0ba4, TLogoBlobID(47, 1, 1, 0, 100, 15).Hash());
        }

        Y_UNIT_TEST(SimpleTestWithDifferentSteps) {
    //      ui64 tabletId, ui32 generation, ui32 step, ui32 channel, ui32 blobSize, ui32 cookie
            UNIT_ASSERT_VALUES_EQUAL(0x13150f70, TLogoBlobID(42, 1, 1, 0, 100, 15).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0xc05a9a80, TLogoBlobID(42, 2, 1, 0, 100, 15).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x6da02590, TLogoBlobID(42, 3, 1, 0, 100, 15).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x1ae5b09f, TLogoBlobID(42, 4, 1, 0, 100, 15).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0xc82b3baf, TLogoBlobID(42, 5, 1, 0, 100, 15).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x7570c6bf, TLogoBlobID(42, 6, 1, 0, 100, 15).Hash());
        }

        Y_UNIT_TEST(SimpleTestWithDifferentChannel) {
    //      ui64 tabletId, ui32 generation, ui32 step, ui32 channel, ui32 blobSize, ui32 cookie
            UNIT_ASSERT_VALUES_EQUAL(0x13150f70, TLogoBlobID(42, 1, 1, 0, 100, 15).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x6dbe758a, TLogoBlobID(42, 1, 1, 1, 100, 15).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0xc867dba4, TLogoBlobID(42, 1, 1, 2, 100, 15).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x231141be, TLogoBlobID(42, 1, 1, 3, 100, 15).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x4309a659, TLogoBlobID(42, 1, 1, 9, 100, 15).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x1854d729, TLogoBlobID(42, 1, 1, 17, 100, 15).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0xbd6e95ea, TLogoBlobID(42, 1, 1, 64, 100, 15).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x67c81c65, TLogoBlobID(42, 1, 1, 128, 100, 15).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0xc271827f, TLogoBlobID(42, 1, 1, 129, 100, 15).Hash());
            UNIT_ASSERT_VALUES_EQUAL(0x61d1c33f, TLogoBlobID(42, 1, 1, 255, 100, 15).Hash());
        }

    }

} // NKikimr
