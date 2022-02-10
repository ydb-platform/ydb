#include "defs.h"
#include "logoblob.h"
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/ptr.h>

namespace NKikimr {
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
