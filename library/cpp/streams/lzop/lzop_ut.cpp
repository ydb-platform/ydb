#include "lzop.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/tempfile.h>
#include <util/stream/file.h>

#define ZDATA "./lzop_data"

Y_UNIT_TEST_SUITE(TLzopTest) {
    static const TString data = "8s7d5vc6s5vc67sa4c65ascx6asd4xcv76adsfxv76s";
    static const TString data2 = "cn8wk2bd9vb3vdfif83g1ks94bfiovtwv";

    Y_UNIT_TEST(Compress) {
        TUnbufferedFileOutput o(ZDATA);
        TLzopCompress c(&o);

        c.Write(data.data(), data.size());
        c.Finish();
        o.Finish();
    }

    Y_UNIT_TEST(Decompress) {
        TTempFile tmpFile(ZDATA);

        {
            TUnbufferedFileInput i(ZDATA);
            TLzopDecompress d(&i);

            UNIT_ASSERT_EQUAL(d.ReadLine(), data);
        }
    }

    Y_UNIT_TEST(DecompressTwoStreams) {
        // Check that Decompress(Compress(X) + Compress(Y)) == X + Y
        TTempFile tmpFile(ZDATA);
        {
            TUnbufferedFileOutput o(ZDATA);
            TLzopCompress c1(&o);
            c1.Write(data.data(), data.size());
            c1.Finish();
            TLzopCompress c2(&o);
            c2.Write(data2.data(), data2.size());
            c2.Finish();
            o.Finish();
        }
        {
            TUnbufferedFileInput i(ZDATA);
            TLzopDecompress d(&i);

            UNIT_ASSERT_EQUAL(d.ReadLine(), data + data2);
        }
    }
}
