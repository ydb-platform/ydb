#include "md5_stream.h"
#include <util/stream/input.h>
#include <util/stream/str.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;

namespace {
TString Consume(const TString& input) {
    TStringInput s(input);

    TString output;
    TStringOutput outputStream(output);
    TMd5OutputStream md5Stream(outputStream);

    UNIT_ASSERT_VALUES_EQUAL(input.size(), TransferData(&s, &md5Stream));
    UNIT_ASSERT_VALUES_EQUAL(input, output);
    return md5Stream.Finalize();
}
}

Y_UNIT_TEST_SUITE(TStreamMd5Tests) {
    Y_UNIT_TEST(Empty) {
        const auto md5 = Consume("");
        const TString emptyStringMd5 = "d41d8cd98f00b204e9800998ecf8427e";
        UNIT_ASSERT_VALUES_EQUAL(md5, emptyStringMd5);
    }

    Y_UNIT_TEST(ShortText) {
        const auto md5 = Consume("hello from Y!");
        const TString expectedMd5 = "abf59ed7b0daa71085e76e461a737cc2";
        UNIT_ASSERT_VALUES_EQUAL(md5, expectedMd5);
    }

    Y_UNIT_TEST(BigText) {
        // TransferData uses TempBuf of 64K
        const TString s(1000000, 'A');
        const auto md5 = Consume(s.c_str());
        /*
        $ for i in {1..1000000};do echo -n A >> 1M.txt;done
        $ md5sum 1M.txt
        48fcdb8b87ce8ef779774199a856091d  1M.txt
        */
        const TString expectedMd5 = "48fcdb8b87ce8ef779774199a856091d";
        UNIT_ASSERT_VALUES_EQUAL(md5, expectedMd5);
    }
}
