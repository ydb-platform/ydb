#include <library/cpp/streams/xz/decompress.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(XzDecompress) {
    Y_UNIT_TEST(decompress) {
        TStringStream in;
        in << Base64Decode("/Td6WFoAAATm1rRGAgAhARYAAAB0L+Wj4ANQAUVdABkMHARbg7qMApbl/qwEvrgQKpvF7Rbp/QJJdquZ88M3I5x3ANhSSpxvtnSoyPDeC6M8vz0vNKiOCsbIqvsGIwxrx+6YNqT87gDxVS8S3fHeoAZTf+zbg1DpDtv7Xh7Q3ug24wxNbPMi2p+WAo3V0LAi+lGUQmA44nJlabRv0XZ5CWhwgYtEWrrbPxoFjONeCa4p5BoX+TVgWegToFQMeJhVXMbDGWOIFL56X/F7nDJ47pjAy2GJIHHI5W/wrGH6uB0TCwpudW96peQaEgwMSZE07PfPE+XkfEymxhkxTs5Mnpc2rmQCiZ+3I6PqP+Qj8fuqaxb0fAJPQrbWYsqqeXP/3VNOeDRk+Szr9H3TMGI6yepUgkrgqNpaIYYcbxTU43eofcnTdwdsgi8fpH99tx3rrKq4zveStkZgZQqeY+MCvineIAAAAAAA2X8RUfmPU3kAAeEC0QYAANTt6P6xxGf7AgAAAAAEWVo=");

        TXzDecompress xz(&in);

        UNIT_ASSERT_VALUES_EQUAL(
            xz.ReadAll(),
            "2020-08-27T18:22:02.332  INFO: Starting blackbox module\n"
            "2020-08-27T18:22:02.850  INFO: Init libauth (root kspace=<yandex_ru>, sign with key #49)\n"
            "2020-08-27T18:22:02.851  DEBUG: KeyRing: randoms: table name loaded. Took 0.000918s\n"
            "2020-08-27T18:22:02.853  DEBUG: KeyRing: randoms: min-max key id loaded. Took 0.001249s\n"
            "2020-08-27T18:22:02.863  DEBUG: KeyRing: randoms: new keys loaded. Took 0.010837s\n"
            "2020-08-27T18:22:02.865  DEBUG: Loaded 2389 new key(s) for keyspace 'yandex_ru'. Key ids: 330589-335364\n"
            "2020-08-27T18:22:02.866  INFO: Attempt to load second time for spacename yandex_ru\n"
            "2020-08-27T18:22:02.867  DEBUG: KeyRing: randoms_ua: table name loaded. Took 0.000926s\n"
            "2020-08-27T18:22:02.868  DEBUG: KeyRing: randoms_ua: min-max key id loaded. Took 0.001212s\n"
            "2020-08-27T18:22:02.871  DEBUG: KeyRing: randoms_ua: new keys loaded. Took 0.003202s\n");
    }
}
