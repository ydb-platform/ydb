#include "sql_highlight_json.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLHighlight;

Y_UNIT_TEST_SUITE(SqlHighlightJsonTests) {

    Y_UNIT_TEST(Smoke) {
        NJson::TJsonValue json = ToJson(MakeHighlighting());
        UNIT_ASSERT(json.Has("units"));
    }

} // Y_UNIT_TEST_SUITE(SqlHighlightJsonTests)
