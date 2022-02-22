#include "pattern_group.h"
#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;

namespace {
void Check(const TPatternGroup& g, const TString& url, bool expectedResult) {
    UNIT_ASSERT_C(g.Match(url) == expectedResult, url);
}

void CheckSuccess(const TPatternGroup& g, const TString& url) {
    Check(g, url, true);
}

void CheckFailure(const TPatternGroup& g, const TString& url) {
    Check(g, url, false);
}
}

Y_UNIT_TEST_SUITE(TPatternGroupTests) {
    Y_UNIT_TEST(All) {
        TPatternGroup g;
        UNIT_ASSERT(g.IsEmpty());

        g.Add(R"XXX(^https://[\w-]+\.yt\.yandex-team\.ru/)XXX");
        g.Add(R"XXX(^https://(paste|storage|nirvana|grafana|bb|fml)\.yandex-team\.ru/)XXX");
        UNIT_ASSERT(!g.IsEmpty());

        CheckSuccess(g, "https://seneca-sas.yt.yandex-team.ru/api/v3/read_file?disposition=attachment&path=//tmp/bsistat02i_202624_1523866684_inclusion_already_moderating");
        CheckSuccess(g, "https://paste.yandex-team.ru/727063/text");

        CheckFailure(g, "https://hahn.yt.yandex-team.ru:80/api/v3/read_file");
        CheckFailure(g, "http://ya.ru");
    }
}
