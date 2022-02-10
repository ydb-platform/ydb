#include "url_mapper.h"
#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;

namespace {
void RemapAndCheck(const TUrlMapper& m, const TString& input, const TString& expectedOutput) {
    TString output;
    UNIT_ASSERT(m.MapUrl(input, output));
    UNIT_ASSERT_VALUES_EQUAL(output, expectedOutput);
}
}

Y_UNIT_TEST_SUITE(TUrlMapperTests) { 
    Y_UNIT_TEST(All) { 
        TUrlMapper m;
        m.AddMapping("sbr:(?://)?(\\d+)", "http://proxy.sandbox.yandex-team.ru/$1");

        m.AddMapping("yt://([a-zA-Z0-9\\-_]+)/(.+)@t=([0-9A-Fa-f]+\\-[0-9A-Fa-f]+\\-[0-9A-Fa-f]+\\-[0-9A-Fa-f]+)", "https://$1.yt.yandex-team.ru/api/v3/read_file?disposition=attachment&path=//$2&transaction_id=$3");
        m.AddMapping("yt://([a-zA-Z0-9\\-_]+)/(.+)", "https://$1.yt.yandex-team.ru/api/v3/read_file?disposition=attachment&path=//$2");

        m.AddMapping("https?://yt\\.yandex(\\.net|-team\\.ru)/([a-zA-Z0-9\\-_]+)/#page=navigation&path=//(.+)", "https://$2.yt.yandex-team.ru/api/v3/read_file?disposition=attachment&path=//$3");
        m.AddMapping("https?://yt\\.yandex(\\.net|-team\\.ru)/([a-zA-Z0-9\\-_]+)/navigation\\?path=//(.+)", "https://$2.yt.yandex-team.ru/api/v3/read_file?disposition=attachment&path=//$3");

        m.AddMapping("https?://a\\.yandex-team\\.ru/arc/(.+)/arcadia/(.+)\\?rev=(\\d+)", "arc://$2?rev=$3&branch=$1");

        TString tmp;
        UNIT_ASSERT(!m.MapUrl("http://ya.ru", tmp));
        UNIT_ASSERT(!m.MapUrl("http://hahn.yt.yandex-team.ru/api/v3/read_file?disposition=attachment&path=//home", tmp));

        RemapAndCheck(m, "sbr:654321", "http://proxy.sandbox.yandex-team.ru/654321");
        RemapAndCheck(m, "sbr://654321", "http://proxy.sandbox.yandex-team.ru/654321");
        RemapAndCheck(m, "yt://hahn/statbox/home/ngc224/nile/tmp/libexamples-pager_ctr.soYDyrqeVMhp", "https://hahn.yt.yandex-team.ru/api/v3/read_file?disposition=attachment&path=//statbox/home/ngc224/nile/tmp/libexamples-pager_ctr.soYDyrqeVMhp");
        // transaction guid : 4 groups of hex
        RemapAndCheck(m, "yt://hahn/statbox/home/nile/tmp/abc.txt@t=9797-164f64-3fe0190-dd3c4184", "https://hahn.yt.yandex-team.ru/api/v3/read_file?disposition=attachment&path=//statbox/home/nile/tmp/abc.txt&transaction_id=9797-164f64-3fe0190-dd3c4184");
        // transaction guid : 0-0-0-0
        RemapAndCheck(m, "yt://hahn/statbox/home/nile/tmp/abc.txt@t=0-0-0-0", "https://hahn.yt.yandex-team.ru/api/v3/read_file?disposition=attachment&path=//statbox/home/nile/tmp/abc.txt&transaction_id=0-0-0-0");
        // transaction guid with bad char z, it will be dropped
        RemapAndCheck(m, "yt://hahn/statbox/home/nile/tmp/abc.txt@t=9797-164f64-3fe0190-dd3c418z", "https://hahn.yt.yandex-team.ru/api/v3/read_file?disposition=attachment&path=//statbox/home/nile/tmp/abc.txt&transaction_id=9797-164f64-3fe0190-dd3c418");
        // transaction guid with capitals
        RemapAndCheck(m, "yt://hahn/statbox/home/nile/tmp/abc.txt@t=9797-164F64-3FE0190-DD3C4184A", "https://hahn.yt.yandex-team.ru/api/v3/read_file?disposition=attachment&path=//statbox/home/nile/tmp/abc.txt&transaction_id=9797-164F64-3FE0190-DD3C4184A");
        // transaction guid with 3 groups instead of 4
        RemapAndCheck(m, "yt://hahn/statbox/home/nile/tmp/abc.txt@t=9797-164f64-3fe0190", "https://hahn.yt.yandex-team.ru/api/v3/read_file?disposition=attachment&path=//statbox/home/nile/tmp/abc.txt@t=9797-164f64-3fe0190");
        RemapAndCheck(m, "https://yt.yandex-team.ru/hahn/#page=navigation&path=//home/yql/dev/1.txt", "https://hahn.yt.yandex-team.ru/api/v3/read_file?disposition=attachment&path=//home/yql/dev/1.txt");
        RemapAndCheck(m, "https://yt.yandex-team.ru/hahn/navigation?path=//home/yql/dev/1.txt", "https://hahn.yt.yandex-team.ru/api/v3/read_file?disposition=attachment&path=//home/yql/dev/1.txt");

        // with dash
        RemapAndCheck(m, "yt://seneca-sas/tmp/bsistat02i_202624_1523866684_inclusion_already_moderating", "https://seneca-sas.yt.yandex-team.ru/api/v3/read_file?disposition=attachment&path=//tmp/bsistat02i_202624_1523866684_inclusion_already_moderating");
        // with underscore and digits
        RemapAndCheck(m, "yt://seneca_sas0123456789/tmp/bsistat02i_202624_1523866684_inclusion_already_moderating", "https://seneca_sas0123456789.yt.yandex-team.ru/api/v3/read_file?disposition=attachment&path=//tmp/bsistat02i_202624_1523866684_inclusion_already_moderating");

        RemapAndCheck(m, "https://a.yandex-team.ru/arc/trunk/arcadia/yql/ya.make?rev=5530789", "arc://yql/ya.make?rev=5530789&branch=trunk");
        RemapAndCheck(m, "https://a.yandex-team.ru/arc/branches/yql/yql-stable-2019-08-16/arcadia/yql/ya.make?rev=5530789", "arc://yql/ya.make?rev=5530789&branch=branches/yql/yql-stable-2019-08-16");

        UNIT_ASSERT(!m.MapUrl("https://a.yandex-team.ru/arc/trunk/arcadia/yql/ya.make", tmp));
    }
}
