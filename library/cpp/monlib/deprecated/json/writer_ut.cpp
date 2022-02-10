#include "writer.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NMonitoring;

Y_UNIT_TEST_SUITE(JsonWriterTests) {
    Y_UNIT_TEST(One) {
        TStringStream ss;
        TDeprecatedJsonWriter w(&ss);
        w.OpenDocument();
        w.OpenMetrics();

        for (int i = 0; i < 5; ++i) {
            w.OpenMetric();
            w.OpenLabels();
            w.WriteLabel("user", TString("") + (char)('a' + i));
            w.WriteLabel("name", "NWrites");
            w.CloseLabels();
            if (i % 2 == 0) {
                w.WriteModeDeriv();
            }
            w.WriteValue(10l);
            w.CloseMetric();
        }

        w.CloseMetrics();
        w.CloseDocument();

        //Cout << ss.Str() << "\n";
    }
}
