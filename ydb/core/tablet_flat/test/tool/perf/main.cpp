#include "app.h"

#include <library/cpp/getopt/last_getopt.h>
#include <util/generic/yexception.h>

int main(int argc, char* argv[])
{
    using namespace NLastGetopt;

    TOpts opts;

    opts.AddLongOption("llev", "log level").RequiredArgument("NUM");
    opts.AddLongOption("mass", "rows mass args").RequiredArgument("STR");
    opts.AddLongOption("aggr", "data sponge model").RequiredArgument("STR");
    opts.AddLongOption("smp",  "sampling settings").RequiredArgument("STR");
    opts.AddLongOption("test", "tests to perform").RequiredArgument("STR");

    THolder<TOptsParseResult> res(new TOptsParseResult(&opts, argc, argv));

    NKikimr::NTable::NPerf::TApp app;

    try {
        if (res->Has("llev"))   app.SetLogger(res->Get("llev"));
        if (res->Has("mass"))   app.SetMass(res->Get("mass"));
        if (res->Has("aggr"))   app.SetSponge(res->Get("aggr"));
        if (res->Has("smp"))    app.SetSampling(res->Get("smp"));
        if (res->Has("test"))   app.SetTests(res->Get("test"));

        return app.Execute();

    } catch (yexception &ex) {
        Cerr << ex.what() << Endl;

        return -1;
    }
}
