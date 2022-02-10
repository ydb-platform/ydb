#include <ydb/library/schlab/mon/mon.h>
#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/monlib/service/monservice.h>
#include <library/cpp/monlib/service/pages/resource_mon_page.h>
#include <library/cpp/resource/resource.h>

namespace {

// Config
int g_MonPort              = 8080;

}

#define SCHLAB_STATIC_FILE(mon, file, type) \
    mon->Register(new TResourceMonPage(file, file, NMonitoring::TResourceMonPage::type)); \
    /**/

int main(int argc, char** argv)
{
    using namespace NMonitoring;

    try {
#if defined(_unix_) || defined(_darwin_)
        signal(SIGPIPE, SIG_IGN);
#endif

#ifdef _win32_
        WSADATA dummy;
        WSAStartup(MAKEWORD(2,2), &dummy);
#endif

        // Configure
        using TMonSrvc = NMonitoring::TMonService2;
        THolder<TMonSrvc> MonSrvc;
        NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
        opts.AddLongOption(0, "mon-port", "port of monitoring service")
                .RequiredArgument("port")
                .StoreResult(&g_MonPort, g_MonPort);
        NLastGetopt::TOptsParseResult res(&opts, argc, argv);

        // Init monservice
        TString schArmJson;

        MonSrvc.Reset(new TMonSrvc(g_MonPort));
        SCHLAB_STATIC_FILE(MonSrvc.Get(), "schlab/schviz-test0.json", JSON);
        NKikimr::CreateSchLabCommonPages(MonSrvc.Get());
        NKikimr::CreateSchArmPages(MonSrvc.Get(), "schlab/scharm", "/schlab/schviz");
        NKikimr::CreateSchVizPages(MonSrvc.Get(), "schlab/schviz", "/schlab/scharm?mode=getschedule");

        // Start monservice
        MonSrvc->Start();

        // Infinite loop
        while (true) {
            Sleep(TDuration::Seconds(1));
        }

        // Finish
        Cout << "bye" << Endl;
        return 0;
    } catch (...) {
        Cerr << "failure: " << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
