#include "myshop.h"

#include <ydb/library/shop/sim_shop/config.pb.h>

#include <ydb/library/shop/probes.h>

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/monlib/service/monservice.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/resource/resource.h>

#include <util/stream/file.h>
#include <util/string/subst.h>
#include <util/system/hp_timer.h>

#include <google/protobuf/text_format.h>

using namespace NShopSim;

#define WWW_CHECK(cond, ...) \
    do { \
        if (!(cond)) { \
            ythrow yexception() << Sprintf(__VA_ARGS__); \
        } \
    } while (false) \
    /**/

#define WWW_HTML_INNER(out, body) HTML(out) { \
    out << "<!DOCTYPE html>" << Endl; \
    HTML_TAG() { \
        HEAD() { OutputCommonHeader(out); } \
        BODY() { \
            body; \
        } \
    } \
}
#define WWW_HTML(out, body) out << NMonitoring::HTTPOKHTML; \
    WWW_HTML_INNER(out, body)

void OutputCommonHeader(IOutputStream& out)
{
    out << "<link rel=\"stylesheet\" href=\"trace-static/css/bootstrap.min.css\">";
    out << "<script language=\"javascript\" type=\"text/javascript\" src=\"trace-static/js/bootstrap.min.js\"></script>";
    out << "<script language=\"javascript\" type=\"text/javascript\" src=\"trace-static/jquery.min.js\"></script>";
}

class TMachineMonPage : public NMonitoring::IMonPage {
public:
    TMachineMonPage()
        : IMonPage("machine")
    {}

    virtual void Output(NMonitoring::IMonHttpRequest& request) {

        const char* urls[] = {
            "/trace?fullscreen=y&id=.SIMSHOP_PROVIDER.FifoDequeue.pmachineid={{machineid}}.d1s&aggr=hist&autoscale=y&refresh=3000&bn=queueTimeMs&linesfill=y&mode=analytics&out=flot&pointsshow=n&xn=queueTimeMs&y1=0&x1=0&yns=_count_share",
            "/trace?fullscreen=y&id=.SIMSHOP_PROVIDER.MachineExecute.pmachineid={{machineid}}.d1s&aggr=hist&autoscale=y&refresh=3000&bn=execTimeMs&linesfill=y&mode=analytics&out=flot&pointsshow=n&xn=execTimeMs&y1=0&x1=0&yns=_count_share",
            "/trace?fullscreen=y&id=.SIMSHOP_PROVIDER.MachineWait.pmachineid={{machineid}}.d1s&aggr=hist&autoscale=y&refresh=3000&bn=waitTimeMs&linesfill=y&mode=analytics&out=flot&pointsshow=n&xn=waitTimeMs&y1=0&x1=0&yns=_count_share",

            "/trace?fullscreen=y&id=.SHOP_PROVIDER.Arrive.pflowctl={{flowctl}}.d200ms:.SHOP_PROVIDER.Depart.pflowctl={{flowctl}}.d200ms:.SIMSHOP_PROVIDER.FifoEnqueue.pmachineid={{machineid}}.d200ms:.SIMSHOP_PROVIDER.FifoDequeue.pmachineid={{machineid}}.d200ms&mode=analytics&out=flot&xn=_thrRTime&y1=0&yns=costInFly:queueCost&cutts=y",
            "/trace?fullscreen=y&id=.SIMSHOP_PROVIDER.FifoEnqueue.pmachineid={{machineid}}.d1s&aggr=hist&autoscale=y&refresh=3000&bn=queueCost&linesfill=y&mode=analytics&out=flot&pointsshow=n&xn=queueCost&y1=0&x1=0&yns=_count_share-stack",
            "/trace?fullscreen=y&id=.SIMSHOP_PROVIDER.FifoEnqueue.pmachineid={{machineid}}.d1s&aggr=hist&autoscale=y&refresh=3000&bn=queueCost&linesfill=y&mode=analytics&out=flot&pointsshow=n&xn=queueCost&y1=0&x1=0&yns=_count_share",

            "/trace?fullscreen=y&id=.SHOP_PROVIDER.Arrive.pflowctl={{flowctl}}.d200ms:.SHOP_PROVIDER.Depart.pflowctl={{flowctl}}.d200ms:.SIMSHOP_PROVIDER.FifoEnqueue.pmachineid={{machineid}}.d200ms:.SIMSHOP_PROVIDER.FifoDequeue.pmachineid={{machineid}}.d200ms&mode=analytics&out=flot&xn=_thrRTime&y1=0&yns=countInFly:queueLength&cutts=y",
            "/trace?fullscreen=y&id=.SIMSHOP_PROVIDER.FifoDequeue.pmachineid={{machineid}}.d200ms&mode=analytics&out=flot&xn=_thrRTime&yns=queueTimeMs&y0=0&pointsshow=n&cutts=y",
            "/trace?fullscreen=y&id=.Group.ShopFlowCtlPeriod.pflowctl={{flowctl}}.d10m&mode=analytics&out=flot&pointsshow=n&xn=periodId&yns=badPeriods:goodPeriods:zeroPeriods",

            "/trace?fullscreen=y&id=.Group.ShopFlowCtlPeriod.pflowctl={{flowctl}}.d10m&g=periodId&mode=analytics&out=flot&pointsshow=n&xn=periodId&y1=0&yns=dT_T:dL_L",
            "/trace?fullscreen=y&id=.Group.ShopFlowCtlPeriod.pflowctl={{flowctl}}.d10m&g=periodId&mode=analytics&out=flot&pointsshow=n&xn=periodId&y1=0&yns=pv",
            "/trace?fullscreen=y&id=.Group.ShopFlowCtlPeriod.pflowctl={{flowctl}}.d10m&g=periodId&linesfill=y&mode=analytics&out=flot&xn=periodId&y1=0&yns=state:window&pointsshow=n",

            "/trace?fullscreen=y&id=.Group.ShopFlowCtlPeriod.pflowctl={{flowctl}}.d10m&g=periodId&mode=analytics&out=flot&xn=periodId&y1=-1&y2=1&yns=error",
            "/trace?fullscreen=y&id=.Group.ShopFlowCtlPeriod.pflowctl={{flowctl}}.d10m&g=periodId&linesshow=n&mode=analytics&out=flot&x1=0&xn=window&y1=0&yns=throughput&cutts=y",
            "/trace?fullscreen=y&id=.Group.ShopFlowCtlPeriod.pflowctl={{flowctl}}.d10m&g=periodId&mode=analytics&out=flot&xn=periodId&y1=0&yns=throughput:throughputMin:throughputMax:throughputLo:throughputHi&pointsshow=n&legendshow=n",

            "/trace?fullscreen=y&id=.Group.ShopFlowCtlPeriod.pflowctl={{flowctl}}.d10m&g=periodId&mode=analytics&out=flot&xn=periodId&yns=mode",
            "/trace?fullscreen=y&id=.Group.ShopFlowCtlPeriod.pflowctl={{flowctl}}.d10m&g=periodId&linesshow=n&mode=analytics&out=flot&x1=0&xn=window&y1=0&yns=latencyAvgMs&cutts=y",
            "/trace?fullscreen=y&id=.Group.ShopFlowCtlPeriod.pflowctl={{flowctl}}.d10m&g=periodId&mode=analytics&out=flot&pointsshow=n&xn=periodId&y1=0&yns=latencyAvgMs:latencyAvgMinMs:latencyAvgMaxMs:latencyAvgLoMs:latencyAvgHiMs&legendshow=n",

            "/trace?fullscreen=y&id=.SIMSHOP_PROVIDER.MachineStats.pmachineid={{machineid}}.d10m&mode=analytics&out=flot&xn=_thrRTime&yns=utilization&linesfill=y&y1=0&y2=1&pointsshow=n",
            "/trace?fullscreen=y&id=.SIMSHOP_PROVIDER.MachineWorkerStats.pmachineid={{machineid}}.d1s&autoscale=y&mode=analytics&out=flot&xn=_thrRTime&yns=activeTimeMs-stack:totalTimeMs-stack&pointsshow=n&linesfill=y&cutts=y&legendshow=n",
            "/trace?fullscreen=y&id=.SHOP_PROVIDER.Depart.pflowctl={{flowctl}}.d1s&mode=analytics&out=flot&xn=realCost&yns=estCost&linesshow=n"
        };

        THashMap<TString, TString> dict;
        for (const auto& kv : request.GetParams()) {
            dict[kv.first] = kv.second;
        }

        TStringStream out;
        out << NMonitoring::HTTPOKHTML;
        HTML(out) {
            out << "<!DOCTYPE html>" << Endl;
            HTML_TAG() {
                BODY() {
                    out << "<table border=\"0\" width=\"100%\"><tr>";
                    for (size_t i = 0; i < Y_ARRAY_SIZE(urls); i++) {
                        if (i > 0 && i % 3 == 0) {
                            out << "</tr><tr>";
                        }
                        out << "<td><iframe style=\"border:none;width:100%;height:100%\" src=\""
                            << Subst(urls[i], dict)
                            << "\"></iframe></td>";
                    }
                    out << "</tr></table>";
                }
            }
        }
        request.Output() << out.Str();
    }
private:
    template <class TDict>
    TString Subst(const TString& str, const TDict& dict)
    {
        TString res = str;
        for (const auto& kv: dict) {
            SubstGlobal(res, "{{" + kv.first + "}}", kv.second);
        }
        return res;
    }
};

class TShopMonPage : public NMonitoring::IMonPage {
private:
    TMyShop* Shop;
    TVector<TString> Templates;
public:
    explicit TShopMonPage(TMyShop* shop)
        : IMonPage("shop", "Shop")
        , Shop(shop)
        , Templates({"one", "two"})
    {}

    virtual void Output(NMonitoring::IMonHttpRequest& request) {

        TStringStream out;
        try {
            if (request.GetParams().Get("mode") == "") {
                OutputMain(request, out);
            } else if (request.GetParams().Get("mode") == "configure") {
                PostConfigure(request, out);
            } else {
                ythrow yexception() << "Bad request";
            }
        } catch (...) {
            out.Clear();
            WWW_HTML(out,
                out << "<h2>Error</h2><pre>"
                    << CurrentExceptionMessage()
                    << Endl;
            )
        }

        request.Output() << out.Str();
    }

    void OutputMain(const NMonitoring::IMonHttpRequest& request, IOutputStream& out)
    {
        out << NMonitoring::HTTPOKHTML;
        HTML(out) {
            out << "<!DOCTYPE html>" << Endl;
            HTML_TAG() {
                OutputHead(out);
                BODY() {
                    out << "<h1>Machines</h1>";
                    Shop->ForEachMachine([&out] (ui64 machineId, TMyMachine* machine, TFlowCtl* flowctl) {
                        out << "<span class=\"glyphicon glyphicon-home\"></span> ";
                        out << "<a href=\"machine?machineid=" << machineId
                            << "&flowctl=" << flowctl->GetName()
                            << "\">" << (machine->GetName()? machine->GetName(): ToString(machineId)) << "</a><br>";
                    });
                    out << "<h1>Configure</h1>";
                    OutputConfigure(request, out);
                }
            }
        }
    }

    void PostConfigure(const NMonitoring::IMonHttpRequest& request, IOutputStream& out)
    {
        TConfigPb shopCfg;
        bool ok = NProtoBuf::TextFormat::ParseFromString(
            request.GetPostParams().Get("config"),
            &shopCfg
        );
        WWW_CHECK(ok, "config parse failed");
        Shop->Configure(shopCfg);
        WWW_HTML(out,
            out <<
            "<div class=\"jumbotron alert-success\">"
            "<h2>Configured successfully</h2>"
            "</div>"
            "<script type=\"text/javascript\">\n"
            "$(function() {\n"
            "    setTimeout(function() {"
            "        window.location.replace('?');"
            "    }, 1000);"
            "});\n"
            "</script>\n";
        )
    }
private:
    void OutputHead(IOutputStream& out)
    {
        out << "<head>\n";
        out << "<title>" << Title << "</title>\n";
        OutputCommonHeader(out);
        out << "<script language=\"javascript\" type=\"text/javascript\">";
        out << "var cfg_templates = {";
        bool first = true;
        for (auto templ : Templates) {
            TString pbtxt = NResource::Find(templ);
            SubstGlobal(pbtxt, "\\", "\\\\");
            SubstGlobal(pbtxt, "\n", "\\n");
            SubstGlobal(pbtxt, "'", "\\'");
            out << (first? "": ",") << templ << ":'" << pbtxt << "'";
            first = false;
        }
        out << "};"
            << "function use_cfg_template(templ) {"
            << "    $('#textareaCfg').text(cfg_templates[templ]);"
            << "}"
            << "</script>";
        out << "</head>\n";
    }

    void OutputConfigure(const NMonitoring::IMonHttpRequest& request, IOutputStream& out)
    {
        Y_UNUSED(request);
        HTML(out) {
            out << "<form class=\"form-horizontal\" action=\"?mode=configure\" method=\"POST\">";
                DIV_CLASS("form-group") {
                    LABEL_CLASS_FOR("col-sm-1 control-label", "textareaQuery") { out << "Templates"; }
                    DIV_CLASS("col-sm-11") {
                        for (auto name : Templates) {
                            out << "<button type=\"button\" class=\"btn btn-default\" onClick=\"use_cfg_template('" << name << "');\">" << name << "</button> ";
                        }
                    }
                }
                DIV_CLASS("form-group") {
                    LABEL_CLASS_FOR("col-sm-1 control-label", "textareaQuery") { out << "Config"; }
                    DIV_CLASS("col-sm-11") {
                        out << "<textarea class=\"form-control\" id=\"textareaCfg\" name=\"config\" rows=\"20\">"
                            << Shop->GetConfig().DebugString()
                            << "</textarea>";
                    }
                }
                DIV_CLASS("form-group") {
                    DIV_CLASS("col-sm-offset-1 col-sm-11") {
                        out << "<button type=\"submit\" class=\"btn btn-default\">Apply</button>";
                    }
                }
            out << "</form>";
        }
    }
};

int main(int argc, char** argv)
{
    try {
        NLWTrace::StartLwtraceFromEnv();
#ifdef _unix_
        signal(SIGPIPE, SIG_IGN);
#endif

#ifdef _win32_
        WSADATA dummy;
        WSAStartup(MAKEWORD(2,2), &dummy);
#endif

        // Configure
        int monPort = 8080;
        using TMonSrvc = NMonitoring::TMonService2;
        THolder<TMonSrvc> MonSrvc;
        NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
        opts.AddLongOption(0, "mon-port", "port of monitoring service")
                .RequiredArgument("port")
                .StoreResult(&monPort, monPort);
        NLastGetopt::TOptsParseResult res(&opts, argc, argv);

        // Init monservice
        MonSrvc.Reset(new TMonSrvc(monPort));
        MonSrvc->Register(new TMachineMonPage());
        NLwTraceMonPage::RegisterPages(MonSrvc->GetRoot());
        NLwTraceMonPage::ProbeRegistry().AddProbesList(
            LWTRACE_GET_PROBES(SHOP_PROVIDER));
        NLwTraceMonPage::ProbeRegistry().AddProbesList(
            LWTRACE_GET_PROBES(SIMSHOP_PROVIDER));

        // Start monservice
        MonSrvc->Start();

        // Initial shop config
        TConfigPb shopCfg;
        bool ok = NProtoBuf::TextFormat::ParseFromString(
            NResource::Find("one"),
            &shopCfg
        );
        Y_ABORT_UNLESS(ok, "config parse failed");

        // Start shop
        TMyShop shop;
        MonSrvc->Register(new TShopMonPage(&shop));
        shop.Configure(shopCfg);

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
