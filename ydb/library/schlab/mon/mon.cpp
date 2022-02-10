#include "mon.h"

#include <ydb/library/schlab/schemu/schemu.h>
#include <library/cpp/monlib/service/pages/mon_page.h>
#include <library/cpp/monlib/service/pages/resource_mon_page.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/html/pcdata/pcdata.h>
#include <util/stream/str.h>

namespace NKikimr {

using namespace NMonitoring;

class TSchArmMonPage : public IMonPage {
private:
    TString SchVizUrl;
    TString JsonConfig;
    bool IsRunning = false;
    TMutex EmuMutex;
    THolder<NSchLab::TSchEmu> SchEmu;
public:
    TSchArmMonPage(const TString& path, const TString& schVizUrl)
        : IMonPage(path, "SchArm")
        , SchVizUrl(schVizUrl)
    {}

    virtual void Output(IMonHttpRequest& request) {
        TGuard<TMutex> guard(EmuMutex);

        TStringStream out;
        try {
            if (request.GetMethod() == HTTP_METHOD_GET) {
                if (request.GetParams().Get("mode") == "") {
                    OutputIndex(request, out);
                } else if (request.GetParams().Get("mode") == "getschedule") {
                    GetJson(request, out);
                } else {
                    ythrow yexception() << "Bad request, expected empty mode argument";
                }
            } else if (request.GetMethod() == HTTP_METHOD_POST) {
                if (strcmp(request.GetParams().Get("mode").c_str(), "setconfig") == 0) {
                    InputConfig(request, out);
                } else {
                    ythrow yexception() << "Bad request, expected mode=setconfig";
                }
            } else {
                ythrow yexception() << "Unexpected request method# " << (i32)request.GetMethod();
            }
        } catch (...) {
            out.Clear();
            out << HTTPOKTEXT;
            out << "exception: " << CurrentExceptionMessage();
        }

        request.Output() << out.Str();
    }

private:
    void OutputIndex(const IMonHttpRequest& request, IOutputStream& out) {
        TGuard<TMutex> guard(EmuMutex);
        Y_UNUSED(request);
        out << HTTPOKHTML;
        HTML(out) {
            out << NResource::Find("schlab/scharm.html")
                << "<script type=\"text/javascript\">\n"
                << "  $('#btnArm').click(function(){scharm.sendCfg(function(){window.open('"
                << SchVizUrl << "','_blank');});});\n"
                << "</script>\n";
        }
    }

    void InputConfig(const IMonHttpRequest& request, IOutputStream& out) {
        TGuard<TMutex> guard(EmuMutex);
        JsonConfig = TString(request.GetPostContent());
        IsRunning = false;
        out << HTTPOKHTML;
    }

    void GetJson(const IMonHttpRequest& request, IOutputStream& out) {
        TGuard<TMutex> guard(EmuMutex);
        Y_UNUSED(request);
        out << HTTPOKHTML;
        if (!IsRunning) {
            SchEmu.Reset(new NSchLab::TSchEmu(JsonConfig));
            SchEmu->Emulate(2000.0);
            SchEmu->OutputLogJson(out);
            IsRunning = true;
        } else {
            SchEmu->OutputLogJson(out);
        }
    }
};

class TSchVizMonPage : public IMonPage {
private:
    TString DataUrl;
public:
    TSchVizMonPage(const TString& path, const TString& dataUrl)
        : IMonPage(path, "SchViz")
        , DataUrl(dataUrl)
    {}

    virtual void Output(IMonHttpRequest& request) {
        TStringStream out;
        try {
            if (request.GetParams().Get("mode") == "") {
                OutputIndex(request, out);
            } else if (request.GetParams().Get("mode") == "js") {
                OutputJavaScript(request, out);
            } else {
                ythrow yexception() << "Bad request";
            }
        } catch (...) {
            out.Clear();
            out << HTTPOKTEXT;
            out << "exception: " << CurrentExceptionMessage();
        }

        request.Output() << out.Str();
    }

private:
    void OutputIndex(const IMonHttpRequest& request, IOutputStream& out)
    {
        Y_UNUSED(request);
        out << HTTPOKHTML;
        HTML(out) {
            out << R"__EOF__(<!DOCTYPE html>
<html>
<head>
    <title>SchViz</title>
    <link type="text/css" href="css/schviz.css" rel="stylesheet" />
</head>
<body></body>
</html>

<script type="text/javascript" src="js/d3.v4.min.js"></script>
<script type="text/javascript" src="js/d3-tip-0.8.0-alpha.1.js"></script>
<script type="text/javascript" src="js/schviz.js"></script>
<script type="text/javascript" src="?mode=js"></script>
)__EOF__";
        }
    }

    void OutputJavaScript(const IMonHttpRequest& request, IOutputStream& out)
    {
        Y_UNUSED(request);
        out << HTTPOKJAVASCRIPT;
        out << "d3.json(\"" << DataUrl << "\", function(data) {";
        out << R"__EOF__(
   var schviz = d3.schviz();
   schviz.timeDomain([0, 1000]);
   schviz(data);
});
)__EOF__";
    }
};

#define SCHLAB_STATIC_FILE(file, type) \
    mon->Register(new TResourceMonPage(file, file, TResourceMonPage::type)); \
    /**/

void CreateSchLabCommonPages(TMonService2* mon) {
    // used external components
    SCHLAB_STATIC_FILE("schlab/js/d3.v4.min.js", JAVASCRIPT);
}

void CreateSchArmPages(TMonService2* mon, const TString& path, const TString& schVizUrl) {
    // used external components
    SCHLAB_STATIC_FILE("schlab/css/bootstrap.min.css", CSS);
    SCHLAB_STATIC_FILE("schlab/js/jquery.min.js", JAVASCRIPT);
    SCHLAB_STATIC_FILE("schlab/js/bootstrap.min.js", JAVASCRIPT);

    // scharm static files
    SCHLAB_STATIC_FILE("schlab/css/scharm.css", CSS);
    SCHLAB_STATIC_FILE("schlab/js/scharm.js", JAVASCRIPT);
    SCHLAB_STATIC_FILE("schlab/js/scharm-test0.js", JAVASCRIPT);

    // scharm page itself
    mon->Register(new TSchArmMonPage(path, schVizUrl));
}

void CreateSchVizPages(TMonService2* mon, const TString& path, const TString& dataUrl) {
    // used external components
    SCHLAB_STATIC_FILE("schlab/js/d3-tip-0.8.0-alpha.1.js", JAVASCRIPT);

    // schviz static files
    SCHLAB_STATIC_FILE("schlab/css/schviz.css", CSS);
    SCHLAB_STATIC_FILE("schlab/js/schviz.js", JAVASCRIPT);

    // schviz page itself
    mon->Register(new TSchVizMonPage(path, dataUrl));
}

#undef SCHLAB_STATIC_FILE

}
