#include "actor_system_mon.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/library/actors/core/actorsystem.h>

#include <ydb/core/mon/crossref.h>
#include <ydb/core/mon/mon.h>
#include <ydb/library/actors/core/mon.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/monlib/service/pages/resource_mon_page.h>
using namespace NActors;

namespace NKikimr::NActorSystemMon {



class TActorSystemMonPage : public NMonitoring::IMonPage {
public:
    TActorSystemMonPage()
        : IMonPage("actor_system", "Actor System")
    {}

    void Output(NMonitoring::IMonHttpRequest& request) override {
        request.Output() << NMonitoring::HTTPOKHTML;
        request.Output() << NResource::Find("actor_system_mon/static/index.html");
    }
};

void RegisterPages(NMonitoring::TIndexMonPage* index, NActors::TActorSystem*) {
    THolder<TActorSystemMonPage> p = MakeHolder<TActorSystemMonPage>();
    index->Register(p.Release());

    /*/
    WWW_STATIC_FILE("lwtrace/mon/static/common.css", CSS);
    WWW_STATIC_FILE("lwtrace/mon/static/common.js", JAVASCRIPT);
    WWW_STATIC_FILE("lwtrace/mon/static/css/bootstrap.min.css", CSS);
    WWW_STATIC_FILE("lwtrace/mon/static/css/d3-gantt.css", CSS);
    WWW_STATIC_FILE("lwtrace/mon/static/css/jquery.treegrid.css", CSS);
    WWW_STATIC_FILE("lwtrace/mon/static/analytics.css", CSS);
    WWW_STATIC_FILE("lwtrace/mon/static/fonts/glyphicons-halflings-regular.eot", FONT_EOT);
    WWW_STATIC_FILE("lwtrace/mon/static/fonts/glyphicons-halflings-regular.svg", SVG);
    WWW_STATIC_FILE("lwtrace/mon/static/fonts/glyphicons-halflings-regular.ttf", FONT_TTF);
    WWW_STATIC_FILE("lwtrace/mon/static/fonts/glyphicons-halflings-regular.woff2", FONT_WOFF2);
    WWW_STATIC_FILE("lwtrace/mon/static/fonts/glyphicons-halflings-regular.woff", FONT_WOFF);
    WWW_STATIC_FILE("lwtrace/mon/static/img/collapse.png", PNG);
    WWW_STATIC_FILE("lwtrace/mon/static/img/expand.png", PNG);
    WWW_STATIC_FILE("lwtrace/mon/static/img/file.png", PNG);
    WWW_STATIC_FILE("lwtrace/mon/static/img/folder.png", PNG);
    WWW_STATIC_FILE("lwtrace/mon/static/js/bootstrap.min.js", JAVASCRIPT);
    WWW_STATIC_FILE("lwtrace/mon/static/js/d3.v4.min.js", JAVASCRIPT);
    WWW_STATIC_FILE("lwtrace/mon/static/js/d3-gantt.js", JAVASCRIPT);
    WWW_STATIC_FILE("lwtrace/mon/static/js/d3-tip-0.8.0-alpha.1.js", JAVASCRIPT);
    WWW_STATIC_FILE("lwtrace/mon/static/js/filesaver.min.js", JAVASCRIPT);
    WWW_STATIC_FILE("lwtrace/mon/static/js/jquery.flot.extents.js", JAVASCRIPT);
    WWW_STATIC_FILE("lwtrace/mon/static/js/jquery.flot.min.js", JAVASCRIPT);
    WWW_STATIC_FILE("lwtrace/mon/static/js/jquery.flot.navigate.min.js", JAVASCRIPT);
    WWW_STATIC_FILE("lwtrace/mon/static/js/jquery.flot.selection.min.js", JAVASCRIPT);
    WWW_STATIC_FILE("lwtrace/mon/static/js/jquery.min.js", JAVASCRIPT);
    WWW_STATIC_FILE("lwtrace/mon/static/js/jquery.treegrid.bootstrap3.js", JAVASCRIPT);
    WWW_STATIC_FILE("lwtrace/mon/static/js/jquery.treegrid.min.js", JAVASCRIPT);
    WWW_STATIC_FILE("lwtrace/mon/static/js/jquery.url.min.js", JAVASCRIPT);
    // Наши новые файлы
#undef WWW_STATIC_FILE
    /*/
#define WWW_STATIC_FILE(file, type) \
        index->Register(new NMonitoring::TResourceMonPage(file, file, NMonitoring::TResourceMonPage::type));

    // Регистрируем статические файлы
    WWW_STATIC_FILE("actor_system_mon/static/index.html", TEXT);
    WWW_STATIC_FILE("actor_system_mon/static/app.js", JAVASCRIPT);
    WWW_STATIC_FILE("actor_system_mon/static/style.css", CSS);

    WWW_STATIC_FILE("actor_system_mon/static/js/charts.js", JAVASCRIPT);
    WWW_STATIC_FILE("actor_system_mon/static/js/data.js", JAVASCRIPT);
    WWW_STATIC_FILE("actor_system_mon/static/js/renderUtils.js", JAVASCRIPT);
    WWW_STATIC_FILE("actor_system_mon/static/js/ui.js", JAVASCRIPT);
    WWW_STATIC_FILE("actor_system_mon/static/js/utils.js", JAVASCRIPT);
}


} // NKikimr::NActorSystemMon
