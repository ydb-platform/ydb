#include "sync_http_mon.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/core/probes.h>
#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <library/cpp/mime/types/mime.h>
#include <library/cpp/monlib/service/pages/version_mon_page.h>
#include <library/cpp/monlib/service/pages/mon_page.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/page.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/string_utils/url/url.h>
#include <util/system/event.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/monitoring_provider.h>
#include <ydb/core/base/ticket_parser.h>

#include "mon_impl.h"

namespace NActors {

    ////////////////////////////////////////////////////////////////////////////////
    // TMON CLASS
    ////////////////////////////////////////////////////////////////////////////////
    TSyncHttpMon::TSyncHttpMon(TSyncHttpMon::TConfig config)
        : TBase(config.Port, config.Address, config.Threads, config.Title)
        , Config(std::move(config))
    {
    }

    TSyncHttpMon::~TSyncHttpMon() {
        Stop();
    }

    void TSyncHttpMon::Start(TActorSystem*) {
        TBase::Register(new TIndexRedirectMonPage(IndexMonPage));
        TBase::Register(new NMonitoring::TVersionMonPage);
        TBase::Register(new NMonitoring::TBootstrapCssMonPage);
        TBase::Register(new NMonitoring::TTablesorterCssMonPage);
        TBase::Register(new NMonitoring::TBootstrapJsMonPage);
        TBase::Register(new NMonitoring::TJQueryJsMonPage);
        TBase::Register(new NMonitoring::TTablesorterJsMonPage);
        TBase::Register(new NMonitoring::TBootstrapFontsEotMonPage);
        TBase::Register(new NMonitoring::TBootstrapFontsSvgMonPage);
        TBase::Register(new NMonitoring::TBootstrapFontsTtfMonPage);
        TBase::Register(new NMonitoring::TBootstrapFontsWoffMonPage);

        NLwTraceMonPage::RegisterPages(IndexMonPage.Get());
        NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(ACTORLIB_PROVIDER));
        NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(MONITORING_PROVIDER));
        TBase::Start();
    }

    void TSyncHttpMon::Stop() {
        if (IndexMonPage) {
            IndexMonPage->ClearPages(); // it's required to avoid loop-reference
            TBase::Stop();
            IndexMonPage.Drop();
        }
    }

    void TSyncHttpMon::Register(NMonitoring::IMonPage* page) {
        TBase::Register(page);
        TBase::SortPages();
    }

    TIndexMonPage* TSyncHttpMon::RegisterIndexPage(const TString& path, const TString& title) {
        auto page = TBase::RegisterIndexPage(path, title);
        TBase::SortPages();
        return page;
    }

    IMonPage* TSyncHttpMon::RegisterActorPage(TMon::TRegisterActorPageFields fields) {
        IMonPage* page = new TActorMonPage(
            fields.RelPath,
            fields.Title,
            Config.Host,
            fields.PreTag,
            fields.ActorSystem,
            fields.ActorId,
            fields.AllowedSIDs ? fields.AllowedSIDs : Config.AllowedSIDs,
            fields.UseAuth ? Config.Authorizer : TRequestAuthorizer(),
            fields.MonServiceName);
        if (fields.Index) {
            fields.Index->Register(page);
            if (fields.SortPages) {
                fields.Index->SortPages();
            }
        } else {
            Register(page);
        }

        return page;
    }

    IMonPage* TSyncHttpMon::RegisterCountersPage(const TString &path, const TString &title, TIntrusivePtr<TDynamicCounters> counters) {
        TDynamicCountersPage* page = new TDynamicCountersPage(path, title, counters);
        page->SetUnknownGroupPolicy(EUnknownGroupPolicy::Ignore);
        Register(page);
        return page;
    }

    void TSyncHttpMon::OutputIndexPage(IOutputStream& out) {
        if (Config.RedirectMainPageTo) {
            // XXX manual http response construction
            out << "HTTP/1.1 302 Found\r\n"
                << "Location: " << Config.RedirectMainPageTo << "\r\n"
                << "Connection: Close\r\n\r\n";
        } else {
            NMonitoring::TMonService2::OutputIndexPage(out);
        }
    }

    IMonPage* TSyncHttpMon::FindPage(const TString& relPath) {
        return TBase::FindPage(relPath);
    }

    void TSyncHttpMon::RegisterHandler(const TString& path, const TActorId& handler) {
        ALOG_ERROR(NActorsServices::HTTP, "Cannot register actor handler " << handler << " in sync mon for " << path);
    }
} // NActors
