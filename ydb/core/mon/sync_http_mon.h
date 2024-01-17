#pragma once

#include <library/cpp/monlib/service/monservice.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/service/pages/resources/css_mon_page.h>
#include <library/cpp/monlib/service/pages/resources/fonts_mon_page.h>
#include <library/cpp/monlib/service/pages/resources/js_mon_page.h>
#include <library/cpp/monlib/service/pages/tablesorter/css_mon_page.h>
#include <library/cpp/monlib/service/pages/tablesorter/js_mon_page.h>

#include <ydb/library/actors/core/mon.h>

#include "mon.h"

namespace NActors {

class TSyncHttpMon : public TMon, public NMonitoring::TMonService2 {
public:
    TSyncHttpMon(TConfig config);
    virtual ~TSyncHttpMon();
    void Start(TActorSystem* actorSystem = {}) override;
    void Stop() override;

    void Register(NMonitoring::IMonPage *page) override;
    NMonitoring::TIndexMonPage* RegisterIndexPage(const TString& path, const TString& title) override;
    NMonitoring::IMonPage* RegisterActorPage(TRegisterActorPageFields fields) override;
    NMonitoring::IMonPage* RegisterCountersPage(const TString& path, const TString& title, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters) override;
    void OutputIndexPage(IOutputStream& out) override;
    NMonitoring::IMonPage* FindPage(const TString& relPath) override;
    void RegisterHandler(const TString& path, const TActorId& handler) override;

protected:
    typedef NMonitoring::TMonService2 TBase;
    TConfig Config;
};

} // NActors
