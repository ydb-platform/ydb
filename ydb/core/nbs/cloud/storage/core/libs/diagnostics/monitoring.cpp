#include "monitoring.h"

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/page.h>
#include <library/cpp/monlib/service/monservice.h>
#include <library/cpp/monlib/service/pages/index_mon_page.h>
#include <library/cpp/monlib/service/pages/tablesorter/css_mon_page.h>
#include <library/cpp/monlib/service/pages/tablesorter/js_mon_page.h>
#include <library/cpp/monlib/service/pages/version_mon_page.h>

#include <util/generic/hash.h>

namespace NYdb::NBS {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TMonitoringService final
    : public TMonService2
    , public IMonitoringService
{
    using TBase = TMonService2;

private:
    TDynamicCountersPtr Counters = new TDynamicCounters();

public:
    TMonitoringService(ui16 port, const TString& address, ui32 threads)
        : TBase(port, address, threads)
    {}

    void Start() override
    {
        TBase::Register(new TVersionMonPage());
        TBase::Register(new TTablesorterCssMonPage());
        TBase::Register(new TTablesorterJsMonPage());
        TBase::Register(
            new TDynamicCountersPage("counters", "Counters", Counters));

        NLwTraceMonPage::RegisterPages(IndexMonPage.Get());

        TBase::Start();
    }

    void Stop() override
    {
        TBase::Stop();
    }

    IMonPagePtr RegisterIndexPage(
        const TString& path,
        const TString& title) override
    {
        return TBase::RegisterIndexPage(path, title);
    }

    void RegisterMonPage(IMonPagePtr page) override
    {
        TBase::Register(std::move(page));
    }

    IMonPagePtr GetMonPage(const TString& path) override
    {
        return TBase::FindPage(path);
    }

    TDynamicCountersPtr GetCounters() override
    {
        return Counters;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMonitoringServiceStub final: public IMonitoringService
{
private:
    TIntrusivePtr<TIndexMonPage> IndexMonPage = new TIndexMonPage("", "");
    TDynamicCountersPtr Counters = new TDynamicCounters();

public:
    void Start() override
    {
        // nothing to do
    }

    void Stop() override
    {
        // nothing to do
    }

    IMonPagePtr RegisterIndexPage(
        const TString& path,
        const TString& title) override
    {
        return IndexMonPage->RegisterIndexPage(path, title);
    }

    void RegisterMonPage(IMonPagePtr page) override
    {
        IndexMonPage->Register(std::move(page));
    }

    IMonPagePtr GetMonPage(const TString& path) override
    {
        return IndexMonPage->FindPage(path);
    }

    TDynamicCountersPtr GetCounters() override
    {
        return Counters;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IMonitoringServicePtr
CreateMonitoringService(ui16 port, const TString& address, ui32 threads)
{
    return std::make_shared<TMonitoringService>(port, address, threads);
}

IMonitoringServicePtr CreateMonitoringServiceStub()
{
    return std::make_shared<TMonitoringServiceStub>();
}

}   // namespace NYdb::NBS
