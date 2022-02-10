#pragma once

#include "counters.h"

#include <library/cpp/monlib/service/pages/mon_page.h>

#include <util/generic/ptr.h>

#include <functional>

// helper class to output json for Golovan.
class TGolovanCountersPage: public NMonitoring::IMonPage {
public:
    using TOutputCallback = std::function<void()>;

    const TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;

    TGolovanCountersPage(const TString& path, TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
                         TOutputCallback outputCallback = nullptr);

    void Output(NMonitoring::IMonHttpRequest& request) override;

private:
    TOutputCallback OutputCallback;
};
