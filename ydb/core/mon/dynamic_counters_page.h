#pragma once

#include "mon.h"
#include <library/cpp/monlib/dynamic_counters/page.h>

namespace NActors {

class TFilteredDynamicCountersPage: public NMonitoring::TDynamicCountersPage {
public:
    TFilteredDynamicCountersPage(const TMon::TConfig& config, const TString& path, const TString& title, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters)
        : NMonitoring::TDynamicCountersPage(path, title, counters)
        , Config(config)
    {}

protected:
    THolder<NMonitoring::ICountableConsumer> CreateEncoder(IOutputStream* out, NMonitoring::EFormat format, TStringBuf nameLabel, NMonitoring::TCountableBase::EVisibility visibility) const override;

private:
    class TFilterCountableConsumer;

private:
    const TMon::TConfig& Config;
};

}
