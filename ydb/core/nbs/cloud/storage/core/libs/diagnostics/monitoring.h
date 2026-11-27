#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/startable.h>

#include <util/generic/string.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

struct IMonitoringService: public virtual IStartable
{
    virtual NMonitoring::IMonPagePtr RegisterIndexPage(
        const TString& path,
        const TString& title) = 0;

    virtual void RegisterMonPage(NMonitoring::IMonPagePtr page) = 0;

    virtual NMonitoring::IMonPagePtr GetMonPage(const TString& path) = 0;

    virtual NMonitoring::TDynamicCountersPtr GetCounters() = 0;
};

////////////////////////////////////////////////////////////////////////////////

IMonitoringServicePtr CreateMonitoringService(
    ui16 port,
    const TString& address = {},
    ui32 threads = 1);

IMonitoringServicePtr CreateMonitoringServiceStub();

}   // namespace NYdb::NBS
