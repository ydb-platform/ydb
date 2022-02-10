#pragma once
#include <ydb/public/lib/base/defs.h>
#include <library/cpp/monlib/service/monservice.h>
#include <library/cpp/monlib/service/pages/mon_page.h>
#include <ydb/core/mon/mon.h>

namespace NKikimr {
namespace NHttp {

class TPing : public NMonitoring::IMonPage {
public:
    TPing();
    virtual void Output(NMonitoring::IMonHttpRequest& request) override;
};

TPing* CreatePing();

}
}
