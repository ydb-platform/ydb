#pragma once

#include <library/cpp/actors/core/mon.h>

namespace NActors {
class IActor;
}

namespace NKikimr {
namespace NViewer {

using namespace NActors;
class IViewer;

struct TRequest {
    NMon::TEvHttpInfo::TPtr Event;
    std::map<TString, TString> PathParams;
};

class TJsonHandlerBase {
public:
    typedef std::shared_ptr<TJsonHandlerBase> TPtr;

public:
    virtual ~TJsonHandlerBase() = default;
    virtual IActor* CreateRequestActor(IViewer* viewer, const TRequest& request) = 0;
    virtual TString GetResponseJsonSchema() = 0;
    virtual TString GetTags() { return TString(); }
    virtual TString GetRequestSummary() { return TString(); }
    virtual TString GetRequestDescription() { return TString(); }
    virtual TString GetRequestParameters() { return TString(); }
};

}
}
