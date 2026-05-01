#pragma once

#include "aclib.h"

#include <ydb/library/actors/wilson/wilson_trace.h>

#include <util/generic/ptr.h>

namespace NACLib {

class TUserContext : public TThrRefBase {
public:
    TUserContext(const TString& userSID, const NWilson::TTraceId& userTraceId):
        UserSID(userSID),
        UserTraceId(userTraceId ? userTraceId.Clone() : NWilson::TTraceId())
    {}

    const TString& GetUserSID() const {
        return UserSID;
    }

    const NWilson::TTraceId& GetUserTraceId() const {
        return UserTraceId;
    }

    template <typename TEvent>
    void SerializeToEvent(TEvent& event) const {
        event.SetUserSID(GetUserSID());
    }

protected:
    TString UserSID;
    NWilson::TTraceId UserTraceId;
};

class TUserContextBuilder {
public:
    TString UserSID{BUILTIN_ACL_NO_USER_SID};
    NWilson::TTraceId UserTraceId;

    TUserContextBuilder() {}

    TUserContextBuilder& WithUserSID(const TString& userSID) {
        UserSID = userSID;
        return *this;
    }

    TUserContextBuilder& WithUserTraceId(const NWilson::TTraceId& userTraceId) {
        UserTraceId = userTraceId ? userTraceId.Clone() : NWilson::TTraceId();
        return *this;
    }

    template <typename TEvent>
    TUserContextBuilder& DeserializeFromEvent(TEvent& event, const NWilson::TTraceId& traceId) {
        return WithUserSID(event.Record.GetUserSID())
            .WithUserTraceId(traceId);
    }

    template <typename TEventHandle>
    TUserContextBuilder& DeserializeFromEventHandle(TEventHandle& eventHandle) {
        return DeserializeFromEvent(*eventHandle.Get(), eventHandle.TraceId);
    }

    TIntrusivePtr<NACLib::TUserContext> Build() {
        return MakeIntrusive<TUserContext>(UserSID, UserTraceId);
    }
};

}
