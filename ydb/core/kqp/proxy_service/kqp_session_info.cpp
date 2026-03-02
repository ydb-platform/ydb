#include "kqp_proxy_service_impl.h"

#include <ydb/core/sys_view/common/registry.h>

namespace NKikimr::NKqp {

using VSessions = NKikimr::NSysView::Schema::QuerySessions;

constexpr size_t QUERY_TEXT_LIMIT = 10_KB;

void TKqpSessionInfo::SerializeTo(::NKikimrKqp::TSessionInfo* proto, const TFieldsMap& fieldsMap) const {
    if (fieldsMap.NeedField(VSessions::SessionId::ColumnId)) {  // 1
        proto->SetSessionId(SessionId);
    }

    if (fieldsMap.NeedField(VSessions::State::ColumnId)) {  // 3
        switch(State) {
            case TKqpSessionInfo::ESessionState::IDLE: {
                proto->SetState("IDLE");
                break;
            }
            case TKqpSessionInfo::ESessionState::EXECUTING: {
                proto->SetState("EXECUTING");
                break;
            }
        }
    }

    // last executed query or currently running query.
    if (fieldsMap.NeedField(VSessions::Query::ColumnId)) {  // 4
        if (QueryText.size() > QUERY_TEXT_LIMIT) {
            TString truncatedText = QueryText.substr(0, QUERY_TEXT_LIMIT);
            proto->SetQuery(QueryText);
        } else {
            proto->SetQuery(QueryText);
        }
    }

    if (fieldsMap.NeedField(VSessions::QueryCount::ColumnId)) {  // 5
        proto->SetQueryCount(QueryCount);
    }

    if (fieldsMap.NeedField(VSessions::ClientAddress::ColumnId)) {  // 6
        proto->SetClientAddress(ClientHost);
    }

    if (fieldsMap.NeedField(VSessions::ClientPID::ColumnId)) { // 7
        proto->SetClientPID(ClientPID);
    }

    if (fieldsMap.NeedField(VSessions::ClientUserAgent::ColumnId)) {  // 8
        proto->SetClientUserAgent(UserAgent);
    }

    if (fieldsMap.NeedField(VSessions::ClientSdkBuildInfo::ColumnId)) {  // 9
        proto->SetClientSdkBuildInfo(SdkBuildInfo);
    }

    if (fieldsMap.NeedField(VSessions::ApplicationName::ColumnId)) {  // 10
        proto->SetApplicationName(ClientApplicationName);
    }

    if (fieldsMap.NeedField(VSessions::SessionStartAt::ColumnId)) { // 11
        proto->SetSessionStartAt(SessionStartedAt.MicroSeconds());
    }

    if (fieldsMap.NeedField(VSessions::QueryStartAt::ColumnId)) { // 12
        proto->SetQueryStartAt(QueryStartAt.MicroSeconds());
    }

    if (fieldsMap.NeedField(VSessions::StateChangeAt::ColumnId)) { // 13
        proto->SetStateChangeAt(StateChangeAt.MicroSeconds());
    }

    if (fieldsMap.NeedField(VSessions::UserSID::ColumnId)) {  // 14
        proto->SetUserSID(ClientSID);
    }

    if (fieldsMap.NeedField(VSessions::WMPoolId::ColumnId)) { // 17
        if (WMState) {
            proto->SetWMPoolId(WMState->GetPoolId());
        }
    }

    if (fieldsMap.NeedField(VSessions::WMState::ColumnId)) { // 18
        if (WMState) {
            using EWMState = IWmSessionUpdater::EWMState;
            switch(WMState->GetState()) {
                case EWMState::NONE: {
                    proto->SetWMState("NONE");
                    break;
                }
                case EWMState::PENDING: {
                    proto->SetWMState("PENDING");
                    break;
                }
                case EWMState::DELAYED: {
                    proto->SetWMState("DELAYED");
                    break;
                }
                case EWMState::EXITED: {
                    proto->SetWMState("EXITED");
                    break;
                }
            }
        }
    }

    if (fieldsMap.NeedField(VSessions::WMEnterTime::ColumnId)) { // 19
        if (WMState) {
            proto->SetWMEnterTime(WMState->GetEnterTime().MicroSeconds());
        }
    }

    if (fieldsMap.NeedField(VSessions::WMExitTime::ColumnId)) { // 20
        if (WMState) {
            proto->SetWMExitTime(WMState->GetExitTime().MicroSeconds());
        }
    }
}

}  // namespace NKikimr::NKqp
