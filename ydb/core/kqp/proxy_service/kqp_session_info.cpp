#include "kqp_proxy_service_impl.h"

#include <ydb/core/sys_view/common/registry.h>

#include <util/generic/algorithm.h>

namespace NKikimr::NKqp {

using VSessions = NKikimr::NSysView::Schema::QuerySessions;

constexpr size_t QUERY_TEXT_LIMIT = 10_KB;

void TKqpSessionInfo::SerializeTo(::NKikimrKqp::TSessionInfo* proto, const TFieldsMap& fieldsMap) const {
    // Snapshot the WM state once so State/StateChangeAt/QueryStartAt stay
    // internally consistent even if a WM callback races with serialization.
    using EWmState = NWorkloadManager::ISessionUpdater::EState;
    const auto wmState = WmState->GetState();
    const bool isInWmQueue = EqualToOneOf(wmState, EWmState::PENDING, EWmState::DELAYED);
    const bool wmExited = (wmState == EWmState::EXITED);

    if (fieldsMap.NeedField(VSessions::SessionId::ColumnId)) {  // 1
        proto->SetSessionId(SessionId);
    }

    if (fieldsMap.NeedField(VSessions::State::ColumnId)) {  // 3
        if (isInWmQueue) {
            proto->SetState("QUEUED");
        } else {
            switch(State) {
                case TKqpSessionInfo::ESessionState::IDLE:
                    proto->SetState("IDLE"); 
                    break;
                case TKqpSessionInfo::ESessionState::EXECUTING:
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
        // QueryStartAt is left unset (NULL) while the session is IDLE or queued.
        if (State == ESessionState::EXECUTING && !isInWmQueue) {
            proto->SetQueryStartAt(wmExited
                ? WmState->GetExitTime().MicroSeconds()
                : QueryStartAt.MicroSeconds());
        }
    }

    if (fieldsMap.NeedField(VSessions::StateChangeAt::ColumnId)) { // 13
        if (isInWmQueue) {
            proto->SetStateChangeAt(WmState->GetEnterTime().MicroSeconds());
        } else if (wmExited) {
            proto->SetStateChangeAt(WmState->GetExitTime().MicroSeconds());
        } else {
            proto->SetStateChangeAt(StateChangeAt.MicroSeconds());
        }
    }

    if (fieldsMap.NeedField(VSessions::UserSID::ColumnId)) {  // 14
        proto->SetUserSID(ClientSID);
    }

    if (fieldsMap.NeedField(VSessions::WmPoolId::ColumnId)) { // 17
        auto poolId = WmState->GetPoolId();
        if (!poolId.empty()) {
            proto->SetWmPoolId(std::move(poolId));
        }
    }

    // Columns 18/19/20 (WmState/WmEnterTime/WmExitTime) are deprecated and
    // always NULL; proto fields are kept reserved for a future removal.

    if (fieldsMap.NeedField(VSessions::TraceId::ColumnId)) { // 21
        if (State == TKqpSessionInfo::EXECUTING && !TraceId.empty()) {
            proto->SetTraceId(TraceId);
        }
    }

    if (fieldsMap.NeedField(VSessions::WmClassifiedBy::ColumnId)) { // 22
        auto classifiedBy = WmState->GetClassifiedBy();
        if (!classifiedBy.empty()) {
            proto->SetWmClassifiedBy(std::move(classifiedBy));
        }
    }
}

}  // namespace NKikimr::NKqp
