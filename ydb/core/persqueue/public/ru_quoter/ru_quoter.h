#pragma once

#include <ydb/core/persqueue/events/events.h>
#include <ydb/core/persqueue/public/pq_rl_helpers.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

// Per-request helper that acquires Request Units (RU) quota and writes a
// yds.serverless.requests.v1 bill, then replies once and dies.
//
// Use it from short-lived request actors that have no persistent connection
// to the server (SQS-over-topic, HTTP APIs, and similar). Long-lived Topic
// read/write sessions keep TRlHelpers subscribed on the connection instead.

namespace NKikimr::NPQ::NRuQuoter {

enum EEv : ui32 {
    EvChargeRequestUnitsResponse = InternalEventSpaceBegin(NPQ::NEvents::EServices::RU_QUOTER),
    EvEnd
};

enum class EStatus {
    SUCCESS,
    THROTTLED,
    UNKNOWN_ERROR
};


struct TRequestUnitsQuoterSettings {
    TString Database;
    ui64 Ru = 0;
    TString Token;
};

struct TEvChargeRequestUnitsResponse : public NActors::TEventLocal<TEvChargeRequestUnitsResponse, EEv::EvChargeRequestUnitsResponse> {

    TEvChargeRequestUnitsResponse() = default;

    TEvChargeRequestUnitsResponse(EStatus status, TString message)
        : Status(status)
        , Message(std::move(message))
    {
    }

    EStatus Status = EStatus::SUCCESS;
    TString Message;
};

struct TMeteringIds {
    TString CloudId;
    TString FolderId;
    TString DatabaseId;

    bool IsComplete() const {
        return !CloudId.empty() && !FolderId.empty() && !DatabaseId.empty();
    }
};

// SQS-over-topic request-unit bills. Native Topics API / Kesus accounting
// keep using ydb.serverless.requests.v1 from the shared rate-limiter resource.
inline constexpr TStringBuf REQUEST_UNITS_SCHEMA = "yds.serverless.requests.v1";

TMaybe<TRlContext> ParseRlContext(
    const THashMap<TString, TString>& attrs,
    const TString& database,
    const TString& token);

TMeteringIds ParseMeteringIds(const THashMap<TString, TString>& attrs);

TString MakeRequestUnitsBill(const TMeteringIds& ids, ui64 ru, TInstant now, const TString& id);

// One-shot actor: navigate database attributes, take RU quota, bill, reply to parent.
NActors::IActor* CreateRequestUnitsQuoter(const NActors::TActorId& parent,
                                          TRequestUnitsQuoterSettings settings);

Ydb::StatusIds::StatusCode Convert(const EStatus status);
TString Description(const EStatus status);

} // namespace NKikimr::NPQ::NRuQuoter
