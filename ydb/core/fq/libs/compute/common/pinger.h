#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/core/fq/libs/config/protos/pinger.pb.h>
#include <ydb/library/yql/providers/common/metrics/service_counters.h>
#include <ydb/public/lib/fq/scope.h>

namespace NFq {

NActors::IActor* CreatePingerActor(
    const TString& tenantName,
    const NYdb::NFq::TScope& scope,
    const TString& userId,
    const TString& id,
    const TString& owner,
    const NActors::TActorId parent,
    const NConfig::TPingerConfig& config,
    TInstant deadline,
    const ::NYql::NCommon::TServiceCounters& queryCounters,
    TInstant createdAt,
    bool replyToSender = false);

} /* NFq */
