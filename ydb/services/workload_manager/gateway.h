#pragma once

#include <ydb/services/workload_manager/query_classifier.h>

#include <ydb/library/actors/core/actorid.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/generic/string.h>

#include <memory>


namespace NKikimr::NWorkloadManager {

enum class EReadyState {
    Ready,
    Pending,
    ClassificationDisabled,
    Failed,
};

struct TReadyInfo {
    EReadyState State;
    Ydb::StatusIds::StatusCode FailureStatus = Ydb::StatusIds::SUCCESS;
    TString FailureMessage;
};

///
/// Client-side interface for the Workload Manager gateway.
/// Instance is created at node initialization and stored in
/// `AppData()->WorkloadManagerGateway`; consumers access it synchronously.
///
class IGateway {
public:
    virtual ~IGateway() = default;

    virtual std::shared_ptr<IQueryClassifier> TryCreateQueryClassifier(
        const TString& databaseId, TClassifyContext context) = 0;

    virtual TReadyInfo EnsureReady(const TString& databaseId) = 0;

    virtual void SubscribeOnReady(const TString& databaseId,
                                   NActors::TActorId subscriber, ui64 cookie) = 0;

    virtual void Warmup(const TString& databasePath) = 0;
};

using TGatewayPtr = std::shared_ptr<IGateway>;

}
