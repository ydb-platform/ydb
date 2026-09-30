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

    /// Returns nullptr when:
    ///  - the workload manager has not published its state yet (still initializing), or
    ///  - resource pools are disabled for the database (feature flag off, DB info
    ///    not yet fetched, fetch failed, or serverless with flag off).
    [[nodiscard]] virtual std::shared_ptr<IQueryClassifier> TryCreateQueryClassifier(
        const TString& databaseId, TClassifyContext context) = 0;

    /// Check whether the workload manager is ready to classify queries for this database.
    [[nodiscard]] virtual TReadyInfo EnsureReady(const TString& databaseId) = 0;

    /// Subscribe to event when workload manager is ready to classify queries for
    /// this database. Delivered as TEvWorkloadManagerReady{cookie, status} to `subscriber`.
    virtual void SubscribeOnReady(const TString& databaseId,
                                   NActors::TActorId subscriber, ui64 cookie) = 0;

    /// Ask the workload manager to prefetch DB info in advance. Meant to be called
    /// early (e.g. at query entry) so the prefetch overlaps with the caller's own work.
    virtual void Warmup(const TString& databasePath) = 0;
};

using TGatewayPtr = std::shared_ptr<IGateway>;

}
