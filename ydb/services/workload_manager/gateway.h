#pragma once

#include <ydb/core/base/appdata_fwd.h>

#include <ydb/services/workload_manager/query_classifier.h>

#include <util/generic/string.h>

#include <memory>

namespace NKikimr::NWorkloadManager {

///
/// Client-side interface for the Workload Manager gateway.
/// Instance is created at node initialization and stored in
/// `AppData()->WorkloadManagerGateway`; consumers access it synchronously.
/// Must be called from an actor handler (uses the activation context).
/// Classifiers are applied as soon as they are loaded; until then queries go to the explicit pool or the default pool.
///
/// TODO: classification context for non-query workloads (compaction, backup).
///
class IGateway {
public:
    virtual ~IGateway() = default;

    /// Returns nullptr when:
    ///  - the workload manager has not published its state yet (still initializing),
    ///  - `databaseId` is empty, or
    ///  - resource pools are disabled for the database (feature flag off, or
    ///    serverless with resource pools on serverless off).
    [[nodiscard]] virtual std::shared_ptr<IQueryClassifier> TryCreateQueryClassifier(const TString& databaseId, TClassifyContext context) = 0;
};

}   // namespace NKikimr::NWorkloadManager
