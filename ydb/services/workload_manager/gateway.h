#pragma once

#include <ydb/services/workload_manager/query_classifier.h>

#include <memory>


namespace NKikimr::NWorkloadManager {

///
/// Client-side interface for the Workload Manager gateway.
/// Instance is created at node initialization and stored in
/// `AppData()->WorkloadManagerGateway`; consumers access it synchronously.
///
class IGateway {
public:
    virtual ~IGateway() = default;

    ///
    /// Attempt to build a Query Classifier for the given database
    /// and query context. Returns `nullptr` when:
    /// - snapshot not yet published (cache actor did not finish Bootstrap);
    /// - resource pools are disabled for the database;
    /// - no matching pool metadata is loaded.
    /// Caller must handle the nullptr case (typically: skip classification).
    ///
    virtual std::shared_ptr<IQueryClassifier> TryCreateQueryClassifier(
        const TString& databaseId, TClassifyContext context) = 0;
};

using TGatewayPtr = std::shared_ptr<IGateway>;

}
