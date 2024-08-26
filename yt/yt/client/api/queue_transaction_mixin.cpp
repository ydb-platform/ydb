#include "queue_transaction_mixin.h"

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/queue_client/consumer_client.h>

namespace NYT::NApi {

using namespace NQueueClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void TQueueTransactionMixin::AdvanceConsumer(
    const NYPath::TRichYPath& consumerPath,
    const NYPath::TRichYPath& queuePath,
    int partitionIndex,
    std::optional<i64> oldOffset,
    i64 newOffset)
{
    THROW_ERROR_EXCEPTION_IF(newOffset < 0, "Queue consumer offset %v cannot be negative", newOffset);

    auto tableMountCache = GetClient()->GetTableMountCache();
    auto queuePhysicalPath = queuePath;
    auto queueTableInfoOrError = WaitFor(tableMountCache->GetTableInfo(queuePath.GetPath()));
    if (queueTableInfoOrError.IsOK()) {
        queuePhysicalPath = NYPath::TRichYPath(queueTableInfoOrError.Value()->PhysicalPath, queuePath.Attributes());
    }

    // TODO(achulkov2): Support consumers from any cluster.
    auto subConsumerClient = CreateSubConsumerClient(GetClient(), /*queueClient*/ nullptr, consumerPath.GetPath(), queuePhysicalPath);
    return subConsumerClient->Advance(this, partitionIndex, oldOffset, newOffset);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
