#include "chaos_commands.h"

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/tablet_client/config.h>

namespace NYT::NDriver {

using namespace NApi;
using namespace NChaosClient;
using namespace NConcurrency;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TUpdateChaosTableReplicaProgressCommand::TUpdateChaosTableReplicaProgressCommand()
{
    RegisterParameter("replica_id", ReplicaId);
    RegisterParameter("progress", Options.Progress);
}

void TUpdateChaosTableReplicaProgressCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();

    auto future = client->UpdateChaosTableReplicaProgress(ReplicaId, Options);
    WaitFor(future)
        .ThrowOnError();
    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TAlterReplicationCardCommand::TAlterReplicationCardCommand()
{
    RegisterParameter("replication_card_id", ReplicationCardId);
    RegisterParameter("replicated_table_options", Options.ReplicatedTableOptions)
        .Optional();
    RegisterParameter("enable_replicated_table_tracker", Options.EnableReplicatedTableTracker)
        .Optional();
    RegisterParameter("replication_card_collocation_id", Options.ReplicationCardCollocationId)
        .Optional();
}

void TAlterReplicationCardCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->AlterReplicationCard(
        ReplicationCardId,
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
