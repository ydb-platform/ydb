#include "chaos_commands.h"

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/tablet_client/config.h>

namespace NYT::NDriver {

using namespace NApi;
using namespace NChaosClient;
using namespace NConcurrency;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void TUpdateChaosTableReplicaProgressCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("replica_id", &TThis::ReplicaId);

    registrar.ParameterWithUniversalAccessor<TReplicationProgress>(
        "progress",
        [] (TThis* command) -> auto& {
            return command->Options.Progress;
        });

    registrar.ParameterWithUniversalAccessor<bool>(
        "force",
        [] (TThis* command) -> auto& {
            return command->Options.Force;
        });
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

void TAlterReplicationCardCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("replication_card_id", &TThis::ReplicationCardId);

    registrar.ParameterWithUniversalAccessor<NTabletClient::TReplicatedTableOptionsPtr>(
        "replicated_table_options",
        [] (TThis* command) -> auto& {
            return command->Options.ReplicatedTableOptions;
        })
        .Optional(/*init*/ false);
    registrar.ParameterWithUniversalAccessor<std::optional<bool>>(
        "enable_replicated_table_tracker",
        [] (TThis* command) -> auto& {
            return command->Options.EnableReplicatedTableTracker;
        })
        .Optional(/*init*/ false);
    registrar.ParameterWithUniversalAccessor<std::optional<TReplicationCardCollocationId>>(
        "replication_card_collocation_id",
        [] (TThis* command) -> auto& {
            return command->Options.ReplicationCardCollocationId;
        })
        .Optional(/*init*/ false);
    registrar.ParameterWithUniversalAccessor<NTabletClient::TReplicationCollocationOptionsPtr>(
        "collocation_options",
        [] (TThis* command) -> auto& {
            return command->Options.CollocationOptions;
        })
        .Optional(/*init*/ false);
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
