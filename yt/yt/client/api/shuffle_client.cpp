#include "shuffle_client.h"

#include <yt/yt/client/table_client/schema.h>

namespace NYT::NApi {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TShuffleHandlePtr& shuffleHandle, TStringBuf /*spec*/)
{
    builder->AppendFormat(
        "{TransactionId: %v, CoordinatorAddress: %v, Account: %v, MediumName: %v, "
        "PartitionCount: %v, ReplicationFactor: %v, UsePushBasedShuffle: %v, HasSchema: %v}",
        shuffleHandle->TransactionId,
        shuffleHandle->CoordinatorAddress,
        shuffleHandle->Account,
        shuffleHandle->Medium,
        shuffleHandle->PartitionCount,
        shuffleHandle->ReplicationFactor,
        shuffleHandle->UsePushBasedShuffle,
        static_cast<bool>(shuffleHandle->Schema));
}

////////////////////////////////////////////////////////////////////////////////

void TShuffleHandle::Register(TRegistrar registrar)
{
    registrar.Parameter("transaction_id", &TThis::TransactionId);
    registrar.Parameter("coordinator_address", &TThis::CoordinatorAddress);
    registrar.Parameter("account", &TThis::Account);
    registrar.Parameter("medium", &TThis::Medium);
    registrar.Parameter("partition_count", &TThis::PartitionCount)
        .GreaterThan(0);
    registrar.Parameter("replication_factor", &TThis::ReplicationFactor)
        .GreaterThan(0);
    registrar.Parameter("use_push_based_shuffle", &TThis::UsePushBasedShuffle)
        .Default(false);
    registrar.Parameter("schema", &TThis::Schema)
        .Default();
    registrar.Parameter("push_config", &TThis::PushConfig)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
