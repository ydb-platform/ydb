#include "shuffle_client.h"

namespace NYT::NApi {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TShuffleHandlePtr& shuffleHandle, TStringBuf /*spec*/)
{
    builder->AppendFormat(
        "{TransactionId: %v, CoordinatorAddress: %v, Account: %v, MediumName: %v, PartitionCount: %v, ReplicationFactor: %v}",
        shuffleHandle->TransactionId,
        shuffleHandle->CoordinatorAddress,
        shuffleHandle->Account,
        shuffleHandle->Medium,
        shuffleHandle->PartitionCount,
        shuffleHandle->ReplicationFactor);
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
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
