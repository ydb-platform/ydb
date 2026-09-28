#include "shuffle_client.h"

#include <yt/yt/client/signature/signature.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NApi {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TShuffleHandlePtr& shuffleHandle, TStringBuf /*spec*/)
{
    builder->AppendFormat(
        "{TransactionId: %v, CoordinatorAddress: %v, Account: %v, MediumName: %v, "
        "PartitionCount: %v, ReplicationFactor: %v, UsePushBasedShuffle: %v, HasSchema: %v, "
        "Codec: %v}",
        shuffleHandle->TransactionId,
        shuffleHandle->CoordinatorAddress,
        shuffleHandle->Account,
        shuffleHandle->Medium,
        shuffleHandle->PartitionCount,
        shuffleHandle->ReplicationFactor,
        shuffleHandle->UsePushBasedShuffle,
        static_cast<bool>(shuffleHandle->Schema),
        shuffleHandle->Codec);
}

void ValidateShuffleHandleCodec(
    const TSignedShuffleHandlePtr& signedHandle,
    NCompression::ECodec requestedCodec)
{
    auto handle = ConvertTo<TShuffleHandlePtr>(TYsonStringBuf(signedHandle.Underlying()->Payload()));
    THROW_ERROR_EXCEPTION_IF(
        handle->Codec != requestedCodec,
        "Shuffle handle has codec %Qlv instead of the requested %Qlv; the coordinator or proxy is "
        "too old to support the codec option; the shuffle has already been started and will be "
        "released when the parent transaction ends",
        handle->Codec,
        requestedCodec);
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
    registrar.Parameter("codec", &TThis::Codec)
        .Default(NCompression::ECodec::None);
    registrar.Parameter("config", &TThis::Config)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
