#include "queue_commands.h"
#include "config.h"

#include <yt/yt/client/api/config.h>

#include <yt/yt/library/formats/format.h>

namespace NYT::NDriver {

using namespace NConcurrency;
using namespace NApi;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TRegisterQueueConsumerCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("queue_path", &TThis::QueuePath);
    registrar.Parameter("consumer_path", &TThis::ConsumerPath);
    registrar.Parameter("vital", &TThis::Vital);
    registrar.Parameter("partitions", &TThis::Partitions)
        .Default();
}

void TRegisterQueueConsumerCommand::DoExecute(ICommandContextPtr context)
{
    Options.Partitions = Partitions;

    auto client = context->GetClient();
    auto asyncResult = client->RegisterQueueConsumer(
        QueuePath,
        ConsumerPath,
        Vital,
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TUnregisterQueueConsumerCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("queue_path", &TThis::QueuePath);
    registrar.Parameter("consumer_path", &TThis::ConsumerPath);
}

void TUnregisterQueueConsumerCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    auto asyncResult = client->UnregisterQueueConsumer(
        QueuePath,
        ConsumerPath,
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TListQueueConsumerRegistrationsCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("queue_path", &TThis::QueuePath)
        .Default();
    registrar.Parameter("consumer_path", &TThis::ConsumerPath)
        .Default();
}

void TListQueueConsumerRegistrationsCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    auto asyncResult = client->ListQueueConsumerRegistrations(
        QueuePath,
        ConsumerPath,
        Options);
    auto registrations = WaitFor(asyncResult)
        .ValueOrThrow();

    ProduceOutput(context, [&] (NYson::IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .DoListFor(registrations, [=] (TFluentList fluent, const TListQueueConsumerRegistrationsResult& registration) {
                fluent
                    .Item().BeginMap()
                        .Item("queue_path").Value(registration.QueuePath)
                        .Item("consumer_path").Value(registration.ConsumerPath)
                        .Item("vital").Value(registration.Vital)
                        .Item("partitions").Value(registration.Partitions)
                    .EndMap();
            });
        });
}

////////////////////////////////////////////////////////////////////////////////

void TPullQueueCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("queue_path", &TThis::QueuePath);
    registrar.Parameter("offset", &TThis::Offset)
        .Optional();
    registrar.Parameter("partition_index", &TThis::PartitionIndex);

    registrar.ParameterWithUniversalAccessor<i64>(
        "max_row_count",
        [] (TThis* command) -> auto& {
            return command->RowBatchReadOptions.MaxRowCount;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<i64>(
        "max_data_weight",
        [] (TThis* command) -> auto& {
            return command->RowBatchReadOptions.MaxDataWeight;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<i64>>(
        "data_weight_per_row_hint",
        [] (TThis* command) -> auto& {
            return command->RowBatchReadOptions.DataWeightPerRowHint;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<EReplicaConsistency>(
        "replica_consistency",
        [] (TThis* command) -> auto& {
            return command->Options.ReplicaConsistency;
        })
        .Optional(/*init*/ false);
}

void TPullQueueCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();

    auto result = WaitFor(client->PullQueue(
        QueuePath,
        Offset,
        PartitionIndex,
        RowBatchReadOptions,
        Options))
        .ValueOrThrow();

    auto format = context->GetOutputFormat();
    auto output = context->Request().OutputStream;
    auto writer = CreateSchemafulWriterForFormat(format, result->GetSchema(), output);

    Y_UNUSED(writer->Write(result->GetRows()));

    WaitFor(writer->Close())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TPullQueueConsumerCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("consumer_path", &TThis::ConsumerPath);

    registrar.Parameter("queue_path", &TThis::QueuePath);

    registrar.Parameter("offset", &TThis::Offset);

    registrar.Parameter("partition_index", &TThis::PartitionIndex);

    registrar.ParameterWithUniversalAccessor<i64>(
        "max_row_count",
        [] (TThis* command) -> auto& {
            return command->RowBatchReadOptions.MaxRowCount;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<i64>(
        "max_data_weight",
        [] (TThis* command) -> auto& {
            return command->RowBatchReadOptions.MaxDataWeight;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<i64>>(
        "data_weight_per_row_hint",
        [] (TThis* command) -> auto& {
            return command->RowBatchReadOptions.DataWeightPerRowHint;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<EReplicaConsistency>(
        "replica_consistency",
        [] (TThis* command) -> auto& {
            return command->Options.ReplicaConsistency;
        })
        .Optional(/*init*/ false);
}

void TPullQueueConsumerCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();

    auto result = WaitFor(client->PullQueueConsumer(
        ConsumerPath,
        QueuePath,
        Offset,
        PartitionIndex,
        RowBatchReadOptions,
        Options))
        .ValueOrThrow();

    auto format = context->GetOutputFormat();
    auto output = context->Request().OutputStream;
    auto writer = CreateSchemafulWriterForFormat(format, result->GetSchema(), output);

    Y_UNUSED(writer->Write(result->GetRows()));

    WaitFor(writer->Close())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TAdvanceConsumerCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("consumer_path", &TThis::ConsumerPath);
    registrar.Parameter("queue_path", &TThis::QueuePath);
    registrar.Parameter("partition_index", &TThis::PartitionIndex);
    registrar.Parameter("old_offset", &TThis::OldOffset)
        .Optional();
    registrar.Parameter("new_offset", &TThis::NewOffset);
    registrar.Parameter("client_side", &TThis::ClientSide)
        .Optional();
}

void TAdvanceConsumerCommand::DoExecute(ICommandContextPtr context)
{
    auto transaction = GetTransaction(context);

    if (ClientSide.value_or(false)) {
        transaction->AdvanceConsumer(ConsumerPath, QueuePath, PartitionIndex, OldOffset, NewOffset);
    } else {
        WaitFor(transaction->AdvanceConsumer(ConsumerPath, QueuePath, PartitionIndex, OldOffset, NewOffset, /*options*/ {}))
            .ThrowOnError();
    }

    if (ShouldCommitTransaction()) {
        WaitFor(transaction->Commit())
            .ThrowOnError();
    }

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TCreateQueueProducerSessionCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("producer_path", &TThis::ProducerPath);
    registrar.Parameter("queue_path", &TThis::QueuePath);
    registrar.Parameter("session_id", &TThis::SessionId);
    registrar.Parameter("user_meta", &TThis::UserMeta)
        .Optional();
}

void TCreateQueueProducerSessionCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();

    std::optional<NYson::TYsonString> requestUserMeta;
    if (UserMeta) {
        requestUserMeta = NYson::ConvertToYsonString(UserMeta);
    }

    auto result = WaitFor(client->CreateQueueProducerSession(
        ProducerPath,
        QueuePath,
        SessionId,
        requestUserMeta))
        .ValueOrThrow();

    ProduceOutput(context, [&] (NYson::IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("epoch").Value(result.Epoch)
                .Item("sequence_number").Value(result.SequenceNumber)
                .Item("user_meta").Value(result.UserMeta)
            .EndMap();
    });
}

////////////////////////////////////////////////////////////////////////////////

void TRemoveQueueProducerSessionCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("producer_path", &TThis::ProducerPath);
    registrar.Parameter("queue_path", &TThis::QueuePath);
    registrar.Parameter("session_id", &TThis::SessionId);
}

void TRemoveQueueProducerSessionCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();

    WaitFor(client->RemoveQueueProducerSession(
        ProducerPath,
        QueuePath,
        SessionId))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
