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

    ProduceOutput(context, [&](NYson::IYsonConsumer* consumer) {
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

    writer->Write(result->GetRows());

    WaitFor(writer->Close())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TPullConsumerCommand::Register(TRegistrar registrar)
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

void TPullConsumerCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();

    auto result = WaitFor(client->PullConsumer(
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

    writer->Write(result->GetRows());

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
}

void TAdvanceConsumerCommand::DoExecute(ICommandContextPtr context)
{
    auto transaction = GetTransaction(context);

    transaction->AdvanceConsumer(ConsumerPath, QueuePath, PartitionIndex, OldOffset, NewOffset);

    if (ShouldCommitTransaction()) {
        WaitFor(transaction->Commit())
            .ThrowOnError();
    }

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
