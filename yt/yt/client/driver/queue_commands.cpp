#include "queue_commands.h"
#include "config.h"

#include <yt/yt/client/api/config.h>

namespace NYT::NDriver {

using namespace NConcurrency;
using namespace NApi;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TRegisterQueueConsumerCommand::TRegisterQueueConsumerCommand()
{
    RegisterParameter("queue_path", QueuePath);
    RegisterParameter("consumer_path", ConsumerPath);
    RegisterParameter("vital", Vital);
    RegisterParameter("partitions", Partitions)
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

TUnregisterQueueConsumerCommand::TUnregisterQueueConsumerCommand()
{
    RegisterParameter("queue_path", QueuePath);
    RegisterParameter("consumer_path", ConsumerPath);
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

TListQueueConsumerRegistrationsCommand::TListQueueConsumerRegistrationsCommand()
{
    RegisterParameter("queue_path", QueuePath)
        .Default();
    RegisterParameter("consumer_path", ConsumerPath)
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

TPullQueueCommand::TPullQueueCommand()
{
    RegisterParameter("queue_path", QueuePath);
    RegisterParameter("offset", Offset);
    RegisterParameter("partition_index", PartitionIndex);

    RegisterParameter("max_row_count", RowBatchReadOptions.MaxRowCount)
        .Optional();
    RegisterParameter("max_data_weight", RowBatchReadOptions.MaxDataWeight)
        .Optional();
    RegisterParameter("data_weight_per_row_hint", RowBatchReadOptions.DataWeightPerRowHint)
        .Optional();
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

TPullConsumerCommand::TPullConsumerCommand()
{
    RegisterParameter("consumer_path", ConsumerPath);
    RegisterParameter("queue_path", QueuePath);
    RegisterParameter("offset", Offset);
    RegisterParameter("partition_index", PartitionIndex);

    RegisterParameter("max_row_count", RowBatchReadOptions.MaxRowCount)
        .Optional();
    RegisterParameter("max_data_weight", RowBatchReadOptions.MaxDataWeight)
        .Optional();
    RegisterParameter("data_weight_per_row_hint", RowBatchReadOptions.DataWeightPerRowHint)
        .Optional();
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

TAdvanceConsumerCommand::TAdvanceConsumerCommand()
{
    RegisterParameter("consumer_path", ConsumerPath);
    RegisterParameter("queue_path", QueuePath);
    RegisterParameter("partition_index", PartitionIndex);
    RegisterParameter("old_offset", OldOffset)
        .Optional();
    RegisterParameter("new_offset", NewOffset);
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
