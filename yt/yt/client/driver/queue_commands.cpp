#include "queue_commands.h"

#include "config.h"
#include "helpers.h"

#include <yt/yt/client/api/config.h>

#include <yt/yt/client/formats/parser.h>

#include <yt/yt/client/table_client/adapters.h>
#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/table_consumer.h>
#include <yt/yt/client/table_client/table_output.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/library/formats/format.h>

namespace NYT::NDriver {

using namespace NApi;
using namespace NConcurrency;
using namespace NQueueClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

NLogging::TLogger WithCommandTag(
    const NLogging::TLogger& logger,
    const ICommandContextPtr& context)
{
    return logger.WithTag("Command: %v",
        context->Request().CommandName);
}

} // namespace

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

void TAdvanceQueueConsumerCommand::Register(TRegistrar registrar)
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

void TAdvanceQueueConsumerCommand::DoExecute(ICommandContextPtr context)
{
    auto transaction = GetTransaction(context);

    if (ClientSide.value_or(false)) {
        transaction->AdvanceConsumer(ConsumerPath, QueuePath, PartitionIndex, OldOffset, NewOffset);
    } else {
        WaitFor(transaction->AdvanceQueueConsumer(ConsumerPath, QueuePath, PartitionIndex, OldOffset, NewOffset, /*options*/ {}))
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
    registrar.ParameterWithUniversalAccessor<INodePtr>(
        "user_meta",
        [] (TThis* command) -> auto& {
            return command->Options.UserMeta;
        })
        .Optional(/*init*/ false);
}

void TCreateQueueProducerSessionCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();

    auto result = WaitFor(client->CreateQueueProducerSession(
        ProducerPath,
        QueuePath,
        SessionId,
        Options))
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
        SessionId,
        Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TPushQueueProducerCommand::Register(TRegistrar registrar)
{
    registrar.ParameterWithUniversalAccessor<std::optional<TQueueProducerSequenceNumber>>(
        "sequence_number",
        [] (TThis* command) -> auto& {
            return command->Options.SequenceNumber;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<INodePtr>(
        "user_meta",
        [] (TThis* command) -> auto& {
            return command->Options.UserMeta;
        })
        .Optional(/*init*/ false);

    registrar.Parameter("producer_path", &TThis::ProducerPath);
    registrar.Parameter("queue_path", &TThis::QueuePath);
    registrar.Parameter("session_id", &TThis::SessionId);
    registrar.Parameter("epoch", &TThis::Epoch);

}

void TPushQueueProducerCommand::DoExecute(ICommandContextPtr context)
{
    auto tableMountCache = context->GetClient()->GetTableMountCache();

    auto queueTableInfoFuture = tableMountCache->GetTableInfo(QueuePath.GetPath());
    auto producerTableInfoFuture = tableMountCache->GetTableInfo(ProducerPath.GetPath());

    auto queueTableInfo = WaitFor(queueTableInfoFuture)
        .ValueOrThrow("Path %v does not point to a valid queue", QueuePath);
    queueTableInfo->ValidateOrdered();

    auto producerTableInfo = WaitFor(producerTableInfoFuture)
        .ValueOrThrow("Path %v does not point to a valid producer", ProducerPath);
    producerTableInfo->ValidateSorted();

    struct TPushQueueProducerBufferTag
    { };

    auto insertRowsFormatConfig = ConvertTo<TInsertRowsFormatConfigPtr>(context->GetInputFormat().Attributes());
    auto typeConversionConfig = ConvertTo<TTypeConversionConfigPtr>(context->GetInputFormat().Attributes());
    // Parse input data.
    TBuildingValueConsumer valueConsumer(
        queueTableInfo->Schemas[ETableSchemaKind::WriteViaQueueProducer],
        WithCommandTag(Logger, context),
        insertRowsFormatConfig->EnableNullToYsonEntityConversion,
        typeConversionConfig);
    valueConsumer.SetTreatMissingAsNull(true);

    TTableOutput output(CreateParserForFormat(
        context->GetInputFormat(),
        &valueConsumer));

    PipeInputToOutput(context->Request().InputStream, &output);
    auto rows = valueConsumer.GetRows();
    auto rowBuffer = New<TRowBuffer>(TPushQueueProducerBufferTag());
    auto capturedRows = rowBuffer->CaptureRows(rows);
    auto rowRange = MakeSharedRange(
        std::vector<TUnversionedRow>(capturedRows.begin(), capturedRows.end()),
        std::move(rowBuffer));

    auto transaction = GetTransaction(context);

    auto result = WaitFor(transaction->PushQueueProducer(
        ProducerPath,
        QueuePath,
        SessionId,
        Epoch,
        valueConsumer.GetNameTable(),
        std::move(rowRange),
        Options))
        .ValueOrThrow();

    if (ShouldCommitTransaction()) {
        WaitFor(transaction->Commit())
            .ThrowOnError();
    }

    ProduceOutput(context, [&] (NYson::IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("last_sequence_number").Value(result.LastSequenceNumber)
                .Item("skipped_row_count").Value(result.SkippedRowCount)
            .EndMap();
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
