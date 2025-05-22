#include "shuffle_commands.h"

#include <yt/yt/client/driver/config.h>

#include <yt/yt/library/formats/format.h>

#include <yt/yt/client/formats/config.h>

#include <yt/yt/client/table_client/adapters.h>
#include <yt/yt/client/table_client/table_output.h>
#include <yt/yt/client/table_client/value_consumer.h>

namespace NYT::NDriver {

using namespace NConcurrency;
using namespace NFormats;
using namespace NTableClient;
using namespace NYson;

//////////////////////////////////////////////////////////////////////////////

void TStartShuffleCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("account", &TThis::Account);
    registrar.Parameter("partition_count", &TThis::PartitionCount);
    registrar.Parameter("parent_transaction_id", &TThis::ParentTransactionId);
    registrar.ParameterWithUniversalAccessor<std::optional<std::string>>(
        "medium",
        [] (TThis* command) -> auto& {
            return command->Options.Medium;
        })
        .Default();
    registrar.ParameterWithUniversalAccessor<std::optional<int>>(
        "replication_factor",
        [] (TThis* command) -> auto& {
            return command->Options.ReplicationFactor;
        })
        .Default();
}

void TStartShuffleCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    auto asyncResult = client->StartShuffle(Account, PartitionCount, ParentTransactionId, Options);
    auto shuffleHandle = WaitFor(asyncResult).ValueOrThrow();

    context->ProduceOutputValue(ConvertToYsonString(shuffleHandle));
}

//////////////////////////////////////////////////////////////////////////////

void TReadShuffleDataCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("shuffle_handle", &TThis::ShuffleHandle);
    registrar.Parameter("partition_index", &TThis::PartitionIndex);
}

void TReadShuffleDataCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();

    auto reader = WaitFor(context->GetClient()->CreateShuffleReader(
        ShuffleHandle,
        PartitionIndex,
        Options))
        .ValueOrThrow();

    auto format = context->GetOutputFormat();

    auto writer = CreateStaticTableWriterForFormat(
        format,
        reader->GetNameTable(),
        /*tableSchemas*/ {New<TTableSchema>()},
        /*columns*/ {std::nullopt},
        context->Request().OutputStream,
        /*enableContextSaving*/ false,
        New<TControlAttributesConfig>(),
        /*keyColumnCount*/ 0);

    NTableClient::TRowBatchReadOptions options{
        .MaxRowsPerRead = context->GetConfig()->ReadBufferRowCount,
        .Columnar = (format.GetType() == EFormatType::Arrow),
    };

    PipeReaderToWriterByBatches(
        reader,
        writer,
        options);
}

//////////////////////////////////////////////////////////////////////////////

void TWriteShuffleDataCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("shuffle_handle", &TThis::ShuffleHandle);
    registrar.Parameter("partition_column", &TThis::PartitionColumn);
    registrar.Parameter("max_row_buffer_size", &TThis::MaxRowBufferSize)
        .Default(1_MB);
}

void TWriteShuffleDataCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();

    auto writer = WaitFor(context->GetClient()->CreateShuffleWriter(
        ShuffleHandle,
        PartitionColumn,
        Options))
        .ValueOrThrow();

    auto schemalessWriter = CreateSchemalessFromApiWriterAdapter(std::move(writer));

    TWritingValueConsumer valueConsumer(
        schemalessWriter,
        ConvertTo<TTypeConversionConfigPtr>(context->GetInputFormat().Attributes()),
        MaxRowBufferSize);

    TTableOutput output(CreateParserForFormat(
        context->GetInputFormat(),
        &valueConsumer));

    PipeInputToOutput(context->Request().InputStream, &output);

    WaitFor(valueConsumer.Flush())
        .ThrowOnError();

    WaitFor(schemalessWriter->Close())
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

//////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
