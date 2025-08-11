#include "shuffle_commands.h"

#include <yt/yt/client/driver/config.h>

#include <yt/yt/library/formats/format.h>

#include <yt/yt/client/formats/config.h>

#include <yt/yt/client/signature/signature.h>
#include <yt/yt/client/signature/validator.h>

#include <yt/yt/client/table_client/adapters.h>
#include <yt/yt/client/table_client/table_output.h>
#include <yt/yt/client/table_client/value_consumer.h>

namespace NYT::NDriver {

using namespace NApi;
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
    auto signedShuffleHandle = WaitFor(asyncResult).ValueOrThrow();

    context->ProduceOutputValue(ConvertToYsonString(signedShuffleHandle));
}

//////////////////////////////////////////////////////////////////////////////

void TReadShuffleDataCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("signed_shuffle_handle", &TThis::SignedShuffleHandle);
    registrar.Parameter("partition_index", &TThis::PartitionIndex);
    registrar.Parameter("writer_index_begin", &TThis::WriterIndexBegin)
        .Default()
        .GreaterThanOrEqual(0);
    registrar.Parameter("writer_index_end", &TThis::WriterIndexEnd)
        .Default();
    registrar.Postprocessor([] (TThis* config) {
        if (config->WriterIndexBegin.has_value() != config->WriterIndexEnd.has_value()) {
            THROW_ERROR_EXCEPTION("Request has only one writer range limit")
                << TErrorAttribute("writer_index_begin", config->WriterIndexBegin)
                << TErrorAttribute("writer_index_end", config->WriterIndexEnd);
        }

        if (config->WriterIndexBegin.has_value() && *config->WriterIndexBegin > *config->WriterIndexEnd) {
            THROW_ERROR_EXCEPTION(
                "Lower limit of mappers range %v cannot be greater than upper limit %v",
                *config->WriterIndexBegin,
                *config->WriterIndexEnd);
        }
    });
}

void TReadShuffleDataCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();

    const auto& signatureValidator = context->GetDriver()->GetSignatureValidator();
    auto validationSuccessful = WaitFor(signatureValidator->Validate(SignedShuffleHandle.Underlying()))
        .ValueOrThrow();
    if (!validationSuccessful) {
        auto shuffleHandle = ConvertTo<TShuffleHandlePtr>(TYsonStringBuf(SignedShuffleHandle.Underlying()->Payload()));
        THROW_ERROR_EXCEPTION("Signature validation failed for shuffle handle")
            << TErrorAttribute("shuffle_handle", shuffleHandle);
    }

    std::optional<IShuffleClient::TIndexRange> writerIndexRange;
    if (WriterIndexBegin.has_value()) {
        writerIndexRange = std::pair(*WriterIndexBegin, *WriterIndexEnd);
    }

    auto reader = WaitFor(context->GetClient()->CreateShuffleReader(
        SignedShuffleHandle,
        PartitionIndex,
        writerIndexRange,
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

    TRowBatchReadOptions options{
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
    registrar.Parameter("signed_shuffle_handle", &TThis::SignedShuffleHandle);
    registrar.Parameter("partition_column", &TThis::PartitionColumn);
    registrar.Parameter("max_row_buffer_size", &TThis::MaxRowBufferSize)
        .Default(1_MB);
    registrar.Parameter("writer_index", &TThis::WriterIndex)
        .Default()
        .GreaterThanOrEqual(0);
    registrar.Parameter("overwrite_existing_writer_data", &TThis::OverwriteExistingWriterData)
        .Default(false);

    registrar.Postprocessor([] (TThis* config) {
        if (config->OverwriteExistingWriterData && !config->WriterIndex.has_value()) {
            THROW_ERROR_EXCEPTION("Writer index must be set when overwrite existing writer data option is enabled");
        }
    });
}

void TWriteShuffleDataCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();

    const auto& signatureValidator = context->GetDriver()->GetSignatureValidator();
    auto validationSuccessful = WaitFor(signatureValidator->Validate(SignedShuffleHandle.Underlying()))
        .ValueOrThrow();
    if (!validationSuccessful) {
        auto shuffleHandle = ConvertTo<TShuffleHandlePtr>(TYsonStringBuf(SignedShuffleHandle.Underlying()->Payload()));
        THROW_ERROR_EXCEPTION("Signature validation failed for shuffle handle")
            << TErrorAttribute("shuffle_handle", shuffleHandle);
    }

    Options.OverwriteExistingWriterData = OverwriteExistingWriterData;

    auto writer = WaitFor(context->GetClient()->CreateShuffleWriter(
        SignedShuffleHandle,
        PartitionColumn,
        WriterIndex,
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
