#include "distributed_table_commands.h"
#include "config.h"
#include "helpers.h"

#include <yt/yt/client/api/distributed_table_session.h>
#include <yt/yt/client/api/table_writer.h>

#include <yt/yt/client/formats/config.h>
#include <yt/yt/client/formats/parser.h>

#include <yt/yt/client/signature/signature.h>
#include <yt/yt/client/signature/validator.h>

#include <yt/yt/client/table_client/adapters.h>
#include <yt/yt/client/table_client/table_output.h>
#include <yt/yt/client/table_client/value_consumer.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/library/formats/format.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NDriver {

using namespace NApi;
using namespace NConcurrency;
using namespace NFormats;
using namespace NTableClient;
using namespace NTracing;
using namespace NYTree;
using namespace NYson;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

void TStartDistributedWriteSessionCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
    registrar.ParameterWithUniversalAccessor<int>(
        "cookie_count",
        [] (TThis* command) -> auto& {
            return command->Options.CookieCount;
        })
        .Default();
}

// -> DistributedWriteSession
void TStartDistributedWriteSessionCommand::DoExecute(ICommandContextPtr context)
{
    auto transaction = AttachTransaction(context, /*required*/ false);

    auto sessionAndCookies = WaitFor(context->GetClient()->StartDistributedWriteSession(Path, Options))
        .ValueOrThrow();

    ProduceOutput(context, [sessionAndCookies = std::move(sessionAndCookies)] (IYsonConsumer* consumer) {
        Serialize(sessionAndCookies, consumer);
    });
}

////////////////////////////////////////////////////////////////////////////////

void TFinishDistributedWriteSessionCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("session", &TThis::Session);
    registrar.Parameter("results", &TThis::Results);
}

// -> Nothing
void TFinishDistributedWriteSessionCommand::DoExecute(ICommandContextPtr context)
{
    auto session = ConvertTo<TSignedDistributedWriteSessionPtr>(Session);
    auto results = ConvertTo<std::vector<NTableClient::TSignedWriteFragmentResultPtr>>(Results);

    auto validator = context->GetDriver()->GetSignatureValidator();
    std::vector<TFuture<bool>> validationFutures;
    validationFutures.reserve(1 + results.size());
    validationFutures.emplace_back(validator->Validate(session.Underlying()));
    for (const auto& result : results) {
        validationFutures.emplace_back(validator->Validate(result.Underlying()));
    }

    auto validationResults = WaitFor(AllSucceeded(std::move(validationFutures)))
        .ValueOrThrow();
    bool allValid = std::all_of(validationResults.begin(), validationResults.end(), [] (bool value) {
        return value;
    });
    THROW_ERROR_EXCEPTION_UNLESS(
        allValid,
        "Signature validation failed for distributed write session finish");

    TDistributedWriteSessionWithResults sessionWithResults(std::move(session), std::move(results));

    WaitFor(context->GetClient()->FinishDistributedWriteSession(sessionWithResults, Options))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TWriteTableFragmentCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("cookie", &TThis::Cookie);
    registrar.Parameter("max_row_buffer_size", &TThis::MaxRowBufferSize)
        .Default(1_MB);
}

ITableFragmentWriterPtr TWriteTableFragmentCommand::CreateTableWriter(
    const ICommandContextPtr& context)
{
    PutMethodInfoInTraceContext("write_table_fragment");

    auto signedCookie = ConvertTo<TSignedWriteFragmentCookiePtr>(Cookie);
    auto validationSuccessful = WaitFor(context->GetDriver()->GetSignatureValidator()->Validate(signedCookie.Underlying()))
        .ValueOrThrow();

    if (!validationSuccessful) {
        auto concreteCookie = ConvertTo<TWriteFragmentCookie>(TYsonStringBuf(signedCookie.Underlying()->Payload()));

        THROW_ERROR_EXCEPTION(
            "Signature validation failed for write table fragment")
                << TErrorAttribute("session_id", concreteCookie.SessionId)
                << TErrorAttribute("cookie_id", concreteCookie.CookieId);
    }

    return WaitFor(context
        ->GetClient()
        ->CreateTableFragmentWriter(
            signedCookie,
            TTypedCommand<TTableFragmentWriterOptions>::Options))
        .ValueOrThrow();
}

// -> Cookie
void TWriteTableFragmentCommand::DoExecute(ICommandContextPtr context)
{
    auto cookie = ConvertTo<TSignedWriteFragmentCookiePtr>(Cookie);

    auto tableWriter = CreateTableWriter(context);

    // NB(pavook): we shouldn't ping transaction here, as this method is executed in parallel
    // and pinging the transaction could cause substantial master load.

    auto schemalessWriter = CreateSchemalessFromApiWriterAdapter(static_cast<ITableWriterPtr>(tableWriter));

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

    auto signedWriteResult = tableWriter->GetWriteFragmentResult();

    ProduceOutput(context, [result = std::move(signedWriteResult)] (IYsonConsumer* consumer) {
        Serialize(
            *result.Underlying(),
            consumer);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
