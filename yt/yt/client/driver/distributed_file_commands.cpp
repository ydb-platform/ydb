#include "distributed_file_commands.h"
#include "helpers.h"

#include <yt/yt/client/api/distributed_file_session.h>
#include <yt/yt/client/api/file_writer.h>

#include <yt/yt/client/signature/signature.h>
#include <yt/yt/client/signature/validator.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NDriver {

using namespace NApi;
using namespace NConcurrency;
using namespace NFileClient;
using namespace NTracing;
using namespace NYTree;
using namespace NYson;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

void TStartDistributedWriteFileSessionCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
    registrar.ParameterWithUniversalAccessor<int>(
        "cookie_count",
        [] (TThis* command) -> auto& {
            return command->Options.CookieCount;
        })
        .Default();
}

void TStartDistributedWriteFileSessionCommand::DoExecute(ICommandContextPtr context)
{
    auto transaction = AttachTransaction(context, /*required*/ false);

    auto sessionAndCookies = WaitFor(context->GetClient()->StartDistributedWriteFileSession(Path, Options))
        .ValueOrThrow();

    ProduceOutput(context, [sessionAndCookies = std::move(sessionAndCookies)] (IYsonConsumer* consumer) {
        Serialize(sessionAndCookies, consumer);
    });
}

////////////////////////////////////////////////////////////////////////////////

void TPingDistributedWriteFileSessionCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("session", &TThis::Session);
}

void TPingDistributedWriteFileSessionCommand::DoExecute(ICommandContextPtr context)
{
    auto session = ConvertTo<TSignedDistributedWriteFileSessionPtr>(Session);
    WaitFor(context->GetClient()->PingDistributedWriteFileSession(session, Options))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TFinishDistributedWriteFileSessionCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("session", &TThis::Session);
    registrar.Parameter("results", &TThis::Results);
}

void TFinishDistributedWriteFileSessionCommand::DoExecute(ICommandContextPtr context)
{
    auto session = ConvertTo<TSignedDistributedWriteFileSessionPtr>(Session);
    auto results = ConvertTo<std::vector<NFileClient::TSignedWriteFileFragmentResultPtr>>(Results);

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

    TDistributedWriteFileSessionWithResults sessionWithResults(std::move(session), std::move(results));

    WaitFor(context->GetClient()->FinishDistributedWriteFileSession(sessionWithResults, Options))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TWriteFileFragmentCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("cookie", &TThis::Cookie);
}

IFileFragmentWriterPtr TWriteFileFragmentCommand::CreateFileWriter(
    const ICommandContextPtr& context)
{
    PutMethodInfoInTraceContext("write_file_fragment");

    auto signedCookie = ConvertTo<TSignedWriteFileFragmentCookiePtr>(Cookie);
    auto validationSuccessful = WaitFor(context->GetDriver()->GetSignatureValidator()->Validate(signedCookie.Underlying()))
        .ValueOrThrow();

    if (!validationSuccessful) {
        auto concreteCookie = ConvertTo<TWriteFileFragmentCookie>(TYsonStringBuf(signedCookie.Underlying()->Payload()));

        THROW_ERROR_EXCEPTION(
            "Signature validation failed for write file fragment")
                << TErrorAttribute("session_id", concreteCookie.SessionId)
                << TErrorAttribute("cookie_id", concreteCookie.CookieId);
    }

    return context
        ->GetClient()
        ->CreateFileFragmentWriter(
            signedCookie,
            TTypedCommand<TFileFragmentWriterOptions>::Options);
}

void TWriteFileFragmentCommand::DoExecute(ICommandContextPtr context)
{
    auto cookie = ConvertTo<TSignedWriteFileFragmentCookiePtr>(Cookie);

    PutMethodInfoInTraceContext("write_distributed_file");

    auto fileWriter = CreateFileWriter(context);

    WaitFor(fileWriter->Open())
        .ThrowOnError();

    // NB(pavook): we shouldn't ping transaction here, as this method is executed in parallel
    // and pinging the transaction could cause substantial master load.

    auto input = context->Request().InputStream;

    while (true) {
        auto data = WaitFor(input->Read())
            .ValueOrThrow();

        if (!data) {
            break;
        }

        WaitFor(fileWriter->Write(std::move(data)))
            .ThrowOnError();
    }

    WaitFor(fileWriter->Close())
        .ThrowOnError();

    auto signedWriteResult = fileWriter->GetWriteFragmentResult();

    ProduceOutput(context, [result = std::move(signedWriteResult)] (IYsonConsumer* consumer) {
        Serialize(
            *result.Underlying(),
            consumer);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
