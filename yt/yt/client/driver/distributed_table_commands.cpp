#include "distributed_table_commands.h"
#include "config.h"
#include "helpers.h"

#include <yt/yt/client/api/distributed_table_session.h>
#include <yt/yt/client/api/table_writer.h>

#include <yt/yt/client/formats/config.h>
#include <yt/yt/client/formats/parser.h>

#include <yt/yt/client/signature/signature.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/library/formats/format.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NDriver {

using namespace NApi;
using namespace NConcurrency;
using namespace NFormats;
using namespace NTracing;
using namespace NYTree;
using namespace NYson;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

void TStartDistributedWriteSessionCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
}

// -> DistributedWriteSession
void TStartDistributedWriteSessionCommand::DoExecute(ICommandContextPtr context)
{
    auto transaction = AttachTransaction(context, /*required*/ false);

    auto sessionAndCookies = WaitFor(context->GetClient()->StartDistributedWriteSession(
        Path,
        Options));

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

    WaitFor(context->GetClient()->FinishDistributedWriteSession(
        TDistributedWriteSessionWithResults{
            .Session = std::move(session),
            .Results = std::move(results),
        },
        Options))
            .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TWriteTableFragmentCommand::Execute(ICommandContextPtr context)
{
    TTypedCommand<NApi::TTableFragmentWriterOptions>::Execute(std::move(context));
}

void TWriteTableFragmentCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("cookie", &TThis::Cookie);
}

NApi::ITableWriterPtr TWriteTableFragmentCommand::CreateTableWriter(
    const ICommandContextPtr& context)
{
    PutMethodInfoInTraceContext("write_table_fragment");

    auto tableWriter = WaitFor(context
        ->GetClient()
        ->CreateTableFragmentWriter(
            ConvertTo<TSignedWriteFragmentCookiePtr>(Cookie),
            TTypedCommand<TTableFragmentWriterOptions>::Options))
                .ValueOrThrow();

    TableWriter = tableWriter;
    return tableWriter;
}

// -> Cookie
void TWriteTableFragmentCommand::DoExecute(ICommandContextPtr context)
{
    auto cookie = ConvertTo<TSignedWriteFragmentCookiePtr>(Cookie);

    DoExecuteImpl(context);

    // Sadly, we are plagued by virtual bases :/.
    auto writer = DynamicPointerCast<NApi::ITableFragmentWriter>(TableWriter);
    ProduceOutput(context, [result = writer->GetWriteFragmentResult()] (IYsonConsumer* consumer) {
        Serialize(
            *result.Underlying(),
            consumer);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
