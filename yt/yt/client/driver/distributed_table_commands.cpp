#include "distributed_table_commands.h"
#include "config.h"
#include "helpers.h"

#include <yt/yt/client/api/distributed_table_session.h>

#include <yt/yt/client/formats/config.h>
#include <yt/yt/client/formats/parser.h>

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

    auto session = WaitFor(context->GetClient()->StartDistributedWriteSession(
        Path,
        Options));

    ProduceOutput(context, [session = std::move(session)] (IYsonConsumer* consumer) {
        Serialize(session, consumer);
    });
}

////////////////////////////////////////////////////////////////////////////////

void TFinishDistributedWriteSessionCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("session", &TThis::Session);
}

// -> Nothing
void TFinishDistributedWriteSessionCommand::DoExecute(ICommandContextPtr context)
{
    auto session = ConvertTo<TDistributedWriteSessionPtr>(Session);

    WaitFor(context->GetClient()->FinishDistributedWriteSession(
        std::move(session),
        Options))
            .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TWriteTableFragmentCommand::Execute(ICommandContextPtr context)
{
    TTypedCommand<NApi::TFragmentTableWriterOptions>::Execute(std::move(context));
}

void TWriteTableFragmentCommand::Register(TRegistrar /*registrar*/)
{ }

TFuture<NApi::ITableWriterPtr> TWriteTableFragmentCommand::CreateTableWriter(
    const ICommandContextPtr& context) const
{
    PutMethodInfoInTraceContext("participant_write_table");

    return context
        ->GetClient()
        ->CreateFragmentTableWriter(
            StaticPointerCast<TFragmentWriteCookie>(ResultingCookie),
            TTypedCommand<TFragmentTableWriterOptions>::Options);
}

// -> Cookie
void TWriteTableFragmentCommand::DoExecute(ICommandContextPtr context)
{
    auto cookie = ConvertTo<TFragmentWriteCookiePtr>(Cookie);
    ResultingCookie = StaticPointerCast<TRefCounted>(std::move(cookie));

    DoExecuteImpl(context);
    ProduceOutput(context, [cookie = std::move(ResultingCookie)] (IYsonConsumer* consumer) {
        Serialize(StaticPointerCast<TFragmentWriteCookie>(cookie), consumer);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
