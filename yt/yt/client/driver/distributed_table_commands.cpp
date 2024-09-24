#include "distributed_table_commands.h"
#include "config.h"
#include "helpers.h"

#include <yt/yt/client/api/distributed_table_sessions.h>

// #include <yt/yt/client/api/rowset.h>
// #include <yt/yt/client/api/skynet.h>

// #include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/formats/config.h>
#include <yt/yt/client/formats/parser.h>

// #include <yt/yt/client/table_client/adapters.h>
// #include <yt/yt/client/table_client/blob_reader.h>
// #include <yt/yt/client/table_client/columnar_statistics.h>
// #include <yt/yt/client/table_client/row_buffer.h>
// #include <yt/yt/client/table_client/table_consumer.h>
// #include <yt/yt/client/table_client/table_output.h>
// #include <yt/yt/client/table_client/unversioned_writer.h>
// #include <yt/yt/client/table_client/versioned_writer.h>
// #include <yt/yt/client/table_client/wire_protocol.h>

// #include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/library/formats/format.h>

#include <yt/yt/core/concurrency/scheduler_api.h>
// #include <yt/yt/core/misc/finally.h>

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

// namespace {

// NLogging::TLogger WithCommandTag(
//     const NLogging::TLogger& logger,
//     const ICommandContextPtr& context)
// {
//     return logger.WithTag("Command: %v",
//         context->Request().CommandName);
// }

// } // namespace

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
    auto transaction = AttachTransaction(context, /*required*/ false);

    auto session = ConvertTo<TDistributedWriteSessionPtr>(Session);

    WaitFor(context->GetClient()->FinishDistributedWriteSession(
        std::move(session),
        Options))
            .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TParticipantWriteTableCommand::Execute(ICommandContextPtr context)
{
    TTypedCommand<NApi::TParticipantTableWriterOptions>::Execute(std::move(context));
}

void TParticipantWriteTableCommand::Register(TRegistrar /*registrar*/)
{ }

TFuture<NApi::ITableWriterPtr> TParticipantWriteTableCommand::CreateTableWriter(
    const ICommandContextPtr& context) const
{
    PutMethodInfoInTraceContext("participant_write_table");

    return context
        ->GetClient()
        ->CreateParticipantTableWriter(
            StaticPointerCast<TDistributedWriteCookie>(ResultingCookie),
            TTypedCommand<TParticipantTableWriterOptions>::Options);
}

// -> Cookie
void TParticipantWriteTableCommand::DoExecute(ICommandContextPtr context)
{
    auto cookie = ConvertTo<TDistributedWriteCookiePtr>(Cookie);
    ResultingCookie = StaticPointerCast<TRefCounted>(std::move(cookie));

    DoExecuteImpl(context);
    ProduceOutput(context, [cookie = std::move(ResultingCookie)] (IYsonConsumer* consumer) {
        Serialize(StaticPointerCast<TDistributedWriteCookie>(cookie), consumer);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
