#include "config.h"
#include "transaction_commands.h"

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/ytree/helpers.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NDriver {

using namespace NYTree;
using namespace NTransactionClient;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TStartTransactionCommand::TStartTransactionCommand()
{
    RegisterParameter("type", Type)
        .Default(NTransactionClient::ETransactionType::Master);
    RegisterParameter("attributes", Attributes)
        .Default(nullptr);
    RegisterParameter("sticky", Options.Sticky)
        .Optional();
    RegisterParameter("timeout", Options.Timeout)
        .Optional();
    RegisterParameter("transaction_id_override", Options.Id)
        .Optional();
    RegisterParameter("start_timestamp_override", Options.StartTimestamp)
        .Optional();
    RegisterParameter("transaction_id", Options.ParentId)
        .Optional();
    RegisterParameter("ping_ancestor_transactions", Options.PingAncestors)
        .Optional();
    RegisterParameter("prerequisite_transaction_ids", Options.PrerequisiteTransactionIds)
        .Optional();
    RegisterParameter("deadline", Options.Deadline)
        .Optional();
    RegisterParameter("atomicity", Options.Atomicity)
        .Optional();
    RegisterParameter("durability", Options.Durability)
        .Optional();
    RegisterParameter("suppress_start_timestamp_generation", Options.SuppressStartTimestampGeneration)
        .Optional();
    RegisterParameter("coordinator_master_cell_tag", Options.CoordinatorMasterCellTag)
        .Optional();
    RegisterParameter("replicate_to_master_cell_tags", Options.ReplicateToMasterCellTags)
        .Optional();
    RegisterParameter("start_cypress_transaction", Options.StartCypressTransaction)
        .Optional();
}

void TStartTransactionCommand::DoExecute(ICommandContextPtr context)
{
    Options.Ping = true;
    Options.AutoAbort = false;

    if (Attributes) {
        Options.Attributes = ConvertToAttributes(Attributes);
    }

    if (Type != ETransactionType::Master) {
        Options.Sticky = true;
    }

    auto transaction = WaitFor(context->GetClient()->StartTransaction(Type, Options))
        .ValueOrThrow();

    if (Options.Sticky) {
        context->GetDriver()->GetStickyTransactionPool()->RegisterTransaction(transaction);
    } else {
        transaction->Detach();
    }

    ProduceSingleOutputValue(context, "transaction_id", transaction->GetId());
}

////////////////////////////////////////////////////////////////////////////////

void TPingTransactionCommand::DoExecute(ICommandContextPtr context)
{
    // Specially for evvers@ :)
    if (!Options.TransactionId) {
        return;
    }

    auto transaction = AttachTransaction(context, true);
    WaitFor(transaction->Ping())
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TCommitTransactionCommand::DoExecute(ICommandContextPtr context)
{
    auto transaction = AttachTransaction(context, true);
    WaitFor(transaction->Commit(Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TAbortTransactionCommand::TAbortTransactionCommand()
{
    RegisterParameter("force", Options.Force)
        .Optional();
}

void TAbortTransactionCommand::DoExecute(ICommandContextPtr context)
{
    auto transaction = AttachTransaction(context, true);
    WaitFor(transaction->Abort(Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TGenerateTimestampCommand::DoExecute(ICommandContextPtr context)
{
    auto timestampProvider = context->GetClient()->GetTimestampProvider();
    auto timestamp = WaitFor(timestampProvider->GenerateTimestamps())
        .ValueOrThrow();

    ProduceSingleOutputValue(context, "timestamp", timestamp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
