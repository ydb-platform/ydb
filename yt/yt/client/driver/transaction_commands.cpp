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
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void TStartTransactionCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("type", &TThis::Type)
        .Default(NTransactionClient::ETransactionType::Master);

    registrar.Parameter("attributes", &TThis::Attributes)
        .Default(nullptr);

    registrar.ParameterWithUniversalAccessor<bool>(
        "sticky",
        [] (TThis* command) -> auto& {
            return command->Options.Sticky;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<TDuration>>(
        "timeout",
        [] (TThis* command) -> auto& {
            return command->Options.Timeout;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<TTransactionId>(
        "transaction_id_override",
        [] (TThis* command) -> auto& {
            return command->Options.Id;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<TTimestamp>(
        "start_timestamp_override",
        [] (TThis* command) -> auto& {
            return command->Options.StartTimestamp;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<TTransactionId>(
        "transaction_id",
        [] (TThis* command) -> auto& {
            return command->Options.ParentId;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "ping_ancestor_transactions",
        [] (TThis* command) -> auto& {
            return command->Options.PingAncestors;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::vector<TTransactionId>>(
        "prerequisite_transaction_ids",
        [] (TThis* command) -> auto& {
            return command->Options.PrerequisiteTransactionIds;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<TInstant>>(
        "deadline",
        [] (TThis* command) -> auto& {
            return command->Options.Deadline;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<EAtomicity>(
        "atomicity",
        [] (TThis* command) -> auto& {
            return command->Options.Atomicity;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<EDurability>(
        "durability",
        [] (TThis* command) -> auto& {
            return command->Options.Durability;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "suppress_start_timestamp_generation",
        [] (TThis* command) -> auto& {
            return command->Options.SuppressStartTimestampGeneration;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<TCellTag>(
        "coordinator_master_cell_tag",
        [] (TThis* command) -> auto& {
            return command->Options.CoordinatorMasterCellTag;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<TCellTagList>>(
        "replicate_to_master_cell_tags",
        [] (TThis* command) -> auto& {
            return command->Options.ReplicateToMasterCellTags;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "start_cypress_transaction",
        [] (TThis* command) -> auto& {
            return command->Options.StartCypressTransaction;
        })
        .Optional(/*init*/ false);
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
    auto transactionCommitResult = WaitFor(transaction->Commit(Options))
        .ValueOrThrow();

    if (context->GetConfig()->ApiVersion >= ApiVersion4) {
        ProduceOutput(
            context,
            [&] (IYsonConsumer* consumer) {
                BuildYsonFluently(consumer)
                    .BeginMap()
                        .Item("primary_commit_timestamp").Value(transactionCommitResult.PrimaryCommitTimestamp)
                        .Item("commit_timestamps").DoMapFor(
                            transactionCommitResult.CommitTimestamps.Timestamps,
                            [&] (auto fluent, const auto& pair) {
                                fluent.Item(ToString(pair.first)).Value(pair.second);
                            })
                    .EndMap();
            });
    } else {
        ProduceEmptyOutput(context);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TAbortTransactionCommand::Register(TRegistrar registrar)
{
    registrar.ParameterWithUniversalAccessor<bool>(
        "force",
        [] (TThis* command) -> auto& {
            return command->Options.Force;
        })
        .Optional(/*init*/ false);
}

void TAbortTransactionCommand::DoExecute(ICommandContextPtr context)
{
    auto transaction = AttachTransaction(context, true);
    WaitFor(transaction->Abort(Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TGenerateTimestampCommand::Register(TRegistrar registrar)
{
    registrar.ParameterWithUniversalAccessor<std::optional<TCellTag>>(
        "clock_cluster_tag",
        [] (TThis* command) -> auto& {
            return command->Options.ClockClusterTag;
        })
        .Optional(/*init*/ false);
}

void TGenerateTimestampCommand::DoExecute(ICommandContextPtr context)
{
    auto timestampProvider = context->GetClient()->GetTimestampProvider();
    auto clockClusterTag = Options.ClockClusterTag.value_or(InvalidCellTag);
    auto timestamp = WaitFor(timestampProvider->GenerateTimestamps(1, clockClusterTag))
        .ValueOrThrow();

    ProduceSingleOutputValue(context, "timestamp", timestamp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
