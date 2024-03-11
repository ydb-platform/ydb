#ifndef COMMAND_INL_H
#error "Direct inclusion of this file is not allowed, include command.h"
// For the sake of sane code completion.
#include "command.h"
#endif

#include "private.h"
#include "driver.h"

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/serialize.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void ProduceSingleOutputValue(
    ICommandContextPtr context,
    TStringBuf name,
    const T& value)
{
    ProduceSingleOutput(context, name, [&] (NYson::IYsonConsumer* consumer) {
        NYTree::BuildYsonFluently(consumer)
            .Value(value);
    });
}

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
void TTransactionalCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, NApi::TTransactionalOptions&>>
>::Register(TRegistrar registrar)
{
    registrar.template ParameterWithUniversalAccessor<NObjectClient::TTransactionId>(
        "transaction_id",
        [] (TThis* command) -> auto& {
            return command->Options.TransactionId;
        })
        .Optional(/*init*/ false);

    registrar.template ParameterWithUniversalAccessor<bool>(
        "ping",
        [] (TThis* command) -> auto& {
            return command->Options.Ping;
        })
        .Optional(/*init*/ false);

    registrar.template ParameterWithUniversalAccessor<bool>(
        "ping_ancestor_transactions",
        [] (TThis* command) -> auto& {
            return command->Options.PingAncestors;
        })
        .Optional(/*init*/ false);

    registrar.template ParameterWithUniversalAccessor<bool>(
        "suppress_transaction_coordinator_sync",
        [] (TThis* command) -> auto& {
            return command->Options.SuppressTransactionCoordinatorSync;
        })
        .Optional(/*init*/ false);

    registrar.template ParameterWithUniversalAccessor<bool>(
        "suppress_upstream_sync",
        [] (TThis* command) ->bool& {
            return command->Options.SuppressUpstreamSync;
        })
        .Optional(/*init*/ false);
}

template <class TOptions>
NApi::ITransactionPtr TTransactionalCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, NApi::TTransactionalOptions&>>
>::AttachTransaction(
    ICommandContextPtr context,
    bool required)
{
    auto transactionId = this->Options.TransactionId;
    if (!transactionId) {
        if (required) {
            THROW_ERROR_EXCEPTION("Transaction is required");
        }
        return nullptr;
    }

    const auto& transactionPool = context->GetDriver()->GetStickyTransactionPool();

    if (!NTransactionClient::IsMasterTransactionId(transactionId)) {
        return transactionPool->GetTransactionAndRenewLeaseOrThrow(transactionId);
    }

    auto transaction = transactionPool->FindTransactionAndRenewLease(transactionId);
    if (!transaction) {
        NApi::TTransactionAttachOptions options;
        options.Ping = this->Options.Ping;
        options.PingAncestors = this->Options.PingAncestors;
        transaction = context->GetClient()->AttachTransaction(transactionId, options);
    }

    return transaction;
}

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
void TMutatingCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, NApi::TMutatingOptions&>>
>::Register(TRegistrar registrar)
{
    registrar.template ParameterWithUniversalAccessor<NRpc::TMutationId>(
        "mutation_id",
        [] (TThis* command) -> auto& {
            return command->Options.MutationId;
        })
        .Optional(/*init*/ false);

    registrar.template ParameterWithUniversalAccessor<bool>(
        "retry",
        [] (TThis* command) -> auto& {
            return command->Options.Retry;
        })
        .Optional(/*init*/ false);
}

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
void TReadOnlyMasterCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, NApi::TMasterReadOptions&>>
>::Register(TRegistrar registrar)
{
    registrar.template ParameterWithUniversalAccessor<NApi::EMasterChannelKind>(
        "read_from",
        [] (TThis* command) -> auto& {
            return command->Options.ReadFrom;
        })
        .Optional(/*init*/ false);

    registrar.template ParameterWithUniversalAccessor<bool>(
        "disable_per_user_cache",
        [] (TThis* command) -> auto& {
            return command->Options.DisablePerUserCache;
        })
        .Optional(/*init*/ false);

    registrar.template ParameterWithUniversalAccessor<TDuration>(
        "expire_after_successful_update_time",
        [] (TThis* command) -> auto& {
            return command->Options.ExpireAfterSuccessfulUpdateTime;
        })
        .Optional(/*init*/ false);

    registrar.template ParameterWithUniversalAccessor<TDuration>(
        "expire_after_failed_update_time",
        [] (TThis* command) -> auto& {
            return command->Options.ExpireAfterFailedUpdateTime;
        })
        .Optional(/*init*/ false);

    registrar.template ParameterWithUniversalAccessor<TDuration>(
        "success_staleness_bound",
        [] (TThis* command) -> auto& {
            return command->Options.SuccessStalenessBound;
        })
        .Optional(/*init*/ false);

    registrar.template ParameterWithUniversalAccessor<std::optional<int>>(
        "cache_sticky_group_size",
        [] (TThis* command) -> auto& {
            return command->Options.CacheStickyGroupSize;
        })
        .Optional(/*init*/ false);
}

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
void TReadOnlyTabletCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, NApi::TTabletReadOptions&>>
>::Register(TRegistrar registrar)
{
    registrar.template ParameterWithUniversalAccessor<NHydra::EPeerKind>(
        "read_from",
        [] (TThis* command) -> auto& {
            return command->Options.ReadFrom;
        })
        .Optional(/*init*/ false);

    registrar.template ParameterWithUniversalAccessor<std::optional<TDuration>>(
        "rpc_hedging_delay",
        [] (TThis* command) -> auto& {
            return command->Options.RpcHedgingDelay;
        })
        .Optional(/*init*/ false);

    registrar.template ParameterWithUniversalAccessor<NTransactionClient::TTimestamp>(
        "timestamp",
        [] (TThis* command) -> auto& {
            return command->Options.Timestamp;
        })
        .Optional(/*init*/ false);

    registrar.template ParameterWithUniversalAccessor<NTransactionClient::TTimestamp>(
        "retention_timestamp",
        [] (TThis* command) -> auto& {
            return command->Options.RetentionTimestamp;
        })
        .Optional(/*init*/ false);
}

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
void TSuppressableAccessTrackingCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, NApi::TSuppressableAccessTrackingOptions&>>
>::Register(TRegistrar registrar)
{
    registrar.template ParameterWithUniversalAccessor<bool>(
        "suppress_access_tracking",
        [] (TThis* command) -> auto& {
            return command->Options.SuppressAccessTracking;
        })
        .Optional(/*init*/ false);

    registrar.template ParameterWithUniversalAccessor<bool>(
        "suppress_modification_tracking",
        [] (TThis* command) -> auto& {
            return command->Options.SuppressModificationTracking;
        })
        .Optional(/*init*/ false);

    registrar.template ParameterWithUniversalAccessor<bool>(
        "suppress_expiration_timeout_renewal",
        [] (TThis* command) -> auto& {
            return command->Options.SuppressExpirationTimeoutRenewal;
        })
        .Optional(/*init*/ false);
}

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
void TPrerequisiteCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, NApi::TPrerequisiteOptions&>>
>::Register(TRegistrar registrar)
{
    registrar.template ParameterWithUniversalAccessor<std::vector<NTransactionClient::TTransactionId>>(
        "prerequisite_transaction_ids",
        [] (TThis* command) -> auto& {
            return command->Options.PrerequisiteTransactionIds;
        })
        .Optional(/*init*/ false);

    registrar.template ParameterWithUniversalAccessor<std::vector<NApi::TPrerequisiteRevisionConfigPtr>>(
        "prerequisite_revisions",
        [] (TThis* command) -> auto& {
            return command->Options.PrerequisiteRevisions;
        })
        .Optional(/*init*/ false);
}

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
void TTimeoutCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, NApi::TTimeoutOptions&>>
>::Register(TRegistrar registrar)
{
    registrar.template ParameterWithUniversalAccessor<std::optional<TDuration>>(
        "timeout",
        [] (TThis* command) -> auto& {
            return command->Options.Timeout;
        })
        .Optional(/*init*/ false);
}

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
void TTabletReadCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, TTabletTransactionOptions&>>
>::Register(TRegistrar registrar)
{
    registrar.template ParameterWithUniversalAccessor<NTransactionClient::TTransactionId>(
        "transaction_id",
        [] (TThis* command) -> auto& {
            return command->Options.TransactionId;
        })
        .Optional(/*init*/ false);
}

template <class TOptions>
NApi::IClientBasePtr TTabletReadCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, TTabletTransactionOptions&>>
>::GetClientBase(ICommandContextPtr context)
{
    if (auto transactionId = this->Options.TransactionId) {
        const auto& transactionPool = context->GetDriver()->GetStickyTransactionPool();
        return transactionPool->GetTransactionAndRenewLeaseOrThrow(transactionId);
    } else {
        return context->GetClient();
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
void TTabletWriteCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, TTabletWriteOptions&>>
>::Register(TRegistrar registrar)
{
    registrar.template ParameterWithUniversalAccessor<NTransactionClient::EAtomicity>(
        "atomicity",
        [] (TThis* command) -> auto& {
            return command->Options.Atomicity;
        })
        .Default();

    registrar.template ParameterWithUniversalAccessor<NTransactionClient::EDurability>(
        "durability",
        [] (TThis* command) -> auto& {
            return command->Options.Durability;
        })
        .Default();
}

template <class TOptions>
NApi::ITransactionPtr TTabletWriteCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, TTabletWriteOptions&>>
>::GetTransaction(ICommandContextPtr context)
{
    if (auto transactionId = this->Options.TransactionId) {
        const auto& transactionPool = context->GetDriver()->GetStickyTransactionPool();
        return transactionPool->GetTransactionAndRenewLeaseOrThrow(transactionId);
    } else {
        NApi::TTransactionStartOptions options;
        options.Atomicity = this->Options.Atomicity;
        options.Durability = this->Options.Durability;
        const auto& client = context->GetClient();
        return NConcurrency::WaitFor(client->StartTransaction(NTransactionClient::ETransactionType::Tablet, options))
            .ValueOrThrow();
    }
}

template <class TOptions>
bool TTabletWriteCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, TTabletWriteOptions&>>
>::ShouldCommitTransaction()
{
    return !this->Options.TransactionId;
}

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
void TSelectRowsCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, NApi::TSelectRowsOptionsBase&>>
>::Register(TRegistrar registrar)
{
    registrar.template ParameterWithUniversalAccessor<ui64>(
        "range_expansion_limit",
        [] (TThis* command) -> auto& {
            return command->Options.RangeExpansionLimit;
        })
        .Optional(/*init*/ false);

    registrar.template ParameterWithUniversalAccessor<int>(
        "max_subqueries",
        [] (TThis* command) -> auto& {
            return command->Options.MaxSubqueries;
        })
        .GreaterThan(0)
        .Optional(/*init*/ false);

    registrar.template ParameterWithUniversalAccessor<std::optional<TString>>(
        "udf_registry_path",
        [] (TThis* command) -> auto& {
            return command->Options.UdfRegistryPath;
        })
        .Default();

    registrar.template ParameterWithUniversalAccessor<bool>(
        "verbose_logging",
        [] (TThis* command) -> auto& {
            return command->Options.VerboseLogging;
        })
        .Optional(/*init*/ false);

    registrar.template ParameterWithUniversalAccessor<bool>(
        "new_range_inference",
        [] (TThis* command) -> auto& {
            return command->Options.NewRangeInference;
        })
        .Optional(/*init*/ false);

    registrar.template ParameterWithUniversalAccessor<int>(
        "syntax_version",
        [] (TThis* command) -> auto& {
            return command->Options.SyntaxVersion;
        })
        .Optional(/*init*/ false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
