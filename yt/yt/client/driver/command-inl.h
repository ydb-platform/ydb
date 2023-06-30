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
TTransactionalCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, NApi::TTransactionalOptions&>>
>::TTransactionalCommandBase()
{
    this->RegisterParameter("transaction_id", this->Options.TransactionId)
        .Optional();
    this->RegisterParameter("ping", this->Options.Ping)
        .Optional();
    this->RegisterParameter("ping_ancestor_transactions", this->Options.PingAncestors)
        .Optional();
    this->RegisterParameter("suppress_transaction_coordinator_sync", this->Options.SuppressTransactionCoordinatorSync)
        .Optional();
    this->RegisterParameter("suppress_upstream_sync", this->Options.SuppressUpstreamSync)
        .Optional();
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
TMutatingCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, NApi::TMutatingOptions&>>
>::TMutatingCommandBase()
{
    this->RegisterParameter("mutation_id", this->Options.MutationId)
        .Optional();
    this->RegisterParameter("retry", this->Options.Retry)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
TReadOnlyMasterCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, NApi::TMasterReadOptions&>>
>::TReadOnlyMasterCommandBase()
{
    this->RegisterParameter("read_from", this->Options.ReadFrom)
        .Optional();
    this->RegisterParameter("disable_per_user_cache", this->Options.DisablePerUserCache)
        .Optional();
    this->RegisterParameter("expire_after_successful_update_time", this->Options.ExpireAfterSuccessfulUpdateTime)
        .Optional();
    this->RegisterParameter("expire_after_failed_update_time", this->Options.ExpireAfterFailedUpdateTime)
        .Optional();
    this->RegisterParameter("success_staleness_bound", this->Options.SuccessStalenessBound)
        .Optional();
    this->RegisterParameter("cache_sticky_group_size", this->Options.CacheStickyGroupSize)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
TReadOnlyTabletCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, NApi::TTabletReadOptions&>>
>::TReadOnlyTabletCommandBase()
{
    this->RegisterParameter("read_from", this->Options.ReadFrom)
        .Optional();
    this->RegisterParameter("rpc_hedging_delay", this->Options.RpcHedgingDelay)
        .Optional();
    this->RegisterParameter("timestamp", this->Options.Timestamp)
        .Optional();
    this->RegisterParameter("retention_timestamp", this->Options.RetentionTimestamp)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
TSuppressableAccessTrackingCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, NApi::TSuppressableAccessTrackingOptions&>>
>::TSuppressableAccessTrackingCommandBase()
{
    this->RegisterParameter("suppress_access_tracking", this->Options.SuppressAccessTracking)
        .Optional();
    this->RegisterParameter("suppress_modification_tracking", this->Options.SuppressModificationTracking)
        .Optional();
    this->RegisterParameter("suppress_expiration_timeout_renewal", this->Options.SuppressExpirationTimeoutRenewal)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
TPrerequisiteCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, NApi::TPrerequisiteOptions&>>
>::TPrerequisiteCommandBase()
{
    this->RegisterParameter("prerequisite_transaction_ids", this->Options.PrerequisiteTransactionIds)
        .Optional();
    this->RegisterParameter("prerequisite_revisions", this->Options.PrerequisiteRevisions)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
TTimeoutCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, NApi::TTimeoutOptions&>>
>::TTimeoutCommandBase()
{
    this->RegisterParameter("timeout", this->Options.Timeout)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
TTabletReadCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, TTabletTransactionOptions&>>
>::TTabletReadCommandBase()
{
    this->RegisterParameter("transaction_id", this->Options.TransactionId)
        .Optional();
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
TTabletWriteCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, TTabletWriteOptions&>>
>::TTabletWriteCommandBase()
{
    this->RegisterParameter("atomicity", this->Options.Atomicity)
        .Default();
    this->RegisterParameter("durability", this->Options.Durability)
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
TSelectRowsCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, NApi::TSelectRowsOptionsBase&>>
>::TSelectRowsCommandBase()
{
    this->RegisterParameter("range_expansion_limit", this->Options.RangeExpansionLimit)
        .Optional();
    this->RegisterParameter("max_subqueries", this->Options.MaxSubqueries)
        .GreaterThan(0)
        .Optional();
    this->RegisterParameter("udf_registry_path", this->Options.UdfRegistryPath)
        .Default();
    this->RegisterParameter("verbose_logging", this->Options.VerboseLogging)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
