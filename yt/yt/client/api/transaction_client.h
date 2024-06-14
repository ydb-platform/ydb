#pragma once

#include "client_common.h"

#include <yt/yt/core/ytree/public.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TTransactionStartOptions
    : public TMutatingOptions
{
    std::optional<TDuration> Timeout;

    //! If not null then the transaction must use this externally provided id.
    //! Only applicable to tablet transactions.
    NTransactionClient::TTransactionId Id;
    //! Cell tag used for id generation. Used for Sequoia transactions.
    std::optional<NObjectClient::TCellTag> CellTag;

    NTransactionClient::TTransactionId ParentId;
    std::vector<NTransactionClient::TTransactionId> PrerequisiteTransactionIds;

    std::optional<TInstant> Deadline;

    bool AutoAbort = true;
    bool Sticky = false;

    std::optional<TDuration> PingPeriod;
    bool Ping = true;
    bool PingAncestors = true;

    NYTree::IAttributeDictionaryPtr Attributes;

    NTransactionClient::EAtomicity Atomicity = NTransactionClient::EAtomicity::Full;
    NTransactionClient::EDurability Durability = NTransactionClient::EDurability::Sync;

    //! If not null then the transaction must use this externally provided start timestamp.
    //! Only applicable to tablet transactions.
    NTransactionClient::TTimestamp StartTimestamp = NTransactionClient::NullTimestamp;

    //! For master transactions only; disables generating start timestamp.
    bool SuppressStartTimestampGeneration = false;

    //! Only for master transactions.
    //! Indicates the master cell the transaction will be initially started at and controlled by
    //! (chosen automatically by default).
    NObjectClient::TCellTag CoordinatorMasterCellTag = NObjectClient::InvalidCellTag;

    //! Only for master transactions.
    //! Indicates the cells the transaction will be replicated to at the start. None by default,
    //! but usually the transaction will be able to be replicated at a later time on demand.
    std::optional<NObjectClient::TCellTagList> ReplicateToMasterCellTags;

    //! Only for master transactions.
    //! By default, all master transactions are Cypress expect for some
    //! system ones (e.g. store flusher transactions).
    bool StartCypressTransaction = true;
};

struct TTransactionAttachOptions
{
    bool AutoAbort = false;
    std::optional<TDuration> PingPeriod;
    bool Ping = true;
    bool PingAncestors = false;

    //! If non-empty, assumes that the transaction is sticky and specifies address of the transaction manager.
    //! Throws if the transaction is not sticky actually.
    //! Only supported by RPC proxy client for now. Ignored by other clients.
    TString StickyAddress;
};

////////////////////////////////////////////////////////////////////////////////

struct ITransactionClientBase
{
    virtual ~ITransactionClientBase() = default;

    virtual TFuture<ITransactionPtr> StartTransaction(
        NTransactionClient::ETransactionType type,
        const TTransactionStartOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ITransactionClient
{
    virtual ~ITransactionClient() = default;

    virtual ITransactionPtr AttachTransaction(
        NTransactionClient::TTransactionId transactionId,
        const TTransactionAttachOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
