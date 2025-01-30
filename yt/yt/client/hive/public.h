#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/client/transaction_client/public.h>

namespace NYT::NHiveClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TTimestampMap;
class TClusterDirectory;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

using NTransactionClient::TTransactionId;
using NTransactionClient::NullTransactionId;
using NTransactionClient::TTimestamp;
using NTransactionClient::NullTimestamp;

using NHydra::TCellId;
using NHydra::NullCellId;

struct TTimestampMap;

DECLARE_REFCOUNTED_STRUCT(ITransactionParticipant)

YT_DEFINE_ERROR_ENUM(
    ((MailboxNotCreatedYet)    (2200))
    ((ParticipantUnregistered) (2201))
    ((TimeEntryNotFound)       (2202))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
