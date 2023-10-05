#pragma once

#include "public.h"

#include <util/datetime/base.h>

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

//! Represents that only values with timestamp in range [|RetentionTimestamp|, |Timestamp|]
//! should be read.
//! Note that both endpoints are inclusive.
struct TReadTimestampRange
{
    NTransactionClient::TTimestamp Timestamp = NTransactionClient::NullTimestamp;
    NTransactionClient::TTimestamp RetentionTimestamp = NTransactionClient::NullTimestamp;
};

////////////////////////////////////////////////////////////////////////////////

//! Checks if #id represents a valid transaction accepted by masters:
//! the type of #id must be either
//! #EObjectType::Transaction, #EObjectType::NestedTransaction,
//! #EObjectType::UploadTransaction, #EObjectType::UploadNestedTransaction.
//! #EObjectType::SytemTransaction or #EObjectType::SystemNestedTransaction.
bool IsMasterTransactionId(TTransactionId id);

//! Checks if #type is either #EObjectType::AtomicTabletTransaction or
//! #EObjectType::NonAtomicTabletTransaction.
bool IsTabletTransactionType(NObjectClient::EObjectType type);

//! Checks if #id represents a valid transaction accepted by tablets:
//! the type of #id must be either #EObjectType::AtomicTabletTransaction or
//! #EObjectType::NonAtomicTabletTransaction.
void ValidateTabletTransactionId(TTransactionId id);

//! Checks if #id represents a valid transaction accepted by masters:
//! the type of #id must be one of
//! #EObjectType::Transaction, #EObjectType::NestedTransaction,
//! #EObjectType::UploadTransaction, #EObjectType::UploadNestedTransaction.
//! #EObjectType::SytemTransaction or #EObjectType::SystemNestedTransaction.
void ValidateMasterTransactionId(TTransactionId id);

//! Returns a range of instants containing a given timestamp.
std::pair<TInstant, TInstant> TimestampToInstant(TTimestamp timestamp);

//! Returns a range of timestamps containing a given timestamp.
std::pair<TTimestamp, TTimestamp> InstantToTimestamp(TInstant instant);

//! Returns a range of durations between given timestamps.
std::pair<TDuration, TDuration> TimestampDiffToDuration(TTimestamp loTimestamp, TTimestamp hiTimestamp);

//! Extracts the "unix time" part of the timestamp.
ui64 UnixTimeFromTimestamp(TTimestamp timestamp);

//! Constructs the timestamp from a given unix time (assuming zero counter part).
TTimestamp TimestampFromUnixTime(ui64 time);

//! Embeds the cell tag into the higher counter bits of the timestamp (assuming zero rest counter part).
TTimestamp EmbedCellTagIntoTimestamp(TTimestamp timestamp, NObjectClient::TCellTag cellTag);

//! Checks if the timestamp with embedded cell tag can be advanced by |delta|
//! without overflowing counter part.
bool CanAdvanceTimestampWithEmbeddedCellTag(TTimestamp timestamp, int delta);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
