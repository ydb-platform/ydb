#pragma once

///
/// @file yt/cpp/mapreduce/interface/distributed_session.h
///
/// Header containing interface for Distributed API session objects.

#include "fwd.h"

#include <yt/cpp/mapreduce/interface/common.h>

#include <library/cpp/yt/misc/strong_typedef.h>

#include <util/datetime/base.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/// @brief Distributed write table session object.
///
/// Holds server representation of distributed session.
/// Created by NYT::IClient::StartDistributedWriteTableSession.
YT_DEFINE_STRONG_TYPEDEF(TDistributedWriteTableSession, TNode);

/// @brief Cookie object for participant of distributed write table session.
///
/// Holds server representation of distributed write table cookie.
/// Created by NYT::IClient::StartDistributedWriteTableSession.
YT_DEFINE_STRONG_TYPEDEF(TDistributedWriteTableCookie, TNode);

/// @brief Fragment write result produced by NYT::ITableFragmentWriter.
///
/// Holds server representation of distributed table write fragment result.
YT_DEFINE_STRONG_TYPEDEF(TWriteTableFragmentResult, TNode);

////////////////////////////////////////////////////////////////////////////////

/// @brief Distributed write file session object.
///
/// Holds server representation of distributed session.
/// Created by NYT::IClient::StartDistributedWriteFileSession.
YT_DEFINE_STRONG_TYPEDEF(TDistributedWriteFileSession, TNode);

/// @brief Cookie object for participant of distributed write file session.
///
/// Holds server representation of distributed write file cookie.
/// Created by NYT::IClient::StartDistributedWriteFileSession.
YT_DEFINE_STRONG_TYPEDEF(TDistributedWriteFileCookie, TNode);

/// @brief Fragment write result produced by NYT::IFileFragmentWriter.
///
/// Holds server representation of distributed file write fragment result.
YT_DEFINE_STRONG_TYPEDEF(TWriteFileFragmentResult, TNode);

////////////////////////////////////////////////////////////////////////////////

struct TDistributedWriteTableSessionWithCookies
{
    using TSelf = TDistributedWriteTableSessionWithCookies;

    FLUENT_FIELD(TDistributedWriteTableSession, Session);

    FLUENT_FIELD(TVector<TDistributedWriteTableCookie>, Cookies);
};

struct TStartDistributedWriteTableOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TStartDistributedWriteTableOptions;
    /// @endcond

    ///
    /// @brief How long session lives after last ping.
    ///
    /// If server doesn't receive any pings for session transaction for this time
    /// session will be aborted. By default timeout is 15 seconds.
    FLUENT_FIELD_OPTION(TDuration, SessionTimeout);
};

struct TPingDistributedWriteTableOptions
{
    using TSelf = TPingDistributedWriteTableOptions;
};

struct TFinishDistributedWriteTableOptions
{
    using TSelf = TFinishDistributedWriteTableOptions;
};

////////////////////////////////////////////////////////////////////////////////

struct TDistributedWriteFileSessionWithCookies
{
    using TSelf = TDistributedWriteFileSessionWithCookies;

    FLUENT_FIELD(TDistributedWriteFileSession, Session);

    FLUENT_FIELD(TVector<TDistributedWriteFileCookie>, Cookies);
};

struct TStartDistributedWriteFileOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TStartDistributedWriteFileOptions;
    /// @endcond

    ///
    /// @brief How long session lives after last ping.
    ///
    /// If server doesn't receive any pings for session transaction for this time
    /// session will be aborted. By default timeout is 15 seconds.
    FLUENT_FIELD_OPTION(TDuration, SessionTimeout);
};

struct TPingDistributedWriteFileOptions
{
    using TSelf = TPingDistributedWriteFileOptions;
};

struct TFinishDistributedWriteFileOptions
{
    using TSelf = TFinishDistributedWriteFileOptions;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
