#pragma once

#include "client_common.h"

#include <yt/yt/client/journal_client/public.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TJournalReaderOptions
    : public TTransactionalOptions
    , public TSuppressableAccessTrackingOptions
{
    std::optional<i64> FirstRowIndex;
    std::optional<i64> RowCount;
    TJournalReaderConfigPtr Config;
};

struct TJournalWriterPerformanceCounters
{
    TJournalWriterPerformanceCounters() = default;
    explicit TJournalWriterPerformanceCounters(const NProfiling::TProfiler& profiler);

    NProfiling::TEventTimer GetBasicAttributesTimer;
    NProfiling::TEventTimer BeginUploadTimer;
    NProfiling::TEventTimer GetExtendedAttributesTimer;
    NProfiling::TEventTimer GetUploadParametersTimer;
    NProfiling::TEventTimer EndUploadTimer;
    NProfiling::TEventTimer OpenSessionTimer;
    NProfiling::TEventTimer CreateChunkTimer;
    NProfiling::TEventTimer AllocateWriteTargetsTimer;
    NProfiling::TEventTimer StartNodeSessionTimer;
    NProfiling::TEventTimer ConfirmChunkTimer;
    NProfiling::TEventTimer AttachChunkTimer;
    NProfiling::TEventTimer SealChunkTimer;
    NProfiling::TEventTimer WriteQuorumLag;
    NProfiling::TEventTimer MaxReplicaLag;
};

struct TJournalWriterOptions
    : public TTransactionalOptions
    , public TPrerequisiteOptions
{
    TJournalWriterConfigPtr Config;
    bool EnableMultiplexing = true;
    // TODO(babenko): enable by default
    bool EnableChunkPreallocation = false;

    i64 ReplicaLagLimit = NJournalClient::DefaultReplicaLagLimit;

    TJournalWriterPerformanceCounters Counters;
};

struct TTruncateJournalOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{ };

////////////////////////////////////////////////////////////////////////////////

struct IJournalClientBase
{
    virtual IJournalReaderPtr CreateJournalReader(
        const NYPath::TYPath& path,
        const TJournalReaderOptions& options = {}) = 0;

    virtual IJournalWriterPtr CreateJournalWriter(
        const NYPath::TYPath& path,
        const TJournalWriterOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IJournalClient
{
    virtual TFuture<void> TruncateJournal(
        const NYPath::TYPath& path,
        i64 rowCount,
        const TTruncateJournalOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
