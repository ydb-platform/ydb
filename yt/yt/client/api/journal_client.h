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

////////////////////////////////////////////////////////////////////////////////

struct IJournalWritesObserver
    : public TRefCounted
{
    virtual void RegisterPayloadWrite(i64 payloadBytes) = 0;
    virtual void RegisterJournalWrite(i64 journalBytes, i64 mediumBytes) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJournalWritesObserver)

////////////////////////////////////////////////////////////////////////////////

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

    NProfiling::TCounter MediumWrittenBytes;
    NProfiling::TCounter IORequestCount;
    NProfiling::TCounter JournalWrittenBytes;

    IJournalWritesObserverPtr JournalWritesObserver;
};

struct TJournalWriterOptions
    : public TTransactionalOptions
    , public TPrerequisiteOptions
{
    TJournalWriterConfigPtr Config;
    bool EnableMultiplexing = true;
    bool EnableChunkPreallocation = true;

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
    virtual ~IJournalClientBase() = default;

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
    virtual ~IJournalClient() = default;

    virtual TFuture<void> TruncateJournal(
        const NYPath::TYPath& path,
        i64 rowCount,
        const TTruncateJournalOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
