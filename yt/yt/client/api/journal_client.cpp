#include "journal_client.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

TJournalWriterPerformanceCounters::TJournalWriterPerformanceCounters(const NProfiling::TProfiler& profiler)
{
#define XX(name) \
    name ## Timer = profiler.Timer("/" + CamelCaseToUnderscoreCase(#name) + "_time");

    XX(GetBasicAttributes)
    XX(BeginUpload)
    XX(GetExtendedAttributes)
    XX(GetUploadParameters)
    XX(EndUpload)
    XX(OpenSession)
    XX(CreateChunk)
    XX(AllocateWriteTargets)
    XX(StartNodeSession)
    XX(ConfirmChunk)
    XX(AttachChunk)
    XX(SealChunk)

#undef XX

    WriteQuorumLag = profiler.Timer("/write_quorum_lag");
    MaxReplicaLag = profiler.Timer("/max_replica_lag");

    MediumWrittenBytes = profiler.Counter("/medium_written_bytes");
    JournalWrittenBytes = profiler.Counter("/journal_written_bytes");
    IORequestCount = profiler.Counter("/io_request_count");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
