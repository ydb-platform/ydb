#include "helpers.h"

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/library/erasure/codec_params.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NJournalClient {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

void ValidateQuorums(int readQuorum, int writeQuorum)
{
    if (readQuorum < 1) {
        THROW_ERROR_EXCEPTION("\"read_quorum\" cannot be less than 1");
    }
    if (writeQuorum < 1) {
        THROW_ERROR_EXCEPTION("\"write_quorum\" cannot be less than 1");
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void ValidateReplicatedJournalAttributes(
    int replicationFactor,
    int readQuorum,
    int writeQuorum)
{
    ValidateQuorums(readQuorum, writeQuorum);

    if (replicationFactor < MinReplicationFactor || replicationFactor > MaxReplicationFactor) {
        THROW_ERROR_EXCEPTION("Replication factor %v is out of range [%v,%v]",
            replicationFactor,
            MinReplicationFactor,
            MaxReplicationFactor);
    }
    if (readQuorum > replicationFactor) {
        THROW_ERROR_EXCEPTION("\"read_quorum\" cannot be greater than \"replication_factor\"");
    }
    if (writeQuorum > replicationFactor) {
        THROW_ERROR_EXCEPTION("\"write_quorum\" cannot be greater than \"replication_factor\"");
    }
    if (readQuorum + writeQuorum <= replicationFactor) {
        THROW_ERROR_EXCEPTION("Read/write quorums are not safe: read_quorum + write_quorum <= replication_factor");
    }
}

void ValidateErasureJournalAttributes(
    NErasure::ECodec codecId,
    const NErasure::TCodecParams& codecParams,
    int replicationFactor,
    int readQuorum,
    int writeQuorum)
{
    ValidateQuorums(readQuorum, writeQuorum);

    if (!codecParams.Bytewise) {
        THROW_ERROR_EXCEPTION("%Qlv codec is not suitable for erasure journals",
            codecId);
    }
    if (replicationFactor != 1) {
        THROW_ERROR_EXCEPTION("\"replication_factor\" must be 1 for erasure journals");
    }
    if (readQuorum > codecParams.TotalPartCount) {
        THROW_ERROR_EXCEPTION("\"read_quorum\" cannot be greater than total part count");
    }
    if (writeQuorum > codecParams.TotalPartCount) {
        THROW_ERROR_EXCEPTION("\"write_quorum\" cannot be greater than total part count");
    }
    int quorumThreshold = 2 * codecParams.TotalPartCount - codecParams.GuaranteedRepairablePartCount - 1;
    if (readQuorum + writeQuorum <= quorumThreshold) {
        THROW_ERROR_EXCEPTION("Read/write quorums are not safe: read_quorum + write_quorum <= 2 * total_parts - guaranteed_repairable_parts - 1");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
