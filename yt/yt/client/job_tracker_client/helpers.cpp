#include "helpers.h"

namespace NYT::NJobTrackerClient {

////////////////////////////////////////////////////////////////////////////////

bool IsJobFinished(EJobState state)
{
    return state == EJobState::Completed ||
        state == EJobState::Failed ||
        state == EJobState::Aborted ||
        state == EJobState::Lost;
}

bool IsJobInProgress(EJobState state)
{
    return state == EJobState::Waiting ||
        state  == EJobState::Running ||
        state == EJobState::Aborting;
}

bool IsMasterJobType(EJobType jobType)
{
    return FirstMasterJobType <= jobType && jobType <= LastMasterJobType;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobTrackerClient
