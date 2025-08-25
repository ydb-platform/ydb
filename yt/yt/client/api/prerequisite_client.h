#pragma once

#include "public.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TPrerequisiteAttachOptions
    : public TTimeoutOptions
{
    bool AutoAbort = false;
    std::optional<TDuration> PingPeriod;
    bool Ping = true;
    bool PingAncestors = false;
};

////////////////////////////////////////////////////////////////////////////////

struct IPrerequisiteClient
{
    virtual ~IPrerequisiteClient() = default;

    virtual IPrerequisitePtr AttachPrerequisite(
        NPrerequisiteClient::TPrerequisiteId preqreuisiteId,
        const TPrerequisiteAttachOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
