#pragma once

#include <memory>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct IJobProfiler
{
    virtual ~IJobProfiler() = default;

    //! Starts job profiling if corresponding options are set
    //! in environment.
    virtual void Start() = 0;

    //! Stops profiling and sends profile to job proxy.
    virtual void Stop() = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IJobProfiler> CreateJobProfiler();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
