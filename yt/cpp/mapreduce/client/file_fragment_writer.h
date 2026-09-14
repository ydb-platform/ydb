#pragma once

#include <yt/cpp/mapreduce/common/fwd.h>

#include <yt/cpp/mapreduce/interface/io.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

IFileFragmentWriterPtr CreateFileFragmentWriter(
    const IRawClientPtr& rawClient,
    const IRequestRetryPolicyPtr& retryPolicy,
    const TDistributedWriteFileCookie& cookie,
    const TFileFragmentWriterOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
