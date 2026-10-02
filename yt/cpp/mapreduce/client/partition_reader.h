#include <yt/cpp/mapreduce/common/fwd.h>

#include <yt/cpp/mapreduce/interface/io.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

TRawTableReaderPtr CreateTablePartitionReader(
    const IRawClientPtr& rawClient,
    const IRequestRetryPolicyPtr& retryPolicy,
    const TString& cookie,
    const TFormat& format,
    const TTablePartitionReaderOptions& options);

IFileReaderPtr CreateFilePartitionReader(
    const IRawClientPtr& rawClient,
    const IRequestRetryPolicyPtr& retryPolicy,
    const TString& cookie,
    const TFilePartitionReaderOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
