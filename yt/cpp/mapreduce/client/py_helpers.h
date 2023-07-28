#include <yt/cpp/mapreduce/interface/client_method_options.h>
#include <yt/cpp/mapreduce/interface/operation.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

using IStructuredJobPtr = TIntrusiveConstPtr<IStructuredJob>;

IStructuredJobPtr ConstructJob(const TString& jobName, const TString& state);

TString GetJobStateString(const IStructuredJob& job);

TString GetIOInfo(
    const IStructuredJob& job,
    const TCreateClientOptions& options,
    const TString& cluster,
    const TString& transactionId,
    const TString& inputPaths,
    const TString& outputPaths,
    const TString& neededColumns);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
