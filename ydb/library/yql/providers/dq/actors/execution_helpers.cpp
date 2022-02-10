#include "execution_helpers.h"

#include <util/generic/ptr.h>
#include <util/string/split.h>
#include <util/system/types.h>



namespace NYql {
    namespace NDqs::NExecutionHelpers {

    TString PrettyPrintWorkerInfo(const Yql::DqsProto::TWorkerInfo& workerInfo, ui64 stageId) {
        TString result;

        {
            TStringOutput output(result);
            output << "Id:" << workerInfo.GetGuid() << ",";
            output << "NodeId:" << workerInfo.GetNodeId() << ",";
            if (!workerInfo.GetClusterName().empty()) {
                output << "ClusterName:" << workerInfo.GetClusterName() << ",";
            }
            if (!workerInfo.GetJobId().empty()) {
                output << "JobId:" << workerInfo.GetJobId() << ",";
            }
            if (!workerInfo.GetOperationId().empty()) {
                output << "OperationId:" << workerInfo.GetOperationId() << ",";
            }
            if (stageId > 0) {
                output << "StageId:" << stageId;
            }
        }

        return result;
    }
    } // namespace NDqs::NExecutionHelpers
} // namespace NYql
