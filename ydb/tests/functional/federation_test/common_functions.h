#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/tools/federation_recipe/proto/logbroker/public/api/admin/config_manager_admin.pb.h>
#include <ydb/public/tools/federation_recipe/proto/logbroker/public/api/grpc/config_manager_admin.grpc.pb.h>
#include <ydb/public/tools/federation_recipe/proto/logbroker/public/api/common/common.pb.h>
#include <ydb/public/tools/federation_recipe/proto/logbroker/public/api/common/ydb_operation.pb.h>
#include <ydb/public/tools/federation_recipe/proto/logbroker/public/api/common/ydb_status_codes.pb.h>


using namespace NYdb;
using namespace NYdb::NTopic;
using AdminStub = NLogBroker::NAdmin::ConfigurationManagerAdminService::Stub;

namespace NFederationTests {

TDriver MakeDriver(const std::string& endpoint, const std::string& database);

void WriteMessages(const std::string& endpoint, const std::string& database,
                   const std::string& topicPath, const std::string& producerId,
                   const std::vector<std::string>& messages);

std::vector<std::string> WriteLoadMessages(const std::string& endpoint, const std::string& database,
                     const std::string& topicPath, const std::string& producerId,
                     size_t count = 10000, size_t smallMessageSize = 2_MB, size_t bigMessageSize = 12_MB);

std::map<uint64_t, std::string> ReadMessages(std::shared_ptr<IReadSession> session, size_t wantCount,
                                          TDuration timeout = TDuration::Seconds(30));

std::map<std::pair<uint64_t, uint64_t>, std::string> ReadAutoscaledTopicMessages(std::shared_ptr<IReadSession> session, size_t wantCount, TDuration timeout = TDuration::Seconds(60));

NLogBroker::Operations::Operation WaitOperation(AdminStub& stub,
                                                 const NLogBroker::Operations::Operation& initial,
                                                 TDuration timeout = TDuration::Seconds(30));

void ExecCmRequest(AdminStub& stub, NLogBroker::NAdmin::ExecuteModifyCommandsRequest& req,
            const TString& comment);

void CmCreateTopic(AdminStub& stub, const std::string& cmPath, const TString& comment, bool autoSplit = false);

void SetClusterWriteEnabled(AdminStub& stub, const std::string& clusterName, bool enabled);


size_t GetActivePartitionCount(const std::string& endpoint, const std::string& database, const std::string& topicPath);

} // namespace
