#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/tools/federation_recipe/proto/config_manager_admin.pb.h>
#include <ydb/public/tools/federation_recipe/proto/config_manager_admin_service.grpc.pb.h>
#include <ydb/public/tools/federation_recipe/proto/common.pb.h>
#include <ydb/public/tools/federation_recipe/proto/ydb_operation.pb.h>
#include <ydb/public/tools/federation_recipe/proto/ydb_status_codes.pb.h>


using namespace NYdb;
using namespace NYdb::NTopic;
using AdminStub = NLogBroker::NAdmin::ConfigurationManagerAdminService::Stub;

namespace NFederationTests {

TDriver MakeDriver(const TString& endpoint, const TString& database);

void WriteMessages(const TString& endpoint, const TString& database,
                   const TString& topicPath, const TString& producerId,
                   const std::vector<TString>& messages);

std::vector<TString> WriteLoadMessages(const TString& endpoint, const TString& database,
                     const TString& topicPath, const TString& producerId,
                     size_t count = 10000, size_t smallMessageSize = 2_MB, size_t bigMessageSize = 12_MB);

std::map<uint64_t, TString> ReadMessages(std::shared_ptr<IReadSession> session, size_t wantCount,
                                          TDuration timeout = TDuration::Seconds(30));

std::map<std::pair<uint64_t, uint64_t>, TString> ReadAutoscaledTopicMessages(std::shared_ptr<IReadSession> session, size_t wantCount, TDuration timeout = TDuration::Seconds(60));

NLogBroker::Operations::Operation WaitOperation(AdminStub& stub,
                                                 const NLogBroker::Operations::Operation& initial,
                                                 TDuration timeout = TDuration::Seconds(30));

void ExecCmRequest(AdminStub& stub, NLogBroker::NAdmin::ExecuteModifyCommandsRequest& req,
            const TString& comment);

void CmCreateTopic(AdminStub& stub, const TString& cmPath, const TString& comment, bool autoSplit = false);

void SetClusterWriteEnabled(AdminStub& stub, const TString& clusterName, bool enabled);


size_t GetActivePartitionCount(const TString& endpoint, const TString& database, const TString& topicPath);

} // namespace
