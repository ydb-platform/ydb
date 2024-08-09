#include "service_keyvalue.h"
#include "rpc_bsconfig_base.h"

#include <ydb/core/base/path.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/mind/local.h>
#include <ydb/core/protos/local.pb.h>

namespace NKikimr::NGRpcService {

using TEvInitRequest =
    TGrpcRequestOperationCall<Ydb::BSConfig::InitRequest,
        Ydb::BSConfig::InitResponse>;

using namespace NActors;
using namespace Ydb;

bool CopyToConfigRequest(const Ydb::BSConfig::InitRequest &from, NKikimrBlobStorage::TConfigRequest *to) {
    THashMap<TDriveDeviceSet, ui64> hostConfigMap;
    THashSet<TString> uniquePaths;
    THashSet<TString> uniqueHostIds; // fqdn:port
    ui64 hostConfigId = 0;
    auto *defineBox = to->AddCommand()->MutableDefineBox();
    defineBox->SetBoxId(1);

    for (const auto& driveInfo: from.drive_info()) {
        TString hostId;
        if (driveInfo.has_node_id()) {
            hostId = ToString(driveInfo.node_id());
        }
        else {
            auto& host = driveInfo.host();
            hostId = ToString(host.fqdn()) + ":" + ToString(host.port());
        }
        if (uniqueHostIds.find(hostId) != uniqueHostIds.end()) {
            return false;
        }
        uniqueHostIds.insert(hostId);
        TDriveDeviceSet driveSet;

        for (const auto& drive: driveInfo.drive()) {
            if (uniquePaths.find(drive.path()) != uniquePaths.end()) {
                return false;
            }
            uniquePaths.insert(drive.path());
            TDriveDevice device{drive.path(), static_cast<NKikimrBlobStorage::EPDiskType>(drive.type())};
            driveSet.AddDevice(device);
        }

        auto it = hostConfigMap.find(driveSet);
        if (it == hostConfigMap.end()) {
            auto *hostConfig = to->AddCommand()->MutableDefineHostConfig();
            hostConfig->SetHostConfigId(++hostConfigId);

            for (const auto& device: driveSet.GetDevices()) {
                auto *hostConfigDrive = hostConfig->AddDrive();
                hostConfigDrive->SetPath(device.path);
                hostConfigDrive->SetType(static_cast<decltype(hostConfigDrive->GetType())>(device.type));
            }
            it = hostConfigMap.emplace(driveSet, hostConfigId).first;
        }

        auto *host = defineBox->AddHost();
        auto& inputHost = driveInfo.host();
        host->MutableKey()->SetNodeId(driveInfo.node_id());
        host->MutableKey()->SetFqdn(inputHost.fqdn());
        host->MutableKey()->SetIcPort(inputHost.port());
        host->SetHostConfigId(it->second);
    }
    return true;
}

void CopyFromConfigResponse(const NKikimrBlobStorage::TConfigResponse &/*from*/, Ydb::BSConfig::InitResult */*to*/) {
}

class TInitRequest : public TBSConfigRequestGrpc<TInitRequest, TEvInitRequest, Ydb::BSConfig::InitResult> {
public:
    using TBase = TBSConfigRequestGrpc<TInitRequest, TEvInitRequest, Ydb::BSConfig::InitResult>;
    using TBase::TBase;

    bool ValidateRequest(Ydb::StatusIds::StatusCode& /*status*/, NYql::TIssues& /*issues*/) override {
        return true;
    }
    NACLib::EAccessRights GetRequiredAccessRights() const {
        return NACLib::GenericManage;
    }
};

void DoInitRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TInitRequest(p.release()));
}


} // namespace NKikimr::NGRpcService
