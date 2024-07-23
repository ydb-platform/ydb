#include "service_keyvalue.h"
#include "rpc_bsconfig_base.h"

#include <ydb/core/base/path.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/mind/local.h>
#include <ydb/core/protos/local.pb.h>

namespace NKikimr::NGRpcService {

using TEvDefineRequest =
    TGrpcRequestOperationCall<Ydb::BSConfig::DefineRequest,
        Ydb::BSConfig::DefineResponse>;
using TEvFetchRequest =
    TGrpcRequestOperationCall<Ydb::BSConfig::FetchRequest,
        Ydb::BSConfig::FetchResponse>;

using namespace NActors;
using namespace Ydb;

bool CopyToConfigRequest(const Ydb::BSConfig::DefineRequest &from, NKikimrBlobStorage::TConfigRequest *to) {
    THashMap<TDriveDeviceSet, ui64> hostConfigMap;
    THashSet<TString> uniquePaths;
    THashSet<TString> uniqueHostIds; // fqdn:port
    ui64 hostConfigId = 0;
    NKikimrBlobStorage::TConfigRequest::TCommand defineBoxCommand;
    auto *defineBox = defineBoxCommand.MutableDefineBox();
    defineBox->SetBoxId(1);
    defineBox->SetItemConfigGeneration(0);
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
                hostConfigDrive->SetPath(device.GetPath());
                hostConfigDrive->SetType(static_cast<decltype(hostConfigDrive->GetType())>(device.GetType()));
            }
            hostConfig->SetItemConfigGeneration(0);
            it = hostConfigMap.emplace(driveSet, hostConfigId).first;
        }

        auto *host = defineBox->AddHost();
        auto& inputHost = driveInfo.host();
        host->MutableKey()->SetNodeId(driveInfo.node_id());
        host->MutableKey()->SetFqdn(inputHost.fqdn());
        host->MutableKey()->SetIcPort(inputHost.port());
        host->SetHostConfigId(it->second);
    }
    *to->AddCommand() = std::move(defineBoxCommand);
    return true;
}

void CopyFromConfigResponse(const NKikimrBlobStorage::TConfigResponse &/*from*/, Ydb::BSConfig::DefineResult */*to*/) {
}

bool CopyToConfigRequest(const Ydb::BSConfig::FetchRequest &from, NKikimrBlobStorage::TConfigRequest *to) {
    to->AddCommand()->MutableReadHostConfig();
    to->AddCommand()->MutableReadBox();
    return true;
}

void CopyFromConfigResponse(const NKikimrBlobStorage::TConfigResponse &from, Ydb::BSConfig::FetchResult *to) {
    auto hostConfigStatus = from.GetStatus()[0];
    auto boxStatus = from.GetStatus()[1];
    THashMap<ui64, TDriveDeviceSet> hostConfigMap;
    for (const auto& hostConfig: hostConfigStatus.GetHostConfig()) {
        TDriveDeviceSet driveDeviceSet;
        for (const auto& drive: hostConfig.GetDrive()) {
            driveDeviceSet.AddDevice({drive.GetPath(), drive.GetType()});
        }
        hostConfigMap[hostConfig.GetHostConfigId()] = std::move(driveDeviceSet);
    }
    auto box = boxStatus.GetBox()[0];
    for (const auto& host: box.GetHost()) {
        auto *driveInfo = to->add_drive_info();
        auto *hostDriveInfo = driveInfo->mutable_host(); 
        auto& key = host.GetKey();
        hostDriveInfo->set_fqdn(key.GetFqdn());
        hostDriveInfo->set_port(key.GetIcPort());
        TDriveDeviceSet driveDevice = hostConfigMap[host.GetHostConfigId()];
        for (const auto& drive: driveDevice.GetDevices()) {
            auto* newDrive = driveInfo->add_drive();
            newDrive->set_path(drive.GetPath());
            newDrive->set_type(static_cast<Ydb::BSConfig::PDiskType>(drive.GetType()));
        }
    }
}

class TDefineRequest : public TBSConfigRequestGrpc<TDefineRequest, TEvDefineRequest, Ydb::BSConfig::DefineResult> {
public:
    using TBase = TBSConfigRequestGrpc<TDefineRequest, TEvDefineRequest, Ydb::BSConfig::DefineResult>;
    using TBase::TBase;

    bool ValidateRequest(Ydb::StatusIds::StatusCode& /*status*/, NYql::TIssues& /*issues*/) override {
        return true;
    }
    NACLib::EAccessRights GetRequiredAccessRights() const {
        return NACLib::GenericManage;
    }
};

class TFetchRequest : public TBSConfigRequestGrpc<TFetchRequest, TEvFetchRequest, Ydb::BSConfig::FetchResult> {
public:
    using TBase = TBSConfigRequestGrpc<TFetchRequest, TEvFetchRequest, Ydb::BSConfig::FetchResult>;
    using TBase::TBase;

    bool ValidateRequest(Ydb::StatusIds::StatusCode& /*status*/, NYql::TIssues& /*issues*/) override {
        return true;
    }
    NACLib::EAccessRights GetRequiredAccessRights() const {
        return NACLib::GenericManage;
    }
};

void DoDefineBSConfig(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TDefineRequest(p.release()));
}

void DoFetchBSConfig(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TFetchRequest(p.release()));
}


} // namespace NKikimr::NGRpcService
