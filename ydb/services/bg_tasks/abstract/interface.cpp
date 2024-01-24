#include "interface.h"
#include <ydb/services/bg_tasks/protos/container.pb.h>

namespace NKikimr::NBackgroundTasks {

bool TStringContainerProcessor::DeserializeFromContainer(const TString& data, TString& className, TString& binary) {
    NKikimrProto::TStringContainer protoData;
    if (!protoData.ParseFromArray(data.data(), data.size())) {
        ALS_ERROR(NKikimrServices::BG_TASKS) << "cannot parse string as proto: " << Base64Encode(data);
        return false;
    }
    className = protoData.GetClassName();
    binary = protoData.GetBinaryData();
    return true;
}

TString TStringContainerProcessor::SerializeToContainer(const TString& className, const TString& binary) {
    NKikimrProto::TStringContainer result;
    result.SetClassName(className);
    if (binary) {
        result.SetBinaryData(binary);
    }
    return result.SerializeAsString();
}

}
