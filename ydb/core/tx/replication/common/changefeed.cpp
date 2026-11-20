#include "changefeed.h"

#include <library/cpp/json/json_reader.h>

namespace NKikimr::NReplication {

const TString ReplicationConsumerName = "replicationConsumer";

bool HasChangefeedCapability(TStringBuf replicationAttribute, TStringBuf capability) {
    NJson::TJsonValue attributes;
    const NJson::TJsonValue* value = nullptr;
    bool enabled = false;
    return NJson::ReadJsonTree(replicationAttribute, &attributes)
        && attributes.GetValuePointer(capability, &value)
        && value->GetBoolean(&enabled) && enabled;
}

} // namespace NKikimr::NReplication
