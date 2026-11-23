#include "target_topic.h"

#include <ydb/core/protos/replication.pb.h>

namespace NKikimr::NReplication::NController {

TTargetTopic::TTargetTopic(TReplication* replication, ui64 id, const IConfig::TPtr& config)
    : TTargetWithStream(replication, ETargetKind::Topic, id, config)
{
}

TString TTargetTopic::BuildStreamConsumerName() const {
    return GetStreamName();
}

TString TTargetTopic::GetStreamPath() const {
    return GetSrcPath();
}

IActor* TTargetTopic::CreateWorkerRegistar(const TActorContext&) const {
    return nullptr;
}

}
