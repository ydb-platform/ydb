#include <ydb/library/yql/dq/runtime/dq_channel_storage.h>
#include "ydb/library/yql/dq/common/dq_common.h"

#include <ydb/library/actors/core/actor.h>

namespace NYql::NDq {

class IDqChannelStorageActor : public IDqChannelStorage
{
public:
    virtual void Terminate() = 0;

    virtual NActors::IActor* GetActor() = 0;
};

IDqChannelStorageActor* CreateDqChannelStorageActor(TTxId txId, ui64 channelId, IDqChannelStorage::TWakeUpCallback&& wakeUp, NActors::TActorSystem* actorSystem);

} // namespace NYql::NDq