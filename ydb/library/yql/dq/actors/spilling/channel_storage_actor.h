#include <ydb/library/yql/dq/runtime/dq_channel_storage.h>
#include "ydb/library/yql/dq/common/dq_common.h"

#include <ydb/library/actors/core/actor.h>

namespace NYql::NDq {

class IDqChannelStorageActor
{
public:
    using TPtr = TIntrusivePtr<IDqChannelStorageActor>;
    using TWakeUpCallback = std::function<void()>;

    virtual ~IDqChannelStorageActor() = default;

    virtual NActors::IActor* GetActor() = 0;

    virtual bool IsEmpty() = 0;
    virtual bool IsFull() = 0;

    // methods Put/Get can throw `TDqChannelStorageException`

    // Data should be owned by `blob` argument since the Put() call is actually asynchronous
    virtual void Put(ui64 blobId, TRope&& blob, ui64 cookie = 0) = 0;

    // TODO: there is no way for client to delete blob.
    // It is better to replace Get() with Pull() which will delete blob after read
    // (current clients read each blob exactly once)
    // Get() will return false if data is not ready yet. Client should repeat Get() in this case
    virtual bool Get(ui64 blobId, TBuffer& data, ui64 cookie = 0)  = 0;
};

IDqChannelStorageActor* CreateDqChannelStorageActor(TTxId txId, ui64 channelId, IDqChannelStorage::TWakeUpCallback&& wakeUp, NActors::TActorSystem* actorSystem);
IDqChannelStorageActor* CreateConcurrentDqChannelStorageActor(TTxId txId, ui64 channelId, IDqChannelStorage::TWakeUpCallback&& wakeUp, NActors::TActorSystem* actorSystem);

} // namespace NYql::NDq