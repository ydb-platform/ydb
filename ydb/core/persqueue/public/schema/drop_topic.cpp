#include "drop_topic_operation.h"

#include <ydb/library/aclib/aclib.h>

namespace NKikimr::NPQ::NSchema {

NActors::IActor* CreateDropTopicActor(const NActors::TActorId& parentId, TDropTopicSettings&& settings) {
    return CreateDropTopicOperationActor(parentId, {
        .Database = std::move(settings.Database),
        .PeerName = std::move(settings.PeerName),
        .Path = std::move(settings.Path),
        .UserToken = std::move(settings.UserToken),
        .IfExists = settings.IfExists,
        .Cookie = settings.Cookie,
        .PathContext = std::move(settings.PathContext),
        .LogicalDatabase = std::move(settings.LogicalDatabase),
    });
}

} // namespace NKikimr::NPQ::NSchema
