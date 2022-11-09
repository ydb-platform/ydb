#pragma once
#include <ydb/services/metadata/abstract/common.h>
#include <library/cpp/actors/core/event_local.h>

namespace NKikimr::NMetadataProvider {

class TEvSubscribeExternal: public NActors::TEventLocal<TEvSubscribeExternal, EEvSubscribe::EvSubscribeExternal> {
private:
    YDB_READONLY_DEF(ISnapshotParser::TPtr, SnapshotParser);
public:
    TEvSubscribeExternal(ISnapshotParser::TPtr parser)
        : SnapshotParser(parser)
    {
        Y_VERIFY(!!SnapshotParser);
    }
};

class TEvUnsubscribeExternal: public NActors::TEventLocal<TEvUnsubscribeExternal, EEvSubscribe::EvUnsubscribeExternal> {
private:
    YDB_READONLY_DEF(ISnapshotParser::TPtr, SnapshotParser);
public:
    TEvUnsubscribeExternal(ISnapshotParser::TPtr parser)
        : SnapshotParser(parser) {
        Y_VERIFY(!!SnapshotParser);
    }
};

NActors::TActorId MakeServiceId(const ui32 node);

}
