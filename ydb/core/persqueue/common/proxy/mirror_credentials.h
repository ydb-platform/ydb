#pragma once

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>

namespace NKikimr::NPQ {

class IPersQueueMirrorReaderFactory;

NActors::IActor* CreateMirrorCredentialsInitializer(
    const NActors::TActorId& parent,
    ui64 cookie,
    const NKikimrPQ::TMirrorPartitionConfig::TCredentials& credentials,
    TString database,
    TString topicPath,
    const IPersQueueMirrorReaderFactory* factory
);

} // namespace NKikimr::NPQ
