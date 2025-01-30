#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NExternalSource {

std::unique_ptr<NActors::IActor> CreateHiveMetastoreFetcherActor(const TString& host, int32_t port);

}
