#include <util/generic/maybe.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NPersQueue {

NActors::IActor* MakeListAllTopicsActor(const NActors::TActorId& respondTo, const TString& databasePath, const TString& token,
                                        bool recursive, const TString& startFrom = {}, const TMaybe<ui64>& limit = Nothing());
}
