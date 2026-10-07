#pragma once

#include <ydb/library/actors/core/actorsystem_fwd.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NKikimr::NPQ {

NActors::IActor* MakeListAllTopicsActor(const NActors::TActorId& respondTo, const TString& databasePath, const TString& token,
                                        bool recursive, const TString& startFrom = {}, const TMaybe<ui64>& limit = Nothing());

} // namespace NKikimr::NPQ
