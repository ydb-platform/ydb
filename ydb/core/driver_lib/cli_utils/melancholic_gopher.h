#pragma once
#include <ydb/library/actors/core/actor.h>

namespace NActors {

IActor* CreateMelancholicGopher(double surveyForSeconds, const TActorId &reportTo); // will spin for survey period and then wakeup next in line
IActor* CreateGopherMother(const TVector<std::pair<ui32, double>> &lineProfile, ui32 lines, ui32 shotsInRound); // would spawn gophers according to profile (poolid, period) in lines number

}
