#pragma once

#include "events.h"

#include <istream>
#include <ostream>

THolder<NActors::IActor> CreateInputReaderActor(std::istream& input, NActors::TActorId target);
THolder<NActors::IActor> CreateOutputWriterActor(std::ostream& output);
THolder<NActors::IActor> CreateMaximumPrimeDevisorActor(int64_t number, NActors::TActorId readerId, NActors::TActorId writerId);
