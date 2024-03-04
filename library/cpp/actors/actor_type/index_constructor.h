#pragma once
#include "common.h"
#include <library/cpp/actors/util/local_process_key.h>
#include <library/cpp/actors/prof/tag.h>

template <>
class TLocalProcessKeyStateIndexConstructor<NActors::TActorActivityTag> {
public:
    static ui32 BuildCurrentIndex(const TStringBuf name, const ui32 /*currentNamesCount*/) {
        return NProfiling::MakeTag(name.data());
    }
};
