#include "gen_step.h"

template<>
void Out<NKikimr::TGenStep>(IOutputStream& s, const NKikimr::TGenStep& x) {
    x.Output(s);
}
