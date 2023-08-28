#include "query_generator.h"

namespace NYdbWorkload {
namespace NTPCC {

TLoadQueryGenerator::TLoadQueryGenerator(TTPCCWorkloadParams& params, ui64 seed)
    : Params(params)
    , Rng(seed, 0)
{
}

TTPCCWorkloadParams TLoadQueryGenerator::GetParams() {
    return Params;
}

TString TLoadQueryGenerator::RandomString(ui64 length) {
    char str[length + 1];
    for (ui64 i = 0; i < length; ++i) {
        str[i] = static_cast<char>(UniformRandom32('A', 'z', Rng)); 
    }
    str[length] = '\0';
    return TString(str);
}

TString TLoadQueryGenerator::RandomNumberString(ui64 length) {
    char str[length + 1];
    for (ui64 i = 0; i < length; ++i) {
        str[i] = static_cast<char>(UniformRandom32('0', '9', Rng)); 
    }
    str[length] = '\0';
    return TString(str);
}

}
}
