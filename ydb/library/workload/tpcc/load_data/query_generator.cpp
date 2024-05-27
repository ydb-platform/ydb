#include "query_generator.h"

namespace NYdbWorkload {
namespace NTPCC {

TLoadQueryGenerator::TLoadQueryGenerator(TLoadParams& params, ETablesType type, TLog& log, ui64 seed)
    : Params(params)
    , Type(type)
    , Log(log)
    , Rng(seed, 0)
{
}

TLoadParams TLoadQueryGenerator::GetParams() {
    return Params;
}

ETablesType TLoadQueryGenerator::GetType() {
    return Type;
}

TString TLoadQueryGenerator::RandomString(ui64 length) {
    std::string str;
    str.reserve(length+1);
    for (ui64 i = 0; i < length; ++i) {
        str += static_cast<char>(UniformRandom32('a', 'z', Rng));
    }
    str += '\0';
    return TString(str);
}

TString TLoadQueryGenerator::RandomNumberString(ui64 length) {
    std::string str;
    str.reserve(length+1);
    for (ui64 i = 0; i < length; ++i) {
        str += static_cast<char>(UniformRandom32('0', '9', Rng));
    }
    str += '\0';
    return TString(str);
}

}
}
