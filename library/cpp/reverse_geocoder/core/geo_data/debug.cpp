#include "debug.h"

#include <library/cpp/reverse_geocoder/library/log.h>
#include <library/cpp/reverse_geocoder/library/memory.h>

using namespace NReverseGeocoder;
using namespace NGeoData;

size_t NReverseGeocoder::NGeoData::Space(const IGeoData& g) {
    size_t space = 0;

#define GEO_BASE_DEF_VAR(TVar, Var) \
    space += sizeof(TVar);

#define GEO_BASE_DEF_ARR(TArr, Arr) \
    space += sizeof(TNumber) + sizeof(TArr) * g.Arr##Number();

    GEO_BASE_DEF_GEO_DATA

#undef GEO_BASE_DEF_VAR
#undef GEO_BASE_DEF_ARR

    return space;
}

template <typename TArr>
static float ArraySpace(TNumber number) {
    return number * sizeof(TArr) * 1.0 / MB;
}

void NReverseGeocoder::NGeoData::Show(IOutputStream& out, const IGeoData& g) {
    out << "GeoData = " << NGeoData::Space(g) * 1.0 / GB << " GB" << '\n';

#define GEO_BASE_DEF_VAR(TVar, Var) \
    out << "  GeoData." << #Var << " = " << (unsigned long long)g.Var() << '\n';

#define GEO_BASE_DEF_ARR(TArr, Arr)                          \
    out << "  GeoData." << #Arr << " = "                     \
        << g.Arr##Number() << " x " << sizeof(TArr) << " = " \
        << ArraySpace<TArr>(g.Arr##Number()) << " MB"        \
        << '\n';

    GEO_BASE_DEF_GEO_DATA

#undef GEO_BASE_DEF_VAR
#undef GEO_BASE_DEF_ARR
}

template <typename TArr>
static bool Equals(const TArr* a, const TArr* b, size_t count) {
    return !memcmp(a, b, sizeof(TArr) * count);
}

bool NReverseGeocoder::NGeoData::Equals(const IGeoData& a, const IGeoData& b) {
#define GEO_BASE_DEF_VAR(TVar, Var)  \
    if (a.Var() != b.Var()) {        \
        LogError(#Var " not equal"); \
        return false;                \
    }

#define GEO_BASE_DEF_ARR(TArr, Arr)                     \
    GEO_BASE_DEF_VAR(TNumber, Arr##Number);             \
    if (!::Equals(a.Arr(), b.Arr(), a.Arr##Number())) { \
        LogError(#Arr " not equal");                    \
        return false;                                   \
    }

    GEO_BASE_DEF_GEO_DATA

#undef GEO_BASE_DEF_VAR
#undef GEO_BASE_DEF_ARR

    return true;
}
