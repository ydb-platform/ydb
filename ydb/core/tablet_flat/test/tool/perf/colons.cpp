#include "colons.h"

#include <util/generic/algorithm.h>

using namespace NKikiSched;

TColons::TColons(const TString &line)
    : At(0)
    , Line(line)
{

}


TColons::~TColons()
{

}


TString TColons::Next() noexcept
{
    TString token;

    if (!*this) {
        At = TString::npos;

    } else {
        const size_t end = Line.find(':', At);

        token = Line.substr(At, end == TString::npos ? end : end - At);

        if ((At = end) != TString::npos) At++;
    }

    return token;
}


TString TColons::Token(const TString &def) noexcept
{
    TString token = Next();

    return token ? token : def;
}


size_t TColons::Large(size_t def)
{
    const TString item = Next();

    return !item ? def : LargeParse(item);
}


size_t TColons::LargeParse(const TString &item)
{
    TString::const_iterator it = FindIf(item.begin(), item.end(), IsNumber);

    if (it == item.begin()) {
        throw yexception() << "Invalid value literal " << item;
    }

    size_t off = (it == item.end()) ? TString::npos : it - item.begin();

    const TString pref = item.substr(0, off);
    const TString suff = item.substr(off);

    size_t value = FromString<size_t>(pref);

    if (suff.empty()) {

    } else if (suff == "k" || suff == "K") {
        value <<= 10;

    } else if (suff == "m" || suff == "M") {
        value <<= 20;

    } else if (suff == "g" || suff == "G") {
        value <<= 30;

    } else if (suff == "t" || suff == "T") {
        value <<= 40;

    } else {
        throw yexception() << "Unknown value suffix " << suff;
    }

    return value;
}
