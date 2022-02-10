#include "symbol.h"

#include <util/stream/output.h>
#include <util/string/cast.h>

template <>
NLWTrace::TSymbol FromStringImpl(const char*, size_t) {
    static TString err("ERROR_dynamic_symbol");
    return NLWTrace::TSymbol(&err);
}

template <>
void Out<NLWTrace::TSymbol>(IOutputStream& o, TTypeTraits<NLWTrace::TSymbol>::TFuncParam t) {
    Out<TString>(o, *t.Str);
}
