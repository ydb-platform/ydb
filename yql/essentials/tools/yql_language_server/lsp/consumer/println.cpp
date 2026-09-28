#include "println.h"

#include "tee.h"

namespace NLsp {

IConsumer<TString>::TPtr LinePrinting(IOutputStream& out, IConsumer<TString>::TPtr consumer) {
    return Tee<TString>([o = &out](auto x) { *o << x << Endl; }, std::move(consumer));
}

} // namespace NLsp
