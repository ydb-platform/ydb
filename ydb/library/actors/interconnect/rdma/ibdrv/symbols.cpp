#include "symbols.h"

#include <util/generic/yexception.h>
#include <util/generic/vector.h>
#include <util/generic/singleton.h>
#include <util/generic/utility.h>
#include <util/system/dynlib.h>
#include <util/string/builder.h>
#include <library/cpp/iterator/zip.h>

#define LOADSYM(name, type) {name = (TId<type>::R*)L->SymOptional(#name);}

const TInfinibandSymbols* IBSym() {
    struct TSymbols: TInfinibandSymbols {
        TSymbols() {
            L.Reset(new TDynamicLibrary("libibverbs.so.1"));

            DOVERBS(LOADSYM)
        }

        THolder<TDynamicLibrary> L;
    };

    return SingletonWithPriority<TSymbols, 100>();
}

const TRdmaSymbols* RDSym() {
    struct TSymbols: TRdmaSymbols {
        TSymbols() {
            L.Reset(new TDynamicLibrary("librdmacm.so.1"));

            DORDMA(LOADSYM)
        }

        THolder<TDynamicLibrary> L;
    };

    return SingletonWithPriority<TSymbols, 100>();
}

const TMlx5Symbols* M5Sym() {
    struct TSymbols: TMlx5Symbols {
        TSymbols() {
            L.Reset(new TDynamicLibrary("libmlx5.so.1"));

            DOMLX5(LOADSYM)
        }

        THolder<TDynamicLibrary> L;
    };

    return SingletonWithPriority<TSymbols, 100>();
}

#undef LOADSYM
