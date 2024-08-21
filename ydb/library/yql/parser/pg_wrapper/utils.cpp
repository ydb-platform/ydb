#include "pg_compat.h"

extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "utils/array.h"
#include "utils/elog.h"
#include "pgstat.h"
#include "catalog/pg_namespace_d.h"
}

#undef Max
constexpr auto PG_ERROR = ERROR;
#undef ERROR

#include "utils.h"

#include <util/system/compiler.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>

#include <util/system/dynlib.h>

#define ERROR PG_ERROR

namespace NYql {

void PrepareVariadicArraySlow(FunctionCallInfoBaseData& callInfo, const NPg::TProcDesc& desc) {
    const auto& elemDesc = NPg::LookupType(desc.VariadicType);

    Datum varArgs[FUNC_MAX_ARGS];
    bool varArgsNulls[FUNC_MAX_ARGS];
    const ui32 nelems = callInfo.nargs - desc.ArgTypes.size();
    Y_ENSURE(nelems >= 1);
    for (ui32 i = desc.ArgTypes.size(); i < callInfo.nargs; ++i) {
        varArgs[i - desc.ArgTypes.size()] = callInfo.args[i].value;
        varArgsNulls[i - desc.ArgTypes.size()] = callInfo.args[i].isnull;
    }

    callInfo.nargs = desc.ArgTypes.size() + 1;
    int dims[MAXDIM];
    int lbs[MAXDIM];
    dims[0] = nelems;
    lbs[0] = 1;

    auto array = construct_md_array(varArgs, varArgsNulls, 1, dims, lbs,
        desc.VariadicType, elemDesc.TypeLen, elemDesc.PassByValue, elemDesc.TypeAlign);

    auto& argDatum = callInfo.args[callInfo.nargs - 1];
    argDatum.value = PointerGetDatum(array);
    argDatum.isnull = false;
}

void FreeVariadicArray(FunctionCallInfoBaseData& callInfo, ui32 originalArgs) {
    pfree(DatumGetPointer(callInfo.args[callInfo.nargs - 1].value));
    callInfo.nargs = originalArgs;
}

static __thread ui32 PgExtIndexMax = 0;

class TExtensionsRegistry::TImpl {
public:
    struct TExtension {
        TExtension(const TString& name, const TString& libraryPath) {
            Lib.Open(libraryPath.c_str(), RTLD_GLOBAL | RTLD_NOW);
            Lib.SetUnloadable(false);
            InitFunc = (TInitFunc)Lib.Sym("YqlPgThreadInit");
            CleanupFunc = (TInitFunc)Lib.Sym("YqlPgThreadCleanup");
        }

        using TInitFunc = void(*)(void);
        using TCleanupFunc = void(*)(void);

        TDynamicLibrary Lib;
        TInitFunc InitFunc = nullptr;
        TInitFunc CleanupFunc = nullptr;
    };

    void InitThread() {
        while (PgExtIndexMax < Extensions.size()) {
            Extensions[PgExtIndexMax]->InitFunc();
            ++PgExtIndexMax;
        }
    }

    void CleanupThread() {
        for (ui32 i = 0; i < PgExtIndexMax; ++i) {
            Extensions[i]->CleanupFunc();
        }
    }

    void Load(ui32 extensionIndex, const TString& name, const TString& path) {
        Cerr << "Loading PG extension " << name << " [" << extensionIndex << "]: " << path << "\n";
        Y_ENSURE(extensionIndex == Extensions.size() + 1);
        Extensions.emplace_back(std::make_unique<TExtension>(name, path));
        Extensions.back()->InitFunc();
        ++PgExtIndexMax;
    }

    PGFunction GetFuncAddr(ui32 extensionIndex, const TString& funcName) {
        Y_ENSURE(extensionIndex > 0 && extensionIndex <= Extensions.size());
        return (PGFunction)Extensions[extensionIndex - 1]->Lib.Sym(funcName.c_str());
    }

private:
    TVector<std::unique_ptr<TExtension>> Extensions;
};

extern "C" ui64 TouchReadTableApi();

TExtensionsRegistry::TExtensionsRegistry()
    : Impl_(std::make_unique<TImpl>())
{
    Y_UNUSED(TouchReadTableApi());
}

TExtensionsRegistry& TExtensionsRegistry::Instance() {
    return *Singleton<TExtensionsRegistry>();
}

void TExtensionsRegistry::InitThread() {
    Impl_->InitThread();
}

void TExtensionsRegistry::CleanupThread() {
    Impl_->CleanupThread();
}

void TExtensionsRegistry::Load(ui32 extensionIndex, const TString& name, const TString& path) {
    Impl_->Load(extensionIndex, name, path);
}

PGFunction TExtensionsRegistry::GetFuncAddr(ui32 extensionIndex, const TString& funcName) {
    return Impl_->GetFuncAddr(extensionIndex, funcName);
}

bool GetPgFuncAddr(ui32 procOid, FmgrInfo& finfo) {
    Zero(finfo);
    const auto& desc = NPg::LookupProc(procOid);
    finfo.fn_strict = desc.IsStrict;
    finfo.fn_retset = desc.ReturnSet;
    finfo.fn_nargs = desc.ArgTypes.size() + (desc.VariadicArgType ? 1 : 0);
    finfo.fn_mcxt = CurrentMemoryContext;
    finfo.fn_oid = procOid;
    finfo.fn_stats = TRACK_FUNC_ALL;

    if (desc.Kind != NPg::EProcKind::Function) {
        return false;
    }

    if (desc.ExtensionIndex == 0) {
        if (desc.Lang != NPg::LangInternal) {
            return false;
        }

        FmgrInfo tmp;
        Zero(tmp);
        fmgr_info(procOid, &tmp);
        Y_ENSURE(tmp.fn_addr);
        Y_ENSURE(finfo.fn_strict == tmp.fn_strict);
        Y_ENSURE(finfo.fn_retset == tmp.fn_retset);
        Y_ENSURE(finfo.fn_nargs == tmp.fn_nargs);
        finfo.fn_addr = tmp.fn_addr;
        return true;
    }

    const auto& extension = NPg::LookupExtension(desc.ExtensionIndex);
    if (extension.TypesOnly) {
        return false;
    }

    if (desc.Lang != NPg::LangC) {
        return false;
    }

    finfo.fn_addr = TExtensionsRegistry::Instance().GetFuncAddr(desc.ExtensionIndex, desc.Src);
    return true;
}

extern "C" Oid get_extension_oid(const char *extname, bool missing_ok)
{
    Oid result = InvalidOid;
    try {
        result = NPg::LookupExtensionByName(extname);
    } catch (const yexception&) {
    }
    
    if (!OidIsValid(result) && !missing_ok)
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("extension \"%s\" does not exist",
                    extname)));

    return result;
}

extern "C" char *get_extension_name(Oid ext_oid) {
    try {
        return pstrdup(NPg::LookupExtension(ext_oid).Name.c_str());
    } catch (const yexception&) {
        return nullptr;
    }
}

extern "C" Oid get_extension_schema(Oid) {
    return PG_CATALOG_NAMESPACE;
}

}
