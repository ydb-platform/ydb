#include "pg_compat.h"

#define SortBy PG_SortBy
#define TypeName PG_TypeName

extern "C" {
#include "utils/syscache.h"
#include "utils/catcache.h"
#include "catalog/pg_database.h"
#include "catalog/pg_class.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "catalog/pg_type_d.h"
#include "catalog/pg_authid.h"
#include "access/htup_details.h"
#include "utils/fmgroids.h"
#include "utils/array.h"
#include "utils/builtins.h"
}

#undef TypeName
#undef SortBy
#undef LOG
#undef INFO
#undef NOTICE
#undef WARNING
//#undef ERROR
#undef FATAL
#undef PANIC
#undef open
#undef fopen
#undef bind
#undef locale_t

#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/context.h>
#include <ydb/library/yql/parser/pg_wrapper/memory_context.h>
#include <ydb/library/yql/parser/pg_wrapper/pg_catalog_consts.h>

#include "arena_ctx.h"
#include "utils.h"

#include <unordered_map>
#include <functional>
#include <tuple>

namespace NYql {
namespace {

using THeapTupleKey = std::tuple<Datum, Datum, Datum, Datum>;
using THeapTupleHasher = std::function<size_t(const THeapTupleKey&)>;
using THeapTupleEquals = std::function<bool(const THeapTupleKey&, const THeapTupleKey&)>;
using TSysCacheHashMap = std::unordered_map<THeapTupleKey, HeapTuple, THeapTupleHasher, THeapTupleEquals>;

struct TRangeItem {
    TVector<HeapTuple> Items;
    CatCList* CList = nullptr;
};

using TSysCacheRangeMap = std::unordered_map<THeapTupleKey, TRangeItem, THeapTupleHasher, THeapTupleEquals>;

size_t OidHasher1(const THeapTupleKey& key) {
    return std::hash<Oid>()((Oid)std::get<0>(key));
}

bool OidEquals1(const THeapTupleKey& key1, const THeapTupleKey& key2) {
    return (Oid)std::get<0>(key1) == (Oid)std::get<0>(key2);
}

size_t NsNameHasher(const THeapTupleKey& key) {
    return CombineHashes(
        std::hash<std::string_view>()((const char*)std::get<0>(key)),
        std::hash<Oid>()((Oid)std::get<1>(key)));
}

bool NsNameEquals(const THeapTupleKey& key1, const THeapTupleKey& key2) {
    return strcmp((const char*)std::get<0>(key1), (const char*)std::get<0>(key2)) == 0 &&
        (Oid)std::get<1>(key1) == (Oid)std::get<1>(key2);
}

size_t OidVectorHash(Datum d) {
    oidvector *v = (oidvector *)d;
    Y_DEBUG_ABORT_UNLESS(v->ndim == 1);
    size_t hash = v->dim1;
    for (int i = 0; i < v->dim1; ++i) {
        hash = CombineHashes(hash, std::hash<Oid>()(v->values[i]));
    }

    return hash;
}

bool OidVectorEquals(Datum d1, Datum d2) {
    oidvector *v1 = (oidvector *)d1;
    oidvector *v2 = (oidvector *)d2;
    Y_DEBUG_ABORT_UNLESS(v1->ndim == 1 && v2->ndim == 1);
    if (v1->dim1 != v2->dim1) {
        return false;
    }

    for (int i = 0; i < v1->dim1; ++i) {
        if (v1->values[i] != v2->values[i]) {
            return false;
        }
    }

    return true;
}

size_t ByNameProcHasher1(const THeapTupleKey& key) {
    return std::hash<std::string_view>()((const char*)std::get<0>(key));
}

size_t ByNameProcHasher3(const THeapTupleKey& key) {
    return CombineHashes(CombineHashes(std::hash<std::string_view>()((const char*)std::get<0>(key)),
        OidVectorHash(std::get<1>(key))),
        std::hash<Oid>()((Oid)std::get<2>(key)));
}

bool ByNameProcEquals1(const THeapTupleKey& key1, const THeapTupleKey& key2) {
    return strcmp((const char*)std::get<0>(key1), (const char*)std::get<0>(key2)) == 0;
}

bool ByNameProcEquals3(const THeapTupleKey& key1, const THeapTupleKey& key2) {
    return strcmp((const char*)std::get<0>(key1), (const char*)std::get<0>(key2)) == 0 &&
        OidVectorEquals(std::get<1>(key1), std::get<1>(key2)) &&
        (Oid)std::get<2>(key1) == (Oid)std::get<2>(key2);
}

struct TSysCacheItem {
    TSysCacheItem(THeapTupleHasher hasher, THeapTupleEquals equals, TupleDesc desc, ui32 numKeys = 1, 
        THeapTupleHasher hasherRange1 = {}, THeapTupleEquals equalsRange1 = {})
        : NumKeys(numKeys)
        , LookupMap(0, hasher, equals)
        , RangeMap1(numKeys > 1 ? TMaybe<TSysCacheRangeMap>(TSysCacheRangeMap(0, hasherRange1, equalsRange1)) : Nothing())
        , Desc(desc)
    {}

    CatCList* BuildCList(const TVector<HeapTuple>& items) {
        auto cl = (CatCList *)palloc(offsetof(CatCList, members) + items.size() * sizeof(CatCTup *));
        cl->cl_magic = CL_MAGIC;
        cl->my_cache = nullptr;
        cl->refcount = 0;
        cl->dead = false;
        cl->ordered = false;
        cl->nkeys = NumKeys;
        cl->hash_value = 0;
        cl->n_members = items.size();
        for (size_t i = 0; i < items.size(); ++i) {
            auto dtp = items[i];
            auto ct = (CatCTup *) palloc(sizeof(CatCTup) + MAXIMUM_ALIGNOF + dtp->t_len);
            ct->ct_magic = CT_MAGIC;
            ct->my_cache = nullptr;
            ct->c_list = NULL;
            ct->refcount = 0;
            ct->dead = false;
            ct->negative = false;
            ct->hash_value = 0;
            Zero(ct->keys);

            ct->tuple.t_len = dtp->t_len;
            ct->tuple.t_self = dtp->t_self;
            ct->tuple.t_tableOid = dtp->t_tableOid;
            ct->tuple.t_data = (HeapTupleHeader)MAXALIGN(((char *) ct) + sizeof(CatCTup));
            /* copy tuple contents */
            std::memcpy((char *) ct->tuple.t_data, (const char *) dtp->t_data, dtp->t_len);
            cl->members[i] = ct;
        }

        return cl;
    }

    void FinalizeRangeMaps() {
        if (RangeMap1) {
            for (auto& x : *RangeMap1) {
                x.second.CList = BuildCList(x.second.Items);
            }
        }

        EmptyCList = BuildCList({});
    }

    const ui32 NumKeys;
    TSysCacheHashMap LookupMap;
    TMaybe<TSysCacheRangeMap> RangeMap1;
    TupleDesc Desc;
    CatCList* EmptyCList = nullptr;
    std::function<std::optional<HeapTuple>(const THeapTupleKey&)> PgThreadContextLookup = nullptr;
};

struct TSysCache {
    TArenaMemoryContext Arena;
    std::unique_ptr<TSysCacheItem> Items[SysCacheSize];

    static const TSysCache& Instance() {
        return *Singleton<TSysCache>();
    }

    TSysCache()
    {
        InitializeProcs();
        InitializeTypes();
        InitializeDatabase();
        InitializeAuthId();
        InitializeNameAndOidNamespaces();
        InitializeRelNameNamespaces();
        for (auto& item : Items) {
            if (item) {
                item->FinalizeRangeMaps();
            }
        }

        Arena.Release();
    }

    ~TSysCache() {
        Arena.Acquire();
    }

    static void FillDatum(ui32 count, Datum* values, bool* nulls, ui32 attrNum, Datum value) {
        Y_ENSURE(attrNum > 0 && attrNum <= count);
        values[attrNum - 1] = value;
        nulls[attrNum - 1] = false;
    }

    static void FillAttr(TupleDesc tupleDesc, ui32 attrNum, Oid type) {
        Y_ENSURE(attrNum > 0 && attrNum <= tupleDesc->natts);
        TupleDescInitEntry(tupleDesc, attrNum, nullptr, type, -1, 0);
    }

    void InitializeProcs() {
        TupleDesc tupleDesc = CreateTemplateTupleDesc(Natts_pg_proc);
        FillAttr(tupleDesc, Anum_pg_proc_oid, OIDOID);
        FillAttr(tupleDesc, Anum_pg_proc_proname, NAMEOID);
        FillAttr(tupleDesc, Anum_pg_proc_pronamespace, OIDOID);
        FillAttr(tupleDesc, Anum_pg_proc_proowner, OIDOID);
        FillAttr(tupleDesc, Anum_pg_proc_prolang, OIDOID);
        FillAttr(tupleDesc, Anum_pg_proc_procost, FLOAT4OID);
        FillAttr(tupleDesc, Anum_pg_proc_prorows, FLOAT4OID);
        FillAttr(tupleDesc, Anum_pg_proc_provariadic, OIDOID);
        FillAttr(tupleDesc, Anum_pg_proc_prosupport, REGPROCOID);
        FillAttr(tupleDesc, Anum_pg_proc_prokind, CHAROID);
        FillAttr(tupleDesc, Anum_pg_proc_prosecdef, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_proc_proleakproof, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_proc_proisstrict, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_proc_proretset, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_proc_provolatile, CHAROID);
        FillAttr(tupleDesc, Anum_pg_proc_proparallel, CHAROID);
        FillAttr(tupleDesc, Anum_pg_proc_pronargs, INT2OID);
        FillAttr(tupleDesc, Anum_pg_proc_pronargdefaults, INT2OID);
        FillAttr(tupleDesc, Anum_pg_proc_prorettype, OIDOID);
        FillAttr(tupleDesc, Anum_pg_proc_proargtypes, OIDVECTOROID);
        FillAttr(tupleDesc, Anum_pg_proc_proallargtypes, OIDARRAYOID);
        FillAttr(tupleDesc, Anum_pg_proc_proargmodes, TEXTARRAYOID);
        FillAttr(tupleDesc, Anum_pg_proc_proargnames, TEXTARRAYOID);
        FillAttr(tupleDesc, Anum_pg_proc_proargdefaults, PG_NODE_TREEOID);
        FillAttr(tupleDesc, Anum_pg_proc_protrftypes, OIDARRAYOID);
        FillAttr(tupleDesc, Anum_pg_proc_prosrc, TEXTOID);
        FillAttr(tupleDesc, Anum_pg_proc_probin, TEXTOID);
        FillAttr(tupleDesc, Anum_pg_proc_prosqlbody, PG_NODE_TREEOID);
        FillAttr(tupleDesc, Anum_pg_proc_proconfig, TEXTARRAYOID);
        FillAttr(tupleDesc, Anum_pg_proc_proacl, ACLITEMARRAYOID);
        auto& cacheItem = Items[PROCOID] = std::make_unique<TSysCacheItem>(OidHasher1, OidEquals1, tupleDesc);
        auto& lookupMap = cacheItem->LookupMap;

        auto& byNameCacheItem = Items[PROCNAMEARGSNSP] = std::make_unique<TSysCacheItem>(ByNameProcHasher3, ByNameProcEquals3, tupleDesc, 3, ByNameProcHasher1, ByNameProcEquals1);
        auto& byNameLookupMap = byNameCacheItem->LookupMap;
        auto& byNameRangeMap1 = *byNameCacheItem->RangeMap1;

        const auto& oidDesc = NPg::LookupType(OIDOID);
        const auto& charDesc = NPg::LookupType(CHAROID);
        const auto& textDesc = NPg::LookupType(TEXTOID);

        NPg::EnumProc([&](ui32 oid, const NPg::TProcDesc& desc){
            auto key = THeapTupleKey(oid, 0, 0, 0);

            Datum values[Natts_pg_proc];
            bool nulls[Natts_pg_proc];
            Zero(values);
            std::fill_n(nulls, Natts_pg_proc, true);
            std::fill_n(nulls, Anum_pg_proc_prorettype, false); // fixed part of Form_pg_proc
            FillDatum(Natts_pg_proc, values, nulls, Anum_pg_proc_oid, oid);
            FillDatum(Natts_pg_proc, values, nulls, Anum_pg_proc_prorettype, desc.ResultType);
            auto name = MakeFixedString(desc.Name, NAMEDATALEN);
            FillDatum(Natts_pg_proc, values, nulls, Anum_pg_proc_proname, (Datum)name);
            FillDatum(Natts_pg_proc, values, nulls, Anum_pg_proc_pronargs, (Datum)desc.ArgTypes.size());
            if (!desc.VariadicType) {
                auto arr = buildoidvector(desc.ArgTypes.data(), (int)desc.ArgTypes.size());
                FillDatum(Natts_pg_proc, values, nulls, Anum_pg_proc_proargtypes, (Datum)arr);
            } else {
                std::unique_ptr<Oid[]> allOids(new Oid[1 + desc.ArgTypes.size()]);
                std::copy(desc.ArgTypes.begin(), desc.ArgTypes.end(), allOids.get());
                allOids[desc.ArgTypes.size()] = desc.VariadicArgType;
                auto arr = buildoidvector(allOids.get(), (int)(1 + desc.ArgTypes.size()));
                FillDatum(Natts_pg_proc, values, nulls, Anum_pg_proc_proargtypes, (Datum)arr);
            }

            auto variadicDelta = desc.VariadicType ? 1 : 0;
            const ui32 fullArgsCount = desc.ArgTypes.size() + desc.OutputArgTypes.size() + variadicDelta;
            if (!desc.OutputArgTypes.empty() || variadicDelta)
            {
                std::unique_ptr<Oid[]> allOids(new Oid[fullArgsCount]);
                std::copy(desc.ArgTypes.begin(), desc.ArgTypes.end(), allOids.get());
                if (variadicDelta) {
                    allOids.get()[desc.ArgTypes.size()] = desc.VariadicArgType;
                }

                std::copy(desc.OutputArgTypes.begin(), desc.OutputArgTypes.end(), allOids.get() + desc.ArgTypes.size() + variadicDelta);

                auto arr = buildoidvector(allOids.get(), (int)fullArgsCount);
                FillDatum(Natts_pg_proc, values, nulls, Anum_pg_proc_proallargtypes, (Datum)arr);
            }
            if (!desc.OutputArgTypes.empty() || variadicDelta)
            {
                int dims[MAXDIM];
                int lbs[MAXDIM];
                dims[0] = fullArgsCount;
                lbs[0] = 1;
                std::unique_ptr<Datum[]> dvalues(new Datum[fullArgsCount]);
                std::unique_ptr<bool[]> dnulls(new bool[fullArgsCount]);
                std::fill_n(dvalues.get(), desc.ArgTypes.size(), CharGetDatum('i'));
                if (variadicDelta) {
                    dvalues.get()[desc.ArgTypes.size()] = CharGetDatum('v');
                }

                std::fill_n(dvalues.get() + desc.ArgTypes.size() + variadicDelta, desc.OutputArgTypes.size(), CharGetDatum('o'));
                std::fill_n(dnulls.get(), fullArgsCount, false);

                auto arr = construct_md_array(dvalues.get(), dnulls.get(), 1, dims, lbs, CHAROID, charDesc.TypeLen, charDesc.PassByValue, charDesc.TypeAlign);
                FillDatum(Natts_pg_proc, values, nulls, Anum_pg_proc_proargmodes, (Datum)arr);
            }
            if (!desc.OutputArgNames.empty() || !desc.InputArgNames.empty() || !desc.VariadicArgName.empty()) {
                Y_ENSURE(desc.InputArgNames.size() + variadicDelta + desc.OutputArgNames.size() == fullArgsCount);
                int dims[MAXDIM];
                int lbs[MAXDIM];
                dims[0] = fullArgsCount;
                lbs[0] = 1;
                std::unique_ptr<Datum[]> dvalues(new Datum[fullArgsCount]);
                std::unique_ptr<bool[]> dnulls(new bool[fullArgsCount]);
                for (ui32 i = 0; i < desc.InputArgNames.size(); ++i) {
                    dvalues[i] = PointerGetDatum(MakeVar(desc.InputArgNames[i]));
                }

                if (variadicDelta) {
                    dvalues[desc.InputArgNames.size()] = PointerGetDatum(MakeVar(desc.VariadicArgName));
                }

                for (ui32 i = 0; i < desc.OutputArgNames.size(); ++i) {
                    dvalues[i + desc.InputArgNames.size() + variadicDelta] = PointerGetDatum(MakeVar(desc.OutputArgNames[i]));
                }
                std::fill_n(dnulls.get(), fullArgsCount, false);

                auto arr = construct_md_array(dvalues.get(), dnulls.get(), 1, dims, lbs, TEXTOID, textDesc.TypeLen, textDesc.PassByValue, textDesc.TypeAlign);
                FillDatum(Natts_pg_proc, values, nulls, Anum_pg_proc_proargnames, (Datum)arr);
                for (ui32 i = 0; i < fullArgsCount; ++i) {
                    pfree(DatumGetPointer(dvalues[i]));
                }
            }

            HeapTuple h = heap_form_tuple(tupleDesc, values, nulls);
            auto row = (Form_pg_proc)GETSTRUCT(h);
            Y_ENSURE(row->oid == oid);
            Y_ENSURE(row->prorettype == desc.ResultType);
            Y_ENSURE(NameStr(row->proname) == desc.Name);
            Y_ENSURE(row->pronargs == desc.ArgTypes.size());
            lookupMap.emplace(key, h);

            THeapTupleKey byNameLookupKey((Datum)name, (Datum)&row->proargtypes, PG_CATALOG_NAMESPACE, 0);
            THeapTupleKey byNameRangeKey1((Datum)name, 0, 0, 0);

            byNameLookupMap.emplace(byNameLookupKey, h);
            byNameRangeMap1[byNameRangeKey1].Items.push_back(h);
        });
    }

    void InitializeTypes() {
        TupleDesc tupleDesc = CreateTemplateTupleDesc(Natts_pg_type);
        FillAttr(tupleDesc, Anum_pg_type_oid, OIDOID);
        FillAttr(tupleDesc, Anum_pg_type_typname, NAMEOID);
        FillAttr(tupleDesc, Anum_pg_type_typnamespace, OIDOID);
        FillAttr(tupleDesc, Anum_pg_type_typowner, OIDOID);
        FillAttr(tupleDesc, Anum_pg_type_typlen, INT2OID);
        FillAttr(tupleDesc, Anum_pg_type_typbyval, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_type_typtype, CHAROID);
        FillAttr(tupleDesc, Anum_pg_type_typcategory, CHAROID);
        FillAttr(tupleDesc, Anum_pg_type_typispreferred, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_type_typisdefined, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_type_typdelim, CHAROID);
        FillAttr(tupleDesc, Anum_pg_type_typrelid, OIDOID);
        FillAttr(tupleDesc, Anum_pg_type_typsubscript, REGPROCOID);
        FillAttr(tupleDesc, Anum_pg_type_typelem, OIDOID);
        FillAttr(tupleDesc, Anum_pg_type_typarray, OIDOID);
        FillAttr(tupleDesc, Anum_pg_type_typinput, REGPROCOID);
        FillAttr(tupleDesc, Anum_pg_type_typoutput, REGPROCOID);
        FillAttr(tupleDesc, Anum_pg_type_typreceive, REGPROCOID);
        FillAttr(tupleDesc, Anum_pg_type_typsend, REGPROCOID);
        FillAttr(tupleDesc, Anum_pg_type_typmodin, REGPROCOID);
        FillAttr(tupleDesc, Anum_pg_type_typmodout, REGPROCOID);
        FillAttr(tupleDesc, Anum_pg_type_typanalyze, REGPROCOID);
        FillAttr(tupleDesc, Anum_pg_type_typalign, CHAROID);
        FillAttr(tupleDesc, Anum_pg_type_typstorage, CHAROID);
        FillAttr(tupleDesc, Anum_pg_type_typnotnull, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_type_typbasetype, OIDOID);
        FillAttr(tupleDesc, Anum_pg_type_typtypmod, INT4OID);
        FillAttr(tupleDesc, Anum_pg_type_typndims, INT4OID);
        FillAttr(tupleDesc, Anum_pg_type_typcollation, OIDOID);
        FillAttr(tupleDesc, Anum_pg_type_typdefaultbin, PG_NODE_TREEOID);
        FillAttr(tupleDesc, Anum_pg_type_typdefault, TEXTOID);
        FillAttr(tupleDesc, Anum_pg_type_typacl, ACLITEMARRAYOID);
        auto& cacheItem1 = Items[TYPEOID] = std::make_unique<TSysCacheItem>(OidHasher1, OidEquals1, tupleDesc);
        auto& lookupMap1 = cacheItem1->LookupMap;
        auto& cacheItem2 = Items[TYPENAMENSP] = std::make_unique<TSysCacheItem>(NsNameHasher, NsNameEquals, tupleDesc);
        auto& lookupMap2 = cacheItem2->LookupMap;

        NPg::EnumTypes([&](ui32 oid, const NPg::TTypeDesc& desc){
            Datum values[Natts_pg_type];
            bool nulls[Natts_pg_type];
            Zero(values);
            std::fill_n(nulls, Natts_pg_type, true);
            std::fill_n(nulls, Anum_pg_type_typcollation, false); // fixed part of Form_pg_type
            FillDatum(Natts_pg_type, values, nulls, Anum_pg_type_oid, oid);
            auto name = MakeFixedString(desc.Name, NAMEDATALEN);
            FillDatum(Natts_pg_type, values, nulls, Anum_pg_type_typname, (Datum)name);
            FillDatum(Natts_pg_type, values, nulls, Anum_pg_type_typbyval, desc.PassByValue);
            FillDatum(Natts_pg_type, values, nulls, Anum_pg_type_typlen, desc.TypeLen);
            FillDatum(Natts_pg_type, values, nulls, Anum_pg_type_typtype, (char)desc.TypType);
            FillDatum(Natts_pg_type, values, nulls, Anum_pg_type_typcategory, desc.Category);
            FillDatum(Natts_pg_type, values, nulls, Anum_pg_type_typispreferred, desc.IsPreferred);
            FillDatum(Natts_pg_type, values, nulls, Anum_pg_type_typisdefined, true);
            FillDatum(Natts_pg_type, values, nulls, Anum_pg_type_typdelim, desc.TypeDelim);
            FillDatum(Natts_pg_type, values, nulls, Anum_pg_type_typarray, desc.ArrayTypeId);
            FillDatum(Natts_pg_type, values, nulls, Anum_pg_type_typsubscript,
                (desc.ArrayTypeId == desc.TypeId) ? F_ARRAY_SUBSCRIPT_HANDLER : desc.TypeSubscriptFuncId);
            FillDatum(Natts_pg_type, values, nulls, Anum_pg_type_typelem, desc.ElementTypeId);
            FillDatum(Natts_pg_type, values, nulls, Anum_pg_type_typinput, desc.InFuncId);
            FillDatum(Natts_pg_type, values, nulls, Anum_pg_type_typoutput, desc.OutFuncId);
            FillDatum(Natts_pg_type, values, nulls, Anum_pg_type_typreceive, desc.ReceiveFuncId);
            FillDatum(Natts_pg_type, values, nulls, Anum_pg_type_typsend, desc.SendFuncId);
            FillDatum(Natts_pg_type, values, nulls, Anum_pg_type_typmodin, desc.TypeModInFuncId);
            FillDatum(Natts_pg_type, values, nulls, Anum_pg_type_typmodout, desc.TypeModOutFuncId);
            FillDatum(Natts_pg_type, values, nulls, Anum_pg_type_typalign, desc.TypeAlign);
            auto storage = desc.TypeId == desc.ArrayTypeId ? TYPSTORAGE_EXTENDED : TYPSTORAGE_PLAIN;
            FillDatum(Natts_pg_type, values, nulls, Anum_pg_type_typstorage, storage);
            HeapTuple h = heap_form_tuple(tupleDesc, values, nulls);
            auto row = (Form_pg_type)GETSTRUCT(h);
            Y_ENSURE(row->oid == oid);
            Y_ENSURE(NameStr(row->typname) == desc.Name);
            Y_ENSURE(row->typlen == desc.TypeLen);
            Y_ENSURE(row->typbyval == desc.PassByValue);
            Y_ENSURE(row->typtype == (char)desc.TypType);
            Y_ENSURE(row->typcategory == desc.Category);
            Y_ENSURE(row->typispreferred == desc.IsPreferred);
            Y_ENSURE(row->typisdefined == true);
            Y_ENSURE(row->typdelim == desc.TypeDelim);
            Y_ENSURE(row->typelem == desc.ElementTypeId);
            Y_ENSURE(row->typarray == desc.ArrayTypeId);
            Y_ENSURE(row->typinput == desc.InFuncId);
            Y_ENSURE(row->typoutput == desc.OutFuncId);
            Y_ENSURE(row->typreceive == desc.ReceiveFuncId);
            Y_ENSURE(row->typsend == desc.SendFuncId);
            Y_ENSURE(row->typmodin == desc.TypeModInFuncId);
            Y_ENSURE(row->typmodout == desc.TypeModOutFuncId);
            Y_ENSURE(row->typalign == desc.TypeAlign);
            Y_ENSURE(row->typstorage == storage);

            auto key1 = THeapTupleKey(oid, 0, 0, 0);            
            lookupMap1.emplace(key1, h);
            auto key2 = THeapTupleKey((Datum)desc.Name.c_str(), PG_CATALOG_NAMESPACE, 0, 0);
            lookupMap2.emplace(key2, h);
        });

    }

    static HeapTuple MakePgDatabaseHeapTuple(ui32 oid, const char* name) {
        TupleDesc tupleDesc = CreateTemplateTupleDesc(Natts_pg_database);
        FillAttr(tupleDesc, Anum_pg_database_oid, OIDOID);
        FillAttr(tupleDesc, Anum_pg_database_datname, NAMEOID);
        FillAttr(tupleDesc, Anum_pg_database_datdba, OIDOID);
        FillAttr(tupleDesc, Anum_pg_database_encoding, INT4OID);
        FillAttr(tupleDesc, Anum_pg_database_datlocprovider, CHAROID);
        FillAttr(tupleDesc, Anum_pg_database_datistemplate, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_database_datallowconn, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_database_datconnlimit, INT4OID);
        FillAttr(tupleDesc, Anum_pg_database_datfrozenxid, XIDOID);
        FillAttr(tupleDesc, Anum_pg_database_datminmxid, XIDOID);
        FillAttr(tupleDesc, Anum_pg_database_dattablespace, OIDOID);
        FillAttr(tupleDesc, Anum_pg_database_datcollate, TEXTOID);
        FillAttr(tupleDesc, Anum_pg_database_datctype, TEXTOID);
        FillAttr(tupleDesc, Anum_pg_database_daticulocale, TEXTOID);
        FillAttr(tupleDesc, Anum_pg_database_datcollversion, TEXTOID);
        FillAttr(tupleDesc, Anum_pg_database_datacl, ACLITEMARRAYOID);
        Datum values[Natts_pg_database];
        bool nulls[Natts_pg_database];
        Zero(values);
        std::fill_n(nulls, Natts_pg_database, true);
        FillDatum(Natts_pg_database, values, nulls, Anum_pg_database_oid, (Datum)oid);
        FillDatum(Natts_pg_database, values, nulls, Anum_pg_database_datname, (Datum)MakeFixedString(name, NAMEDATALEN));
        return heap_form_tuple(tupleDesc, values, nulls);
    }

    void InitializeDatabase() {
        TupleDesc tupleDesc = CreateTemplateTupleDesc(Natts_pg_database);
        FillAttr(tupleDesc, Anum_pg_database_oid, OIDOID);
        FillAttr(tupleDesc, Anum_pg_database_datname, NAMEOID);
        FillAttr(tupleDesc, Anum_pg_database_datdba, OIDOID);
        FillAttr(tupleDesc, Anum_pg_database_encoding, INT4OID);
        FillAttr(tupleDesc, Anum_pg_database_datlocprovider, CHAROID);
        FillAttr(tupleDesc, Anum_pg_database_datistemplate, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_database_datallowconn, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_database_datconnlimit, INT4OID);
        FillAttr(tupleDesc, Anum_pg_database_datfrozenxid, XIDOID);
        FillAttr(tupleDesc, Anum_pg_database_datminmxid, XIDOID);
        FillAttr(tupleDesc, Anum_pg_database_dattablespace, OIDOID);
        FillAttr(tupleDesc, Anum_pg_database_datcollate, TEXTOID);
        FillAttr(tupleDesc, Anum_pg_database_datctype, TEXTOID);
        FillAttr(tupleDesc, Anum_pg_database_daticulocale, TEXTOID);
        FillAttr(tupleDesc, Anum_pg_database_datcollversion, TEXTOID);
        FillAttr(tupleDesc, Anum_pg_database_datacl, ACLITEMARRAYOID);
        auto& cacheItem = Items[DATABASEOID] = std::make_unique<TSysCacheItem>(OidHasher1, OidEquals1, tupleDesc);
        auto& lookupMap = cacheItem->LookupMap;

        for (ui32 oid = 1; oid <= 3; ++oid) {
            auto key = THeapTupleKey(oid, 0, 0, 0);

            Datum values[Natts_pg_database];
            bool nulls[Natts_pg_database];
            Zero(values);
            std::fill_n(nulls, Natts_pg_database, true);
            FillDatum(Natts_pg_database, values, nulls, Anum_pg_database_oid, (Datum)oid);
            const char* name = nullptr;

            switch (oid) {
            case 1: name = "template1"; break;
            case 2: name = "template0"; break;
            case PG_POSTGRES_DATABASE_ID: name = "postgres"; break;
            }
            Y_ENSURE(name);
            FillDatum(Natts_pg_database, values, nulls, Anum_pg_database_datname, (Datum)MakeFixedString(name, NAMEDATALEN));
            HeapTuple h = heap_form_tuple(tupleDesc, values, nulls);
            auto row = (Form_pg_database) GETSTRUCT(h);
            Y_ENSURE(row->oid == oid);
            Y_ENSURE(strcmp(NameStr(row->datname), name) == 0);
            lookupMap.emplace(key, h);
        }

        //add specific lookup for 4 to cacheItem. save heaptuple to MainContext_.
        auto threadContextLookup = [&] (const THeapTupleKey& key) -> std::optional<HeapTuple> {
            if (std::get<0>(key) == PG_CURRENT_DATABASE_ID && NKikimr::NMiniKQL::TlsAllocState) {
                auto ctx = (TMainContext*)NKikimr::NMiniKQL::TlsAllocState->MainContext;
                if (ctx && ctx->CurrentDatabaseName) {
                    return ctx->CurrentDatabaseName;
                }
            }
            return std::nullopt;
        };

        cacheItem->PgThreadContextLookup = std::move(threadContextLookup);
    }

    static HeapTuple MakePgRolesHeapTuple(ui32 oid, const char* rolname) {
        TupleDesc tupleDesc = CreateTemplateTupleDesc(Natts_pg_authid);
        FillAttr(tupleDesc, Anum_pg_authid_oid, OIDOID);
        FillAttr(tupleDesc, Anum_pg_authid_rolname, NAMEOID);
        FillAttr(tupleDesc, Anum_pg_authid_rolsuper, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_authid_rolinherit, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_authid_rolcreaterole, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_authid_rolcreatedb, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_authid_rolcanlogin, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_authid_rolreplication, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_authid_rolbypassrls, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_authid_rolconnlimit, INT4OID);
        FillAttr(tupleDesc, Anum_pg_authid_rolpassword, TEXTOID);
        FillAttr(tupleDesc, Anum_pg_authid_rolvaliduntil, TIMESTAMPTZOID);
        Datum values[Natts_pg_authid];
        bool nulls[Natts_pg_authid];
        Zero(values);
        std::fill_n(nulls, Natts_pg_authid, true);
        FillDatum(Natts_pg_authid, values, nulls, Anum_pg_authid_oid, (Datum)oid);
        FillDatum(Natts_pg_authid, values, nulls, Anum_pg_authid_rolname, (Datum)MakeFixedString(rolname, NAMEDATALEN));
        FillDatum(Natts_pg_authid, values, nulls, Anum_pg_authid_rolsuper, BoolGetDatum(true));
        FillDatum(Natts_pg_authid, values, nulls, Anum_pg_authid_rolinherit, BoolGetDatum(true));
        FillDatum(Natts_pg_authid, values, nulls, Anum_pg_authid_rolcreaterole, BoolGetDatum(true));
        FillDatum(Natts_pg_authid, values, nulls, Anum_pg_authid_rolcreatedb, BoolGetDatum(true));
        FillDatum(Natts_pg_authid, values, nulls, Anum_pg_authid_rolcanlogin, BoolGetDatum(true));
        FillDatum(Natts_pg_authid, values, nulls, Anum_pg_authid_rolreplication, BoolGetDatum(true));
        FillDatum(Natts_pg_authid, values, nulls, Anum_pg_authid_rolbypassrls, BoolGetDatum(true));
        FillDatum(Natts_pg_authid, values, nulls, Anum_pg_authid_rolconnlimit, Int32GetDatum(-1));
        HeapTuple h = heap_form_tuple(tupleDesc, values, nulls);
        auto row = (Form_pg_authid) GETSTRUCT(h);
        Y_ENSURE(row->oid == oid);
        Y_ENSURE(strcmp(NameStr(row->rolname), rolname) == 0);
        Y_ENSURE(row->rolsuper);
        Y_ENSURE(row->rolinherit);
        Y_ENSURE(row->rolcreaterole);
        Y_ENSURE(row->rolcreatedb);
        Y_ENSURE(row->rolcanlogin);
        Y_ENSURE(row->rolreplication);
        Y_ENSURE(row->rolbypassrls);
        Y_ENSURE(row->rolconnlimit == -1);
        return h;
    }

    void InitializeAuthId() {
        TupleDesc tupleDesc = CreateTemplateTupleDesc(Natts_pg_authid);
        FillAttr(tupleDesc, Anum_pg_authid_oid, OIDOID);
        FillAttr(tupleDesc, Anum_pg_authid_rolname, NAMEOID);
        FillAttr(tupleDesc, Anum_pg_authid_rolsuper, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_authid_rolinherit, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_authid_rolcreaterole, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_authid_rolcreatedb, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_authid_rolcanlogin, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_authid_rolreplication, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_authid_rolbypassrls, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_authid_rolconnlimit, INT4OID);
        FillAttr(tupleDesc, Anum_pg_authid_rolpassword, TEXTOID);
        FillAttr(tupleDesc, Anum_pg_authid_rolvaliduntil, TIMESTAMPTZOID);
        auto& cacheItem = Items[AUTHOID] = std::make_unique<TSysCacheItem>(OidHasher1, OidEquals1, tupleDesc);
        auto& lookupMap = cacheItem->LookupMap;

        auto key = THeapTupleKey(1, 0, 0, 0);
        lookupMap.emplace(key, MakePgRolesHeapTuple(1, "postgres"));

        auto threadContextLookup = [&] (const THeapTupleKey& key) -> std::optional<HeapTuple> {
            if (std::get<0>(key) == PG_CURRENT_USER_ID && NKikimr::NMiniKQL::TlsAllocState) {
                auto ctx = (TMainContext*)NKikimr::NMiniKQL::TlsAllocState->MainContext;
                if (ctx && ctx->CurrentUserName) {
                    return ctx->CurrentUserName;
                }
            }
            return std::nullopt;
        };

        cacheItem->PgThreadContextLookup = std::move(threadContextLookup);
    }

    void InitializeRelNameNamespaces() {
        TupleDesc tupleDesc = CreateTemplateTupleDesc(Natts_pg_class);
        FillAttr(tupleDesc, Anum_pg_class_oid, OIDOID);
        FillAttr(tupleDesc, Anum_pg_class_relname, NAMEOID);
        FillAttr(tupleDesc, Anum_pg_class_relnamespace, OIDOID);
        FillAttr(tupleDesc, Anum_pg_class_reltype, OIDOID);
        FillAttr(tupleDesc, Anum_pg_class_reloftype, OIDOID);
        FillAttr(tupleDesc, Anum_pg_class_relowner, OIDOID);
        FillAttr(tupleDesc, Anum_pg_class_relam, OIDOID);
        FillAttr(tupleDesc, Anum_pg_class_relfilenode, OIDOID);
        FillAttr(tupleDesc, Anum_pg_class_reltablespace, OIDOID);
        FillAttr(tupleDesc, Anum_pg_class_relpages, INT4OID);
        FillAttr(tupleDesc, Anum_pg_class_reltuples, FLOAT4OID);
        FillAttr(tupleDesc, Anum_pg_class_relallvisible, INT4OID);
        FillAttr(tupleDesc, Anum_pg_class_reltoastrelid, OIDOID);
        FillAttr(tupleDesc, Anum_pg_class_relhasindex, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_class_relisshared, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_class_relpersistence, CHAROID);
        FillAttr(tupleDesc, Anum_pg_class_relkind, CHAROID);
        FillAttr(tupleDesc, Anum_pg_class_relnatts, INT2OID);
        FillAttr(tupleDesc, Anum_pg_class_relchecks, INT2OID);
        FillAttr(tupleDesc, Anum_pg_class_relhasrules, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_class_relhastriggers, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_class_relhassubclass, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_class_relrowsecurity, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_class_relforcerowsecurity, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_class_relispopulated, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_class_relreplident, CHAROID);
        FillAttr(tupleDesc, Anum_pg_class_relispartition, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_class_relrewrite, OIDOID);
        FillAttr(tupleDesc, Anum_pg_class_relfrozenxid, XIDOID);
        FillAttr(tupleDesc, Anum_pg_class_relminmxid, XIDOID);
        FillAttr(tupleDesc, Anum_pg_class_relacl, ACLITEMARRAYOID);
        FillAttr(tupleDesc, Anum_pg_class_reloptions, TEXTARRAYOID);
        FillAttr(tupleDesc, Anum_pg_class_relpartbound, PG_NODE_TREEOID);
        auto& cacheItem = Items[RELNAMENSP] = std::make_unique<TSysCacheItem>(NsNameHasher, NsNameEquals, tupleDesc);
        auto& lookupMap = cacheItem->LookupMap;

        Datum values[Natts_pg_class];
        bool nulls[Natts_pg_class];
        Zero(values);
        std::fill_n(nulls, Natts_pg_class, true);
        std::fill_n(nulls, Anum_pg_class_relminmxid, false); // fixed part of Form_pg_class
        for (const auto& t : NPg::GetStaticTables()) {
            auto name = (Datum)MakeFixedString(t.Name, NAMEDATALEN);
            auto ns = (Datum)(t.Schema == "pg_catalog" ? PG_CATALOG_NAMESPACE : 1);
            FillDatum(Natts_pg_class, values, nulls, Anum_pg_class_oid, (Datum)t.Oid);
            FillDatum(Natts_pg_class, values, nulls, Anum_pg_class_relname, name);
            FillDatum(Natts_pg_class, values, nulls, Anum_pg_class_relnamespace, ns);
            FillDatum(Natts_pg_class, values, nulls, Anum_pg_class_relowner, (Datum)1);
            HeapTuple h = heap_form_tuple(tupleDesc, values, nulls);
            auto row = (Form_pg_class) GETSTRUCT(h);
            Y_ENSURE(row->oid == t.Oid);
            Y_ENSURE(strcmp(NameStr(row->relname), t.Name.c_str()) == 0);
            Y_ENSURE(row->relowner == 1);
            Y_ENSURE(row->relnamespace == ns);

            auto key = THeapTupleKey(name, ns, 0, 0);
            lookupMap.emplace(key, h);
        }
    }

    void InitializeNameAndOidNamespaces() {
        TupleDesc tupleDesc = CreateTemplateTupleDesc(Natts_pg_namespace);
        FillAttr(tupleDesc, Anum_pg_namespace_oid, OIDOID);
        FillAttr(tupleDesc, Anum_pg_namespace_nspname, NAMEOID);
        FillAttr(tupleDesc, Anum_pg_namespace_nspowner, OIDOID);
        FillAttr(tupleDesc, Anum_pg_namespace_nspacl, ACLITEMARRAYOID);
        auto& cacheItem1 = Items[NAMESPACENAME] = std::make_unique<TSysCacheItem>(NsNameHasher, NsNameEquals, tupleDesc);
        auto& lookupMap1 = cacheItem1->LookupMap;
        auto& cacheItem2 = Items[NAMESPACEOID] = std::make_unique<TSysCacheItem>(OidHasher1, OidEquals1, tupleDesc);
        auto& lookupMap2 = cacheItem2->LookupMap;

        NPg::EnumNamespace([&](ui32 oid, const NPg::TNamespaceDesc& desc) {
            Datum values[Natts_pg_namespace];
            bool nulls[Natts_pg_namespace];
            Zero(values);
            std::fill_n(nulls, Natts_pg_namespace, true);
            FillDatum(Natts_pg_namespace, values, nulls, Anum_pg_namespace_oid, oid);
            auto name = MakeFixedString(desc.Name, NAMEDATALEN);
            FillDatum(Natts_pg_namespace, values, nulls, Anum_pg_namespace_nspname, (Datum)name);
            FillDatum(Natts_pg_namespace, values, nulls, Anum_pg_namespace_nspowner, (Datum)1);
            HeapTuple h = heap_form_tuple(tupleDesc, values, nulls);
            auto row = (Form_pg_namespace)GETSTRUCT(h);
            Y_ENSURE(row->oid == oid);
            Y_ENSURE(NameStr(row->nspname) == desc.Name);
            Y_ENSURE(row->nspowner == 1);

            auto key1 = THeapTupleKey((Datum)name, 0, 0, 0);
            lookupMap1.emplace(key1, h);
            auto key2 = THeapTupleKey((Datum)oid, 0, 0, 0);
            lookupMap2.emplace(key2, h);
        });
    }
};

}
}
namespace NKikimr {
namespace NMiniKQL {

void PgCreateSysCacheEntries(void* ctx) {
    auto main = (TMainContext*)ctx;
    if (main->GUCSettings) {
        if (main->GUCSettings->Get("ydb_database")) {
            main->CurrentDatabaseName = NYql::TSysCache::MakePgDatabaseHeapTuple(NYql::PG_CURRENT_DATABASE_ID, main->GUCSettings->Get("ydb_database")->c_str());
        }
        if (main->GUCSettings->Get("ydb_user")) {
            main->CurrentUserName = NYql::TSysCache::MakePgRolesHeapTuple(NYql::PG_CURRENT_USER_ID, main->GUCSettings->Get("ydb_user")->c_str());
        }
    }
}

} //namespace NKikimr
} //namespace NMiniKQL

HeapTuple SearchSysCache(int cacheId, Datum key1, Datum key2, Datum key3, Datum key4) {
    Y_ENSURE(cacheId >= 0 && cacheId < SysCacheSize);
    const auto& cacheItem = NYql::TSysCache::Instance().Items[cacheId];
    if (!cacheItem) {
        return nullptr;
    }
    const auto& lookupMap = cacheItem->LookupMap;
    auto it = lookupMap.find(std::make_tuple(key1, key2, key3, key4));
    if (it == lookupMap.end()) {
        if (cacheItem->PgThreadContextLookup) {
            if (auto value = cacheItem->PgThreadContextLookup(std::make_tuple(key1, key2, key3, key4))) {
                return *value;
            }
        }
        return nullptr;
    }

    return it->second;
}

HeapTuple SearchSysCache1(int cacheId, Datum key1) {
    return SearchSysCache(cacheId, key1, 0, 0, 0);
}

HeapTuple SearchSysCache2(int cacheId, Datum key1, Datum key2) {
    return SearchSysCache(cacheId, key1, key2, 0, 0);
}

HeapTuple SearchSysCache3(int cacheId, Datum key1, Datum key2, Datum key3) {
    return SearchSysCache(cacheId, key1, key2, key3, 0);
}

HeapTuple SearchSysCache4(int cacheId, Datum key1, Datum key2, Datum key3, Datum key4) {
    return SearchSysCache(cacheId, key1, key2, key3, key4);
}

void ReleaseSysCache(HeapTuple tuple) {
    Y_UNUSED(tuple);
}

Oid GetSysCacheOid(int cacheId, AttrNumber oidcol, Datum key1, Datum key2, Datum key3, Datum key4) {
    Y_ENSURE(cacheId >= 0 && cacheId < SysCacheSize);
    const auto& cacheItem = NYql::TSysCache::Instance().Items[cacheId];
    HeapTuple tuple;
    bool isNull;
    Oid result;

    tuple = SearchSysCache(cacheId, key1, key2, key3, key4);
    if (!HeapTupleIsValid(tuple)) {
        return InvalidOid;
    }

    result = heap_getattr(tuple, oidcol, cacheItem->Desc, &isNull);
    Y_ENSURE(!isNull); /* columns used as oids should never be NULL */
    ReleaseSysCache(tuple);
    return result;
}

Datum SysCacheGetAttr(int cacheId, HeapTuple tup, AttrNumber attributeNumber, bool *isNull) {
    Y_ENSURE(cacheId >= 0 && cacheId < SysCacheSize);
    const auto& cacheItem = NYql::TSysCache::Instance().Items[cacheId];
    Y_ENSURE(cacheItem);
    return heap_getattr(tup, attributeNumber, cacheItem->Desc, isNull);
}

struct catclist* SearchSysCacheList(int cacheId, int nkeys, Datum key1, Datum key2, Datum key3) {
    Y_ENSURE(cacheId >= 0 && cacheId < SysCacheSize);
    const auto& cacheItem = NYql::TSysCache::Instance().Items[cacheId];
    Y_ENSURE(cacheItem);
    if (nkeys == 1 && cacheItem->NumKeys > 1) {
       const auto& rangeMap1 = *cacheItem->RangeMap1;
        auto it = rangeMap1.find(std::make_tuple(key1, 0, 0, 0));
        if (it == rangeMap1.end()) {
            return cacheItem->EmptyCList;
        }

        return it->second.CList;
    }

    return cacheItem->EmptyCList;
}

