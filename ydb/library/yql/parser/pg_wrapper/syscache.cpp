#define SortBy PG_SortBy
#define TypeName PG_TypeName

#include "pg_compat.h"

extern "C" {
#include "utils/syscache.h"
#include "catalog/pg_database.h"
#include "catalog/pg_class.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "catalog/pg_type_d.h"
#include "catalog/pg_authid.h"
#include "access/htup_details.h"
#include "utils/fmgroids.h"
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

#include "arena_ctx.h"
#include "utils.h"

#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <unordered_map>
#include <functional>
#include <tuple>

namespace NYql {
namespace {

using THeapTupleKey = std::tuple<Datum, Datum, Datum, Datum>;
using THeapTupleHasher = std::function<size_t(const THeapTupleKey&)>;
using THeapTupleEquals = std::function<bool(const THeapTupleKey&, const THeapTupleKey&)>;
using TSysCacheHashMap = std::unordered_map<THeapTupleKey, HeapTuple, THeapTupleHasher, THeapTupleEquals>;

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

struct TSysCacheItem {
    TSysCacheItem(THeapTupleHasher hasher, THeapTupleEquals equals, TupleDesc desc)
        : Map(0, hasher, equals)
        , Desc(desc)
    {}

    TSysCacheHashMap Map;
    TupleDesc Desc;
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
        InitializeNameNamespaces();
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
        auto& map = cacheItem->Map;

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
            HeapTuple h = heap_form_tuple(tupleDesc, values, nulls);
            auto row = (Form_pg_proc)GETSTRUCT(h);
            Y_ENSURE(row->oid == oid);
            Y_ENSURE(row->prorettype == desc.ResultType);
            Y_ENSURE(NameStr(row->proname) == desc.Name);
            map.emplace(key, h);
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
        auto& cacheItems = Items[TYPEOID] = std::make_unique<TSysCacheItem>(OidHasher1, OidEquals1, tupleDesc);
        auto& map = cacheItems->Map;

        NPg::EnumTypes([&](ui32 oid, const NPg::TTypeDesc& desc){
            auto key = THeapTupleKey(oid, 0, 0, 0);

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
            FillDatum(Natts_pg_type, values, nulls, Anum_pg_type_typstorage, TYPSTORAGE_PLAIN);
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
            Y_ENSURE(row->typstorage == TYPSTORAGE_PLAIN);
            map.emplace(key, h);
        });

    }

    void InitializeDatabase() {
        TupleDesc tupleDesc = CreateTemplateTupleDesc(Natts_pg_database);
        FillAttr(tupleDesc, Anum_pg_database_oid, OIDOID);
        FillAttr(tupleDesc, Anum_pg_database_datname, NAMEOID);
        FillAttr(tupleDesc, Anum_pg_database_datdba, OIDOID);
        FillAttr(tupleDesc, Anum_pg_database_encoding, INT4OID);
        FillAttr(tupleDesc, Anum_pg_database_datcollate, NAMEOID);
        FillAttr(tupleDesc, Anum_pg_database_datctype, NAMEOID);
        FillAttr(tupleDesc, Anum_pg_database_datistemplate, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_database_datallowconn, BOOLOID);
        FillAttr(tupleDesc, Anum_pg_database_datconnlimit, INT4OID);
        FillAttr(tupleDesc, Anum_pg_database_datlastsysoid, OIDOID);
        FillAttr(tupleDesc, Anum_pg_database_datfrozenxid, XIDOID);
        FillAttr(tupleDesc, Anum_pg_database_datminmxid, XIDOID);
        FillAttr(tupleDesc, Anum_pg_database_dattablespace, OIDOID);
        FillAttr(tupleDesc, Anum_pg_database_datacl, ACLITEMARRAYOID);
        auto& cacheItems = Items[DATABASEOID] = std::make_unique<TSysCacheItem>(OidHasher1, OidEquals1, tupleDesc);
        auto& map = cacheItems->Map;

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
            case 3: name = "postgres"; break;
            }
            Y_ENSURE(name);
            FillDatum(Natts_pg_database, values, nulls, Anum_pg_database_datname, (Datum)MakeFixedString(name, NAMEDATALEN));
            HeapTuple h = heap_form_tuple(tupleDesc, values, nulls);
            auto row = (Form_pg_database) GETSTRUCT(h);
            Y_ENSURE(row->oid == oid);
            Y_ENSURE(strcmp(NameStr(row->datname), name) == 0);
            map.emplace(key, h);
        }
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
        auto& cacheItems = Items[AUTHOID] = std::make_unique<TSysCacheItem>(OidHasher1, OidEquals1, tupleDesc);
        auto& map = cacheItems->Map;

        auto key = THeapTupleKey(1, 0, 0, 0);

        const char* rolname = "postgres";
        const ui32 oid = 1;
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
        map.emplace(key, h);
    }

    void InitializeNameNamespaces() {
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
        auto& cacheItems = Items[RELNAMENSP] = std::make_unique<TSysCacheItem>(NsNameHasher, NsNameEquals, tupleDesc);
        auto& map = cacheItems->Map;

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
            map.emplace(key, h);
        }
    }
};

}
}


HeapTuple SearchSysCache(int cacheId, Datum key1, Datum key2, Datum key3, Datum key4) {
    Y_ENSURE(cacheId >= 0 && cacheId < SysCacheSize);
    const auto& cacheItem = NYql::TSysCache::Instance().Items[cacheId];
    if (!cacheItem) {
        return nullptr;
    }

    const auto& map = cacheItem->Map;
    auto it = map.find(std::make_tuple(key1, key2, key3, key4));
    if (it == map.end()) {
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
    if (!HeapTupleIsValid(tuple))
        return InvalidOid;
    result = heap_getattr(tuple, oidcol, cacheItem->Desc, &isNull);
    Y_ENSURE(!isNull); /* columns used as oids should never be NULL */
    ReleaseSysCache(tuple);
    return result;
}


