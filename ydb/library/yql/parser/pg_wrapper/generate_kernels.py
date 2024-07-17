#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import zlib
from collections import defaultdict

SPLIT_FILES = 20

def is_strict(oid_per_name, funcs, name):
    found = None
    for oid in oid_per_name[name]:
        strict = funcs[oid][1]
        if found is None:
            found = strict
        else:
            assert found == strict
    return "true" if found else "false"

def is_result_fixed(oid_per_name, catalog_by_oid, name):
    found = None
    for oid in oid_per_name[name]:
        fixed = catalog_by_oid[oid]["ret_type_fixed"]
        if found is None:
            found = fixed
        else:
            assert found == fixed
    return "true" if found else "false"

def get_fixed_args(oid_per_name, catalog_by_oid, name):
    found = None
    for oid in oid_per_name[name]:
        if "var_type" in catalog_by_oid[oid]:
            return None
        fixed = [x["arg_type_fixed"] for x in catalog_by_oid[oid]["args"]]
        if found is None:
            found = fixed
        else:
            # e.g. range_constructor2
            if found != fixed:
                return None
    return found

def main():
    pg_sources = []
    with open("pg_sources.inc") as f:
        for line in f:
            pg_sources.append(line.rstrip())
    with open("../../tools/pg_catalog_dump/dump.json") as f:
        catalog = json.load(f)
    catalog_by_oid = {}
    catalog_funcs = set()
    for proc in catalog["proc"]:
        catalog_by_oid[proc["oid"]] = proc
        catalog_funcs.add(proc["src"])
    catalog_aggs_by_id = {}
    for agg in catalog["aggregation"]:
        if not agg["combine_func_id"]:
            continue
        catalog_aggs_by_id[agg["agg_id"]] = agg
        assert len(agg["args"]) <= 2

    funcs={}
    with open("postgresql/src/backend/utils/fmgrtab.c") as f:
        parse=False
        for line in f:
            if "fmgr_builtins[]" in line:
                parse=True
                continue
            if not parse:
                continue
            if line.startswith("}"):
                parse=False
                continue
            c=line.strip()[1:-2].split(", ")
            oid=int(c[0])
            nargs=int(c[1])
            strict=c[2].strip()=="true"
            retset=c[3].strip()=="true"
            name=c[4].strip().strip('"')
            func=c[5].strip()
            if retset: continue
            if name!=func:
                print(name,func)
                continue
            if not oid in catalog_by_oid:
                print("skipped by catalog: ",name)
                continue
            funcs[oid] = (nargs, strict, name)
    print("funcs: ", len(funcs))
    func_names=set(x[2] for x in funcs.values())
    print("unique names: ", len(func_names))
    print("aggs: ", len(catalog_aggs_by_id))
    oid_per_name={}
    all_found_funcs=set()
    for x in funcs:
        name = funcs[x][2]
        if not name in oid_per_name:
            oid_per_name[name]=[]
        oid_per_name[name].append(x)
    symbols={}
    for i in range(len(pg_sources)):
        line = pg_sources[i]
        if not line.endswith(".c"):
            continue
        cfile = line.strip()
        found_funcs = set()
        #print(cfile)
        with open(cfile) as f:
            for srcline in f:
                pos=srcline.find("(PG_FUNCTION_ARGS)")
                if pos!=-1:
                    names=[srcline[0:pos].strip()]
                elif srcline.startswith("CMPFUNC("):
                    pos=srcline.find(",")
                    names=[srcline[8:pos]]
                elif srcline.startswith("TSVECTORCMPFUNC("):
                    pos=srcline.find(",")
                    names=["tsvector_"+srcline[16:pos]]
                elif srcline.startswith("PSEUDOTYPE_DUMMY_IO_FUNCS("):
                    pos=srcline.find(")")
                    names=[srcline[26:pos]+"_in", srcline[26:pos]+"_out"]
                elif srcline.startswith("PSEUDOTYPE_DUMMY_INPUT_FUNC(") and "\\" not in srcline:
                    pos=srcline.find(")")
                    names=[srcline[28:pos]+"_in"]
                elif srcline.startswith("PSEUDOTYPE_DUMMY_BINARY_IO_FUNCS"):
                    pos=srcline.find(")")
                    names=[srcline[33:pos]+"_send", srcline[33:pos]+"_recv"]
                elif srcline.startswith("PSEUDOTYPE_DUMMY_RECEIVE_FUNC(") and "\\" not in srcline:
                    pos=srcline.find(")")
                    names=[srcline[30:pos]+"_recv"]
                elif srcline.startswith("PG_STAT_GET_DBENTRY_FLOAT8_MS("):
                    pos=srcline.find(")")
                    names=["pg_stat_get_db_" + srcline[30:pos]]
                elif srcline.startswith("PG_STAT_GET_DBENTRY_INT64("):
                    pos=srcline.find(")")
                    names=["pg_stat_get_db_" + srcline[26:pos]]
                elif srcline.startswith("PG_STAT_GET_RELENTRY_INT64("):
                    pos=srcline.find(")")
                    names=["pg_stat_get_" + srcline[27:pos]]
                elif srcline.startswith("PG_STAT_GET_RELENTRY_TIMESTAMPTZ("):
                    pos=srcline.find(")")
                    names=["pg_stat_get_" + srcline[33:pos]]
                elif srcline.startswith("PG_STAT_GET_XACT_RELENTRY_INT64("):
                    pos=srcline.find(")")
                    names=["pg_stat_get_xact_" + srcline[32:pos]]
                elif srcline.startswith("PG_STAT_GET_FUNCENTRY_FLOAT8_MS("):
                    pos=srcline.find(")")
                    names=["pg_stat_get_function_" + srcline[32:pos]]
                elif srcline.startswith("PG_STAT_GET_XACT_FUNCENTRY_FLOAT8_MS("):
                    pos=srcline.find(")")
                    names=["pg_stat_get_xact_function_" + srcline[37:pos]]
                else:
                    continue
                for name in names:
                    if name in func_names:
                        found_funcs.add(name)
                        all_found_funcs.add(name)
        if not found_funcs:
            continue                        
        print(cfile, len(found_funcs))
        symbols[cfile] = found_funcs
    split_symbols = []
    split_all_found_funcs = []
    for i in range(SPLIT_FILES):
        curr_symbols = {}
        curr_all_found_funcs = set()
        for cfile in symbols:
            if zlib.crc32(cfile.encode("utf8")) % SPLIT_FILES != i: continue
            curr_symbols[cfile] = symbols[cfile]
            curr_all_found_funcs.update(symbols[cfile])

        split_symbols.append(curr_symbols)
        split_all_found_funcs.append(curr_all_found_funcs)
    # check if all functions are available
    split_for_agg = {}
    for agg in catalog_aggs_by_id.values():
        oids = set()
        oids.add(agg["trans_func_id"])
        if agg["serialize_func_id"]:
            assert catalog_by_oid[agg["serialize_func_id"]]["strict"]
            oids.add(agg["serialize_func_id"])
        if agg["deserialize_func_id"]:
            assert catalog_by_oid[agg["deserialize_func_id"]]["strict"]
            oids.add(agg["deserialize_func_id"])
        if agg["final_func_id"]:
            oids.add(agg["final_func_id"])
        oids.add(agg["combine_func_id"])
        names = [catalog_by_oid[oid]["src"] for oid in oids]

    for i in range(SPLIT_FILES):
        with open("pg_bc."+str(i)+".inc", "w") as bc:
            bc.write("LLVM_BC(\n" + \
                "\n".join(("    " + x) for x in sorted(split_symbols[i].keys())) + \
                "\n    pg_kernels."+str(i)+".cpp\n" + \
                "\n    NAME PgFuncs" + str(i) + "\n" + \
                "\n    SYMBOLS\n" + \
                "\n".join(("    arrow_" + x) for x in sorted(split_all_found_funcs[i])) + \
                "\n)\n")
    for i in range(SPLIT_FILES):                
        with open("pg_proc_policies." + str(i) + ".inc", "w") as p:
            for x in sorted(split_all_found_funcs[i]):
                fixed_args = get_fixed_args(oid_per_name, catalog_by_oid, x)
                if fixed_args is not None:
                    p.write("struct TArgs_NAME_Policy {\n".replace("NAME", x))
                    p.write("    static constexpr bool VarArgs = false;\n")
                    p.write("    static constexpr std::array<bool, N> IsFixedArg = {V};\n" \
                        .replace("N", str(len(fixed_args))) \
                        .replace("V", ",".join("true" if x else "false" for x in fixed_args)))
                    p.write("};\n")
                else:
                    print("polymorphic args:", x)
    for i in range(SPLIT_FILES):
        with open("pg_kernels." + str(i) + ".inc", "w") as k:
            for x in sorted(split_all_found_funcs[i]):
                fixed_args = get_fixed_args(oid_per_name, catalog_by_oid, x)
                k.write(
                    "TExecFunc arrow_NAME() { return TGenericExec<TPgDirectFunc<&NAME>, STRICT, IS_RESULT_FIXED, POLICY>({}); }\n" \
                        .replace("NAME", x) \
                        .replace("STRICT", is_strict(oid_per_name, funcs, x)) \
                        .replace("IS_RESULT_FIXED", is_result_fixed(oid_per_name, catalog_by_oid, x))
                        .replace("POLICY", "TArgs_" + x + "_Policy" if fixed_args is not None else "TDefaultArgsPolicy"))
    for i in range(SPLIT_FILES):
        with open("pg_kernels.slow." + str(i) + ".inc", "w") as k:
            for x in sorted(split_all_found_funcs[i]):
                k.write(
                    "TExecFunc arrow_NAME() { return MakeIndirectExec<STRICT, IS_RESULT_FIXED>(&NAME); }\n" \
                        .replace("NAME", x) \
                        .replace("STRICT", is_strict(oid_per_name, funcs, x)) \
                        .replace("IS_RESULT_FIXED", is_result_fixed(oid_per_name, catalog_by_oid, x)))
    with open("pg_kernels_fwd.inc", "w") as k:
        k.write(\
            "\n".join("extern TExecFunc arrow_NAME();".replace("NAME", x) for x in sorted(all_found_funcs)) + \
            "\n")
    for i in range(SPLIT_FILES):            
        with open("pg_kernels_register." + str(i) + ".inc", "w") as r:
            for name in oid_per_name:
                if not name in split_all_found_funcs[i]: continue
                for oid in sorted(oid_per_name[name]):
                    r.write("RegisterExec(" + str(oid) + ", arrow_" + name + "());\n")
    for slow in [False, True]:
        with open("pg_aggs" + (".slow" if slow else "") + ".inc","w") as p:
            for agg_id in sorted(catalog_aggs_by_id.keys()):
                agg = catalog_aggs_by_id[agg_id]
                trans_func = catalog_by_oid[agg["trans_func_id"]]["src"]
                trans_fixed_args = None if slow else get_fixed_args(oid_per_name, catalog_by_oid, trans_func)
                combine_func = catalog_by_oid[agg["combine_func_id"]]["src"]
                combine_fixed_args = None if slow else get_fixed_args(oid_per_name, catalog_by_oid, combine_func)
                serialize_func = ""
                serialize_fixed_args = None
                if agg["serialize_func_id"]:
                    serialize_func = catalog_by_oid[agg["serialize_func_id"]]["src"]
                    serialize_fixed_args = None if slow else get_fixed_args(oid_per_name, catalog_by_oid, serialize_func)
                deserialize_func = ""
                deserialize_fixed_args = None
                if agg["deserialize_func_id"]:
                    deserialize_func = catalog_by_oid[agg["deserialize_func_id"]]["src"]
                    deserialize_fixed_args = None if slow else get_fixed_args(oid_per_name, catalog_by_oid, deserialize_func)
                final_func = ""
                final_fixed_args = None
                if agg["final_func_id"]:
                    final_func = catalog_by_oid[agg["final_func_id"]]["src"]
                    final_fixed_args = None if slow else get_fixed_args(oid_per_name, catalog_by_oid, final_func)

                p.write("auto MakePgAgg_" + agg["name"] + "_" + str(agg_id) + "() {\n"
                    "    return TGenericAgg<\n \
                    TRANS_FUNC, IS_TRANS_STRICT, TRANS_ARGS_POLICY,\n \
                    COMBINE_FUNC, IS_COMBINE_STRICT, COMBINE_ARGS_POLICY,\n \
                    HAS_SERIALIZE_FUNC, SERIALIZE_FUNC1, SERIALIZE_ARGS_POLICY1,\n \
                    HAS_DESERIALIZE_FUNC, DESERIALIZE_FUNC, DESERIALIZE_ARGS_POLICY,\n \
                    HAS_FINAL_FUNC, FINAL_FUNC, IS_FINAL_STRICT, FINAL_ARGS_POLICY,\n \
                    TRANS_TYPE_FIXED, SERIALIZED_TYPE_FIXED, FINAL_TYPE_FIXED, HAS_INIT_VALUE\n \
                    >(TRANS_OBJ, COMBINE_OBJ, SERIALIZE1_OBJ, DESERIALIZE_OBJ, FINAL_OBJ);\n" \
                    .replace("TRANS_FUNC", "TPgIndirectFunc" if slow else "TPgDirectFunc<&" + trans_func + ">") \
                    .replace("IS_TRANS_STRICT", "true" if catalog_by_oid[agg["trans_func_id"]]["strict"] else "false") \
                    .replace("TRANS_ARGS_POLICY", "TArgs_" + trans_func + "_Policy" if trans_fixed_args is not None else "TDefaultArgsPolicy") \
                    .replace("COMBINE_FUNC", "TPgIndirectFunc" if slow else "TPgDirectFunc<&" + combine_func + ">") \
                    .replace("IS_COMBINE_STRICT", "true" if catalog_by_oid[agg["combine_func_id"]]["strict"] else "false") \
                    .replace("COMBINE_ARGS_POLICY", "TArgs_" + combine_func + "_Policy" if combine_fixed_args is not None else "TDefaultArgsPolicy") \
                    .replace("HAS_SERIALIZE_FUNC", "true" if serialize_func else "false") \
                    .replace("SERIALIZE_FUNC1", "TPgDirectFunc<&" + serialize_func + ">" if serialize_func and not slow else "TPgIndirectFunc") \
                    .replace("SERIALIZE_ARGS_POLICY1", "TArgs_" + serialize_func + "_Policy" if serialize_fixed_args is not None else "TDefaultArgsPolicy") \
                    .replace("HAS_DESERIALIZE_FUNC", "true" if deserialize_func else "false") \
                    .replace("DESERIALIZE_FUNC", "TPgDirectFunc<&" + deserialize_func + ">" if deserialize_func and not slow else "TPgIndirectFunc") \
                    .replace("DESERIALIZE_ARGS_POLICY", "TArgs_" + deserialize_func + "_Policy" if deserialize_fixed_args is not None else "TDefaultArgsPolicy") \
                    .replace("HAS_FINAL_FUNC", "true" if final_func else "false") \
                    .replace("FINAL_FUNC", "TPgDirectFunc<&" + final_func + ">" if final_func and not slow else "TPgIndirectFunc") \
                    .replace("IS_FINAL_STRICT", "true" if final_func and catalog_by_oid[agg["final_func_id"]]["strict"] else "false") \
                    .replace("FINAL_ARGS_POLICY", "TArgs_" + final_func + "_Policy" if final_fixed_args is not None else "TDefaultArgsPolicy") \
                    .replace("TRANS_TYPE_FIXED", "true" if agg["trans_type_fixed"] else "false") \
                    .replace("SERIALIZED_TYPE_FIXED", "true" if agg["serialized_type_fixed"] else "false") \
                    .replace("FINAL_TYPE_FIXED", "true" if agg["ret_type_fixed"] else "false") \
                    .replace("HAS_INIT_VALUE", "true" if agg["has_init_value"] else "false") \
                    .replace("TRANS_OBJ", "&" + trans_func if slow else "{}") \
                    .replace("COMBINE_OBJ", "&" + combine_func if slow else "{}") \
                    .replace("SERIALIZE1_OBJ", ("&" + serialize_func if slow else "{}") if serialize_func else "nullptr") \
                    .replace("DESERIALIZE_OBJ", ("&" +deserialize_func if slow else "{}") if deserialize_func else "nullptr") \
                    .replace("FINAL_OBJ", ("&" + final_func if slow else "{}") if final_func else "nullptr") \
                )
                p.write("}\n")

    agg_names = defaultdict(list)
    with open("pg_aggs_register.inc","w") as p:
        for agg_id in sorted(catalog_aggs_by_id.keys()):
            agg_names[catalog_aggs_by_id[agg_id]["name"]].append(agg_id)
        for name in agg_names:
            p.write(
                ("class TPgAggFactory_NAME: public IBlockAggregatorFactory {\n" \
                "std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(\n" \
                "    TTupleType* tupleType,\n" \
                "    std::optional<ui32> filterColumn,\n" \
                "    const std::vector<ui32>& argsColumns,\n" \
                "    const TTypeEnvironment& env) const final {\n" \
                "    const auto& aggDesc = ResolveAggregation(\"NAME\", tupleType, argsColumns, nullptr);\n" \
                "    switch (aggDesc.AggId) {\n" +
                "".join(["    case " + str(agg_id) + ": return MakePgAgg_NAME_" + str(agg_id) + "().PrepareCombineAll(filterColumn, argsColumns, aggDesc);\n" for agg_id in agg_names[name]]) +
                "    default: throw yexception() << \"Unsupported agg id: \" << aggDesc.AggId;\n" \
                "    }\n" \
                "}\n" \
                "\n" \
                "std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(\n" \
                "    TTupleType* tupleType,\n" \
                "    const std::vector<ui32>& argsColumns,\n" \
                "    const TTypeEnvironment& env) const final {\n" \
                "    const auto& aggDesc = ResolveAggregation(\"NAME\", tupleType, argsColumns, nullptr);\n"                
                "    switch (aggDesc.AggId) {\n" +
                "".join(["    case " + str(agg_id) + ": return MakePgAgg_NAME_" + str(agg_id) + "().PrepareCombineKeys(argsColumns, aggDesc);\n" for agg_id in agg_names[name]]) +
                "    default: throw yexception() << \"Unsupported agg id: \" << aggDesc.AggId;\n" \
                "    }\n" \
                "}\n" \
                "\n" \
                "std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(\n" \
                "    TTupleType* tupleType,\n" \
                "    const std::vector<ui32>& argsColumns,\n" \
                "    const TTypeEnvironment& env,\n" \
                "    TType* returnType) const final {\n" \
                "    const auto& aggDesc = ResolveAggregation(\"NAME\", tupleType, argsColumns, returnType);\n"
                "    switch (aggDesc.AggId) {\n" +
                "".join(["    case " + str(agg_id) + ": return MakePgAgg_NAME_" + str(agg_id) + "().PrepareFinalizeKeys(argsColumns.front(), aggDesc);\n" for agg_id in agg_names[name]]) +
                "    default: throw yexception() << \"Unsupported agg id: \" << aggDesc.AggId;\n" \
                "    }\n" \
                "}\n" \
                "};\n").replace("NAME", name))
        for name in agg_names:
            p.write('registry.emplace("pg_' + name + '", std::make_unique<TPgAggFactory_' + name + '>());\n')

    for i in range(SPLIT_FILES):
        with open("pg_kernels." + str(i) + ".cpp","w") as f:
            f.write(
                'extern "C" {\n'
                '#include "postgres.h"\n'
                '#include "fmgr.h"\n'
                '#include "postgresql/src/backend/utils/fmgrprotos.h"\n'
                '#undef Abs\n'
                '#undef Min\n'
                '#undef Max\n'
                '#undef TypeName\n'
                '#undef SortBy\n'
                '#undef Sort\n'
                '#undef Unique\n'
                '#undef LOG\n'
                '#undef INFO\n'
                '#undef NOTICE\n'
                '#undef WARNING\n'
                '#undef ERROR\n'
                '#undef FATAL\n'
                '#undef PANIC\n'
                '#undef open\n'
                '#undef fopen\n'
                '#undef bind\n'
                '#undef locale_t\n'
                '#undef strtou64\n'
                '}\n'
                '\n'
                '#include "arrow.h"\n'
                '\n'
                'namespace NYql {\n'
                '\n'
                'extern "C" {\n'
                '\n'
                'Y_PRAGMA_DIAGNOSTIC_PUSH\n'
                'Y_PRAGMA("GCC diagnostic ignored \\"-Wreturn-type-c-linkage\\"")\n'
                '#ifdef USE_SLOW_PG_KERNELS\n'
                '#include "pg_kernels.slow.INDEX.inc"\n'
                '#else\n'
                '#include "pg_proc_policies.INDEX.inc"\n'
                '#include "pg_kernels.INDEX.inc"\n'
                '#endif\n'
                'Y_PRAGMA_DIAGNOSTIC_POP\n'
                '\n'
                '}\n'
                '\n'
                '}\n'.replace("INDEX",str(i))
            )

    with open("pg_kernels_register.all.inc","w") as f:
        for i in range(SPLIT_FILES):
            f.write('#include "pg_kernels_register.INDEX.inc"\n'.replace("INDEX", str(i)))

    with open("pg_proc_policies.all.inc","w") as f:
        for i in range(SPLIT_FILES):
            f.write('#include "pg_proc_policies.INDEX.inc"\n'.replace("INDEX", str(i)))

    with open("pg_kernel_sources.inc","w") as f:
        f.write("SRCS(\n")
        for i in range(SPLIT_FILES):
            f.write('    pg_kernels.INDEX.cpp\n'.replace("INDEX", str(i)))
        f.write(")\n")

    with open("pg_bc.all.inc","w") as f:
        for i in range(SPLIT_FILES):
            f.write('INCLUDE(pg_bc.INDEX.inc)\n'.replace("INDEX", str(i)))

    print("found funcs: ",len(all_found_funcs))
    print("agg names: ",len(agg_names))
    print("agg funcs: ",len(catalog_aggs_by_id))
    missing=func_names.difference(all_found_funcs)
    if missing:
        print("missing funcs: ",len(missing))
        print(missing)

if __name__ == "__main__":
    main()
