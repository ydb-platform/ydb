#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
from shutil import Error, copy2, rmtree
import subprocess
from collections import defaultdict

all_vars = set()
all_funcs_with_statics = defaultdict(list)
thread_funcs = []
define_for_yylval = None
skip_func = False
erase_func = False
split_def = False
def_type = None
def_var = None
ignore_func = False
inside_func = None

to_add_const = set([
    "nullSemAction",
    "sentinel",
    "backslash_quote",
    "Dummy_trace",
    "escape_string_warning",
    "standard_conforming_strings",
    "gistBufferingOptValues",
    "StdRdOptIndexCleanupValues",
    "boolRelOpts",
    "intRelOpts",
    "realRelOpts",
    "viewCheckOptValues",
    "enumRelOpts",
    "stringRelOpts"])

source_dirs = [
    "postgresql/src/backend",
    "postgresql/src/common",
    "postgresql/src/include",
    "postgresql/src/port",
    "postgresql/src/timezone",
]

def is_inside_source_dirs(filename):
    for dir in source_dirs:
        if filename.startswith(dir):
            return True
    return False

no_copy_sources = [
    # not used (and linux/darwin-only)
    "postgresql/src/backend/port/posix_sema.c",
    "postgresql/src/backend/port/sysv_shmem.c",
    # not used, was excluded earlier
    "postgresql/src/include/utils/help_config.h",
    "postgresql/src/backend/utils/misc/help_config.c",
    # provided in libc_compat
    "postgresql/src/port/strlcat.c",
    "postgresql/src/port/strlcpy.c",

    # unneded headers
    "postgresql/src/common/md5_int.h",
    "postgresql/src/common/sha1_int.h",
    "postgresql/src/common/sha2_int.h",
    "postgresql/src/include/common/connect.h",
    "postgresql/src/include/common/logging.h",
    "postgresql/src/include/common/restricted_token.h",
    "postgresql/src/include/fe_utils/",
    "postgresql/src/include/getopt_long.h",
    "postgresql/src/include/jit/llvmjit.h",
    "postgresql/src/include/jit/llvmjit_emit.h",
    "postgresql/src/include/libpq/be-gssapi-common.h",
    "postgresql/src/include/port/aix.h",
    "postgresql/src/include/port/atomics/arch-hppa.h",
    "postgresql/src/include/port/atomics/arch-ia64.h",
    "postgresql/src/include/port/atomics/arch-ppc.h",
    "postgresql/src/include/port/atomics/generic-acc.h",
    "postgresql/src/include/port/atomics/generic-sunpro.h",
    "postgresql/src/include/port/cygwin.h",
    "postgresql/src/include/port/darwin.h",
    "postgresql/src/include/port/freebsd.h",
    "postgresql/src/include/port/hpux.h",
    "postgresql/src/include/port/linux.h",
    "postgresql/src/include/port/netbsd.h",
    "postgresql/src/include/port/openbsd.h",
    "postgresql/src/include/port/pg_pthread.h",
    "postgresql/src/include/port/solaris.h",
    "postgresql/src/include/port/win32/dlfcn.h",
    "postgresql/src/include/port/win32.h",
    "postgresql/src/include/replication/pgoutput.h",
    "postgresql/src/include/snowball/",
    "postgresql/src/port/pthread-win32.h",
]

def need_copy(filename):
    if not is_inside_source_dirs(filename):
        return False
    for prefix in no_copy_sources:
        if filename.startswith(prefix):
            return False
    return True

exclude_from_source_list = set([
    # platform-specific, explicitly added in ya.make
    "postgresql/src/port/pg_crc32c_sse42.c",
    "postgresql/src/port/pg_crc32c_sse42_choose.c",
])

def fix_line(line, all_lines, pos):
    global inside_func
    global define_for_yylval
    if line.startswith("#define yylval"):
        define_for_yylval=line[14:].strip()

    if line.startswith("#define HAVE_EXECINFO_H 1"):
        return "#undef HAVE_EXECINFO_H\n"

    if line.startswith("#define HAVE_BACKTRACE_SYMBOLS 1"):
        return "#undef HAVE_BACKTRACE_SYMBOLS\n"

    if "static YYSTYPE yyval_default" in line or \
       "static YYLTYPE yyloc_default" in line:
        return line.replace("static","static __thread")

    global skip_func
    global erase_func
    if line.startswith("build_guc_variables(void)"):
       skip_func = True
       return line

    global ignore_func
    if line.startswith("yyparse"):
       ignore_func = True
       return line

    if inside_func is not None:
       for v in all_funcs_with_statics[inside_func]:
          if v in line and "static" in line:
              return line.replace("static","static __thread")

    if inside_func:
       if line.startswith("}"):
           inside_func=None

    if skip_func:
       if line.startswith("{"):
          return None if erase_func else line
       if not line.startswith("}"):
          return None
       skip_func=False
       if erase_func:
          erase_func=False
          return None

    if ignore_func:
       if line.startswith("{"):
          return line
       if not line.startswith("}"):
          return line
       ignore_func=False

    global split_def
    global def_type
    global def_var
    if line.startswith("static struct xllist"):
       split_def = True
       def_type = "xllist"
       def_var = "records";
       return "typedef struct xllist\n";

    if line.startswith("static struct RELCACHECALLBACK"):
       split_def = True
       def_type = "RELCACHECALLBACK"
       def_var = "relcache_callback_list[MAX_RELCACHE_CALLBACKS]";
       return "typedef struct RELCACHECALLBACK\n";

    if line.startswith("static struct SYSCACHECALLBACK"):
       split_def = True
       def_type = "SYSCACHECALLBACK"
       def_var = "syscache_callback_list[MAX_SYSCACHE_CALLBACKS]";
       return "typedef struct SYSCACHECALLBACK\n";

    if split_def and line.startswith("}"):
       split_def = False;
       return "} " + def_type + "; static __thread " + def_type + " " + def_var + ";\n"

    if line.strip()=="static struct":
       i = pos
       while i < len(all_lines):
          if all_lines[i].startswith("}"):
             name = all_lines[i][1:].replace(";","").strip()
             split_def = True
             def_type = name + "_t"
             def_var = name
             return "typedef struct " + def_type + "\n";
          i += 1

    if "ConfigureNames" in line and line.strip().endswith("[] ="):
       skip_func = True
       erase_func = True
       return None

    if line.startswith("#") or line.startswith(" ") or line.startswith("\t"):
        return line

    for f in all_funcs_with_statics:
       if f in line and ";" not in line:
           inside_func = f
           return line

    if not "=" in line:
        line2=line
        if "//" in line2: line2 = line2[:line2.find("//")]
        if "/*" in line2: line2 = line2[:line2.find("/*")]

        if "(" in line2 or "{" in line2 or "}" in line2:
            return line

        if ";" not in line2:
            return line

    if line.startswith("YYSTYPE yylval;"):
        line = line.replace("yylval", define_for_yylval)

    norm = line.replace("\t"," ")

    ret = None
    found_v = None
    for v in to_add_const:
       if v in norm:
          ret = line \
              .replace("static","static const") \
              .replace("relopt_enum_elt_def","const relopt_enum_elt_def")

          if v == "backslash_quote":
              ret = ret.replace("int","const int")

          if v == "escape_string_warning" or v == "standard_conforming_strings":
              ret = ret.replace("bool","const bool")

          if v == "nullSemAction":
              ret = ret.replace("JsonSemAction","const JsonSemAction")

          return ret

    for v in all_vars:
        if " " + v + " " in norm or " " + v + ";" in norm or " " + v + "[" in norm or \
           "*" + v + " " in norm or "*" + v + ";" in norm or "*" + v + "[" in norm:
           found_v = v
           if line.startswith("static"):
               ret = "static __thread" + line[6:]
           elif line.startswith("extern"):
               ret = "extern __thread" + line[6:]
           else:
               ret = "__thread " + line
           break

    if ret is None:
        return line

    if "DLIST_STATIC_INIT" in ret:
        # rewrite without {{}} inits
        pos=ret.find("=");
        ret=ret[:pos] + ";";
        ret+="void "+found_v+"_init(void) { dlist_init(&" + found_v + "); }";
        ret+="\n";
        thread_funcs.append(found_v+"_init");

    if "DCLIST_STATIC_INIT" in ret:
        # rewrite without {{}} inits
        pos=ret.find("=");
        ret=ret[:pos] + ";";
        ret+="void "+found_v+"_init(void) { dlist_init(&" + found_v + ".dlist); " + found_v + ".count = 0; }";
        ret+="\n";
        thread_funcs.append(found_v+"_init");

    if "CurrentTransactionState" in ret or "mainrdata_last" in ret:
        # rewrite with address of TLS var
        pos=ret.find("=");
        init_val=ret[pos+1:];
        ret=ret[:pos] + ";";
        ret+="void "+found_v+"_init(void) { "+found_v+"="+init_val +" };"
        ret+="\n";
        thread_funcs.append(found_v+"_init");

    return ret

def mycopy2(src, dst):
    global define_for_yylval
    define_for_yylval = None
    if not (src.endswith(".h") or src.endswith(".c")):
        return
    with open(src,"r") as fsrc:
        with open(dst,"w") as fdst:
            all_lines = list(fsrc)
            for pos,line in enumerate(all_lines):
                line = fix_line(line,all_lines,pos)
                if line is not None:
                    fdst.write(line)

def copy_and_patch_sources(src_dir):
    errors = []
    with open(os.path.join(src_dir, "src_files"), "r") as fd:
        for line in fd:
            name = line.strip()
            if not need_copy(name):
                continue
            srcname = os.path.join(src_dir, name)
            if name == "postgresql/src/include/pg_config.h":
                dstname = "postgresql/src/include/pg_config-linux.h"
            else:
                dstname = name
            try:
                os.makedirs(os.path.dirname(dstname), mode=0o755, exist_ok=True)
                if os.path.islink(srcname):
                    target_full = os.path.realpath(srcname)
                    target = os.path.relpath(target_full, start=os.path.realpath(os.path.dirname(srcname)))
                    with open(dstname, "w") as f:
                        print('#include "' + target + '" /* inclink generated by yamaker */', file=f)
                else:
                    mycopy2(srcname, dstname)
            except OSError as why:
                errors.append((srcname, dstname, str(why)))
            except Error as err:
                errors.extend(err.args[0])
        if errors:
            raise Error(errors)

def make_sources_list(build_dir):
    with open(f"{build_dir}/src_files","r") as fsrc:
        with open("pg_sources.inc","w") as fdst:
            fdst.write("SRCS(\n")
            for line in fsrc:
                #print(line.strip())
                name = line.strip()
                if name.endswith(".funcs.c"): continue
                if name.endswith(".switch.c"): continue
                basename = os.path.basename(name)
                if basename.startswith("regc_") and basename.endswith(".c"): continue
                if basename == "rege_dfa.c": continue
                if name.endswith(".c") and need_copy(name) and name not in exclude_from_source_list:
                    fdst.write("    " + name + "\n")
            fdst.write(")\n")

def get_vars(build_dir):
    s=subprocess.check_output(f"objdump {build_dir}/postgresql/src/backend/postgres.a -tw",shell=True).decode("utf-8")
    for a in s.replace("\t"," ").split("\n"):
        for b in a.split(" "):
            sym=None
            if b.startswith(".bss."): sym=b[5:]
            elif b.startswith(".data.") and not b.startswith(".data.rel.ro."): sym=b[6:]
            if sym is not None:
                all_vars.add(sym.replace("yql_",""))

    for x in to_add_const:
        all_vars.remove(x)

    all_vars.remove("BlockSig")
    all_vars.remove("StartupBlockSig")
    all_vars.remove("UnBlockSig")
    all_vars.remove("on_proc_exit_index")
    all_vars.remove("on_shmem_exit_index")
    all_vars.remove("before_shmem_exit_index")

    all_vars.add("yychar")
    all_vars.add("yyin")
    all_vars.add("yyout")
    all_vars.add("yyleng")
    all_vars.add("yynerrs")
    all_vars.add("yytext")
    all_vars.add("yy_flex_debug")
    all_vars.add("yylineno")

    all_vars.remove("UsedShmemSegID")
    all_vars.remove("UsedShmemSegAddr")
    all_vars.remove("local_my_wait_event_info")
    all_vars.remove("my_wait_event_info")

    with open("vars.txt","w") as f:
        for a in sorted(all_vars):
            print(a, file=f)

    for a in all_vars:
       l=a.split(".")
       if len(l)==2:
           all_funcs_with_statics[l[0]].append(l[1])

def write_thread_inits():
    with open("thread_inits.c","w") as f:
        print("""#include "thread_inits.h"
static __thread int pg_thread_init_flag;

void pg_thread_init(void) {
    if (pg_thread_init_flag) return;
    pg_thread_init_flag=1;
    my_wait_event_info_init();""", file=f)

        for a in sorted(thread_funcs):
            print("    " + a + "();", file=f)
        print("""
    setup_pg_thread_cleanup();
    pg_timezone_initialize();
}""", file=f)

    with open("thread_inits.h","w") as f:
        print("#pragma once", file=f)
        print("extern void pg_thread_init();", file=f)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage ", sys.argv[0], " <build directory>");
        sys.exit(1)
    build_dir=sys.argv[1]
    get_vars(build_dir)
    make_sources_list(build_dir)
    copy_and_patch_sources(build_dir)
    write_thread_inits()
