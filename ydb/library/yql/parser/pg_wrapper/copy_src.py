#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
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

def copytree(src, dst):
    names = os.listdir(src)
    os.makedirs(dst)
    errors = []
    for name in names:
        if name == "help_config.c": continue
        srcname = os.path.join(src, name)
        dstname = os.path.join(dst, name)
        #print(srcname)
        try:
            if os.path.isdir(srcname):
                copytree(srcname, dstname)
            else:
                mycopy2(srcname, dstname)
        except OSError as why:
            errors.append((srcname, dstname, str(why)))
        except Error as err:
            errors.extend(err.args[0])
    if errors:
        raise Error(errors)

def make_sources_list():
   with open("../../../../../contrib/libs/postgresql/ya.make","r") as fsrc:
       with open("pg_sources.inc","w") as fdst:
          fdst.write("SRCS(\n")
          for line in fsrc:
             if line.startswith("    src/"):
                 #print(line.strip())
                 if "/help_config.c" in line: continue
                 fdst.write("    postgresql/" + line.strip() + "\n")
          fdst.write(")\n")

def get_vars():
    s=subprocess.check_output("objdump ../../../../../contrib/libs/postgresql/libpostgres.a -tw",shell=True).decode("utf-8")
    for a in s.replace("\t"," ").split("\n"):
        for b in a.split(" "):
            sym=None
            if b.startswith(".bss."): sym=b[5:]
            elif b.startswith(".data."): sym=b[6:]
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
    setup_pg_thread_cleanup();
    pg_timezone_initialize();
""", file=f)

        for a in sorted(thread_funcs):
            print("    " + a + "();", file=f)
        print("}", file=f)

    with open("thread_inits.h","w") as f:
        print("#pragma once", file=f)
        print("extern void pg_thread_init();", file=f)

if __name__ == "__main__":
    get_vars()
    make_sources_list()
    if os.path.isdir("postgresql"):
        rmtree("postgresql")
    copytree("../../../../../contrib/libs/postgresql", "postgresql")
    write_thread_inits()
