#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from shutil import Error, copy2, rmtree
import subprocess

all_vars = set()
thread_funcs = []
define_for_yylval = None
skip_func = False

def fix_line(line):
    global define_for_yylval
    if line.startswith("#define yylval"):
        define_for_yylval=line[14:].strip()

    global skip_func
    if line.startswith("build_guc_variables(void)"):
       skip_func = True
       return line

    if "ConfigureNames" in line and line.strip().endswith("[] ="):
       skip_func = True
       return line

    if skip_func:
       if line.startswith("{"):
          return line
       if not line.startswith("}"):
          return None
       skip_func=False

    if line.startswith("#") or line.startswith(" ") or line.startswith("\t"):
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
            for line in fsrc:
                line = fix_line(line)
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
             if "    src/" in line:
                 #print(line.strip())
                 if "/help_config.c" in line: continue
                 fdst.write("    postgresql/" + line.strip() + "\n")
          fdst.write(")\n")

def get_vars():
    s=subprocess.check_output("objdump ../../../../../contrib/libs/postgresql/libpostgres.a -tw",shell=True).decode("utf-8")
    for a in s.replace("\t"," ").split("\n"):
        for b in a.split(" "):
            if b.startswith(".bss."): all_vars.add(b[5:])
            elif b.startswith(".data."): all_vars.add(b[6:])
    all_vars.remove("Dummy_trace")

    all_vars.remove("backslash_quote")
    all_vars.remove("sentinel") # rbtree.c

    all_vars.remove("gistBufferingOptValues")
    all_vars.remove("boolRelOpts")
    all_vars.remove("intRelOpts")
    all_vars.remove("realRelOpts")
    all_vars.remove("viewCheckOptValues")
    all_vars.remove("enumRelOpts")
    all_vars.remove("stringRelOpts")

    with open("vars.txt","w") as f:
        for a in sorted(all_vars):
            print(a, file=f)

def write_thread_inits():
    with open("thread_inits.c","w") as f:
        print("#include \"thread_inits.h\"", file=f)
        print("static __thread int pg_thread_init_flag;", file=f)
        print("void pg_thread_init(void) {", file=f)
        print("    if (pg_thread_init_flag) return;", file=f)
        print("    pg_thread_init_flag=1;", file=f)
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
