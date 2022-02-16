#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from shutil import Error, copy2, rmtree

def mycopy2(src, dst):
    if src.endswith("ya.make"):
        return
    else:
        copy2(src, dst)

def copytree(src, dst):
    names = os.listdir(src)
    os.makedirs(dst)
    errors = []
    for name in names:
        srcname = os.path.join(src, name)
        dstname = os.path.join(dst, name)
        print(srcname)
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
                 print(line.strip())
                 fdst.write("    postgresql/" + line.strip() + "\n")
          fdst.write(")\n")

if __name__ == "__main__":
    make_sources_list()
    if os.path.isdir("postgresql"):
        rmtree("postgresql")
    copytree("../../../../../contrib/libs/postgresql", "postgresql")

