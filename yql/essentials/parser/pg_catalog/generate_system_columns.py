#!/usr/bin/env python3

with open("pg_class.txt") as f:
    pg_class_file = f.readlines()

with open("pg_class.generated.h","w") as f:
    for p in pg_class_file[2:-2]:
        s=p.split("|")
        oid=int(s[0].strip())
        relkind=s[1].strip()
        relname=s[2].strip()
        nspname=s[3].strip()
        print(oid,relkind,relname,nspname)
        print('{{"' + nspname + '", "' + relname + '"}, ERelKind::' + ("Relation" if relkind == 'r' else "View") +", " + str(oid) + "},", file=f)

with open("columns.txt") as f:
    columns_file = f.readlines()

with open("columns.generated.h","w") as f:
    for p in columns_file[2:-2]:
        s=p.split("|")
        print(s)
        name=s[0].strip()
        relname=s[1].strip()
        schemaname=s[2].strip()
        udt=s[3].strip()
        print(schemaname,relname,name,udt)
        print('{"' + schemaname + '", "' + relname + '", "' + name + '", "' + udt + '"},', file=f)
