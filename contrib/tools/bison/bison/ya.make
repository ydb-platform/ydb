PROGRAM()

VERSION(3.0)

LICENSE(
    GPL-3.0-only AND
    GPL-3.0-or-later
)

ORIGINAL_SOURCE(git://git.sv.gnu.org/bison)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

IF (NOT MUSL)
    NO_RUNTIME()
ENDIF()

NO_COMPILER_WARNINGS()

ADDINCLSELF()

SRCS(
    src/AnnotationList.c
    src/InadequacyList.c
    src/LR0.c
    src/Sbitset.c
    src/assoc.c
    src/closure.c
    src/complain.c
    src/conflicts.c
    src/derives.c
    src/files.c
    src/getargs.c
    src/gram.c
    src/graphviz.c
    src/ielr.c
    src/lalr.c
    src/location.c
    src/main.c
    src/muscle-tab.c
    src/named-ref.c
    src/nullable.c
    src/output.c
    src/parse-gram.c
    src/print-xml.c
    src/print.c
    src/print_graph.c
    src/reader.c
    src/reduce.c
    src/relation.c
    src/scan-code-c.c
    src/scan-gram-c.c
    src/scan-skel-c.c
    src/state.c
    src/symlist.c
    src/symtab.c
    src/tables.c
    src/uniqstr.c
    arcadia_root.cpp.in
)

CFLAGS(
    -Daccept=bison_accept
    -DBISON_DATA_DIR="contrib/tools/bison/bison/data"
)

PEERDIR(
    contrib/tools/bison/gnulib
)

END()
