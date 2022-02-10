LIBRARY()

LICENSE(BSD-3-Clause)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

OWNER(
    g:contrib
    g:cpp-contrib
)

NO_UTIL()

NO_RUNTIME()

NO_COMPILER_WARNINGS()

SRCS(
    vdso_support.cc
    elf_mem_image.cc
)

END()
