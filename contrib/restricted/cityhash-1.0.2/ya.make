# Origin: https://github.com/google/cityhash.git

LIBRARY()

VERSION(bc38ef45ddbbe640e48db7b8ef65e80ea7f71298)

LICENSE(MIT)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

NO_UTIL()

ADDINCL(
    GLOBAL contrib/restricted/cityhash-1.0.2
)

SRCS(
    city.cc
)

END()
