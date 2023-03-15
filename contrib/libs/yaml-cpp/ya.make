LIBRARY()

LICENSE(MIT)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(0.5.3)

NO_UTIL()

NO_WSHADOW()

ADDINCL(GLOBAL contrib/libs/yaml-cpp/include)

SRCS(
    src/binary.cpp
    src/convert.cpp
    src/directives.cpp
    src/emit.cpp
    src/emitfromevents.cpp
    src/emitter.cpp
    src/emitterstate.cpp
    src/emitterutils.cpp
    src/exceptions.cpp
    src/exp.cpp
    src/memory.cpp
    src/nodebuilder.cpp
    src/node.cpp
    src/node_data.cpp
    src/nodeevents.cpp
    src/null.cpp
    src/ostream_wrapper.cpp
    src/parse.cpp
    src/parser.cpp
    src/regex_yaml.cpp
    src/scanner.cpp
    src/scanscalar.cpp
    src/scantag.cpp
    src/scantoken.cpp
    src/simplekey.cpp
    src/singledocparser.cpp
    src/stream.cpp
    src/tag.cpp
)

END()
