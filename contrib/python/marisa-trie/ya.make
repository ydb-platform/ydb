PY23_LIBRARY()

LICENSE(MIT)

VERSION(0.7.5)

NO_COMPILER_WARNINGS()

ADDINCL(
    contrib/python/marisa-trie
)

SRCS(
    marisa/agent.cc
    marisa/keyset.cc
    marisa/trie.cc

    marisa/grimoire/io/mapper.cc
    marisa/grimoire/io/reader.cc
    marisa/grimoire/io/writer.cc
    marisa/grimoire/trie/louds-trie.cc
    marisa/grimoire/trie/tail.cc
    marisa/grimoire/vector/bit-vector.cc
)

PY_SRCS(
    TOP_LEVEL
    marisa_trie.pyx
)

NO_LINT()

END()
