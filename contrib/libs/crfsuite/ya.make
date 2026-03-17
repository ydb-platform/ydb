LIBRARY()

LICENSE(
    BSD-3-Clause AND
    MIT AND
    Public-Domain
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(0.12.2)

NO_COMPILER_WARNINGS()

NO_UTIL()

PEERDIR(
    contrib/libs/lbfgs
)

ADDINCL(
    contrib/libs/crfsuite
    contrib/libs/crfsuite/lib/cqdb/include
    contrib/libs/crfsuite/lib/crf
    GLOBAL contrib/libs/crfsuite/include
)

IF (NOT OS_WINDOWS)
    CFLAGS(
        -DHAVE_CONFIG_H
        -DUSE_SSE
        -ffast-math
        -fno-finite-math-only
        -mfpmath=sse
    )
ENDIF()

SRCS(
    crfsuite.cpp
    lib/crf/src/crf1d_context.c
    lib/crf/src/crf1d_encode.c
    lib/crf/src/crf1d_feature.c
    lib/crf/src/crf1d_model.c
    lib/crf/src/crf1d_tag.c
    lib/crf/src/crfsuite.c
    lib/crf/src/crfsuite_train.c
    lib/crf/src/dataset.c
    lib/crf/src/dictionary.c
    lib/crf/src/holdout.c
    lib/crf/src/logging.c
    lib/crf/src/params.c
    lib/crf/src/quark.c
    lib/crf/src/rumavl.c
    lib/crf/src/train_arow.c
    lib/crf/src/train_averaged_perceptron.c
    lib/crf/src/train_l2sgd.c
    lib/crf/src/train_lbfgs.c
    lib/crf/src/train_passive_aggressive.c
    lib/cqdb/src/cqdb.c
    lib/cqdb/src/lookup3.c
)

END()
