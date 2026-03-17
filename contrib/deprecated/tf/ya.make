LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause AND
    CC-BY-4.0 AND
    GPL-3.0-only AND
    LGPL-2.1-only AND
    LGPL-2.1-or-later AND
    LGPL-3.0-only AND
    LicenseRef-scancode-other-permissive AND
    MPL-2.0 AND
    Minpack
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(1.10.1)

PROVIDES(tensorflow)

NO_COMPILER_WARNINGS()

PEERDIR(
    contrib/deprecated/tf/minimal
    contrib/deprecated/tf/all_ops
)

END()

RECURSE(
    tensorflow
    tensorflow/compiler
)
