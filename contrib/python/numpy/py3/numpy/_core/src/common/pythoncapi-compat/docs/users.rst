+++++++++++++++++++++++
pythoncapi-compat users
+++++++++++++++++++++++

Examples of projects using pythoncapi_compat.h
==============================================

* `bitarray <https://github.com/ilanschnell/bitarray/>`_:
  ``bitarray/_bitarray.c`` uses ``Py_SET_SIZE()``
  (`pythoncapi_compat.h copy
  <https://github.com/ilanschnell/bitarray/blob/master/bitarray/pythoncapi_compat.h>`__)
* `datatable <https://github.com/h2oai/datatable>`_
  (`commit <https://github.com/h2oai/datatable/commit/02f13114828ed4567e4410f5bac579895e20355a>`__)
* `guppy3 <https://github.com/zhuyifei1999/guppy3/>`_
  (`commit
  <https://github.com/zhuyifei1999/guppy3/commit/4cb9fcb5d75327544a6875b6caabfdffb70a7e29>`__)
* `immutables <https://github.com/MagicStack/immutables/>`_:
  ``immutables/_map.c`` uses ``Py_SET_SIZE()``
  (`pythoncapi_compat.h copy
  <https://github.com/MagicStack/immutables/blob/master/immutables/pythoncapi_compat.h>`__)
* `Mercurial (hg) <https://www.mercurial-scm.org/>`_ uses ``Py_SET_TYPE()``
  (`commit
  <https://www.mercurial-scm.org/repo/hg/rev/e92ca942ddca>`__,
  `pythoncapi_compat.h copy
  <https://www.mercurial-scm.org/repo/hg/file/tip/mercurial/pythoncapi_compat.h>`__)
* `mypy <https://github.com/python/mypy>`_
  (mypyc,
  `commit <https://github.com/python/mypy/commit/2b7e2df923f7e4a3a199915b3c8563f45bc69dfa>`__)
* `numpy <https://numpy.org/>`_:
  `pythoncapi-compat Git submodule
  <https://github.com/numpy/numpy/blob/main/.gitmodules>`_
* `pybluez <https://github.com/pybluez/pybluez>`_
  (`commit <https://github.com/pybluez/pybluez/commit/5096047f90a1f6a74ceb250aef6243e144170f92>`__)
* `python-snappy <https://github.com/andrix/python-snappy/>`_
  (`commit <https://github.com/andrix/python-snappy/commit/1a539d71d5b1ceaf9a2291f21f686cf53a46d707>`__)
* `python-zstandard <https://github.com/indygreg/python-zstandard/>`_
  uses ``Py_SET_TYPE()`` and ``Py_SET_SIZE()``
  (`commit <https://github.com/indygreg/python-zstandard/commit/e5a3baf61b65f3075f250f504ddad9f8612bfedf>`__):
  Mercurial extension.
* `python-zstd <https://github.com/sergey-dryabzhinsky/python-zstd/>`_
  (`commit <https://github.com/sergey-dryabzhinsky/python-zstd/commit/8aa6d7a4b250e1f0a4e27b4107c39dc516c87f96>`__)
* `hollerith <https://github.com/pyansys/hollerith/>`_
  ``src/writer.c`` uses ``PyObject_CallOneArg() and other Python 3.9 apis``
  (`pythoncapi_compat.h copy
  <https://github.com/pyansys/hollerith/blob/main/src/pythoncapi_compat.h>`__)
* `PyTorch <https://github.com/pytorch/pytorch/>`_ (`pythoncapi_compat.h copy
  <https://github.com/pytorch/pytorch/blob/main/torch/csrc/utils/pythoncapi_compat.h>`__)
* `PyGObject <https://gitlab.gnome.org/GNOME/pygobject/>`_
  (`commit <https://gitlab.gnome.org/GNOME/pygobject/-/commit/074c0348417b87d3003dbb409e6fc26b61121de3>`__,
  `pythoncapi-compat Meson subproject
  <https://gitlab.gnome.org/GNOME/pygobject/-/blob/main/subprojects/pythoncapi-compat.wrap?ref_type=heads>`__)

Projects not using pythoncapi_compat.h
======================================

Projects not using ``pythoncapi_compat.h``:

* numpy has its own compatibility layer, ``npy_pycompat.h`` and
  ``npy_3kcompat.h`` header files. It supports more C compilers than
  pythoncapi_compat.h: it supports ``__STRICT_ANSI__`` (ISO C90) for example.
  Rejected `PR 18713: MAINT: Use pythoncapi_compat.h in npy_3kcompat.h
  <https://github.com/numpy/numpy/pull/18713>`_ (when it was rejected, numpy
  still had code for compatibility with Python 2.7).
* Cython doesn't use pythoncapi_compat.h:
  `see Cython issue #3934
  <https://github.com/cython/cython/issues/3934>`_.
  For example, `ModuleSetupCode.c
  <https://github.com/cython/cython/blob/0.29.x/Cython/Utility/ModuleSetupCode.c>`_
  provides functions like ``__Pyx_SET_REFCNT()``.

Project with a strict contributor agreement:

* `zodbpickle
  <https://github.com/zopefoundation/zodbpickle/pull/64>`_
