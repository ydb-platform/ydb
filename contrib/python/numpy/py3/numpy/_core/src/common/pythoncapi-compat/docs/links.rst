Links
=====

* `Python/C API Reference Manual <https://docs.python.org/dev/c-api/>`_
* `HPy: a better API for Python
  <https://hpy.readthedocs.io/>`_
* `Cython: C-extensions for Python
  <https://cython.org/>`_
* `Old 2to3c project <https://github.com/davidmalcolm/2to3c>`_ by David Malcolm
  which uses `Coccinelle <https://coccinelle.gitlabpages.inria.fr/website/>`_
  to ease migration of C extensions from Python 2 to Python 3. See
  also `2to3c: an implementation of Python's 2to3 for C code
  <https://dmalcolm.livejournal.com/3935.html>`_ article (2009).
* PEPs related to the C API:

  * `PEP 620 -- Hide implementation details from the C API
    <https://www.python.org/dev/peps/pep-0620/>`_
  * `PEP 670 -- Convert macros to functions in the Python C API
    <https://www.python.org/dev/peps/pep-0670/>`_
  * `PEP 674 -- Disallow using macros as l-values
    <https://www.python.org/dev/peps/pep-0674/>`_

* Make structures opaque

  * `bpo-39573: PyObject <https://bugs.python.org/issue39573>`_
  * `bpo-40170: PyTypeObject <https://bugs.python.org/issue40170>`_
  * `bpo-39947: PyThreadState <https://bugs.python.org/issue39947>`_
  * `bpo-40421: PyFrameObject <https://bugs.python.org/issue40421>`_
  * `bpo-47241: PyCodeObject <https://bugs.python.org/issue47241>`_
