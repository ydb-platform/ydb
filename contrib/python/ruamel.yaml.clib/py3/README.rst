
ruamel.yaml.clib
================

``ruamel.yaml.clib`` is the C based reader/scanner and emitter for ruamel.yaml

:version:       0.2.7
:updated:       2022-10-19
:documentation: http://yaml.readthedocs.io
:repository:    https://sourceforge.net/projects/ruamel-yaml-clib/
:pypi:          https://pypi.org/project/ruamel.yaml.clib/

This package was split of from ruamel.yaml, so that ruamel.yaml can be build as  
a universal wheel. Apart from the C code seldom changing, and taking a long
time to compile for all platforms, this allows installation of the .so
on Linux systems under /usr/lib64/pythonX.Y (without a .pth file or a ruamel 
directory) and the Python code for ruamel.yaml under /usr/lib/pythonX.Y.


.. image:: https://bestpractices.coreinfrastructure.org/projects/1128/badge
   :target: https://bestpractices.coreinfrastructure.org/projects/1128

.. image:: https://sourceforge.net/p/ruamel-yaml-clib/code/ci/default/tree/_doc/_static/license.svg?format=raw
   :target: https://opensource.org/licenses/MIT
 
This release in loving memory of Johanna Clasina van der Neut-Bandel [1922-10-19 - 2015-11-21]
