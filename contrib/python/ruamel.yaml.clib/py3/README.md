
# ruamel.yaml.clib

`ruamel.yaml.clib` is the C based reader/scanner and emitter for ruamel.yaml

<table class="docutils">
  <tr>
    <td>version</td>
    <td>0.2.12</td>
  </tr>
  <tr>
    <td>updated</td>
    <td>2024-10-20</td>
  </tr>
  <tr>
    <td>documentation</td>
    <td><https://yaml.dev/doc/ruamel.yaml.clib></td>
  </tr>
  <tr>
    <td>repository</td>
    <td><https://sourceforge.net/projects/ruamel-yaml-clib/></td>
  </tr>
  <tr>
    <td>pypi</td>
    <td><https://pypi.org/project/ruamel.yaml.clib/></td>
  </tr>
</table>


This package was split of from ruamel.yaml, so that ruamel.yaml can be
build as a universal wheel. Apart from the C code seldom changing, and
taking a long time to compile for all platforms, this allows
installation of the .so on Linux systems under /usr/lib64/pythonX.Y
(without a .pth file or a ruamel directory) and the Python code for
ruamel.yaml under /usr/lib/pythonX.Y.

[![image](https://bestpractices.coreinfrastructure.org/projects/1128/badge)](https://bestpractices.coreinfrastructure.org/projects/1128)
[![image](https://sourceforge.net/p/ruamel-yaml-clib/code/ci/default/tree/_doc/_static/license.svg?format=raw)](https://opensource.org/licenses/MIT)
[![image](https://sourceforge.net/p/ruamel-yaml-clib/code/ci/default/tree/_doc/_static/pypi.svg?format=raw)](https://pypi.org/project/ruamel.yaml.clib/)

This release in loving memory of Johanna Clasina van der Neut-Bandel
\[1922-10-19 &ndash; 2015-11-21\]
