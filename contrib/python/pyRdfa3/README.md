
[![DOI](https://zenodo.org/badge/doi/10.5281/zenodo.14547.svg)](http://dx.doi.org/10.5281/zenodo.14547)

> **Note:** since I retired a few months ago I do not really maintain this package any more. I would be more than happy if an interested party was interested to take over. In the meantime, I have "archived" the repository to clearly signal that there is no maintenance. I would be happy to unarchive it and transfer ownership if someone is interested.    
> [@iherman](https://github.com/iherman)

> **This new version 3.6.5 is now built and maintained on [prrvchr.github.io/pyrdfa3][1]**

The package can be installed from [PyPI][2] with command:  

`pip install pyRdfa3`

PyRDFA
======

What is it
----------

pyRdfa distiller/parser library. The distribution contains:

- [./src/pyRdfa][3]: the Python library. You should copy the directory somewhere into your PYTHONPATH.  
  Alternatively, you can also install from [Python Package Index][2] with command:  
  `pip install pyRdfa3`

- [./scripts/CGI_RDFa.py][4]: can be used as a CGI script to invoke the library.  
  It has to be adapted to the local server setup, namely in setting the right paths.  
  This script has not been ported to Python 3.x. Open a [new issue][5] if you need it.

- [./scripts/localRdfa.py][6]: script that can be run locally on to transform a file into RDF (on the standard output).  
  Run the script with `-h` to get the available flags.  
  This script has not been ported to Python 3.x. Open a [new issue][5] if you need it.

- [./doc][7]: pyRdfa3 [documentation][8] of the classes and functions (thanks to [pdoc][9]).

The package primarily depends on:
- Python version 3.8 or higher.
- [requests][10]: version 2.32.3 or higher.
- [rdflib][11]: version 7.0.0 or higher.
- [html5lib][12]: version 1.1.

The package has been tested on Python version 3.8 and higher (no more support for Python 2.x).

For the details on RDFa 1.1, see:

- <http://www.w3.org/TR/rdfa-core>
- <http://www.w3.org/TR/rdfa-lite/>
- <http://www.w3.org/TR/xhtml-rdfa/>
- <http://www.w3.org/TR/rdfa-in-html/>

possibly:

- <http://www.w3.org/TR/rdfa-primer/>

[1]: <https://github.com/prrvchr/pyrdfa3/>
[2]: <https://pypi.org/project/pyRdfa3/>
[3]: <https://github.com/prrvchr/pyrdfa3/tree/master/src/pyRdfa>
[4]: <https://github.com/prrvchr/pyrdfa3/blob/master/scripts/CGI_RDFa.py>
[5]: <https://github.com/prrvchr/pyrdfa3/issues/new>
[6]: <https://github.com/prrvchr/pyrdfa3/blob/master/scripts/localRdfa.py>
[7]: <https://github.com/prrvchr/pyrdfa3/blob/master/doc/>
[8]: <https://prrvchr.github.io/pyrdfa3/doc/pyRdfa.html>
[9]: <https://pdoc.dev/docs/pdoc.html>
[10]: <https://pypi.org/project/requests/2.32.3/>
[11]: <https://pypi.org/project/rdflib/7.0.0/>
[12]: <https://pypi.org/project/html5lib/1.1/>
