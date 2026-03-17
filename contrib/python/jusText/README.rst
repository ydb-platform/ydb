.. _jusText: http://code.google.com/p/justext/
.. _Python: http://www.python.org/
.. _lxml: http://lxml.de/

jusText
=======
.. image:: https://github.com/miso-belica/jusText/actions/workflows/run-tests.yml/badge.svg
  :target: https://github.com/miso-belica/jusText/actions/workflows/run-tests.yml

Program jusText is a tool for removing boilerplate content, such as navigation
links, headers, and footers from HTML pages. It is
`designed <doc/algorithm.rst>`_ to preserve
mainly text containing full sentences and it is therefore well suited for
creating linguistic resources such as Web corpora. You can
`try it online <http://nlp.fi.muni.cz/projects/justext/>`_.

This is a fork of original (currently unmaintained) code of jusText_ hosted
on Google Code.


Adaptations of the algorithm to other languages:

- `C++ <https://github.com/endredy/jusText>`_
- `Go <https://github.com/JalfResi/justext>`_
- `Java <https://github.com/wizenoze/justext-java>`_


Some libraries using jusText:

- `chirp <https://github.com/9b/chirp>`_
- `lazynlp <https://github.com/chiphuyen/lazynlp>`_
- `off-topic-memento-toolkit <https://github.com/oduwsdl/off-topic-memento-toolkit>`_
- `pears <https://github.com/PeARSearch/PeARS-orchard>`_
- `readability calculator <https://github.com/joaopalotti/readability_calculator>`_
- `sky <https://github.com/kootenpv/sky>`_


Some currently (Jan 2020) maintained alternatives:

- `dragnet <https://github.com/dragnet-org/dragnet>`_
- `html2text <https://github.com/Alir3z4/html2text>`_
- `inscriptis <https://github.com/weblyzard/inscriptis>`_
- `newspaper <https://github.com/codelucas/newspaper>`_
- `python-readability <https://github.com/buriy/python-readability>`_
- `trafilatura <https://github.com/adbar/trafilatura>`_


Installation
------------
Make sure you have Python_ 2.7+/3.5+ and `pip <https://pip.pypa.io/en/stable/>`_
(`Windows <http://docs.python-guide.org/en/latest/starting/install/win/>`_,
`Linux <http://docs.python-guide.org/en/latest/starting/install/linux/>`_) installed.
Run simply:

.. code-block:: bash

  $ [sudo] pip install justext


Dependencies
------------
::

  lxml (version depends on your Python version)


Usage
-----
.. code-block:: bash

  $ python -m justext -s Czech -o text.txt http://www.zdrojak.cz/clanky/automaticke-zabezpeceni/
  $ python -m justext -s English -o plain_text.txt english_page.html
  $ python -m justext --help # for more info


Python API
----------
.. code-block:: python

  import requests
  import justext

  response = requests.get("http://planet.python.org/")
  paragraphs = justext.justext(response.content, justext.get_stoplist("English"))
  for paragraph in paragraphs:
    if not paragraph.is_boilerplate:
      print paragraph.text


Testing
-------
Run tests via

.. code-block:: bash

  $ py.test-2.7 && py.test-3.5 && py.test-3.6 && py.test-3.7 && py.test-3.8 && py.test-3.9


Acknowledgements
----------------
.. _`Natural Language Processing Centre`: http://nlp.fi.muni.cz/en/nlpc
.. _`Masaryk University in Brno`: http://nlp.fi.muni.cz/en
.. _PRESEMT: http://presemt.eu/
.. _`Lexical Computing Ltd.`: http://lexicalcomputing.com/
.. _`PhD research`: http://is.muni.cz/th/45523/fi_d/phdthesis.pdf

This software has been developed at the `Natural Language Processing Centre`_ of
`Masaryk University in Brno`_ with a financial support from PRESEMT_ and
`Lexical Computing Ltd.`_ It also relates to `PhD research`_ of Jan Pomikálek.
