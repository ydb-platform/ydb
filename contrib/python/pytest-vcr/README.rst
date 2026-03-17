##########
pytest-vcr
##########

|Travis| |AppVeyor| |ReadTheDocs|

.. |Travis| image:: https://travis-ci.org/ktosiek/pytest-vcr.svg?branch=master
    :target: https://travis-ci.org/ktosiek/pytest-vcr
    :alt: See Build Status on Travis CI
.. |AppVeyor| image:: https://ci.appveyor.com/api/projects/status/github/ktosiek/pytest-vcr?branch=master
    :target: https://ci.appveyor.com/project/ktosiek/pytest-vcr/branch/master
    :alt: See Build Status on AppVeyor
.. |ReadTheDocs| image:: https://readthedocs.org/projects/pytest-vcr/badge/?version=latest
    :target: http://pytest-vcr.readthedocs.io/en/latest/?badge=latest
    :alt: See the documentation


Py.test plugin for managing `VCR.py <https://vcrpy.readthedocs.io/>`_ cassettes.


Quick Start
===========

Install the plugin:

.. code-block:: sh

    pip install pytest-vcr


Annotate your tests:

.. code-block:: python

    @pytest.mark.vcr()
    def test_iana():
        response = urlopen('http://www.iana.org/domains/reserved').read()
        assert b'Example domains' in response

And that's it!
A new file ``cassettes/test_iana.yaml`` will be created next to your test file on the first run.
This file should be committed to a VCS.

For more examples and configuration options see the `documention on ReadTheDocs <http://pytest-vcr.readthedocs.io/en/latest/>`_.
