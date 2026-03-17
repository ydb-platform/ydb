=============
gemfileparser
=============

gemfileparser parses Ruby Gemfile using Python with supports Ruby Gemfiles
and .gemspec files as well as Cocoapod .podspec files.


Installation
~~~~~~~~~~~~

In a virtualenv, use the command::

    pip install gemfileparser

Otherwise from a git clone, use the following commands in a virtualenv::

    git clone https://github.com/gemfileparser/gemfileparser.git
    cd gemfileparser
	python setup.py install


Usage
~~~~~

::

    from gemfileparser import GemfileParser
    parser = GemfileParser(<path to Gemfile>, <name of the application (optional)>)
    dependency_dictionary = parser.parse()

The parse() method returns a dict object of the following format::

    {
        'development': [list of dependency objects inside group 'development'],
        'runtime': [list of runtime dependency objects],
        .
        .
    }

Each dependency object contains the following attributes:

- name - Name of the gem
- requirement - Version requirement
- autorequire - Autorequire value
- source - Source URL of the gem
- parent - Dependency of which gem
- group - Group that a gem is a member of (default : runtime)


Example
~~~~~~~

::

    from gemfileparser import GemfileParser
    n = GemfileParser('Gemfile', 'diaspora')
    deps = n.parse()
    for key in deps:
       if deps[key]:
           print key
           for dependency in deps[key]:
               print("\t", dependency)


Tests
~~~~~

Do this to run tests::

    pip install -e .
    pip install pytest
    pytest -vvs tests


Copyright
~~~~~~~~~
* Copyright (c) 2020 Gemfileparser authors (listed in AUTHORS file)
* Copyright (c) 2015-2018 Balasankar C <balasankarc@autistici.org>


License
~~~~~~~

gemfileparser is dual-licensed under your choice of the
`GNU GPL version 3 (or later) License <http://www.gnu.org/licenses/gpl>`_
or the `MIT License <https://opensource.org/licenses/MIT>`_.

It is preferred anyone using this project to respect the GPL-3+ license and use
that itself for derivative works - thus making them also Free Software. But,
your call.

When making contributions to gemfileparser you agree to license these contributions
under the same choice of licenses.
