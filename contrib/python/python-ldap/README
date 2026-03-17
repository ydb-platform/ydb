---------------------------------------
python-ldap: LDAP client API for Python
---------------------------------------

What is python-ldap?
====================

python-ldap provides an object-oriented API to access LDAP
directory servers from Python programs. Mainly it wraps the
OpenLDAP client libs for that purpose.

Additionally the package contains modules for other LDAP-related
stuff (e.g. processing LDIF, LDAPURLs, LDAPv3 sub-schema, etc.).

Not included: Direct BER support

See INSTALL for version compatibility

See TODO for planned features. Contributors welcome.

For module documentation, see:

	https://www.python-ldap.org/

Quick usage example:
====================

.. code-block:: python

    import ldap
    l = ldap.initialize("ldap://my_ldap_server.my_domain")
    l.simple_bind_s("","")
    l.search_s("o=My Organisation, c=AU", ldap.SCOPE_SUBTREE, "objectclass=*")

See directory ``Demo/`` of source distribution package for more
example code.

Author(s) contact and documentation:
====================================

   https://www.python-ldap.org/

If you are looking for help, please try the mailing list archives
first, then send a question to the mailing list.
Be warned that questions will be ignored if they can be
trivially answered by referring to the documentation.

If you are interested in helping, please contact the mailing list.
If you want new features or upgrades, please check the mailing list
archives and then enquire about any progress.

Acknowledgements:
=================

Thanks to Konstantin Chuguev <Konstantin.Chuguev at dante.org.uk>
and Steffen Ries <steffen.ries at sympatico.ca> for working
on support for OpenLDAP 2.0.x features.

Thanks to Michael Stroeder <michael at stroeder.com> for the
modules ``ldif``, ``ldapurl``, ``ldap/schema/*.py``, ``ldap/*.py`` and ``ldap/controls/*.py``.

Thanks to Hans Aschauer <Hans.Aschauer at Physik.uni-muenchen.de>
for the C wrapper schema and SASL support.

Thanks to Mauro Cicognini <mcicogni at siosistemi.it> for the
WIN32/MSVC6 bits, and the pre-built WIN32 ``ldap.pyd``.

Thanks to Waldemar Osuch <waldemar.osuch at gmail.com> for contributing
the new-style docs based on reStructuredText.

Thanks to Torsten Kurbad <torsten at tk-webart.de> for the
easy_install support.

Thanks to James Andrewartha <jamesa at daa.com.au> for
significant contribution to ``Doc/*.tex``.

Thanks to Rich Megginson <rmeggins at redhat.com> for extending
support for LDAPv3 controls and adding support for LDAPv3 extended
operations.

Thanks to Peter Gietz, DAASI for funding some control modules.

Thanks to Chris Mikkelson for various fixes and ldap.syncrepl.

These very kind people have supplied patches or suggested changes:

* Federico Di Gregorio <fog at mixadlive.com>
* John Benninghoff <johnb at netscape.com>
* Donn Cave <donn at u.washington.edu>
* Jason Gunthorpe <jgg at debian.org>
* gurney_j <gurney_j at 4j.lane.edu>
* Eric S. Johansson <esj at harvee.billerica.ma.us>
* David Margrave <davidma at premier1.net>
* Uche Ogbuji <uche.ogbuji at fourthought.com>
* Neale Pickett <neale at lanl.gov>
* Blake Weston <weston at platinum1.cambridge.scr.slb.com>
* Wido Depping <wido.depping at gmail.com>
* Deepak Giridharagopal <deepak at arlut.utexas.edu>
* Ingo Steuwer <steuwer at univention.de>
* Andreas Hasenack <ahasenack at terra.com.br>
* Matej Vela <vela at debian.org>

These people contributed to Python 3 porting (at https://github.com/pyldap/):

* ​A. Karl Kornel
* Alex Willmer
* Aymeric Augustin
* Bradley Baetz
* Christian Heimes
* Dirk Mueller
* Jon Dufresne
* Martin Basti
* Miro Hrončok
* Paul Aurich
* Petr Viktorin
* Pieterjan De Potter
* Raphaël Barrois
* Robert Kuska
* Stanislav Láznička
* Tobias Bräutigam
* Tom van Dijk
* Wentao Han
* William Brown

Thanks to all the guys on the python-ldap mailing list for
their contributions and input into this package.

   Thanks! We may have missed someone: please mail us if we have omitted
   your name.

Licence
=======

The python-ldap project comes with a LICENCE file.

We are aware that its text is unclear, but it cannot be changed:
all authors of python-ldap would need to approve the licence change,
but a complete list of all the authors is not available.
(Note that the Git repository of the project is incomplete.
Furthermore, commits imported from CVS lack authorship information; users
"stroeder" or "leonard" are commiters (reviewers), but sometimes not
authors of the committed code.)

The current maintainers assume that the license is the sentence that refers
to "Python-style license" and assume this means a highly permissive open source
license that only requires preservation of the text of the LICENCE file
(including the disclaimer paragraph).

-------------------------------------------------------------------------------

All contributions committed since July 1st, 2021, as well as some past
contributions, are licensed under the MIT license.
The MIT licence and more details are listed in the file LICENCE.MIT.
