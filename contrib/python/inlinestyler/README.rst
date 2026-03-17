============================================
inlinestyler - Making Styled HTML Email Easy
============================================

:Version: 0.2.4
:Download: http://pypi.python.org/pypi/inlinestyler/
:Source: http://github.com/dlanger/inlinestyler/
:Keywords: inline, HTML, CSS, email, preflight

`inlinestyler` is an easy way to locally inline CSS into an HTML email message.

Styling HTML email is a `black art`_. CSS works, but only when it's been placed
inline on the individual elements (and event then, not always) - which makes
development frustrating, and iteration slow. 

The general solution is to use an inlining service, which takes a message with 
the CSS placed externally, and rewrites it so that all CSS is applied to the
individual elements. The most widely used of these services - and as far as I 
can tell, the one that powers CampaignMonitor - is `Premailer`_. It's a great 
service, and the guys behind it put a lot of work into keeping it up to date
with the most recent discoveries in what works and what doesn't.

`inlinestyler` takes (most) of the functionality of Premailer, and makes it 
available locally, accessible without having call a remote service. 

To see what `inline-styler` can do, check out this `demo`_.

.. _`black art`: http://www.campaignmonitor.com/css/
.. _`Premailer`: http://premailer.dialect.ca/
.. _`demo`: http://inlinestyler.torchboxapps.com/


Caveat Emptor
=============

This project is relatively unmaintained. I will continue to do simple bugfixes 
(and patches with tests are welcome), but I won't be adding features or making
new CSS attributes work.

If this doesn't do what you need, check out the `premailer project`_.

.. _`premailer project`: https://github.com/peterbe/premailer/

History
=======

`Dave Cranwell`_ wrote the original `inline-styler`_ single-app Django project. 
`inlinestyler` is a refactor of that project into a free-standing package usable 
outside of Django.

.. _`inline-styler`: https://github.com/davecranwell/inline-styler
.. _`Dave Cranwell`: http://www.twitter.com/davecranwell

Requirements
============

`inlinestyler` requires the following packages in order to run:

* ``cssutils`` 
* ``lxml`` 

It also requires a ``css_complaiance.csv`` file, which indicates the 
compatibility of various email clients with certain CSS features. This
is included with the package, but can be updated manually from 
`Campaign Monitor`_'s spreadsheet.

.. _`Campaign Monitor`: http://www.campaignmonitor.com/css/

Usage
=====

::

     from inlinestyler.utils import inline_css
     message_inline_css = inline_css(message_external_css)


``message_external_css`` must be a string containing the message to be inlined, 
with the CSS presented in the HTML as one of:

* an absolute link ``<link rel="stylesheet" href="http://mysite.com/styles.css" />`` 
* a ``<style>`` block in the ``<head>``, without the use of ``@import``.

The code will also calculate an estimate for how compatible your message is with 
various clients (using the ``css_compliance.csv`` file), but this number isn't 
yet exposed. 

Contributions
=============

All development happens at github: http://github.com/dlanger/inlinestyler.

To get yourself started:

#. Clone this repo somewhere
#. ``make init`` to install the right dependencies
#. ``make test`` to run the test suite

Contributions are always more than welcome. If you see something missing, add it
in and send me a pull request.

**NOTE**: Ubuntu 12.04 (and some other distros) include ``libxslt`` version
``1.1.26``, which changes the now-empty ``<head>`` tag to ``<head/>`` - which 
isn't valid HTML 5. To see which version of ``libxslt`` was used to build
your ``libxml``, examine the output of ``make init`` and look for the 
line that looks like ``Using build configuration of libxslt 1.1.XX``; if
that says ``26``, some test failures are expected (at which point, you
can rely on `TravisCI`_ to run your tests for you). 

You could also install your own version of ``libxslt`` from source, but 
you're probably going to have a bad time.

.. _`TravisCI`: https://travis-ci.org/dlanger/inlinestyler

License
=======

This distribution is licensed under the ``New BSD License``. Please see the 
``LICENSE`` file for a full copy of the license text.

As far as I can tell, Dave Cranwell `released`_ the underlying `inline-styler`
project into the public domain:

   I'm [...] releasing it to the public after many requests for the source.

.. _`released`: https://github.com/davecranwell/inline-styler/blob/c22a5fb67771d082ce0e999ea814dbdf2f05cdfe/README
