===============
aiohttp-remotes
===============

The library is a set of useful tools for ``aiohttp.web`` server.

The full list of tools is:

* ``AllowedHosts`` -- restrict a set of incoming connections to
  allowed hosts only.
* ``BasicAuth`` -- protect web application by *basic auth*
  authorization.
* ``Cloudflare`` -- make sure that web application is protected
  by CloudFlare.
* ``ForwardedRelaxed`` and ``ForwardedStrict`` -- process
  ``Forwarded`` HTTP header and modify corresponding
  ``scheme``, ``host``, ``remote`` attributes in strong secured and
  relaxed modes.
* ``Secure`` -- ensure that web application is handled by HTTPS
  (SSL/TLS) only, redirect plain HTTP to HTTPS automatically.
* ``XForwardedRelaxed`` and ``XForwardedStrict`` -- the same
  as ``ForwardedRelaxed`` and ``ForwardedStrict`` but process old-fashion
  ``X-Forwarded-*`` headers instead of new standard ``Forwarded``.


Read https://aiohttp-remotes.readthedocs.io for more information.



The library was donated by Ocean S.A. https://ocean.io/

Thanks to the company for contribution.
