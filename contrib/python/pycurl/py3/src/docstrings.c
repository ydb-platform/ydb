/* Generated file - do not edit. */
/* See doc/docstrings/ *.rst. */

#include "pycurl.h"

PYCURL_INTERNAL const char curl_doc[] = "Curl() -> New Curl object\n\
\n\
Creates a new :ref:`curlobject` which corresponds to a\n\
``CURL`` handle in libcurl. Curl objects automatically set\n\
CURLOPT_VERBOSE to 0, CURLOPT_NOPROGRESS to 1, provide a default\n\
CURLOPT_USERAGENT and setup CURLOPT_ERRORBUFFER to point to a\n\
private error buffer.\n\
\n\
Implicitly calls :py:func:`pycurl.global_init` if the latter has not yet been called.";

PYCURL_INTERNAL const char curl_close_doc[] = "close() -> None\n\
\n\
Close handle and end curl session.\n\
\n\
Corresponds to `curl_easy_cleanup`_ in libcurl. This method is\n\
automatically called by pycurl when a Curl object no longer has any\n\
references to it, but can also be called explicitly.\n\
\n\
.. _curl_easy_cleanup:\n\
    https://curl.haxx.se/libcurl/c/curl_easy_cleanup.html";

PYCURL_INTERNAL const char curl_duphandle_doc[] = "duphandle() -> Curl\n\
\n\
Clone a curl handle. This function will return a new curl handle,\n\
a duplicate, using all the options previously set in the input curl handle.\n\
Both handles can subsequently be used independently.\n\
\n\
The new handle will not inherit any state information, no connections,\n\
no SSL sessions and no cookies. It also will not inherit any share object\n\
states or options (it will be made as if SHARE was unset).\n\
\n\
Corresponds to `curl_easy_duphandle`_ in libcurl.\n\
\n\
Example usage::\n\
\n\
    import pycurl\n\
    curl = pycurl.Curl()\n\
    curl.setopt(pycurl.URL, \"https://python.org\")\n\
    dup = curl.duphandle()\n\
    curl.perform()\n\
    dup.perform()\n\
\n\
.. _curl_easy_duphandle:\n\
    https://curl.se/libcurl/c/curl_easy_duphandle.html";

PYCURL_INTERNAL const char curl_errstr_doc[] = "errstr() -> string\n\
\n\
Return the internal libcurl error buffer of this handle as a string.\n\
\n\
Return value is a ``str`` instance on all Python versions.\n\
On Python 3, error buffer data is decoded using Python's default encoding\n\
at the time of the call. If this decoding fails, ``UnicodeDecodeError`` is\n\
raised. Use :ref:`errstr_raw <errstr_raw>` to retrieve the error buffer\n\
as a byte string in this case.\n\
\n\
On Python 2, ``errstr`` and ``errstr_raw`` behave identically.";

PYCURL_INTERNAL const char curl_errstr_raw_doc[] = "errstr_raw() -> byte string\n\
\n\
Return the internal libcurl error buffer of this handle as a byte string.\n\
\n\
Return value is a ``str`` instance on Python 2 and ``bytes`` instance\n\
on Python 3. Unlike :ref:`errstr_raw <errstr_raw>`, ``errstr_raw``\n\
allows reading libcurl error buffer in Python 3 when its contents is not\n\
valid in Python's default encoding.\n\
\n\
On Python 2, ``errstr`` and ``errstr_raw`` behave identically.\n\
\n\
*Added in version 7.43.0.2.*";

PYCURL_INTERNAL const char curl_getinfo_doc[] = "getinfo(option) -> Result\n\
\n\
Extract and return information from a curl session,\n\
decoding string data in Python's default encoding at the time of the call.\n\
Corresponds to `curl_easy_getinfo`_ in libcurl.\n\
The ``getinfo`` method should not be called unless\n\
``perform`` has been called and finished.\n\
\n\
*option* is a constant corresponding to one of the\n\
``CURLINFO_*`` constants in libcurl. Most option constant names match\n\
the respective ``CURLINFO_*`` constant names with the ``CURLINFO_`` prefix\n\
removed, for example ``CURLINFO_CONTENT_TYPE`` is accessible as\n\
``pycurl.CONTENT_TYPE``. Exceptions to this rule are as follows:\n\
\n\
- ``CURLINFO_FILETIME`` is mapped as ``pycurl.INFO_FILETIME``\n\
- ``CURLINFO_COOKIELIST`` is mapped as ``pycurl.INFO_COOKIELIST``\n\
- ``CURLINFO_CERTINFO`` is mapped as ``pycurl.INFO_CERTINFO``\n\
- ``CURLINFO_RTSP_CLIENT_CSEQ`` is mapped as ``pycurl.INFO_RTSP_CLIENT_CSEQ``\n\
- ``CURLINFO_RTSP_CSEQ_RECV`` is mapped as ``pycurl.INFO_RTSP_CSEQ_RECV``\n\
- ``CURLINFO_RTSP_SERVER_CSEQ`` is mapped as ``pycurl.INFO_RTSP_SERVER_CSEQ``\n\
- ``CURLINFO_RTSP_SESSION_ID`` is mapped as ``pycurl.INFO_RTSP_SESSION_ID``\n\
\n\
The type of return value depends on the option, as follows:\n\
\n\
- Options documented by libcurl to return an integer value return a\n\
  Python integer (``long`` on Python 2, ``int`` on Python 3).\n\
- Options documented by libcurl to return a floating point value\n\
  return a Python ``float``.\n\
- Options documented by libcurl to return a string value\n\
  return a Python string (``str`` on Python 2 and Python 3).\n\
  On Python 2, the string contains whatever data libcurl returned.\n\
  On Python 3, the data returned by libcurl is decoded using the\n\
  default string encoding at the time of the call.\n\
  If the data cannot be decoded using the default encoding, ``UnicodeDecodeError``\n\
  is raised. Use :ref:`getinfo_raw <getinfo_raw>`\n\
  to retrieve the data as ``bytes`` in these\n\
  cases.\n\
- ``SSL_ENGINES`` and ``INFO_COOKIELIST`` return a list of strings.\n\
  The same encoding caveats apply; use :ref:`getinfo_raw <getinfo_raw>`\n\
  to retrieve the\n\
  data as a list of byte strings.\n\
- ``INFO_CERTINFO`` returns a list with one element\n\
  per certificate in the chain, starting with the leaf; each element is a\n\
  sequence of *(key, value)* tuples where both ``key`` and ``value`` are\n\
  strings. String encoding caveats apply; use :ref:`getinfo_raw <getinfo_raw>`\n\
  to retrieve\n\
  certificate data as byte strings.\n\
\n\
On Python 2, ``getinfo`` and ``getinfo_raw`` behave identically.\n\
\n\
Example usage::\n\
\n\
    import pycurl\n\
    c = pycurl.Curl()\n\
    c.setopt(pycurl.OPT_CERTINFO, 1)\n\
    c.setopt(pycurl.URL, \"https://python.org\")\n\
    c.setopt(pycurl.FOLLOWLOCATION, 1)\n\
    c.perform()\n\
    print(c.getinfo(pycurl.HTTP_CODE))\n\
    # --> 200\n\
    print(c.getinfo(pycurl.EFFECTIVE_URL))\n\
    # --> \"https://www.python.org/\"\n\
    certinfo = c.getinfo(pycurl.INFO_CERTINFO)\n\
    print(certinfo)\n\
    # --> [(('Subject', 'C = AU, ST = Some-State, O = PycURL test suite,\n\
             CN = localhost'), ('Issuer', 'C = AU, ST = Some-State,\n\
             O = PycURL test suite, OU = localhost, CN = localhost'),\n\
            ('Version', '0'), ...)]\n\
\n\
\n\
Raises pycurl.error exception upon failure.\n\
\n\
.. _curl_easy_getinfo:\n\
    https://curl.haxx.se/libcurl/c/curl_easy_getinfo.html";

PYCURL_INTERNAL const char curl_getinfo_raw_doc[] = "getinfo_raw(option) -> Result\n\
\n\
Extract and return information from a curl session,\n\
returning string data as byte strings.\n\
Corresponds to `curl_easy_getinfo`_ in libcurl.\n\
The ``getinfo_raw`` method should not be called unless\n\
``perform`` has been called and finished.\n\
\n\
*option* is a constant corresponding to one of the\n\
``CURLINFO_*`` constants in libcurl. Most option constant names match\n\
the respective ``CURLINFO_*`` constant names with the ``CURLINFO_`` prefix\n\
removed, for example ``CURLINFO_CONTENT_TYPE`` is accessible as\n\
``pycurl.CONTENT_TYPE``. Exceptions to this rule are as follows:\n\
\n\
- ``CURLINFO_FILETIME`` is mapped as ``pycurl.INFO_FILETIME``\n\
- ``CURLINFO_COOKIELIST`` is mapped as ``pycurl.INFO_COOKIELIST``\n\
- ``CURLINFO_CERTINFO`` is mapped as ``pycurl.INFO_CERTINFO``\n\
- ``CURLINFO_RTSP_CLIENT_CSEQ`` is mapped as ``pycurl.INFO_RTSP_CLIENT_CSEQ``\n\
- ``CURLINFO_RTSP_CSEQ_RECV`` is mapped as ``pycurl.INFO_RTSP_CSEQ_RECV``\n\
- ``CURLINFO_RTSP_SERVER_CSEQ`` is mapped as ``pycurl.INFO_RTSP_SERVER_CSEQ``\n\
- ``CURLINFO_RTSP_SESSION_ID`` is mapped as ``pycurl.INFO_RTSP_SESSION_ID``\n\
\n\
The type of return value depends on the option, as follows:\n\
\n\
- Options documented by libcurl to return an integer value return a\n\
  Python integer (``long`` on Python 2, ``int`` on Python 3).\n\
- Options documented by libcurl to return a floating point value\n\
  return a Python ``float``.\n\
- Options documented by libcurl to return a string value\n\
  return a Python byte string (``str`` on Python 2, ``bytes`` on Python 3).\n\
  The string contains whatever data libcurl returned.\n\
  Use :ref:`getinfo <getinfo>` to retrieve this data as a Unicode string on Python 3.\n\
- ``SSL_ENGINES`` and ``INFO_COOKIELIST`` return a list of byte strings.\n\
  The same encoding caveats apply; use :ref:`getinfo <getinfo>` to retrieve the\n\
  data as a list of potentially Unicode strings.\n\
- ``INFO_CERTINFO`` returns a list with one element\n\
  per certificate in the chain, starting with the leaf; each element is a\n\
  sequence of *(key, value)* tuples where both ``key`` and ``value`` are\n\
  byte strings. String encoding caveats apply; use :ref:`getinfo <getinfo>`\n\
  to retrieve\n\
  certificate data as potentially Unicode strings.\n\
\n\
On Python 2, ``getinfo`` and ``getinfo_raw`` behave identically.\n\
\n\
Example usage::\n\
\n\
    import pycurl\n\
    c = pycurl.Curl()\n\
    c.setopt(pycurl.OPT_CERTINFO, 1)\n\
    c.setopt(pycurl.URL, \"https://python.org\")\n\
    c.setopt(pycurl.FOLLOWLOCATION, 1)\n\
    c.perform()\n\
    print(c.getinfo_raw(pycurl.HTTP_CODE))\n\
    # --> 200\n\
    print(c.getinfo_raw(pycurl.EFFECTIVE_URL))\n\
    # --> b\"https://www.python.org/\"\n\
    certinfo = c.getinfo_raw(pycurl.INFO_CERTINFO)\n\
    print(certinfo)\n\
    # --> [((b'Subject', b'C = AU, ST = Some-State, O = PycURL test suite,\n\
             CN = localhost'), (b'Issuer', b'C = AU, ST = Some-State,\n\
             O = PycURL test suite, OU = localhost, CN = localhost'),\n\
            (b'Version', b'0'), ...)]\n\
\n\
\n\
Raises pycurl.error exception upon failure.\n\
\n\
*Added in version 7.43.0.2.*\n\
\n\
.. _curl_easy_getinfo:\n\
    https://curl.haxx.se/libcurl/c/curl_easy_getinfo.html";

PYCURL_INTERNAL const char curl_pause_doc[] = "pause(bitmask) -> None\n\
\n\
Pause or unpause a curl handle. Bitmask should be a value such as\n\
PAUSE_RECV or PAUSE_CONT.\n\
\n\
Corresponds to `curl_easy_pause`_ in libcurl. The argument should be\n\
derived from the ``PAUSE_RECV``, ``PAUSE_SEND``, ``PAUSE_ALL`` and\n\
``PAUSE_CONT`` constants.\n\
\n\
Raises pycurl.error exception upon failure.\n\
\n\
.. _curl_easy_pause: https://curl.haxx.se/libcurl/c/curl_easy_pause.html";

PYCURL_INTERNAL const char curl_perform_doc[] = "perform() -> None\n\
\n\
Perform a file transfer.\n\
\n\
Corresponds to `curl_easy_perform`_ in libcurl.\n\
\n\
Raises pycurl.error exception upon failure.\n\
\n\
.. _curl_easy_perform:\n\
    https://curl.haxx.se/libcurl/c/curl_easy_perform.html";

PYCURL_INTERNAL const char curl_perform_rb_doc[] = "perform_rb() -> response_body\n\
\n\
Perform a file transfer and return response body as a byte string.\n\
\n\
This method arranges for response body to be saved in a StringIO\n\
(Python 2) or BytesIO (Python 3) instance, then invokes :ref:`perform <perform>`\n\
to perform the file transfer, then returns the value of the StringIO/BytesIO\n\
instance which is a ``str`` instance on Python 2 and ``bytes`` instance\n\
on Python 3. Errors during transfer raise ``pycurl.error`` exceptions\n\
just like in :ref:`perform <perform>`.\n\
\n\
Use :ref:`perform_rs <perform_rs>` to retrieve response body as a string\n\
(``str`` instance on both Python 2 and 3).\n\
\n\
Raises ``pycurl.error`` exception upon failure.\n\
\n\
*Added in version 7.43.0.2.*";

PYCURL_INTERNAL const char curl_perform_rs_doc[] = "perform_rs() -> response_body\n\
\n\
Perform a file transfer and return response body as a string.\n\
\n\
On Python 2, this method arranges for response body to be saved in a StringIO\n\
instance, then invokes :ref:`perform <perform>`\n\
to perform the file transfer, then returns the value of the StringIO instance.\n\
This behavior is identical to :ref:`perform_rb <perform_rb>`.\n\
\n\
On Python 3, this method arranges for response body to be saved in a BytesIO\n\
instance, then invokes :ref:`perform <perform>`\n\
to perform the file transfer, then decodes the response body in Python's\n\
default encoding and returns the decoded body as a Unicode string\n\
(``str`` instance). *Note:* decoding happens after the transfer finishes,\n\
thus an encoding error implies the transfer/network operation succeeded.\n\
\n\
Any transfer errors raise ``pycurl.error`` exception,\n\
just like in :ref:`perform <perform>`.\n\
\n\
Use :ref:`perform_rb <perform_rb>` to retrieve response body as a byte\n\
string (``bytes`` instance on Python 3) without attempting to decode it.\n\
\n\
Raises ``pycurl.error`` exception upon failure.\n\
\n\
*Added in version 7.43.0.2.*";

PYCURL_INTERNAL const char curl_reset_doc[] = "reset() -> None\n\
\n\
Reset all options set on curl handle to default values, but preserves\n\
live connections, session ID cache, DNS cache, cookies, and shares.\n\
\n\
Corresponds to `curl_easy_reset`_ in libcurl.\n\
\n\
.. _curl_easy_reset: https://curl.haxx.se/libcurl/c/curl_easy_reset.html";

PYCURL_INTERNAL const char curl_set_ca_certs_doc[] = "set_ca_certs() -> None\n\
\n\
Load ca certs from provided unicode string.\n\
\n\
Note that certificates will be added only when cURL starts new connection.";

PYCURL_INTERNAL const char curl_setopt_doc[] = "setopt(option, value) -> None\n\
\n\
Set curl session option. Corresponds to `curl_easy_setopt`_ in libcurl.\n\
\n\
*option* specifies which option to set. PycURL defines constants\n\
corresponding to ``CURLOPT_*`` constants in libcurl, except that\n\
the ``CURLOPT_`` prefix is removed. For example, ``CURLOPT_URL`` is\n\
exposed in PycURL as ``pycurl.URL``, with some exceptions as detailed below.\n\
For convenience, ``CURLOPT_*``\n\
constants are also exposed on the Curl objects themselves::\n\
\n\
    import pycurl\n\
    c = pycurl.Curl()\n\
    c.setopt(pycurl.URL, \"http://www.python.org/\")\n\
    # Same as:\n\
    c.setopt(c.URL, \"http://www.python.org/\")\n\
\n\
The following are exceptions to option constant naming convention:\n\
\n\
- ``CURLOPT_FILETIME`` is mapped as ``pycurl.OPT_FILETIME``\n\
- ``CURLOPT_CERTINFO`` is mapped as ``pycurl.OPT_CERTINFO``\n\
- ``CURLOPT_COOKIELIST`` is mapped as ``pycurl.COOKIELIST``\n\
  and, as of PycURL 7.43.0.2, also as ``pycurl.OPT_COOKIELIST``\n\
- ``CURLOPT_RTSP_CLIENT_CSEQ`` is mapped as ``pycurl.OPT_RTSP_CLIENT_CSEQ``\n\
- ``CURLOPT_RTSP_REQUEST`` is mapped as ``pycurl.OPT_RTSP_REQUEST``\n\
- ``CURLOPT_RTSP_SERVER_CSEQ`` is mapped as ``pycurl.OPT_RTSP_SERVER_CSEQ``\n\
- ``CURLOPT_RTSP_SESSION_ID`` is mapped as ``pycurl.OPT_RTSP_SESSION_ID``\n\
- ``CURLOPT_RTSP_STREAM_URI`` is mapped as ``pycurl.OPT_RTSP_STREAM_URI``\n\
- ``CURLOPT_RTSP_TRANSPORT`` is mapped as ``pycurl.OPT_RTSP_TRANSPORT``\n\
\n\
*value* specifies the value to set the option to. Different options accept\n\
values of different types:\n\
\n\
- Options specified by `curl_easy_setopt`_ as accepting ``1`` or an\n\
  integer value accept Python integers, long integers (on Python 2.x) and\n\
  booleans::\n\
\n\
    c.setopt(pycurl.FOLLOWLOCATION, True)\n\
    c.setopt(pycurl.FOLLOWLOCATION, 1)\n\
    # Python 2.x only:\n\
    c.setopt(pycurl.FOLLOWLOCATION, 1L)\n\
\n\
- Options specified as accepting strings by ``curl_easy_setopt`` accept\n\
  byte strings (``str`` on Python 2, ``bytes`` on Python 3) and\n\
  Unicode strings with ASCII code points only.\n\
  For more information, please refer to :ref:`unicode`. Example::\n\
\n\
    c.setopt(pycurl.URL, \"http://www.python.org/\")\n\
    c.setopt(pycurl.URL, u\"http://www.python.org/\")\n\
    # Python 3.x only:\n\
    c.setopt(pycurl.URL, b\"http://www.python.org/\")\n\
\n\
- ``HTTP200ALIASES``, ``HTTPHEADER``, ``POSTQUOTE``, ``PREQUOTE``,\n\
  ``PROXYHEADER`` and\n\
  ``QUOTE`` accept a list or tuple of strings. The same rules apply to these\n\
  strings as do to string option values. Example::\n\
\n\
    c.setopt(pycurl.HTTPHEADER, [\"Accept:\"])\n\
    c.setopt(pycurl.HTTPHEADER, (\"Accept:\",))\n\
\n\
- ``READDATA`` accepts a file object or any Python object which has\n\
  a ``read`` method. On Python 2, a file object will be passed directly\n\
  to libcurl and may result in greater transfer efficiency, unless\n\
  PycURL has been compiled with ``AVOID_STDIO`` option.\n\
  On Python 3 and on Python 2 when the value is not a true file object,\n\
  ``READDATA`` is emulated in PycURL via ``READFUNCTION``.\n\
  The file should generally be opened in binary mode. Example::\n\
\n\
    f = open('file.txt', 'rb')\n\
    c.setopt(c.READDATA, f)\n\
\n\
- ``WRITEDATA`` and ``WRITEHEADER`` accept a file object or any Python\n\
  object which has a ``write`` method. On Python 2, a file object will\n\
  be passed directly to libcurl and may result in greater transfer efficiency,\n\
  unless PycURL has been compiled with ``AVOID_STDIO`` option.\n\
  On Python 3 and on Python 2 when the value is not a true file object,\n\
  ``WRITEDATA`` is emulated in PycURL via ``WRITEFUNCTION``.\n\
  The file should generally be opened in binary mode. Example::\n\
\n\
    f = open('/dev/null', 'wb')\n\
    c.setopt(c.WRITEDATA, f)\n\
\n\
- ``*FUNCTION`` options accept a function. Supported callbacks are documented\n\
  in :ref:`callbacks`. Example::\n\
\n\
    # Python 2\n\
    import StringIO\n\
    b = StringIO.StringIO()\n\
    c.setopt(pycurl.WRITEFUNCTION, b.write)\n\
\n\
- ``SHARE`` option accepts a :ref:`curlshareobject`.\n\
\n\
- ``STDERR`` option is not currently supported.\n\
\n\
It is possible to set integer options - and only them - that PycURL does\n\
not know about by using the numeric value of the option constant directly.\n\
For example, ``pycurl.VERBOSE`` has the value 42, and may be set as follows::\n\
\n\
    c.setopt(42, 1)\n\
\n\
*setopt* can reset some options to their default value, performing the job of\n\
:py:meth:`pycurl.Curl.unsetopt`, if ``None`` is passed\n\
for the option value. The following two calls are equivalent::\n\
\n\
    c.setopt(c.URL, None)\n\
    c.unsetopt(c.URL)\n\
\n\
Raises TypeError when the option value is not of a type accepted by the\n\
respective option, and pycurl.error exception when libcurl rejects the\n\
option or its value.\n\
\n\
.. _curl_easy_setopt: https://curl.haxx.se/libcurl/c/curl_easy_setopt.html";

PYCURL_INTERNAL const char curl_setopt_string_doc[] = "setopt_string(option, value) -> None\n\
\n\
Set curl session option to a string value.\n\
\n\
This method allows setting string options that are not officially supported\n\
by PycURL, for example because they did not exist when the version of PycURL\n\
being used was released.\n\
:py:meth:`pycurl.Curl.setopt` should be used for setting options that\n\
PycURL knows about.\n\
\n\
**Warning:** No checking is performed that *option* does, in fact,\n\
expect a string value. Using this method incorrectly can crash the program\n\
and may lead to a security vulnerability.\n\
Furthermore, it is on the application to ensure that the *value* object\n\
does not get garbage collected while libcurl is using it.\n\
libcurl copies most string options but not all; one option whose value\n\
is not copied by libcurl is `CURLOPT_POSTFIELDS`_.\n\
\n\
*option* would generally need to be given as an integer literal rather than\n\
a symbolic constant.\n\
\n\
*value* can be a binary string or a Unicode string using ASCII code points,\n\
same as with string options given to PycURL elsewhere.\n\
\n\
Example setting URL via ``setopt_string``::\n\
\n\
    import pycurl\n\
    c = pycurl.Curl()\n\
    c.setopt_string(10002, \"http://www.python.org/\")\n\
\n\
.. _CURLOPT_POSTFIELDS: https://curl.haxx.se/libcurl/c/CURLOPT_POSTFIELDS.html";

PYCURL_INTERNAL const char curl_unsetopt_doc[] = "unsetopt(option) -> None\n\
\n\
Reset curl session option to its default value.\n\
\n\
Only some curl options may be reset via this method.\n\
\n\
libcurl does not provide a general way to reset a single option to its default value;\n\
:py:meth:`pycurl.Curl.reset` resets all options to their default values,\n\
otherwise :py:meth:`pycurl.Curl.setopt` must be called with whatever value\n\
is the default. For convenience, PycURL provides this unsetopt method\n\
to reset some of the options to their default values.\n\
\n\
Raises pycurl.error exception on failure.\n\
\n\
``c.unsetopt(option)`` is equivalent to ``c.setopt(option, None)``.";

PYCURL_INTERNAL const char multi_doc[] = "CurlMulti() -> New CurlMulti object\n\
\n\
Creates a new :ref:`curlmultiobject` which corresponds to\n\
a ``CURLM`` handle in libcurl.";

PYCURL_INTERNAL const char multi_add_handle_doc[] = "add_handle(Curl object) -> None\n\
\n\
Corresponds to `curl_multi_add_handle`_ in libcurl. This method adds an\n\
existing and valid Curl object to the CurlMulti object.\n\
\n\
*Changed in version 7.43.0.2:* add_handle now ensures that the Curl object\n\
is not garbage collected while it is being used by a CurlMulti object.\n\
Previously application had to maintain an outstanding reference to the Curl\n\
object to keep it from being garbage collected.\n\
\n\
.. _curl_multi_add_handle:\n\
    https://curl.haxx.se/libcurl/c/curl_multi_add_handle.html";

PYCURL_INTERNAL const char multi_assign_doc[] = "assign(sock_fd, object) -> None\n\
\n\
Creates an association in the multi handle between the given socket and\n\
a private object in the application.\n\
Corresponds to `curl_multi_assign`_ in libcurl.\n\
\n\
.. _curl_multi_assign: https://curl.haxx.se/libcurl/c/curl_multi_assign.html";

PYCURL_INTERNAL const char multi_close_doc[] = "close() -> None\n\
\n\
Corresponds to `curl_multi_cleanup`_ in libcurl. This method is\n\
automatically called by pycurl when a CurlMulti object no longer has any\n\
references to it, but can also be called explicitly.\n\
\n\
.. _curl_multi_cleanup:\n\
    https://curl.haxx.se/libcurl/c/curl_multi_cleanup.html";

PYCURL_INTERNAL const char multi_fdset_doc[] = "fdset() -> tuple of lists with active file descriptors, readable, writeable, exceptions\n\
\n\
Returns a tuple of three lists that can be passed to the select.select() method.\n\
\n\
Corresponds to `curl_multi_fdset`_ in libcurl. This method extracts the\n\
file descriptor information from a CurlMulti object. The returned lists can\n\
be used with the ``select`` module to poll for events.\n\
\n\
Example usage::\n\
\n\
    import pycurl\n\
    c = pycurl.Curl()\n\
    c.setopt(pycurl.URL, \"https://curl.haxx.se\")\n\
    m = pycurl.CurlMulti()\n\
    m.add_handle(c)\n\
    while 1:\n\
        ret, num_handles = m.perform()\n\
        if ret != pycurl.E_CALL_MULTI_PERFORM: break\n\
    while num_handles:\n\
        apply(select.select, m.fdset() + (1,))\n\
        while 1:\n\
            ret, num_handles = m.perform()\n\
            if ret != pycurl.E_CALL_MULTI_PERFORM: break\n\
\n\
.. _curl_multi_fdset:\n\
    https://curl.haxx.se/libcurl/c/curl_multi_fdset.html";

PYCURL_INTERNAL const char multi_info_read_doc[] = "info_read([max_objects]) -> tuple(number of queued messages, a list of successful objects, a list of failed objects)\n\
\n\
Corresponds to the `curl_multi_info_read`_ function in libcurl.\n\
\n\
This method extracts at most *max* messages from the multi stack and returns\n\
them in two lists. The first list contains the handles which completed\n\
successfully and the second list contains a tuple *(curl object, curl error\n\
number, curl error message)* for each failed curl object. The curl error\n\
message is returned as a Python string which is decoded from the curl error\n\
string using the `surrogateescape`_ error handler. The number of\n\
queued messages after this method has been called is also returned.\n\
\n\
.. _curl_multi_info_read:\n\
    https://curl.haxx.se/libcurl/c/curl_multi_info_read.html\n\
\n\
.. _surrogateescape:\n\
    https://www.python.org/dev/peps/pep-0383/";

PYCURL_INTERNAL const char multi_perform_doc[] = "perform() -> tuple of status and the number of active Curl objects\n\
\n\
Corresponds to `curl_multi_perform`_ in libcurl.\n\
\n\
.. _curl_multi_perform:\n\
    https://curl.haxx.se/libcurl/c/curl_multi_perform.html";

PYCURL_INTERNAL const char multi_remove_handle_doc[] = "remove_handle(Curl object) -> None\n\
\n\
Corresponds to `curl_multi_remove_handle`_ in libcurl. This method\n\
removes an existing and valid Curl object from the CurlMulti object.\n\
\n\
.. _curl_multi_remove_handle:\n\
    https://curl.haxx.se/libcurl/c/curl_multi_remove_handle.html";

PYCURL_INTERNAL const char multi_select_doc[] = "select([timeout]) -> number of ready file descriptors or 0 on timeout\n\
\n\
Returns result from doing a select() on the curl multi file descriptor\n\
with the given timeout.\n\
\n\
This is a convenience function which simplifies the combined use of\n\
``fdset()`` and the ``select`` module.\n\
\n\
Example usage::\n\
\n\
    import pycurl\n\
    c = pycurl.Curl()\n\
    c.setopt(pycurl.URL, \"https://curl.haxx.se\")\n\
    m = pycurl.CurlMulti()\n\
    m.add_handle(c)\n\
    while 1:\n\
        ret, num_handles = m.perform()\n\
        if ret != pycurl.E_CALL_MULTI_PERFORM: break\n\
    while num_handles:\n\
        ret = m.select(1.0)\n\
        if ret == 0:  continue\n\
        while 1:\n\
            ret, num_handles = m.perform()\n\
            if ret != pycurl.E_CALL_MULTI_PERFORM: break";

PYCURL_INTERNAL const char multi_setopt_doc[] = "setopt(option, value) -> None\n\
\n\
Set curl multi option. Corresponds to `curl_multi_setopt`_ in libcurl.\n\
\n\
*option* specifies which option to set. PycURL defines constants\n\
corresponding to ``CURLMOPT_*`` constants in libcurl, except that\n\
the ``CURLMOPT_`` prefix is replaced with ``M_`` prefix.\n\
For example, ``CURLMOPT_PIPELINING`` is\n\
exposed in PycURL as ``pycurl.M_PIPELINING``. For convenience, ``CURLMOPT_*``\n\
constants are also exposed on CurlMulti objects::\n\
\n\
    import pycurl\n\
    m = pycurl.CurlMulti()\n\
    m.setopt(pycurl.M_PIPELINING, 1)\n\
    # Same as:\n\
    m.setopt(m.M_PIPELINING, 1)\n\
\n\
*value* specifies the value to set the option to. Different options accept\n\
values of different types:\n\
\n\
- Options specified by `curl_multi_setopt`_ as accepting ``1`` or an\n\
  integer value accept Python integers, long integers (on Python 2.x) and\n\
  booleans::\n\
\n\
    m.setopt(pycurl.M_PIPELINING, True)\n\
    m.setopt(pycurl.M_PIPELINING, 1)\n\
    # Python 2.x only:\n\
    m.setopt(pycurl.M_PIPELINING, 1L)\n\
\n\
- ``*FUNCTION`` options accept a function. Supported callbacks are\n\
  ``CURLMOPT_SOCKETFUNCTION`` AND ``CURLMOPT_TIMERFUNCTION``. Please refer to\n\
  the PycURL test suite for examples on using the callbacks.\n\
\n\
Raises TypeError when the option value is not of a type accepted by the\n\
respective option, and pycurl.error exception when libcurl rejects the\n\
option or its value.\n\
\n\
.. _curl_multi_setopt: https://curl.haxx.se/libcurl/c/curl_multi_setopt.html";

PYCURL_INTERNAL const char multi_socket_action_doc[] = "socket_action(sock_fd, ev_bitmask) -> (result, num_running_handles)\n\
\n\
Returns result from doing a socket_action() on the curl multi file descriptor\n\
with the given timeout.\n\
Corresponds to `curl_multi_socket_action`_ in libcurl.\n\
\n\
The return value is a two-element tuple. The first element is the return\n\
value of the underlying ``curl_multi_socket_action`` function, and it is\n\
always zero (``CURLE_OK``) because any other return value would cause\n\
``socket_action`` to raise an exception. The second element is the number of\n\
running easy handles within this multi handle. When the number of running\n\
handles reaches zero, all transfers have completed. Note that if the number\n\
of running handles has decreased by one compared to the previous invocation,\n\
this is not mean the handle corresponding to the ``sock_fd`` provided as\n\
the argument to this function was the completed handle.\n\
\n\
.. _curl_multi_socket_action: https://curl.haxx.se/libcurl/c/curl_multi_socket_action.html";

PYCURL_INTERNAL const char multi_socket_all_doc[] = "socket_all() -> tuple\n\
\n\
Returns result from doing a socket_all() on the curl multi file descriptor\n\
with the given timeout.";

PYCURL_INTERNAL const char multi_timeout_doc[] = "timeout() -> int\n\
\n\
Returns how long to wait for action before proceeding.\n\
Corresponds to `curl_multi_timeout`_ in libcurl.\n\
\n\
.. _curl_multi_timeout: https://curl.haxx.se/libcurl/c/curl_multi_timeout.html";

PYCURL_INTERNAL const char pycurl_global_cleanup_doc[] = "global_cleanup() -> None\n\
\n\
Cleanup curl environment.\n\
\n\
Corresponds to `curl_global_cleanup`_ in libcurl.\n\
\n\
.. _curl_global_cleanup: https://curl.haxx.se/libcurl/c/curl_global_cleanup.html";

PYCURL_INTERNAL const char pycurl_global_init_doc[] = "global_init(option) -> None\n\
\n\
Initialize curl environment.\n\
\n\
*option* is one of the constants pycurl.GLOBAL_SSL, pycurl.GLOBAL_WIN32,\n\
pycurl.GLOBAL_ALL, pycurl.GLOBAL_NOTHING, pycurl.GLOBAL_DEFAULT.\n\
\n\
Corresponds to `curl_global_init`_ in libcurl.\n\
\n\
.. _curl_global_init: https://curl.haxx.se/libcurl/c/curl_global_init.html";

PYCURL_INTERNAL const char pycurl_module_doc[] = "This module implements an interface to the cURL library.\n\
\n\
Types:\n\
\n\
Curl() -> New object.  Create a new curl object.\n\
CurlMulti() -> New object.  Create a new curl multi object.\n\
CurlShare() -> New object.  Create a new curl share object.\n\
\n\
Functions:\n\
\n\
global_init(option) -> None.  Initialize curl environment.\n\
global_cleanup() -> None.  Cleanup curl environment.\n\
version_info() -> tuple.  Return version information.";

PYCURL_INTERNAL const char pycurl_version_info_doc[] = "version_info() -> tuple\n\
\n\
Returns a 12-tuple with the version info.\n\
\n\
Corresponds to `curl_version_info`_ in libcurl. Returns a tuple of\n\
information which is similar to the ``curl_version_info_data`` struct\n\
returned by ``curl_version_info()`` in libcurl.\n\
\n\
Example usage::\n\
\n\
    >>> import pycurl\n\
    >>> pycurl.version_info()\n\
    (3, '7.33.0', 467200, 'amd64-portbld-freebsd9.1', 33436, 'OpenSSL/0.9.8x',\n\
    0, '1.2.7', ('dict', 'file', 'ftp', 'ftps', 'gopher', 'http', 'https',\n\
    'imap', 'imaps', 'pop3', 'pop3s', 'rtsp', 'smtp', 'smtps', 'telnet',\n\
    'tftp'), None, 0, None)\n\
\n\
.. _curl_version_info: https://curl.haxx.se/libcurl/c/curl_version_info.html";

PYCURL_INTERNAL const char share_doc[] = "CurlShare() -> New CurlShare object\n\
\n\
Creates a new :ref:`curlshareobject` which corresponds to a\n\
``CURLSH`` handle in libcurl. CurlShare objects is what you pass as an\n\
argument to the SHARE option on :ref:`Curl objects <curlobject>`.";

PYCURL_INTERNAL const char share_close_doc[] = "close() -> None\n\
\n\
Close shared handle.\n\
\n\
Corresponds to `curl_share_cleanup`_ in libcurl. This method is\n\
automatically called by pycurl when a CurlShare object no longer has\n\
any references to it, but can also be called explicitly.\n\
\n\
.. _curl_share_cleanup:\n\
    https://curl.haxx.se/libcurl/c/curl_share_cleanup.html";

PYCURL_INTERNAL const char share_setopt_doc[] = "setopt(option, value) -> None\n\
\n\
Set curl share option.\n\
\n\
Corresponds to `curl_share_setopt`_ in libcurl, where *option* is\n\
specified with the ``CURLSHOPT_*`` constants in libcurl, except that the\n\
``CURLSHOPT_`` prefix has been changed to ``SH_``. Currently, *value* must be\n\
one of: ``LOCK_DATA_COOKIE``, ``LOCK_DATA_DNS``, ``LOCK_DATA_SSL_SESSION`` or\n\
``LOCK_DATA_CONNECT``.\n\
\n\
Example usage::\n\
\n\
    import pycurl\n\
    curl = pycurl.Curl()\n\
    s = pycurl.CurlShare()\n\
    s.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_COOKIE)\n\
    s.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_DNS)\n\
    curl.setopt(pycurl.URL, 'https://curl.haxx.se')\n\
    curl.setopt(pycurl.SHARE, s)\n\
    curl.perform()\n\
    curl.close()\n\
\n\
Raises pycurl.error exception upon failure.\n\
\n\
.. _curl_share_setopt:\n\
    https://curl.haxx.se/libcurl/c/curl_share_setopt.html";

