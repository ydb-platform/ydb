|badge1| |badge2| |badge3|

.. |badge1| image:: https://img.shields.io/github/actions/workflow/status/vsajip/python-gnupg/python-package.yml
   :alt: GitHub test status

.. |badge2| image:: https://img.shields.io/codecov/c/github/vsajip/python-gnupg
   :target: https://app.codecov.io/gh/vsajip/python-gnupg
   :alt: GitHub coverage status

.. |badge3| image:: https://img.shields.io/pypi/v/python-gnupg
   :target: https://pypi.org/project/python-gnupg/
   :alt: PyPI package


What is it?
===========

The GNU Privacy Guard (gpg, or gpg.exe on Windows) is a command-line program
which provides support for programmatic access via spawning a separate process
to run it and then communicating with that process from your program.

This project, ``python-gnupg``, implements a Python library which takes care
of the internal details and allows its users to generate and manage keys,
encrypt and decrypt data, and sign and verify messages.

Installation
============

Installing from PyPI
--------------------

You can install this package from the Python Package Index (pyPI) by running::

    pip install python-gnupg

.. important::
   There is at least one fork of this project, which was apparently created
   because an earlier version of this software used the ``subprocess`` module
   with ``shell=True``, making it vulnerable to shell injection. **This is no
   longer the case**.

   Forks may not be drop-in compatible with this software, so take care to use
   the correct version, as indicated in the ``pip install`` command above.


Installing from a source distribution archive
---------------------------------------------
To install this package from a source distribution archive, do the following:

1. Extract all the files in the distribution archive to some directory on your
   system.
2. In that directory, run ``pip install .``, referencing a suitable ``pip`` (e.g. one
   from a specific venv which you want to install to).
3. Optionally, run ``python test_gnupg.py`` to ensure that the package is
   working as expected.

Credits
=======

* The developers of the GNU Privacy Guard.
* The original version of this module was developed by Andrew Kuchling.
* It was improved by Richard Jones.
* It was further improved by Steve Traugott.

The present incarnation, based on the earlier versions, uses the ``subprocess``
module and so works on Windows as well as Unix/Linux platforms. It's not,
however, 100% backwards-compatible with earlier incarnations.

Change log
==========

.. note:: GCnn refers to an issue nn on Google Code.


0.5.7 (future)
--------------

Released: Not yet

0.5.6
-----

Released: 2925-12-31

* Fix #261: Ensure capability, fingerprint and keygrip are added to subkey_info.

* Set username in the result when Verify uses a signing key that has expired or been
  revoked. Thanks to Steven Galgano for the patch.

0.5.5
-----

Released: 2025-08-04

* Fix #249: Handle fetching GPG version when not the first item in the configuration.

* Fix #250: Capture uid info in a uid_map attribute of ScanKeys/ListKeys.

* Fix #255: Improve handling of exceptions raised in background threads.


0.5.4
-----

Released: 2025-01-07

* Fix #242: Handle exceptions in ``on_data`` callable.


0.5.3
-----

Released: 2024-09-20

* Fix #117: Add WKD (Web Key Directory) support for auto-locating keys. Thanks to Myzel394
  for the patch.

* Fix #237: Ensure local variable is initialized even when an exception occurs.

* Fix #239: Remove logging of decryption result.


0.5.2
-----

Released: 2023-12-12

* Fix #228: Clarify documentation for encryption/decryption.

* Make I/O buffer size configurable via ``buffer_size`` attribute on a ``GPG`` instance.


0.5.1
-----

Released: 2023-07-22

* Added ``TRUST_EXPIRED`` to ``trust_keys``. Thanks to Leif Liddy for the patch.

* Fix #206: Remove deprecated ``--always-trust`` in favour of ``--trust-model always``

* Fix #208: Add ``status_detail`` attribute to result objects which is populated when
  the status is ``'invalid recipient'`` (encryption/decryption) or ``'invalid signer'``
  (signing). This attribute will be set when the result object's ``status`` attribute is
  set to ``invalid recipient`` and will contain more information about the failure in the
  form of ``reason:ident`` where ``reason`` is a text description of the reason, and
  ``ident`` identifies the recipient key.

* Add ``scan_keys_mem()`` function to scan keys in a string. Thanks to Sky Moore
  for the patch.

* Fix #214: Handle multiple signatures when one of them is invalid or unverified.

* A ``problems`` attribute was added which holds problems reported by ``gpg``
  during verification. This is a list of dictionaries, one for each reported
  problem. Each dictionary will have ``status`` and ``keyid`` keys indicating
  the problem and the corresponding key; other information in the dictionaries
  will be error specific.

* Fix #217: Use machine-readable interface to query the ``gpg`` version. Thanks to Justus
  Winter for the patch.

* Added the ability to export keys to a file. Thanks to Leif Liddy for the patch.


0.5.0
-----

Released: 2022-08-23

* Fixed #181: Added the ability to pass file paths to encrypt_file, decrypt_file,
  sign_file, verify_file, get_recipients_file and added import_keys_file.

* Fixed #183: Handle FAILURE and UNEXPECTED conditions correctly. Thanks to sebbASF for
  the patch.

* Fixed #185: Handle VALIDSIG arguments more robustly.

* Fixed #188: Remove handling of DECRYPTION_FAILED from Verify code, as not required
  there. Thanks to sebbASF for the patch.

* Fixed #190: Handle KEY_CREATED more robustly.

* Fixed #191: Handle NODATA messages during verification.

* Fixed #196: Don't log chunk data by default, as it could contain sensitive
  information (during decryption, for example).

* Added the ability to pass an environment to the gpg executable. Thanks to Edvard
  Rejthar for the patch.


0.4.9
-----

Released: 2022-05-20

* Fixed #161: Added a status attribute to the returned object from gen_key() which
  is set to 'ok' if a key was successfully created, or 'key not created' if that
  was reported by gpg, or None in any other case.

* Fixed #164: Provided the ability to add subkeys. Thanks to Daniel Kilimnik for the
  feature request and patch.

* Fixed #166: Added keygrip values to the information collected when keys are listed.
  Thanks to Daniel Kilimnik for the feature request and patch.

* Fixed #173: Added extra_args to send_keys(), recv_keys() and search_keys() to allow
  passing options relating to key servers.

0.4.8
-----

Released: 2021-11-24

* Fixed #147: Return gpg's return code in all result instances.

* Fixed #152: Add check for invalid file objects.

* Fixed #157: Provide more useful status message when a secret key is absent.

* Fixed #158: Added a get_recipients() API to find the recipients of an encrypted
  message without decrypting it.


0.4.7
-----

Released: 2021-03-11

* Fixed #129, #141: Added support for no passphrase during key generation.

* Fixed #143: Improved permission-denied test. Thanks to Elliot Cameron for the patch.

* Fixed #144: Updated logging to only show partial results.

* Fixed #146: Allowed a passphrase to be passed to import_keys(). Thanks to Chris de
  Graaf for the patch.


0.4.6
-----

Released: 2020-04-17

* Fixed #122: Updated documentation about gnupghome needing to be an existing
  directory.

* Fixed #123: Handled error conditions from gpg when calling trust_keys().

* Fixed #124: Avoided an exception being raised when ImportResult.summary()
  was called after a failed recv_keys().

* Fixed #128: Added ECC support by changing key generation parameters. (The Key-Length
  value isn't added if a curve is specified.)

* Fixed #130: Provided a mechanism to provide more complete error messages.

Support for Python versions 3.5 and under is discontinued, except for Python 2.7.


0.4.5
-----

Released: 2019-08-12

* Fixed #107: Improved documentation.

* Fixed #112: Raised a ValueError if a gnupghome is specified which is not an
  existing directory.

* Fixed #113: Corrected stale link in the documentation.

* Fixed #116: Updated documentation to clarify when spurious key-expired/
  signature-expired messages might be seen.

* Fixed #119: Added --yes to avoid pinentry when deleting secret keys with
  GnuPG >= 2.1.

* A warning is logged if gpg returns a non-zero return code.

* Added ``extra_args`` to ``import_keys``.

* Added support for CI using AppVeyor.


0.4.4
-----

Released: 2019-01-24

* Fixed #108: Changed how any return value from the ``on_data`` callable is
  processed. In earlier versions, the return value was ignored. In this version,
  if the return value is ``False``, the data received from ``gpg`` is not
  buffered. Otherwise (if the value is ``None`` or ``True``, for example), the
  data is buffered as normal. This functionality can be used to do your own
  buffering, or to prevent buffering altogether.

  The ``on_data`` callable is also called once with an empty byte-string to
  signal the end of data from ``gpg``.

* Fixed #97: Added an additional attribute ``check_fingerprint_collisions`` to
  ``GPG`` instances, which defaults to ``False``. It seems that ``gpg`` is happy
  to have duplicate keys and fingerprints in a keyring, so we can't be too
  strict. A user can set this attribute of an instance to ``True`` to trigger a
  check for collisions.

* Fixed #111: With GnuPG 2.2.7 or later, provide the fingerprint of a signing
  key for a failed signature verification, if available.

* Fixed #21: For verification where multiple signatures are involved, a
  mapping of signature_ids to fingerprint, keyid, username, creation date,
  creation timestamp and expiry timestamp is provided.

* Added a check to disallow certain control characters ('\r', '\n', NUL) in
  passphrases.


0.4.3
-----

Released: 2018-06-13

* Added --no-verbose to the gpg command line, in case verbose is specified in
  gpg.conf - we don't need verbose output.


0.4.2
-----

Released: 2018-03-28

* Fixed #81: Subkey information is now collected and returned in a ``subkey_info``
  dictionary keyed by the subkey's ID.

* Fixed #84: GPG2 version is now correctly detected on OS X.

* Fixed #94: Added ``expect_passphrase`` password for use on GnuPG >= 2.1 when
  passing passphrase to ``gpg`` via pinentry.

* Fixed #95: Provided a ``trust_keys`` method to allow setting the trust level
  for keys. Thanks to William Foster for a suggested implementation.

* Made the exception message when the gpg executable is not found contain the
  path of the executable that was tried. Thanks to Kostis Anagnostopoulos for
  the suggestion.

* Fixed #100: Made the error message less categorical in the case of a failure
  with an unspecified reason, adding some information from gpg error codes when
  available.


0.4.1
-----

Released: 2017-07-06

* Updated message handling logic to no longer raise exceptions when a message
  isn't recognised. Thanks to Daniel Kahn Gillmor for the patch.

* Always use always use ``--fixed-list-mode``, ``--batch`` and
  ``--with-colons``. Thanks to Daniel Kahn Gillmor for the patch.

* Improved ``scan_keys()`` handling on GnuPG >= 2.1. Thanks to Daniel Kahn
  Gillmor for the patch.

* Improved test behaviour with GnuPG >= 2.1. Failures when deleting test
  directory trees are now ignored. Thanks to Daniel Kahn Gillmor for the patch.

* Added ``close_file`` keyword argument to verify_file to allow the file closing
  to be made optional. Current behaviour is maintained - ``close_file=False``
  can be passed to skip closing the file being verified.

* Added the ``extra_args`` keyword parameter to allow custom arguments to be
  passed to the ``gpg`` executable.

* Instances of the ``GPG`` class now have an additional ``on_data`` attribute,
  which defaults to ``None``. It can be set to a callable which will be called
  with a single argument - a binary chunk of data received from the ``gpg``
  executable. The callable can do whatever it likes with the chunks passed to it
  - e.g. write them to a separate stream. The callable should not raise any
  exceptions (unless it wants the current operation to fail).


0.4.0
-----

Released: 2017-01-29

* Added support for ``KEY_CONSIDERED`` in more places - encryption /
  decryption, signing, key generation and key import.

* Partial fix for #32 (GPG 2.1 compatibility). Unfortunately, better
  support cannot be provided at this point, unless there are certain
  changes (relating to pinentry popups) in how GPG 2.1 works.

* Fixed #60: An IndexError was being thrown by ``scan_keys()``.

* Ensured that utf-8 encoding is used when the ``--with-column`` mode is
  used. Thanks to Yann Leboulanger for the patch.

* ``list_keys()`` now uses ``--fixed-list-mode``. Thanks to Werner Koch
  for the pointer.


0.3.9
-----

Released: 2016-09-10

* Fixed #38: You can now request information about signatures against
  keys. Thanks to SunDwarf for the suggestion and patch, which was used
  as a basis for this change.

* Fixed #49: When exporting keys, no attempt is made to decode the output when
  armor=False is specified.

* Fixed #53: A ``FAILURE`` message caused by passing an incorrect passphrase
  is handled.

* Handled ``EXPORTED`` and ``EXPORT_RES`` messages while exporting keys. Thanks
  to Marcel Pörner for the patch.

* Fixed #54: Improved error message shown when gpg is not available.

* Fixed #55: Added support for ``KEY_CONSIDERED`` while verifying.

* Avoided encoding problems with filenames under Windows. Thanks to Kévin
  Bernard-Allies for the patch.

* Fixed #57: Used a better mechanism for comparing keys.


0.3.8
-----

Released: 2015-09-24

* Fixed #22: handled ``PROGRESS`` messages during verification and signing.

* Fixed #26: handled ``PINENTRY_LAUNCHED`` messages during verification,
  decryption and key generation.

* Fixed #28: Allowed a default Name-Email to be computed even when neither of
  ``LOGNAME`` and ``USERNAME`` are in the environment.

* Fixed #29: Included test files missing from the tarball in previous versions.

* Fixed #39: On Python 3.x, passing a text instead of a binary stream caused
  file decryption to hang due to a ``UnicodeDecodeError``. This has now been
  correctly handled: The decryption fails with a "no data" status.

* Fixed #41: Handled Unicode filenames correctly by encoding them on 2.x using
  the file system encoding.

* Fixed #43: handled ``PINENTRY_LAUNCHED`` messages during key export. Thanks
  to Ian Denhardt for looking into this.

* Hide the console window which appears on Windows when gpg is spawned.
  Thanks to Kévin Bernard-Allies for the patch.

* Subkey fingerprints are now captured.

* The returned value from the ``list_keys`` method now has a new attribute,
  ``key_map``, which is a dictionary mapping key and subkey fingerprints to
  the corresponding key's dictionary. With this change, you don't need to
  iterate over the (potentially large) returned list to search for a key with
  a given fingerprint - the ``key_map`` dict will take you straight to the key
  info, whether the fingerprint you have is for a key or a subkey. Thanks to
  Nick Daly for the initial suggestion.

0.3.7
-----

Released: 2014-12-07

Signed with PGP key: Vinay Sajip (CODE SIGNING KEY) <vinay_sajip@yahoo.co.uk>

Key Fingerprint    : CA74 9061 914E AC13 8E66 EADB 9147 B477 339A 9B86

* Added an ``output`` keyword parameter to the ``sign`` and
  ``sign_file`` methods, to allow writing the signature to a file.
  Thanks to Jannis Leidel for the patch.

* Allowed specifying ``True`` for the ``sign`` keyword parameter,
  which allows use of the default key for signing and avoids having to
  specify a key id when it's desired to use the default. Thanks to
  Fabian Beutel for the patch.

* Used a uniform approach with subprocess on Windows and POSIX: shell=True
  is not used on either.

* When signing/verifying, the status is updated to reflect any expired or
  revoked keys or signatures.

* Handled 'NOTATION_NAME' and 'NOTATION_DATA' during verification.

* Fixed #1, #16, #18, #20: Quoting approach changed, since now shell=False.

* Fixed #14: Handled 'NEED_PASSPHRASE_PIN' message.

* Fixed #8: Added a scan_keys method to allow scanning of keys without the
  need to import into a keyring. Thanks to Venzen Khaosan for the suggestion.

* Fixed #5: Added '0x' prefix when searching for keys. Thanks to Aaron Toponce
  for the report.

* Fixed #4: Handled 'PROGRESS' message during encryption. Thanks to Daniel
  Mills for the report.

* Fixed #3: Changed default encoding to Latin-1.

* Fixed #2: Raised ValueError if no recipients were specified
  for an asymmetric encryption request.

* Handled 'UNEXPECTED' message during verification. Thanks to
  David Andersen for the patch.

* Replaced old range(len(X)) idiom with enumerate().

* Refactored ``ListKeys`` / ``SearchKeys`` classes to maximise use of common
  functions.

* Fixed GC94: Added ``export-minimal`` and ``armor`` options when exporting
  keys. This addition was inadvertently left out of 0.3.6.

0.3.6
-----

Released: 2014-02-05

* Fixed GC82: Enabled fast random tests on gpg as well as gpg2.
* Fixed GC85: Avoided deleting temporary file to preserve its permissions.
* Fixed GC87: Avoided writing passphrase to log.
* Fixed GC95: Added ``verify_data()`` method to allow verification of
  signatures in memory.
* Fixed GC96: Regularised end-of-line characters.
* Fixed GC98: Rectified problems with earlier fix for shell injection.

0.3.5
-----

Released: 2013-08-30

* Added improved shell quoting to guard against shell injection.
* Fixed GC76: Added ``search_keys()`` and ``send_keys()`` methods.
* Fixed GC77: Allowed specifying a symmetric cipher algorithm.
* Fixed GC78: Fell back to utf-8 encoding when no other could be determined.
* Fixed GC79: Default key length is now 2048 bits.
* Fixed GC80: Removed the Name-Comment default in key generation.

0.3.4
-----

Released: 2013-06-05

* Fixed GC65: Fixed encoding exception when getting version.
* Fixed GC66: Now accepts sets and frozensets where appropriate.
* Fixed GC67: Hash algorithm now captured in sign result.
* Fixed GC68: Added support for ``--secret-keyring``.
* Fixed GC70: Added support for multiple keyrings.

0.3.3
-----

Released: 2013-03-11

* Fixed GC57: Handled control characters in ``list_keys()``.
* Fixed GC61: Enabled fast random for testing.
* Fixed GC62: Handled ``KEYEXPIRED`` status.
* Fixed GC63: Handled ``NO_SGNR`` status.

0.3.2
-----

Released: 2013-01-17

* Fixed GC56: Disallowed blank values in key generation.
* Fixed GC57: Handled colons and other characters in ``list_keys()``.
* Fixed GC59/GC60: Handled ``INV_SGNR`` status during verification and removed
  calls requiring interactive password input from doctests.

0.3.1
-----

Released: 2012-09-01

* Fixed GC45: Allowed additional arguments to gpg executable.
* Fixed GC50: Used latin-1 encoding in tests when it's known to be required.
* Fixed GC51: Test now returns non-zero exit status on test failure.
* Fixed GC53: Now handles ``INV_SGNR`` and ``KEY_NOT_CREATED`` statuses.
* Fixed GC55: Verification and decryption now return trust level of signer in
  integer and text form.

0.3.0
-----

Released: 2012-05-12

* Fixed GC49: Reinstated Yann Leboulanger's change to support subkeys
  (accidentally left out in 0.2.7).

0.2.9
-----

Released: 2012-03-29

* Fixed GC36: Now handles ``CARDCTRL`` and ``POLICY_URL`` messages.
* Fixed GC40: Now handles ``DECRYPTION_INFO``, ``DECRYPTION_FAILED`` and
  ``DECRYPTION_OKAY`` messages.
* The ``random_binary_data file`` is no longer shipped, but constructed by the
  test suite if needed.

0.2.8
-----

Released: 2011-09-02

* Fixed GC29: Now handles ``IMPORT_RES`` while verifying.
* Fixed GC30: Fixed an encoding problem.
* Fixed GC33: Quoted arguments for added safety.

0.2.7
-----

Released: 2011-04-10

* Fixed GC24: License is clarified as BSD.
* Fixed GC25: Incorporated Daniel Folkinshteyn's changes.
* Fixed GC26: Incorporated Yann Leboulanger's subkey change.
* Fixed GC27: Incorporated hysterix's support for symmetric encryption.
* Did some internal cleanups of Unicode handling.

0.2.6
-----

Released: 2011-01-25

* Fixed GC14: Should be able to accept passphrases from GPG-Agent.
* Fixed GC19: Should be able to create a detached signature.
* Fixed GC21/GC23: Better handling of less common responses from GPG.

0.2.5
-----

Released: 2010-10-13

* Fixed GC11/GC16: Detached signatures can now be created.
* Fixed GC3: Detached signatures can be verified.
* Fixed GC12: Better support for RSA and IDEA.
* Fixed GC15/GC17: Better support for non-ASCII input.

0.2.4
-----

Released: 2010-03-01

* Fixed GC9: Now allows encryption without armor and the ability to encrypt
  and decrypt directly to/from files.

0.2.3
-----

Released: 2010-01-07

* Fixed GC7: Made sending data to process threaded and added a test case.
  With a test data file used by the test case, the archive size has gone up
  to 5MB (the size of the test file).

0.2.2
-----

Released: 2009-10-06

* Fixed GC5/GC6: Added ``--batch`` when specifying ``--passphrase-fd`` and
  changed the name of the distribution file to add the ``python-`` prefix.

0.2.1
-----

Released: 2009-08-07

* Fixed GC2: Added ``handle_status()`` method to the ``ListKeys`` class.

0.2.0
-----

Released: 2009-07-16

* Various changes made to support Python 3.0.

0.1.0
-----

Released: 2009-07-04

* Initial release.
