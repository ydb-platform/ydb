ncclient: Python library for NETCONF clients
--------------------------------------------

ncclient is a Python library that facilitates client-side scripting and
application development around the NETCONF protocol. ``ncclient`` was
developed by `Shikar Bhushan <http://schmizz.net>`. It is now
maintained by `Leonidas Poulopoulos (@leopoul) <http://ncclient.org>`
and `Einar Nilsen-Nygaard (@einarnn)`.

Docs:
`http://ncclient.readthedocs.org <http://ncclient.readthedocs.org>`_

Github:
`https://github.com/ncclient/ncclient <https://github.com/ncclient/ncclient>`_

Requirements:
^^^^^^^^^^^^^

-  Python 2.7 or Python 3.4+
-  setuptools 0.6+
-  Paramiko 1.7+
-  lxml 3.3.0+
-  libxml2
-  libxslt

If you are on Debian/Ubuntu install the following libs (via aptitude or
apt-get):

-  libxml2-dev
-  libxslt1-dev

Installation:
^^^^^^^^^^^^^

::

    [ncclient] $ sudo python setup.py install

or via pip:

::

    pip install ncclient

Examples:
^^^^^^^^^

::

    [ncclient] $ python examples/juniper/*.py

Usage
~~~~~

Get device running config
'''''''''''''''''''''''''

Use either an interactive Python console (ipython) or integrate the
following in your code:

::

    from ncclient import manager

    with manager.connect(host=host, port=830, username=user, hostkey_verify=False) as m:
        c = m.get_config(source='running').data_xml
        with open("%s.xml" % host, 'w') as f:
            f.write(c)

As of 0.4.1 ncclient integrates Juniper's and Cisco's forks, lots of new concepts
have been introduced that ease management of Juniper and Cisco devices respectively.
The biggest change is the introduction of device handlers in connection paramms.
For example to invoke Juniper's functions annd params one has to re-write the above with 
**device\_params={'name':'junos'}**:

::

    from ncclient import manager

    with manager.connect(host=host, port=830, username=user, hostkey_verify=False, device_params={'name':'junos'}) as m:
        c = m.get_config(source='running').data_xml
        with open("%s.xml" % host, 'w') as f:
            f.write(c)

Device handlers are easy to implement and prove to be futureproof.

Supported device handlers
'''''''''''''''''''''''''

* Alcatel Lucent: `device_params={'name':'alu'}`
* Ciena: `device_params={'name':'ciena'}`
* Cisco:
    - CSR: `device_params={'name':'csr'}`
    - Nexus: `device_params={'name':'nexus'}`
    - IOS XR: `device_params={'name':'iosxr'}`
    - IOS XE: `device_params={'name':'iosxe'}`
* H3C: `device_params={'name':'h3c'}`
* HP Comware: `device_params={'name':'hpcomware'}`
* Huawei:
    - `device_params={'name':'huawei'}`
    - `device_params={'name':'huaweiyang'}`
* Juniper: `device_params={'name':'junos'}`
* Server or anything not in above: `device_params={'name':'default'}`

Changes \| brief
~~~~~~~~~~~~~~~~

**v0.6.12**

* Fix for accidental breakage of Juniper ExecuteRPC support

**v0.6.11**

* Support for custom client capabilities
* Restructuring/refactoring of example scripts
* Minor bugfixes
* Minor unit test refactoring

**v0.6.10**

* NETCONF call-home (RFC8071) support
* YANG 1.1 `action` support
* Nokia SR OS device handler support
* Removal of old ALU base-r13 API documentation
* Increased test coverage
* Variety of bugfixes and minor enhancements from a variety of contributors since 0.6.9 (see commit history)
* Thanks to all contributors!

**v0.6.9**

* Fix for breaking API change

**v0.6.8**

* Pulled due to accidental breaking API change
* Variety of small updates and bugfixes, but of note:
    - Support for namespace prefixes for XPath queries
    - `edit-config` parameter validation
    - Support for multiple RPC errors
    - API to get supported device types
    - Support for subtree filters with multiple top-level tags
* Thanks to all contributors!

**v0.6.7**

- Variety of bugfixes from a variety of contributors since 0.6.6 (see commit history)

**v0.6.6**

- Read ssh timeout from config file if not specified in method call
- Tox support
- Huge XML tree parser support
- Adding optional bind address to connect

**v0.6.5**

- Updated README for 0.6.5 release

**v0.6.4**

- Pin selectors2 to Python versions <= 3.4
- Fix config examples to actually use the nc namespace
- Fix: correctly set port for paramiko when using ssh_config file
- Test: add test to check ProxyCommand uses correct port
- Update commits for py3
- Enhance Alcatel-Lucent-support
- Juniper RPC: allow specifying format in CompareConfiguration
- Parsing of NETCONF 1.1 frames no longer decodes each chunk of bytes
- Fix filter in create_subscription
- Validate 'with-defaults' mode based on supported modes advertised in capability URI

**v0.6.3**

- Fix homepage link registered with PyPi
- SSH Host Key checking
- Updated junos.py to resolve RestrictedUser error
- Close the channel when closing SSH session
- Invoke self.parse() to ensure errors, if any, have been detected before check in ok()

**v0.6.2**

- Migration to user selectors instead of select, allowing higher scale operations
- Improved netconf:base:1.1 parsing
- Graceful exit on session close

**v0.6.0**

- Fix use of new Python 3.7 keyword, async
- Re-enable Python 3.7

**v0.5.4**

- Rollup of minor changes since 0.5.3
- Disablement of Python 3.7 due to async keyword issue

**v0.5.3**

- Add notifications support
- Add support for ecdsa keys
- Various bug fixes

**v0.5.2**

- Add support for Python 3
- Improve Junos ioproc performance
- Performance improvements
- Updated test cases
- Many bug and performance fixes


**v0.4.7**

- Add support for netconf 1.1

**v0.4.6**

- Fix multiple RPC error generation
- Add support for cancel-commit and persist param
- Add more examples

**v0.4.5**

- Add Huawei device support
- Add cli command support for hpcomware v7 devices
- Add H3C support, Support H3C CLI,Action,Get_bulk,Save,Rollback,etc.
- Add alcatel lucent support

- Rewrite multiple error handling
- Add coveralls support, with shield in README.md
- Set severity level to higher when multiple
- Simplify logging and multi-error reporting
- Keep stacktrace of errors
- Check for known hosts on hostkey_verify only
- Add check for device sending back null error_text
- Fix RPC.raise_mode
- Specifying hostkey_verify=False should not load_known_hosts
- Check the correct field on rpc-error element

**v0.4.3**

- Nexus exec_command operation
- Allow specifying multiple cmd elements in Cisco Nexus
- Update rpc for nested rpc-errors
- Prevent race condition in threading
- Prevent hanging in session close

**v0.4.2**

- Support for paramiko ProxyCommand via ~/.ssh/config parsing
- Add Juniper-specific commit operations
- Add Huawei devices support
- Tests/Travis support
- ioproc transport support for Juniper devices
- Update Cisco CSR device handler
- Many minor and major fixes

**v0.4.1**

-  Switch between replies if custom handler is found
-  Add Juniper, Cisco and default device handlers
-  Allow preferred SSH subsystem name in device params
-  Allow iteration over multiple SSH subsystem names.




Acknowledgements
~~~~~~~~~~~~~~~~
-  v0.6.11: @musicinmybrain, @sstancu, @earies
-  v0.6.10: @vnitinv, @omaxx, @einarnn, @musicinmybrain, @tonynii, @sstancu, Martin Volf, @fredgan, @avisom, Viktor Velichkin, @ogenstad, @earies
-  v0.6.9: [Fred Gan](https://github.com/fredgan)
-  v0.6.8: [Fred Gan](https://github.com/fredgan), @vnitinv, @kbijakowski, @iwanb, @badguy99, @liuyong, Andrew Mallory, William Lvory
-  v0.6.7: @vnitinv, @chaitu-tk, @sidhujasminder, @crutcha, @markgoddard, @ganeshrn, @songxl, @doesitblend, @psikala, @xuxiaowei0512, @muffizone
-  v0.6.6: @sstancu, @hemna, @ishayansheikh
-  v0.6.4: @davidhankins, @mzagozen, @knobix, @markafarrell, @psikala, @moepman, @apt-itude, @yuekyang
-  v0.6.3: @rdkls, @Anthony25, @rsmekala, @vnitinv, @siming85
-  v0.6.2: @einarnn, @glennmatthews, @bryan-stripe, @nickylba
-  v0.6.0: `Einar Nilsen-Nygaard`_
-  v0.5.4: Various
-  v0.5.3: `Justin Wilcox`_, `Stacy W. Smith`_, `Mircea Ulinic`_,
   `Ebben Aries`_, `Einar Nilsen-Nygaard`_, `QijunPan`_
-  v0.5.2: `Nitin Kumar`_, `Kristian Larsson`_, `palashgupta`_,
   `Jonathan Provost`_, `Jainpriyal`_, `sharang`_, `pseguel`_,
   `nnakamot`_, `Алексей Пастухов`_, `Christian Giese`_, `Peipei Guo`_,
   `Time Warner Cable Openstack Team`_
-  v0.4.7: `Einar Nilsen-Nygaard`_, `Vaibhav Bajpai`_, Norio Nakamoto
-  v0.4.6: `Nitin Kumar`_, `Carl Moberg`_, `Stavros Kroustouris`_
-  v0.4.5: `Sebastian Wiesinger`_, `Vincent Bernat`_, `Matthew Stone`_,
   `Nitin Kumar`_
-  v0.4.3: `Jeremy Schulman`_, `Ray Solomon`_, `Rick Sherman`_,
   `subhak186`_
-  v0.4.2: `katharh`_, `Francis Luong (Franco)`_, `Vincent Bernat`_,
   `Juergen Brendel`_, `Quentin Loos`_, `Ray Solomon`_, `Sebastian
   Wiesinger`_, `Ebben Aries`_
-  v0.4.1: `Jeremy Schulman`_, `Ebben Aries`_, Juergen Brendel

.. _Nitin Kumar: https://github.com/vnitinv
.. _Kristian Larsson: https://github.com/plajjan
.. _palashgupta: https://github.com/palashgupta
.. _Jonathan Provost: https://github.com/JoProvost
.. _Jainpriyal: https://github.com/Jainpriyal
.. _sharang: https://github.com/sharang
.. _pseguel: https://github.com/pseguel
.. _nnakamot: https://github.com/nnakamot
.. _Алексей Пастухов: https://github.com/p-alik
.. _Christian Giese: https://github.com/GIC-de
.. _Peipei Guo: https://github.com/peipeiguo
.. _Time Warner Cable Openstack Team: https://github.com/twc-openstack
.. _Einar Nilsen-Nygaard: https://github.com/einarnn
.. _Vaibhav Bajpai: https://github.com/vbajpai
.. _Carl Moberg: https://github.com/cmoberg
.. _Stavros Kroustouris: https://github.com/kroustou
.. _Sebastian Wiesinger: https://github.com/sebastianw
.. _Vincent Bernat: https://github.com/vincentbernat
.. _Matthew Stone: https://github.com/bigmstone
.. _Jeremy Schulman: https://github.com/jeremyschulman
.. _Ray Solomon: https://github.com/rsolomo
.. _Rick Sherman: https://github.com/shermdog
.. _subhak186: https://github.com/subhak186
.. _katharh: https://github.com/katharh
.. _Francis Luong (Franco): https://github.com/francisluong
.. _Juergen Brendel: https://github.com/juergenbrendel
.. _Quentin Loos: https://github.com/Kent1
.. _Ebben Aries: https://github.com/earies
.. _Justin Wilcox: https://github.com/jwwilcox
.. _Stacy W. Smith: https://github.com/stacywsmith
.. _Mircea Ulinic: https://github.com/mirceaulinic
.. _QijunPan: https://github.com/QijunPan
