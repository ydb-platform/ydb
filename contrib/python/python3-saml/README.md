# SAML Python Toolkit (compatible with Python3)

[![Python package](https://github.com/SAML-Toolkits/python3-saml/actions/workflows/python-package.yml/badge.svg)](https://github.com/SAML-Toolkits/python3-saml/actions/workflows/python-package.yml)
![PyPI Downloads](https://img.shields.io/pypi/dm/python3-saml.svg?label=PyPI%20Downloads)
[![Coverage Status](https://coveralls.io/repos/github/SAML-Toolkits/python3-saml/badge.svg?branch=master)](https://coveralls.io/github/SAML-Toolkits/python3-saml?branch=master)
[![PyPi Version](https://img.shields.io/pypi/v/python3-saml.svg)](https://pypi.python.org/pypi/python3-saml)
![Python versions](https://img.shields.io/pypi/pyversions/python3-saml.svg)

Add SAML support to your Python software using this library.
Forget those complicated libraries and use the open source library provided by the SAML tool community.

This version supports Python3. Python 2 support was deprecated on Jan 1st, 2020: [python-saml](https://github.com/onelogin/python-saml)

#### Warning ####

Version 1.16.X is the latest version supporting Python2, consider its use deprecated. 1.17 won't be Python2 compatible.

Version 1.13.0 sets sha256 and rsa-sha256 as default algorithms

Version 1.8.0 sets strict mode active by default

Update ``python3-saml`` to ``1.5.0``, this version includes security improvements for preventing XEE and Xpath Injections.

Update ``python3-saml`` to ``1.4.0``, this version includes a fix for the [CVE-2017-11427](https://www.cvedetails.com/cve/CVE-2017-11427/) vulnerability.

This version also changes how the calculate fingerprint method works, and will expect as input a formatted X.509 certificate.

Update ``python3-saml`` to ``1.2.6`` that adds the use defusedxml that will prevent XEE and other attacks based on the abuse of XML. (CVE-2017-9672)

Update ``python3-saml`` to ``>= 1.2.1``, ``1.2.0`` had a bug on signature validation process (when using ``wantAssertionsSigned`` and ``wantMessagesSigned``). [CVE-2016-1000251](https://github.com/distributedweaknessfiling/DWF-Database-Artifacts/blob/master/DWF/2016/1000251/CVE-2016-1000251.json)

``1.2.0`` version includes a security patch that contains extra validations that will prevent signature wrapping attacks.

``python3-saml < v1.2.0`` is vulnerable and allows signature wrapping!

#### Security Guidelines ####

If you believe you have discovered a security vulnerability in this toolkit, please report it by mail to the maintainer: sixto.martin.garcia+security@gmail.com

Why add SAML support to my software?
------------------------------------

SAML is an XML-based standard for web browser single sign-on and is defined by
the OASIS Security Services Technical Committee. The standard has been around
since 2002, but lately it is becoming popular due its advantages:

 * **Usability** - One-click access from portals or intranets, deep linking,
   password elimination and automatically renewing sessions make life
   easier for the user.
 * **Security** - Based on strong digital signatures for authentication and
   integrity, SAML is a secure single sign-on protocol that the largest
   and most security conscious enterprises in the world rely on.
 * **Speed** - SAML is fast. One browser redirect is all it takes to securely
   sign a user into an application.
 * **Phishing Prevention** - If you don’t have a password for an app, you
   can’t be tricked into entering it on a fake login page.
 * **IT Friendly** - SAML simplifies life for IT because it centralizes
   authentication, provides greater visibility and makes directory
   integration easier.
 * **Opportunity** - B2B cloud vendor should support SAML to facilitate the
   integration of their product.

General Description
-------------------

SAML Python toolkit lets you turn your Python application into a SP
(Service Provider) that can be connected to an IdP (Identity Provider).

**Supports:**

 * SSO and SLO (SP-Initiated and IdP-Initiated).
 * Assertion and nameId encryption.
 * Assertion signatures.
 * Message signatures: ``AuthNRequest``, ``LogoutRequest``, ``LogoutResponses``.
 * Enable an Assertion Consumer Service endpoint.
 * Enable a Single Logout Service endpoint.
 * Publish the SP metadata (which can be signed).

**Key Features:**

 * **saml2int** - Implements the SAML 2.0 Web Browser SSO Profile.
 * **Session-less** - Forget those common conflicts between the SP and
   the final app, the toolkit delegate session in the final app.
 * **Easy to use** - Programmer will be allowed to code high-level and
   low-level programming, 2 easy to use APIs are available.
 * **Tested** - Thoroughly tested.

Installation
------------

### Dependencies ###

 * python 2.7 (deprecated) // python 3.6
 * [xmlsec](https://pypi.python.org/pypi/xmlsec) Python bindings for the XML Security Library.
 * [lxml](https://pypi.python.org/pypi/lxml) Python bindings for the libxml2 and libxslt libraries.
 * [isodate](https://pypi.python.org/pypi/isodate) An ISO 8601 date/time/
 duration parser and formatter

Review the ``setup.py`` file to know the version of the library that ``python3-saml`` is using

### Code ###

#### Option 1. Download from GitHub ####

The toolkit is hosted on GitHub. You can download it from:

 * Latest release: https://github.com/saml-toolkits/python3-saml/releases/latest
 * Master repo: https://github.com/saml-toolkits/python3-saml/tree/master

Copy the core of the library ``(src/onelogin/saml2 folder)`` and merge the ``setup.py`` inside the Python application. (Each application has its structure so take your time to locate the Python SAML toolkit in the best place).

#### Option 2. Download from pypi ####

The toolkit is hosted in pypi, you can find the ``python3-saml`` package at https://pypi.python.org/pypi/python3-saml

You can install it executing:
```
$ pip install python3-saml
```

If you want to know how a project can handle python packages review this [guide](https://packaging.python.org/en/latest/tutorial.html) and review this [sampleproject](https://github.com/pypa/sampleproject)

#### NOTE ####
To avoid ``libxml2`` library version incompatibilities between ``xmlsec`` and ``lxml`` it is recommended that ``lxml`` is not installed from binary.

This can be ensured by executing:
```
$ pip install --force-reinstall --no-binary lxml lxml
```

Security Warning
----------------

In production, the **strict** parameter MUST be set as **"true"**. Otherwise
your environment is not secure and will be exposed to attacks.

In production also we highly recommend to register on the settings the IdP certificate instead of using the fingerprint method. The fingerprint, is a hash, so at the end is open to a collision attack that can end on a signature validation bypass. Other SAML toolkits deprecated that mechanism, we maintain it for compatibility and also to be used on test environment.


### Avoiding Open Redirect attacks ###

Some implementations uses the RelayState parameter as a way to control the flow when SSO and SLO succeeded. So basically the
user is redirected to the value of the RelayState.

If you are using Signature Validation on the HTTP-Redirect binding, you will have the RelayState value integrity covered, otherwise, and
on HTTP-POST binding, you can't trust the RelayState so before
executing the validation, you need to verify that its value belong
a trusted and expected URL.

Read more about Open Redirect [CWE-601](https://cwe.mitre.org/data/definitions/601.html).

### Avoiding Replay attacks ###

A replay attack is basically try to reuse an intercepted valid SAML Message in order to impersonate a SAML action (SSO or SLO).

SAML Messages have a limited timelife (NotBefore, NotOnOrAfter) that
make harder this kind of attacks, but they are still possible.

In order to avoid them, the SP can keep a list of SAML Messages or Assertion IDs already validated and processed. Those values only need
to be stored the amount of time of the SAML Message life time, so
we don't need to store all processed message/assertion Ids, but the most recent ones.

The OneLogin_Saml2_Auth class contains the [get_last_request_id](https://github.com/onelogin/python3-saml/blob/ab62b0d6f3e5ac2ae8e95ce3ed2f85389252a32d/src/onelogin/saml2/auth.py#L357), [get_last_message_id](https://github.com/onelogin/python3-saml/blob/ab62b0d6f3e5ac2ae8e95ce3ed2f85389252a32d/src/onelogin/saml2/auth.py#L364) and [get_last_assertion_id](https://github.com/onelogin/python3-saml/blob/ab62b0d6f3e5ac2ae8e95ce3ed2f85389252a32d/src/onelogin/saml2/auth.py#L371) methods to retrieve the IDs

Checking that the ID of the current Message/Assertion does not exists in the list of the ones already processed will prevent replay attacks.


Getting Started
---------------

### Knowing the toolkit ###

The new SAML Toolkit contains different folders (``certs``, ``lib``, ``demo-django``, ``demo-flask`` and ``tests``) and some files.

Let's start describing them:

#### src ####

This folder contains the heart of the toolkit, **onelogin/saml2** folder contains the new version of
the classes and methods that are described in a later section.

#### demo-django ####

This folder contains a Django project that will be used as demo to show how to add SAML support to the Django Framework. **demo** is the main folder of the Django project (with its ``settings.py``, ``views.py``, ``urls.py``), **templates** is the Django templates of the project and **saml** is a folder that contains the ``certs`` folder that could be used to store the X.509 public and private key, and the SAML toolkit settings (``settings.json`` and ``advanced_settings.json``).

***Notice about certs***

SAML requires a X.509 cert to sign and encrypt elements like ``NameID``, ``Message``, ``Assertion``, ``Metadata``.

If our environment requires sign or encrypt support, the certs folder may contain the X.509 cert and the private key that the SP will use:

* sp.crt The public cert of the SP
* sp.key The private key of the SP

Or also we can provide those data in the setting file at the ``x509cert`` and the ``privateKey`` JSON parameters of the ``sp`` element.

Sometimes we could need a signature on the metadata published by the SP, in this case we could use the X.509 cert previously mentioned or use a new X.509 cert: ``metadata.crt`` and ``metadata.key``.

Use ``sp_new.crt`` if you are in a key rollover process and you want to
publish that X.509 certificate on Service Provider metadata.

If you want to create self-signed certs, you can do it at the https://www.samltool.com/self_signed_certs.php service, or using the command:

```bash
openssl req -new -x509 -days 3652 -nodes -out sp.crt -keyout sp.key
```

#### demo-flask ####

This folder contains a Flask project that will be used as demo to show how to add SAML support to the Flask Framework. ``index.py`` is the main Flask file that has all the code, this file uses the templates stored at the ``templates`` folder. In the ``saml`` folder we found the ``certs`` folder to store the X.509 public and private key, and the SAML toolkit settings (``settings.json`` and ``advanced_settings.json``).

#### demo_pyramid ####

This folder contains a Pyramid project that will be used as demo to show how to add SAML support to the [Pyramid Web Framework](http://docs.pylonsproject.org/projects/pyramid/en/latest/).  ``\_\_init__.py`` is the main file that configures the app and its routes, ``views.py`` is where all the logic and SAML handling takes place, and the templates are stored in the ``templates`` folder. The ``saml`` folder is the same as in the other two demos.

#### demo-tornado ####

This folder contains a Tornado project that will be used as demo to show how to add SAML support to the Tornado Framework. ``views.py`` (with its ``settings.py``) is the main Flask file that has all the code, this file uses the templates stored at the ``templates`` folder. In the ``saml`` folder we found the ``certs`` folder to store the X.509 public and private key, and the SAML toolkit settings (``settings.json`` and ``advanced_settings.json``).

It requires python3.5 (it's using tornado 6.0.3)

#### setup.py ####

Setup script is the centre of all activity in building, distributing, and installing modules.
Read more at https://pythonhosted.org/an_example_pypi_project/setuptools.html

#### tests ####

Contains the unit test of the toolkit.

In order to execute the test you only need to load the virtualenv with the toolkit installed on it properly:
```
pip install -e ".[test]"
```

and execute:
```
python setup.py test
```
The previous line will run the tests for the whole toolkit. You can also run the tests for a specific module. To do so for the auth module you would have to execute this:
```
python setup.py test --test-suite tests.src.OneLogin.saml2_tests.auth_test.OneLogin_Saml2_Auth_Test
```

With the ``--test-suite`` parameter you can specify the module to test. You'll find all the module available and their class names at ``tests/src/OneLogin/saml2_tests/``.

### How It Works ###

#### Settings ####

First of all we need to configure the toolkit. The SP's info, the IdP's info, and in some cases, configure advanced security issues like signatures and encryption.

There are two ways to provide the settings information:

* Use a ``settings.json`` file that we should locate in any folder, but indicates its path with the ``custom_base_path`` parameter.

* Use a JSON object with the setting data and provide it directly to the constructor of the class (if your toolkit integation requires certs, remember to provide the ``custom_base_path`` as part of the settings or as a parameter in the constructor).

In the demo-django and in the demo-flask folders you will find a ``saml`` folder, inside there is a ``certs`` folder and a ``settings.json`` and ``advanced_settings.json`` file. Those files contain the settings for the SAML toolkit. Copy them in your project and set the correct values.

This is the ``settings.json`` file:

```javascript
{
    // If strict is True, then the Python Toolkit will reject unsigned
    // or unencrypted messages if it expects them to be signed or encrypted.
    // Also it will reject the messages if the SAML standard is not strictly
    // followed. Destination, NameId, Conditions ... are validated too.
    "strict": true,

    // Enable debug mode (outputs errors).
    "debug": true,

    // Service Provider Data that we are deploying.
    "sp": {
        // Identifier of the SP entity  (must be a URI)
        "entityId": "https://<sp_domain>/metadata/",
        // Specifies info about where and how the <AuthnResponse> message MUST be
        // returned to the requester, in this case our SP.
        "assertionConsumerService": {
            // URL Location where the <Response> from the IdP will be returned
            "url": "https://<sp_domain>/?acs",
            // SAML protocol binding to be used when returning the <Response>
            // message. SAML Toolkit supports this endpoint for the
            // HTTP-POST binding only.
            "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"
        },
        // Specifies info about where and how the <Logout Request/Response> message MUST be sent.
        "singleLogoutService": {
            // URL Location where the <LogoutRequest> from the IdP will be sent (IdP-initiated logout)
            "url": "https://<sp_domain>/?sls",
            // URL Location where the <LogoutResponse> from the IdP will sent (SP-initiated logout, reply)
            // OPTIONAL: only specify if different from url parameter
            //"responseUrl": "https://<sp_domain>/?sls",
            // SAML protocol binding to be used when returning the <Response>
            // message. SAML Toolkit supports the HTTP-Redirect binding
            // only for this endpoint.
            "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"
        },
        // If you need to specify requested attributes, set a
        // attributeConsumingService. nameFormat, attributeValue and
        // friendlyName can be omitted
        "attributeConsumingService": {
                // OPTIONAL: only specify if SP requires this.
                // index is an integer which identifies the attributeConsumingService used
                // to the SP. SAML toolkit supports configuring only one attributeConsumingService
                // but in certain cases the SP requires a different value.  Defaults to '1'.
                // "index": '1',
                "serviceName": "SP test",
                "serviceDescription": "Test Service",
                "requestedAttributes": [
                    {
                        "name": "",
                        "isRequired": false,
                        "nameFormat": "",
                        "friendlyName": "",
                        "attributeValue": []
                    }
                ]
        },
        // Specifies the constraints on the name identifier to be used to
        // represent the requested subject.
        // Take a look on src/onelogin/saml2/constants.py to see the NameIdFormat that are supported.
        "NameIDFormat": "urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified",
        // Usually X.509 cert and privateKey of the SP are provided by files placed at
        // the certs folder. But we can also provide them with the following parameters
        "x509cert": "",
        "privateKey": ""

        /*
         * Key rollover
         * If you plan to update the SP X.509cert and privateKey
         * you can define here the new X.509cert and it will be
         * published on the SP metadata so Identity Providers can
         * read them and get ready for rollover.
         */
        // 'x509certNew': '',
    },

    // Identity Provider Data that we want connected with our SP.
    "idp": {
        // Identifier of the IdP entity  (must be a URI)
        "entityId": "https://app.onelogin.com/saml/metadata/<onelogin_connector_id>",
        // SSO endpoint info of the IdP. (Authentication Request protocol)
        "singleSignOnService": {
            // URL Target of the IdP where the Authentication Request Message
            // will be sent.
            "url": "https://app.onelogin.com/trust/saml2/http-post/sso/<onelogin_connector_id>",
            // SAML protocol binding to be used when returning the <Response>
            // message. SAML Toolkit supports the HTTP-Redirect binding
            // only for this endpoint.
            "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"
        },
        // SLO endpoint info of the IdP.
        "singleLogoutService": {
            // URL Location where the <LogoutRequest> from the IdP will be sent (IdP-initiated logout)
            "url": "https://app.onelogin.com/trust/saml2/http-redirect/slo/<onelogin_connector_id>",
            // URL Location where the <LogoutResponse> from the IdP will sent (SP-initiated logout, reply)
            // OPTIONAL: only specify if different from url parameter
            "responseUrl": "https://app.onelogin.com/trust/saml2/http-redirect/slo_return/<onelogin_connector_id>",
            // SAML protocol binding to be used when returning the <Response>
            // message. SAML Toolkit supports the HTTP-Redirect binding
            // only for this endpoint.
            "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"
        },
        // Public X.509 certificate of the IdP
        "x509cert": "<onelogin_connector_cert>"
        /*
         *  Instead of using the whole X.509cert you can use a fingerprint in order to
         *  validate a SAMLResponse (but you still need the X.509cert to validate LogoutRequest and LogoutResponse using the HTTP-Redirect binding).
         *  But take in mind that the algorithm for the fingerprint should be as strong as the algorithm in a normal certificate signature
	 *  (e.g. SHA256 or strong)
         *
         *  (openssl x509 -noout -fingerprint -in "idp.crt" to generate it,
         *  or add for example the -sha256 , -sha384 or -sha512 parameter)
         *
         *  If a fingerprint is provided, then the certFingerprintAlgorithm is required in order to
         *  let the toolkit know which algorithm was used.
         Possible values: sha1, sha256, sha384 or sha512
         *  'sha1' is the default value.
         *
         *  Notice that if you want to validate any SAML Message sent by the HTTP-Redirect binding, you
         *  will need to provide the whole X.509cert.
         */
        // "certFingerprint": "",
        // "certFingerprintAlgorithm": "sha1",

        /* In some scenarios the IdP uses different certificates for
         * signing/encryption, or is under key rollover phase and
         * more than one certificate is published on IdP metadata.
         * In order to handle that the toolkit offers that parameter.
         * (when used, 'X.509cert' and 'certFingerprint' values are
         * ignored).
         */
        // 'x509certMulti': {
        //      'signing': [
        //          '<cert1-string>'
        //      ],
        //      'encryption': [
        //          '<cert2-string>'
        //      ]
        // }
    }
}
```

In addition to the required settings data (idp, sp), extra settings can be defined in `advanced_settings.json`:

```javascript
{
    // Security settings
    "security": {

        /** signatures and encryptions offered **/

        // Indicates that the nameID of the <samlp:logoutRequest> sent by this SP
        // will be encrypted.
        "nameIdEncrypted": false,

        // Indicates whether the <samlp:AuthnRequest> messages sent by this SP
        // will be signed.  [Metadata of the SP will offer this info]
        "authnRequestsSigned": false,

        // Indicates whether the <samlp:logoutRequest> messages sent by this SP
        // will be signed.
        "logoutRequestSigned": false,

        // Indicates whether the <samlp:logoutResponse> messages sent by this SP
        // will be signed.
        "logoutResponseSigned": false,

        /* Sign the Metadata
         false || true (use sp certs) || {
                                            "keyFileName": "metadata.key",
                                            "certFileName": "metadata.crt"
                                         }
        */
        "signMetadata": false,

        /** signatures and encryptions required **/

        // Indicates a requirement for the <samlp:Response>, <samlp:LogoutRequest>
        // and <samlp:LogoutResponse> elements received by this SP to be signed.
        "wantMessagesSigned": false,

        // Indicates a requirement for the <saml:Assertion> elements received by
        // this SP to be signed. [Metadata of the SP will offer this info]
        "wantAssertionsSigned": false,

        // Indicates a requirement for the <saml:Assertion>
        // elements received by this SP to be encrypted.
        "wantAssertionsEncrypted": false,

        // Indicates a requirement for the NameID element on the SAMLResponse
        // received by this SP to be present.
        "wantNameId": true,

        // Indicates a requirement for the NameID received by
        // this SP to be encrypted.
        "wantNameIdEncrypted": false,

        // Indicates a requirement for the AttributeStatement element
        "wantAttributeStatement": true,

        // Authentication context.
        // Set to false and no AuthContext will be sent in the AuthNRequest,
        // Set true or don't present this parameter and you will get an AuthContext 'exact' 'urn:oasis:names:tc:SAML:2.0:ac:classes:PasswordProtectedTransport'
        // Set an array with the possible auth context values: array ('urn:oasis:names:tc:SAML:2.0:ac:classes:Password', 'urn:oasis:names:tc:SAML:2.0:ac:classes:X509'),
        "requestedAuthnContext": true,
	// Allows the authn comparison parameter to be set, defaults to 'exact' if the setting is not present.
        "requestedAuthnContextComparison": "exact",
        // Set to true to check that the AuthnContext(s) received match(es) the requested.
        "failOnAuthnContextMismatch": false,

        // In some environment you will need to set how long the published metadata of the Service Provider gonna be valid.
        // is possible to not set the 2 following parameters (or set to null) and default values will be set (2 days, 1 week)
        // Provide the desire TimeStamp, for example 2015-06-26T20:00:00Z
        "metadataValidUntil": null,
        // Provide the desire Duration, for example PT518400S (6 days)
        "metadataCacheDuration": null,

        // If enabled, URLs with single-label-domains will
        // be allowed and not rejected by the settings validator (Enable it under Docker/Kubernetes/testing env, not recommended on production)
        "allowSingleLabelDomains": false,

        // Algorithm that the toolkit will use on signing process. Options:
        //    'http://www.w3.org/2000/09/xmldsig#rsa-sha1'
        //    'http://www.w3.org/2000/09/xmldsig#dsa-sha1'
        //    'http://www.w3.org/2001/04/xmldsig-more#rsa-sha256'
        //    'http://www.w3.org/2001/04/xmldsig-more#rsa-sha384'
        //    'http://www.w3.org/2001/04/xmldsig-more#rsa-sha512'
        "signatureAlgorithm": "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256",

        // Algorithm that the toolkit will use on digest process. Options:
        //    'http://www.w3.org/2000/09/xmldsig#sha1'
        //    'http://www.w3.org/2001/04/xmlenc#sha256'
        //    'http://www.w3.org/2001/04/xmldsig-more#sha384'
        //    'http://www.w3.org/2001/04/xmlenc#sha512'
        'digestAlgorithm': "http://www.w3.org/2001/04/xmlenc#sha256",

        // Specify if you want the SP to view assertions with duplicated Name or FriendlyName attributes to be valid
        // Defaults to false if not specified
        'allowRepeatAttributeName': false,

        // If the toolkit receive a message signed with a
        // deprecated algorithm (defined at the constant class)
        // will raise an error and reject the message
        "rejectDeprecatedAlgorithm": true
    },

    // Contact information template, it is recommended to suply a
    // technical and support contacts.
    "contactPerson": {
        "technical": {
            "givenName": "technical_name",
            "emailAddress": "technical@example.com"
        },
        "support": {
            "givenName": "support_name",
            "emailAddress": "support@example.com"
        }
    },

    // Organization information template, the info in en_US lang is
    // recommended, add more if required.
    "organization": {
        "en-US": {
            "name": "sp_test",
            "displayname": "SP test",
            "url": "http://sp.example.com"
        }
    }
}
```

In the ``security`` section, you can set the way that the SP will handle the messages and assertions. Contact the admin of the IdP and ask them what the IdP expects, and decide what validations will handle the SP and what requirements the SP will have and communicate them to the IdP's admin too.

Once we know what kind of data could be configured, let's talk about the way settings are handled within the toolkit.

The settings files described (``settings.json`` and ``advanced_settings.json``) are loaded by the toolkit if not other dict with settings info is provided in the constructors of the toolkit. Let's see some examples.

```python
# Initializes toolkit with settings.json & advanced_settings.json files.
auth = OneLogin_Saml2_Auth(req)
# or
settings = OneLogin_Saml2_Settings()

# Initializes toolkit with settings.json & advanced_settings.json files from a custom base path.
custom_folder = '/var/www/django-project'
auth = OneLogin_Saml2_Auth(req, custom_base_path=custom_folder)
# or
settings = OneLogin_Saml2_Settings(custom_base_path=custom_folder)

# Initializes toolkit with the dict provided.
auth = OneLogin_Saml2_Auth(req, settings_data)
# or
settings = OneLogin_Saml2_Settings(settings_data)
```

You can declare the ``settings_data`` in the file that contains the constructor execution or locate them in any file and load the file in order to get the dict available as we see in the following example:

```python
filename = "/var/www/django-project/custom_settings.json" # The custom_settings.json contains a
json_data_file = open(filename, 'r')                      # settings_data dict.
settings_data = json.load(json_data_file)
json_data_file.close()

auth = OneLogin_Saml2_Auth(req, settings_data)
```

#### Metadata Based Configuration

The method above requires a little extra work to manually specify attributes about the IdP. (And your SP application)

There's an easier method -- use a metadata exchange.  Metadata is just an XML file that defines the capabilities of both the IdP and the SP application.  It also contains the X.509 public key certificates which add to the trusted relationship.  The IdP administrator can also configure custom settings for an SP based on the metadata.

Using ````parse_remote```` IdP metadata can be obtained and added to the settings without further ado.

Take in mind that the OneLogin_Saml2_IdPMetadataParser class does not validate in any way the URL that is introduced in order to be parsed.

Usually the same administrator that handles the Service Provider also sets the URL to the IdP, which should be a trusted resource.

But there are other scenarios, like a SAAS app where the administrator of the app delegates this functionality to other users. In this case, extra precaution should be taken in order to validate such URL inputs and avoid attacks like SSRF.


``
idp_data = OneLogin_Saml2_IdPMetadataParser.parse_remote('https://example.com/auth/saml2/idp/metadata')
``

You can specify a timeout in seconds for metadata retrieval, without it is not guaranteed that the request will complete

``
idp_data = OneLogin_Saml2_IdPMetadataParser.parse_remote('https://example.com/auth/saml2/idp/metadata', timeout=5)
``

If the Metadata contains several entities, the relevant ``EntityDescriptor`` can be specified when retrieving the settings from the ``IdpMetadataParser`` by its ``entityId`` value:

``idp_data = OneLogin_Saml2_IdPMetadataParser.parse_remote(https://example.com/metadatas, entity_id='idp_entity_id')``


#### How load the library ####

In order to use the toolkit library you need to import the file that contains the class that you will need
on the top of your python file.

``` python
from onelogin.saml2.auth import OneLogin_Saml2_Auth
from onelogin.saml2.settings import OneLogin_Saml2_Settings
from onelogin.saml2.utils import OneLogin_Saml2_Utils
```

#### The Request ####

Building a ``OneLogin\_Saml2\_Auth`` object requires a ``request`` parameter:

```python
auth = OneLogin_Saml2_Auth(req)
```

This parameter has the following scheme:

```python
req = {
    "http_host": "",
    "script_name": "",
    "get_data": "",
    "post_data": "",

    # Advanced request options
    "https": "",
    "request_uri": "",
    "query_string": "",
    "validate_signature_from_qs": False,
    "lowercase_urlencoding": False
}
```

Each Python framework builds its own ``request`` object, you may map its data to match what the SAML toolkit expects.
Let`s see some examples:

```python
def prepare_from_django_request(request):
    return {
        'http_host': request.META['HTTP_HOST'],
        'script_name': request.META['PATH_INFO'],
        'get_data': request.GET.copy(),
        'post_data': request.POST.copy()
    }

def prepare_from_flask_request(request):
    url_data = urlparse(request.url)
    return {
        'http_host': request.host,
        'script_name': request.path,
        'get_data': request.args.copy(),
        'post_data': request.form.copy()
    }
```

An explanation of some advanced request parameters:

* `https` - Defaults to ``off``. Set this to ``on`` if you receive responses over HTTPS.

* `request_uri` - The path where your SAML server receives requests. Set this if requests are not received at the server's root.

* `query_string` - Set this with additional query parameters that should be passed to the request endpoint.

* `validate_signature_from_qs` - If `True`, use `query_string` to validate request and response signatures. Otherwise, use `get_data`. Defaults to `False`. Note that when using `get_data`, query parameters need to be url-encoded for validation. By default we use upper-case url-encoding. Some IdPs, notably Microsoft AD, use lower-case url-encoding, which makes signature validation to fail. To fix this issue, either pass `query_string` and set `validate_signature_from_qs` to `True`, which works for all IdPs, or set `lowercase_urlencoding` to `True`, which only works for AD.


#### Initiate SSO ####

In order to send an ``AuthNRequest`` to the IdP:

```python
from onelogin.saml2.auth import OneLogin_Saml2_Auth

req = prepare_request_for_toolkit(request)
auth = OneLogin_Saml2_Auth(req)   # Constructor of the SP, loads settings.json
                                  # and advanced_settings.json

auth.login()      # This method will build and return a AuthNRequest URL that can be
                  # either redirected to, or printed out onto the screen as a hyperlink
```

The ``AuthNRequest`` will be sent signed or unsigned based on the security info of the ``advanced_settings.json`` file (i.e. ``authnRequestsSigned``).

The IdP will then return the SAML Response to the user's client. The client is then forwarded to the **Assertion Consumer Service (ACS)** of the SP with this information.

We can set a ``return_to`` url parameter to the login function and that will be converted as a ``RelayState`` parameter:

```python
target_url = 'https://example.com'
auth.login(return_to=target_url)
```
The login method can receive 3 more optional parameters:

* ``force_authn``       When ``true``, the ``AuthNReuqest`` will set the ``ForceAuthn='true'``
* ``is_passive``        When true, the ``AuthNReuqest`` will set the ``Ispassive='true'``
* ``set_nameid_policy`` When true, the ``AuthNReuqest`` will set a ``nameIdPolicy`` element.

If a match on the future ``SAMLResponse`` ID and the ``AuthNRequest`` ID to be sent is required, that ``AuthNRequest`` ID must to be extracted and stored for future validation, we can get that ID by

``auth.get_last_request_id()``

#### The SP Endpoints ####

Related to the SP there are 3 important endpoints: The metadata view, the ACS view and the SLS view.
The toolkit provides examples of those views in the demos, but let's see an example.

***SP Metadata***

This code will provide the XML metadata file of our SP, based on the info that we provided in the settings files.

```python
req = prepare_request_for_toolkit(request)
auth = OneLogin_Saml2_Auth(req)
saml_settings = auth.get_settings()
metadata = saml_settings.get_sp_metadata()
errors = saml_settings.validate_metadata(metadata)
if len(errors) == 0:
    print(metadata)
else:
    print("Error found on Metadata: %s" % (', '.join(errors)))
```

The ``get_sp_metadata`` will return the metadata signed or not based on the security info of the ``advanced_settings.json`` (``signMetadata``).

Before the XML metadata is exposed, a check takes place to ensure that the info to be provided is valid.

Instead of using the Auth object, you can directly use
```
saml_settings = OneLogin_Saml2_Settings(settings=None, custom_base_path=None, sp_validation_only=True)
```
to get the settings object and with the ``sp_validation_only=True`` parameter we will avoid the IdP settings validation.

***Assertion Consumer Service (ACS)***

This code handles the SAML response that the IdP forwards to the SP through the user's client.

```python
req = prepare_request_for_toolkit(request)
auth = OneLogin_Saml2_Auth(req)
auth.process_response()
errors = auth.get_errors()
if not errors:
    if auth.is_authenticated():
        request.session['samlUserdata'] = auth.get_attributes()
        if 'RelayState' in req['post_data'] and
          OneLogin_Saml2_Utils.get_self_url(req) != req['post_data']['RelayState']:
            # To avoid 'Open Redirect' attacks, before execute the redirection confirm
                # the value of the req['post_data']['RelayState'] is a trusted URL.
            auth.redirect_to(req['post_data']['RelayState'])
        else:
            for attr_name in request.session['samlUserdata'].keys():
                print('%s ==> %s' % (attr_name, '|| '.join(request.session['samlUserdata'][attr_name])))
    else:
      print('Not authenticated')
else:
    print("Error when processing SAML Response: %s %s" % (', '.join(errors), auth.get_last_error_reason()))
```

The SAML response is processed and then checked that there are no errors. It also verifies that the user is authenticated and stored the userdata in session.

At that point there are 2 possible alternatives:

* If no ``RelayState`` is provided, we could show the user data in this view or however we wanted.
* If ``RelayState`` is provided, a redirection takes place.

Notice that we saved the user data in the session before the redirection to have the user data available at the ``RelayState`` view.

In order to retrieve attributes we use:

```python
attributes = auth.get_attributes();
```

With this method we get a dict with all the user data provided by the IdP in the assertion of the SAML response.

If we execute print attributes we could get:

```python
{
    "cn": ["Jhon"],
    "sn": ["Doe"],
    "mail": ["Doe"],
    "groups": ["users", "members"]
}
```

Each attribute name can be used as a key to obtain the value. Every attribute is a list of values. A single-valued attribute is a list of a single element.

The following code is equivalent:

```python
attributes = auth.get_attributes();
print(attributes['cn'])

print(auth.get_attribute('cn'))
```

Before trying to get an attribute, check that the user is authenticated. If the user isn't authenticated, an empty dict will be returned. For example, if we call to ``get_attributes`` before a ``auth.process_response``, the ``get_attributes()`` will return an empty dict.


***Single Logout Service (SLS)***

This code handles the Logout Request and the Logout Responses.

```python
delete_session_callback = lambda: request.session.flush()
url = auth.process_slo(delete_session_cb=delete_session_callback)
errors = auth.get_errors()
if len(errors) == 0:
    if url is not None:
        # To avoid 'Open Redirect' attacks, before execute the redirection confirm
        # the value of the url is a trusted URL.
        return redirect(url)
    else:
        print("Successfully Logged out")
else:
    print("Error when processing SLO: %s %s" % (', '.join(errors), auth.get_last_error_reason()))
```

If the SLS endpoints receives a Logout Response, the response is validated and the session could be closed, using the callback.

```python
# Part of the process_slo method
logout_response = OneLogin_Saml2_Logout_Response(self.__settings, self.__request_data['get_data']['SAMLResponse'])
if not logout_response.is_valid(self.__request_data, request_id):
    self.__errors.append('invalid_logout_response')
elif logout_response.get_status() != OneLogin_Saml2_Constants.STATUS_SUCCESS:
    self.__errors.append('logout_not_success')
elif not keep_local_session:
    OneLogin_Saml2_Utils.delete_local_session(delete_session_cb)
```

If the SLS endpoints receives an Logout Request, the request is validated, the session is closed and a Logout Response is sent to the SLS endpoint of the IdP.

```python
# Part of the process_slo method
request = OneLogin_Saml2_Utils.decode_base64_and_inflate(self.__request_data['get_data']['SAMLRequest'])
if not OneLogin_Saml2_Logout_Request.is_valid(self.__settings, request, self.__request_data):
    self.__errors.append('invalid_logout_request')
else:
    if not keep_local_session:
        OneLogin_Saml2_Utils.delete_local_session(delete_session_cb)

    in_response_to = request.id
    response_builder = OneLogin_Saml2_Logout_Response(self.__settings)
    response_builder.build(in_response_to)
    logout_response = response_builder.get_response()

    parameters = {'SAMLResponse': logout_response}
    if 'RelayState' in self.__request_data['get_data']:
        parameters['RelayState'] = self.__request_data['get_data']['RelayState']

    security = self.__settings.get_security_data()
    if 'logoutResponseSigned' in security and security['logoutResponseSigned']:
        parameters['SigAlg'] = OneLogin_Saml2_Constants.RSA_SHA1
        parameters['Signature'] = self.build_response_signature(logout_response, parameters.get('RelayState', None))

    return self.redirect_to(self.get_slo_url(), parameters)
```

If we don't want that ``process_slo`` to destroy the session, pass a ``true`` parameter to the ``process_slo`` method:

```python
keepLocalSession = true
auth.process_slo(keep_local_session=keepLocalSession);
```

#### Initiate SLO ####

In order to send a Logout Request to the IdP:

The Logout Request will be sent signed or unsigned based on the security info of the ``advanced_settings.json`` (``logoutRequestSigned``).

The IdP will return the Logout Response through the user's client to the Single Logout Service (SLS) of the SP.

We can set a ``return_to`` url parameter to the logout function and that will be converted as a ``RelayState`` parameter:

```python
target_url = 'https://example.com'
auth.logout(return_to=target_url)
```

Also there are another 5 optional parameters that can be set:

* ``name_id``: That will be used to build the ``LogoutRequest``. If no ``name_id`` parameter is set and the auth object processed a
SAML Response with a ``NameId``, then this ``NameId`` will be used.
* ``session_index``: ``SessionIndex`` that identifies the session of the user.
* ``nq``: IDP Name Qualifier.
* ``name_id_format``: The ``NameID`` Format that will be set in the ``LogoutRequest``.
* ``spnq``: The ``NameID SP NameQualifier`` will be set in the ``LogoutRequest``.

If no ``name_id`` is provided, the ``LogoutRequest`` will contain a ``NameID`` with the entity Format.
If ``name_id`` is provided and no ``name_id_format`` is provided, the ``NameIDFormat`` of the settings will be used.

If a match on the ``LogoutResponse`` ID and the ``LogoutRequest`` ID to be sent is required, that ``LogoutRequest`` ID must to be extracted and stored for future validation, we can get that ID by:

```python
auth.get_last_request_id()
```

#### Example of a view that initiates the SSO request and handles the response (is the acs target) ####

We can code a unique file that initiates the SSO process, handle the response, get the attributes, initiate the SLO and processes the logout response.

Note: Review the demos, in a later section we explain the demo use case further in detail.

```python
req = prepare_request_for_toolkit(request)  # Process the request and build the request dict that
                                            # the toolkit expects

auth = OneLogin_Saml2_Auth(req)             # Initialize the SP SAML instance

if 'sso' in request.args:                   # SSO action (SP-SSO initited).  Will send an AuthNRequest to the IdP
    return redirect(auth.login())
elif 'sso2' in request.args:                       # Another SSO init action
    return_to = '%sattrs/' % request.host_url      # but set a custom RelayState URL
    return redirect(auth.login(return_to))
elif 'slo' in request.args:                     # SLO action. Will sent a Logout Request to IdP
    nameid = request.session['samlNameId']
    nameid_format = request.session['samlNameIdFormat']
    nameid_nq = request.session['samlNameIdNameQualifier']
    nameid_spnq = request.session['samlNameIdSPNameQualifier']
    session_index = request.session['samlSessionIndex']
    return redirect(auth.logout(None, nameid, session_index, nameid_nq, nameid_format, nameid_spnq))
elif 'acs' in request.args:                 # Assertion Consumer Service
    auth.process_response()                     # Process the Response of the IdP
    errors = auth.get_errors()              # This method receives an array with the errors
    if len(errors) == 0:                    # that could took place during the process
        if not auth.is_authenticated():         # This check if the response was ok and the user
            msg = "Not authenticated"           # data retrieved or not (user authenticated)
        else:
            request.session['samlUserdata'] = auth.get_attributes()     # Retrieves user data
            request.session['samlNameId'] = auth.get_nameid()
            request.session['samlNameIdFormat'] = auth.get_nameid_format()
            request.session['samlNameIdNameQualifier'] = auth.get_nameid_nq()
            request.session['samlNameIdSPNameQualifier'] = auth.get_nameid_spnq()
            request.session['samlSessionIndex'] = auth.get_session_index()
            self_url = OneLogin_Saml2_Utils.get_self_url(req)
            if 'RelayState' in request.form and self_url != request.form['RelayState']:
                # To avoid 'Open Redirect' attacks, before execute the redirection confirm
                # the value of the request.form['RelayState'] is a trusted URL.
                return redirect(auth.redirect_to(request.form['RelayState']))   # Redirect if there is a relayState
            else:                           # If there is user data we save that to print it later.
                msg = ''
                for attr_name in request.session['samlUserdata'].keys():
                    msg += '%s ==> %s' % (attr_name, '|| '.join(request.session['samlUserdata'][attr_name]))
elif 'sls' in request.args:                                             # Single Logout Service
    delete_session_callback = lambda: session.clear()           # Obtain session clear callback
    url = auth.process_slo(delete_session_cb=delete_session_callback)   # Process the Logout Request & Logout Response
    errors = auth.get_errors()              #  Retrieves possible validation errors
    if len(errors) == 0:
        if url is not None:
            # To avoid 'Open Redirect' attacks, before execute the redirection confirm
            # the value of the url is a trusted URL.
            return redirect(url)
        else:
            msg = "Successfully logged out"

if len(errors) == 0:
  print(msg)
else:
  print(', '.join(errors))
```


### SP Key rollover ###

If you plan to update the SP ``x509cert`` and ``privateKey`` you can define the new ``x509cert`` as ``settings['sp']['x509certNew']`` and it will be
published on the SP metadata so Identity Providers can read them and get ready for rollover.


### IdP with multiple certificates ###

In some scenarios the IdP uses different certificates for
signing/encryption, or is under key rollover phase and more than one certificate is published on IdP metadata.

In order to handle that the toolkit offers the ``settings['idp']['x509certMulti']`` parameter.

When that parameter is used, ``x509cert`` and ``certFingerprint`` values will be ignored by the toolkit.

The ``x509certMulti`` is an array with 2 keys:
- ``signing``: An array of certs that will be used to validate IdP signature
- ``encryption``: An array with one unique cert that will be used to encrypt data to be sent to the IdP.


### Replay attacks ###

In order to avoid replay attacks, you can store the ID of the SAML messages already processed, to avoid processing them twice. Since the Messages expires and will be invalidated due that fact, you don't need to store those IDs longer than the time frame that you currently accepting.

Get the ID of the last processed message/assertion with the ``get_last_message_id/get_last_assertion_id`` method of the ``Auth`` object.


### Main classes and methods ###

Described below are the main classes and methods that can be invoked from the SAML2 library.

#### OneLogin_Saml2_Auth - auth.py ####

Main class of SAML Python Toolkit

* `__init__` Initializes the SP SAML instance.
* ***login*** Initiates the SSO process.
* ***logout*** Initiates the SLO process.
* ***process_response*** Process the SAML Response sent by the IdP.
* ***process_slo*** Process the SAML Logout Response / Logout Request sent by the IdP.
* ***redirect_to*** Redirects the user to the url past by parameter or to the url that we defined in our SSO Request.
* ***is_authenticated*** Checks if the user is authenticated or not.
* ***get_attributes*** Returns the set of SAML attributes.
* ***get_attribute*** Returns the requested SAML attribute.
* ***get_nameid*** Returns the ``nameID``.
* ***get_session_index*** Gets the ``SessionIndex`` from the ``AuthnStatement``.
* ***get_session_expiration*** Gets the ``SessionNotOnOrAfter`` from the ``AuthnStatement``.
* ***get_errors*** Returns a list with code errors if something went wrong.
* ***get_last_error_reason*** Returns the reason of the last error
* ***get_sso_url*** Gets the SSO url.
* ***get_slo_url*** Gets the SLO url.
* ***get_last_request_id*** The ID of the last Request SAML message generated (``AuthNRequest``, ``LogoutRequest``).
* ***get_last_authn_contexts*** Returns the list of authentication contexts sent in the last SAML Response.
* ***build_request_signature*** Builds the Signature of the SAML Request.
* ***build_response_signature*** Builds the Signature of the SAML Response.
* ***get_settings*** Returns the settings info.
* ***set_strict*** Set the strict mode active/disable.
* ***get_last_request_xml*** Returns the most recently-constructed/processed XML SAML request (``AuthNRequest``, ``LogoutRequest``)
* ***get_last_response_xml*** Returns the most recently-constructed/processed XML SAML response (``SAMLResponse``, ``LogoutResponse``). If the SAMLResponse had an encrypted assertion, decrypts it.
* ***get_last_response_in_response_to*** The `InResponseTo` ID of the most recently processed SAML Response.
* ***get_last_message_id*** The ID of the last Response SAML message processed.
* ***get_last_assertion_id*** The ID of the last assertion processed.
* ***get_last_assertion_not_on_or_after*** The ``NotOnOrAfter`` value of the valid ``SubjectConfirmationData`` node (if any) of the last assertion processed (is only calculated with strict = true)
* ***get_last_assertion_issue_instant*** The `IssueInstant` value of the last assertion processed.

#### OneLogin_Saml2_Auth - authn_request.py ####

SAML 2 Authentication Request class

* `__init__` This class handles an ``AuthNRequest``. It builds an ``AuthNRequest`` object.
* ***get_request*** Returns unsigned ``AuthnRequest``.
* ***get_id*** Returns the ``AuthNRequest`` ID.
* ***get_xml*** Returns the XML that will be sent as part of the request.

#### OneLogin_Saml2_Response - response.py ####

SAML 2 Authentication Response class

* `__init__` Constructs the SAML Response object.
* ***is_valid*** Determines if the SAML Response is valid. Includes checking of the signature by a certificate.
* ***check_status*** Check if the status of the response is success or not
* ***get_audiences*** Gets the audiences
* ***get_issuers*** Gets the issuers (from message and from assertion)
* ***get_nameid_data*** Gets the NameID Data provided by the SAML Response from the IdP (returns a dict)
* ***get_nameid*** Gets the NameID provided by the SAML Response from the IdP (returns a string)
* ***get_session_not_on_or_after*** Gets the ``SessionNotOnOrAfter`` from the ``AuthnStatement``
* ***get_session_index*** Gets the ``SessionIndex`` from the ``AuthnStatement``
* ***get_attributes*** Gets the Attributes from the ``AttributeStatement`` element.
* ***validate_num_assertions*** Verifies that the document only contains a single Assertion (encrypted or not)
* ***validate_timestamps*** Verifies that the document is valid according to Conditions Element
* ***get_error*** After execute a validation process, if fails this method returns the cause
* ***get_xml_document*** Returns the SAML Response document (If contains an encrypted assertion, decrypts it).
* ***get_id*** the ID of the response
* ***get_assertion_id*** the ID of the assertion in the response
* ***get_assertion_not_on_or_after*** the ``NotOnOrAfter`` value of the valid ``SubjectConfirmationData`` if any

#### OneLogin_Saml2_LogoutRequest - logout_request.py ####

SAML 2 Logout Request class

* `__init__` Constructs the Logout Request object.
* ***get_request*** Returns the Logout Request deflated, base64-encoded.
* ***get_id*** Returns the ID of the Logout Request. (If you have the object you can access to the id attribute)
* ***get_nameid_data*** Gets the NameID Data of the the Logout Request (returns a dict).
* ***get_nameid*** Gets the NameID of the Logout Request Message (returns a string).
* ***get_issuer*** Gets the Issuer of the Logout Request Message.
* ***get_session_indexes*** Gets the ``SessionIndexes`` from the Logout Request.
* ***is_valid*** Checks if the Logout Request received is valid.
* ***get_error*** After execute a validation process, if fails this method returns the cause.
* ***get_xml*** Returns the XML that will be sent as part of the request or that was received at the SP

#### OneLogin_Saml2_LogoutResponse - logout_response.py ####

SAML 2 Logout Response class

* `__init__` Constructs a Logout Response object.
* ***get_issuer*** Gets the Issuer of the Logout Response Message
* ***get_status*** Gets the Status of the Logout Response.
* ***is_valid*** Determines if the SAML ``LogoutResponse`` is valid
* ***build*** Creates a Logout Response object.
* ***get_response*** Returns a Logout Response object.
* ***get_error*** After execute a validation process, if fails this method returns the cause.
* ***get_xml*** Returns the XML that will be sent as part of the response or that was received at the SP

#### OneLogin_Saml2_Settings - settings.py ####

Configuration of the SAML Python Toolkit

* `__init__`  Initializes the settings: Sets the paths of the different folders and Loads settings info from settings file or array/object provided.
* ***check_settings*** Checks the settings info.
* ***check_idp_settings*** Checks the IdP settings info.
* ***check_sp_settings*** Checks the SP settings info.
* ***get_errors*** Returns an array with the errors, the array is empty when the settings is ok.
* ***get_sp_metadata*** Gets the SP metadata. The XML representation.
* ***validate_metadata*** Validates an XML SP Metadata.
* ***get_base_path*** Returns base path.
* ***get_cert_path*** Returns cert path.
* ***get_lib_path*** Returns lib path.
* ***get_ext_lib_path*** Returns external lib path.
* ***get_schemas_path*** Returns schema path.
* ***check_sp_certs*** Checks if the X.509 certs of the SP exists and are valid.
* ***get_sp_key*** Returns the X.509 private key of the SP.
* ***get_sp_cert*** Returns the X.509 public cert of the SP.
* ***get_sp_cert_new*** Returns the future X.509 public cert of the SP.
* ***get_idp_cert*** Returns the X.509 public cert of the IdP.
* ***get_sp_data*** Gets the SP data.
* ***get_idp_data*** Gets the IdP data.
* ***get_security_data***  Gets security data.
* ***get_contacts*** Gets contacts data.
* ***get_organization*** Gets organization data.
* ***format_idp_cert*** Formats the IdP cert.
* ***format_idp_cert_multi*** Formats all registered IdP certs.
* ***format_sp_cert*** Formats the SP cert.
* ***format_sp_cert_new*** Formats the SP cert new.
* ***format_sp_key*** Formats the private key.
* ***set_strict*** Activates or deactivates the strict mode.
* ***is_strict*** Returns if the ``strict`` mode is active.
* ***is_debug_active*** Returns if the debug is active.

#### OneLogin_Saml2_Metadata - metadata.py ####

A class that contains functionality related to the metadata of the SP

* ***builder*** Generates the metadata of the SP based on the settings.
* ***sign_metadata*** Signs the metadata with the key/cert provided.
* ***add_x509_key_descriptors*** Adds the X.509 descriptors (sign/encryption) to the metadata

#### OneLogin_Saml2_Utils - utils.py ####

Auxiliary class that contains several methods

* ***decode_base64_and_inflate*** Base64 decodes and then inflates according to RFC1951.
* ***deflate_and_base64_encode*** Deflates and the base64 encodes a string.
* ***format_cert*** Returns a X.509 cert (adding header & footer if required).
* ***format_private_key*** Returns a private key (adding header & footer if required).
* ***redirect*** Executes a redirection to the provided url (or return the target url).
* ***get_self_url_host*** Returns the protocol + the current host + the port (if different than common ports).
* ***get_self_host*** Returns the current host.
* ***is_https*** Checks if https or http.
* ***get_self_url_no_query*** Returns the URL of the current host + current view.
* ***get_self_routed_url_no_query*** Returns the routed URL of the current host + current view.
* ***get_self_url*** Returns the URL of the current host + current view + query.
* ***generate_unique_id*** Generates an unique string (used for example as ID for assertions).
* ***parse_time_to_SAML*** Converts a UNIX timestamp to SAML2 timestamp on the form yyyy-mm-ddThh:mm:ss(\.s+)?Z.
* ***parse_SAML_to_time*** Converts a SAML2 timestamp on the form yyyy-mm-ddThh:mm:ss(\.s+)?Z to a UNIX timestamp.
* ***now*** Returns unix timestamp of actual time.
* ***parse_duration*** Interprets a ISO8601 duration value relative to a given timestamp.
* ***get_expire_time*** Compares 2 dates and returns the earliest.
* ***delete_local_session*** Deletes the local session.
* ***calculate_X.509_fingerprint*** Calculates the fingerprint of a X.509 cert.
* ***format_finger_print*** Formats a fingerprint.
* ***generate_name_id*** Generates a nameID.
* ***get_status*** Gets Status from a Response.
* ***decrypt_element*** Decrypts an encrypted element.
* ***write_temp_file*** Writes some content into a temporary file and returns it.
* ***add_sign*** Adds signature key and senders certificate to an element (Message or Assertion).
* ***validate_sign*** Validates a signature (Message or Assertion).
* ***validate_binary_sign*** Validates signed bynary data (Used to validate GET Signature).

#### OneLogin_Saml2_XML- xml_utils.py ####

A class that contains methods to handle XMLs

* ***to_string*** Serialize an element to an encoded string representation of its XML tree.
* ***to_etree*** Parses an XML document or fragment from a string.
* ***validate_xml*** Validates a xml against a schema
* ***query*** Extracts nodes that match the query from the Element
* ***extract_tag_text***

#### OneLogin_Saml2_IdPMetadataParser - idp_metadata_parser.py ####

A class that contains methods to obtain and parse metadata from IdP

* ***get_metadata*** Get the metadata XML from the provided URL
* ***parse_remote*** Get the metadata XML from the provided URL and parse it, returning a dict with extracted data
* ***parse*** Parse the Identity Provider metadata and returns a dict with extracted data
* ***merge_settings*** Will update the settings with the provided new settings data extracted from the IdP metadata


For more info, look at the source code. Each method is documented and details about what does and how to use it are provided. Make sure to also check the doc folder where HTML documentation about the classes and methods is provided.

Demos included in the toolkit
-----------------------------

The toolkit includes 3 demos to teach how use the toolkit (A Django, Flask and a Tornado project), take a look on it.
Demos require that SP and IdP are well configured before test it, so edit the settings files.

Notice that each python framework has it own way to handle routes/urls and process request, so focus on
how it deployed. New demos using other python frameworks are welcome as a contribution.

### Getting Started ###

We said that this toolkit includes a Django application demo and a Flask application demo,
let's see how fast is it to deploy them.

***Virtualenv***

The use of a [virtualenv](http://virtualenv.readthedocs.org/en/latest/) is
highly recommended.

Virtualenv helps isolating the python environment used to run the toolkit. You
can find more details and an installation guide in the
[official documentation](http://virtualenv.readthedocs.org/en/latest/).

Once you have your virtualenv ready and loaded, then you can install the
toolkit on it in development mode executing this:
```
 python setup.py develop
```

Using this method of deployment the toolkit files will be linked instead of
copied, so if you make changes on them you won't need to reinstall the toolkit.

If you want install it in a normal mode, execute:
```
 python setup.py install
```

### Demo Flask ###

You'll need a virtualenv with the toolkit installed on it.

To run the demo you need to install the requirements first. Load your
virtualenv and execute:

```
 pip install -r demo-flask/requirements.txt
```

This will install flask and its dependencies. Once it has finished, you have to complete the configuration
of the toolkit. You'll find it at `demo-flask/settings.json`

Now, with the virtualenv loaded, you can run the demo like this:
```
 cd demo-flask
 python index.py
```

You'll have the demo running at http://localhost:8000

#### Content ####

The flask project contains:


* ***index.py*** Is the main flask file, where or the SAML handle take place.

* ***templates***. Is the folder where flask stores the templates of the project. It was implemented a base.html template that is extended by index.html and attrs.html, the templates of our simple demo that shows messages, user attributes when available and login and logout links.

* ***saml*** Is a folder that contains the 'certs' folder that could be used to store the X.509 public and private key, and the saml toolkit settings (settings.json and advanced_settings.json).


#### SP setup ####

The SAML Python Toolkit allows you to provide the settings info in 2 ways: Settings files or define a setting dict. In the ``demo-flask``, it uses the first method.

In the ``index.py`` file we define the ``app.config['SAML_PATH']``, that will target to the ``saml`` folder. We require it in order to load the settings files.

First we need to edit the ``saml/settings.json`` file, configure the SP part and review the metadata of the IdP and complete the IdP info.  Later edit the ``saml/advanced_settings.json`` files and configure the how the toolkit will work. Check the settings section of this document if you have any doubt.

#### IdP setup ####

Once the SP is configured, the metadata of the SP is published at the ``/metadata`` url. Based on that info, configure the IdP.

#### How it works ####

 1. First time you access to the main view (http://localhost:8000), you can select to login and return to the same view or login and be redirected to ``/?attrs`` (attrs view).

 2. When you click:

    2.1 in the first link, we access to ``/?sso`` (index view). An ``AuthNRequest`` is sent to the IdP, we authenticate at the IdP and then a Response is sent through the user's client to the SP, specifically the Assertion Consumer Service view: ``/?acs``. Notice that a ``RelayState`` parameter is set to the url that initiated the process, the index view.

    2.2 in the second link we access to ``/?attrs`` (attrs view), we will expetience have the same process described at 2.1 with the diference that as ``RelayState`` is set the ``attrs`` url.

 3. The SAML Response is processed in the ACS ``/?acs``, if the Response is not valid, the process stops here and a message is shown. Otherwise we are redirected to the ``RelayState`` view. a) / or b) ``/?attrs``

 4. We are logged in the app and the user attributes are showed. At this point, we can test the single log out functionality.

 The single log out functionality could be tested by 2 ways.

    5.1 SLO Initiated by SP. Click on the ``logout`` link at the SP, after that a Logout Request is sent to the IdP, the session at the IdP is closed and replies through the client to the SP with a Logout Response (sent to the Single Logout Service endpoint). The SLS endpoint ``/?sls`` of the SP process the Logout Response and if is valid, close the user session of the local app. Notice that the SLO Workflow starts and ends at the SP.

    5.2 SLO Initiated by IdP. In this case, the action takes place on the IdP side, the logout process is initiated at the IdP, sends a Logout Request to the SP (SLS endpoint, ``/?sls``). The SLS endpoint of the SP process the Logout Request and if is valid, close the session of the user at the local app and send a Logout Response to the IdP (to the SLS endpoint of the IdP). The IdP receives the Logout Response, process it and close the session at of the IdP. Notice that the SLO Workflow starts and ends at the IdP.

Notice that all the SAML Requests and Responses are handled at a unique view (index) and how GET parameters are used to know the action that must be done.

### Demo Tornado ###

You'll need a virtualenv with the toolkit installed on it.

First of all you need some packages, execute:
```
apt-get install libxml2-dev libxmlsec1-dev libxmlsec1-openssl
```

To run the demo you need to install the requirements first. Load your
virtualenv and execute:
```
 pip install -r demo-tornado/requirements.txt
```


This will install tornado and its dependencies. Once it has finished, you have to complete the configuration
of the toolkit. You'll find it at `demo-tornado/saml/settings.json`

Now, with the virtualenv loaded, you can run the demo like this:
```
 cd demo-tornado
 python views.py
```

You'll have the demo running at http://localhost:8000

#### Content ####

The tornado project contains:

* ***views.py*** Is the main flask file, where or the SAML handle take place.

* ***settings.py*** Contains the base path and the path where is located the ``saml`` folder and the ``template`` folder

* ***templates***. Is the folder where tornado stores the templates of the project. It was implemented a base.html template that is extended by index.html and attrs.html, the templates of our simple demo that shows messages, user attributes when available and login and logout links.

* ***saml*** Is a folder that contains the 'certs' folder that could be used to store the X.509 public and private key, and the saml toolkit settings (settings.json and advanced_settings.json).

#### SP setup ####

The SAML Python Toolkit allows you to provide the settings info in 2 ways: Settings files or define a setting dict. In the ``demo-tornado``, it uses the first method.

In the ``settings.py`` file we define the ``SAML_PATH``, that will target to the ``saml`` folder. We require it in order to load the settings files.

First we need to edit the ``saml/settings.json`` file, configure the SP part and review the metadata of the IdP and complete the IdP info.  Later edit the ``saml/advanced_settings.json`` files and configure the how the toolkit will work. Check the settings section of this document if you have any doubt.

#### IdP setup ####

Once the SP is configured, the metadata of the SP is published at the ``/metadata`` url. Based on that info, configure the IdP.

#### How it works ####

1. First time you access to the main view (http://localhost:8000), you can select to login and return to the same view or login and be redirected to ``/?attrs`` (attrs view).

 2. When you click:

    2.1 in the first link, we access to ``/?sso`` (index view). An ``AuthNRequest`` is sent to the IdP, we authenticate at the IdP and then a Response is sent through the user's client to the SP, specifically the Assertion Consumer Service view: ``/?acs``. Notice that a ``RelayState`` parameter is set to the url that initiated the process, the index view.

    2.2 in the second link we access to ``/?attrs`` (attrs view), we will expetience have the same process described at 2.1 with the diference that as ``RelayState`` is set the ``attrs`` url.

 3. The SAML Response is processed in the ACS ``/?acs``, if the Response is not valid, the process stops here and a message is shown. Otherwise we are redirected to the ``RelayState`` view. a) / or b) ``/?attrs``

 4. We are logged in the app and the user attributes are showed. At this point, we can test the single log out functionality.

 The single log out functionality could be tested by 2 ways.

    5.1 SLO Initiated by SP. Click on the ``logout`` link at the SP, after that a Logout Request is sent to the IdP, the session at the IdP is closed and replies through the client to the SP with a Logout Response (sent to the Single Logout Service endpoint). The SLS endpoint ``/?sls`` of the SP process the Logout Response and if is valid, close the user session of the local app. Notice that the SLO Workflow starts and ends at the SP.

    5.2 SLO Initiated by IdP. In this case, the action takes place on the IdP side, the logout process is initiated at the IdP, sends a Logout Request to the SP (SLS endpoint, ``/?sls``). The SLS endpoint of the SP process the Logout Request and if is valid, close the session of the user at the local app and send a Logout Response to the IdP (to the SLS endpoint of the IdP). The IdP receives the Logout Response, process it and close the session at of the IdP. Notice that the SLO Workflow starts and ends at the IdP.

Notice that all the SAML Requests and Responses are handled at a unique view (index) and how GET parameters are used to know the action that must be done.

### Demo Django ###

You'll need a virtualenv with the toolkit installed on it.

To run the demo you need to install the requirements first. Load your
virtualenv and execute:
```
 pip install -r demo-django/requirements.txt
```
This will install django and its dependencies. Once it has finished, you have to complete the configuration of the toolkit.

Later, with the virtualenv loaded, you can run the demo like this:
```
 cd demo-django
 python manage.py runserver 0.0.0.0:8000
```

You'll have the demo running at http://localhost:8000.

Note that many of the configuration files expect HTTPS. This is not required by the demo, as replacing these SP URLs with HTTP will work just fine. HTTPS is however highly encouraged, and left as an exercise for the reader for their specific needs.

If you want to integrate a production django application, take a look on this SAMLServiceProviderBackend that uses our toolkit to add SAML support: https://github.com/KristianOellegaard/django-saml-service-provider

#### Content ####

The django project contains:

* ***manage.py***. A file that is automatically created in each Django project. Is a thin wrapper around django-admin.py that takes care of putting the project’s package on ``sys.path`` and sets the ``DJANGO_SETTINGS_MODULE`` environment variable.

* ***saml*** Is a folder that contains the 'certs' folder that could be used to store the X.509 public and private key, and the saml toolkit settings (``settings.json`` and ``advanced_settings.json``).

* ***demo*** Is the main folder of the django project, that contains the typical files:
  * ***settings.py*** Contains the default parameters of a django project except the ``SAML_FOLDER`` parameter, that may contain the path where is located the ``saml`` folder.
  * ***urls.py*** A file that define url routes. In the demo we defined ``'/'`` that is related to the index view, ``'/attrs'`` that is related with the attrs view and ``'/metadata'``, related to the metadata view.
  * ***views.py*** This file contains the views of the django project and some aux methods.
  * ***wsgi.py*** A file that let as deploy django using WSGI, the Python standard for web servers and applications.

* ***templates***. Is the folder where django stores the templates of the project. It was implemented a ``base.html`` template that is extended by ``index.html`` and ``attrs.html``, the templates of our simple demo that shows messages, user attributes when available and login and logout links.

#### SP setup ####

The SAML Python Toolkit allows you to provide the settings info in 2 ways: settings files or define a setting dict. In the demo-django it used the first method.

After set the ``SAML_FOLDER`` in the ``demo/settings.py``, the settings of the Python toolkit will be loaded on the Django web.

First we need to edit the ``saml/settings.json``, configure the SP part and review the metadata of the IdP and complete the IdP info.  Later edit the ``saml/advanced_settings.json`` files and configure the how the toolkit will work. Check the settings section of this document if you have any doubt.

#### IdP setup ####

Once the SP is configured, the metadata of the SP is published at the ``/metadata`` url. Based on that info, configure the IdP.

#### How it works ####

This demo works very similar to the ``flask-demo`` (We did it intentionally).

### Getting up and running on Heroku ###

Getting ``python3-saml`` up and running on Heroku will require some extra legwork: ``python3-saml`` depends on ``python-xmlsec`` which depends on headers from the ``xmlsec1-dev`` Linux package to install correctly.

First you will need to add the ```apt``` buildpack to your build server:

```
heroku buildpacks:add --index=1 -a your-app heroku-community/apt
heroku buildpacks:add --index=2 -a your-app heroku/python
```

You can confirm the buildpacks have been added in the correct order with ```heroku buildpacks -a your-app```, you should see the apt buildpack first followed by the Python buildpack.

Then add an ```Aptfile``` into the root of your repository containing the ```libxmlsec1-dev``` package, the file should look like:
```
libxmlsec1-dev

```

Finally, add ``python3-saml`` to your ``requirements.txt`` and ```git push``` to trigger a build.

### Demo Pyramid ###

Unlike the other two projects, you don't need a pre-existing virtualenv to get
up and running here, since Pyramid comes from the
[buildout](http://www.buildout.org/en/latest/) school of thought.

To run the demo you need to install Pyramid, the requirements, etc.:
```
 cd demo_pyramid
 python3 -m venv env
 env/bin/pip install --upgrade pip setuptools
 env/bin/pip install -e ".[testing]"
```

If you want to make sure the tests pass, run:
```
 env/bin/pytest
```

Next, edit the settings in `demo_pyramid/saml/settings.json`. (Pyramid runs on
port 6543 by default.)

Now you can run the demo like this:
```
 env/bin/pserve development.ini
```

If that worked, the demo is now running at http://localhost:6543.

#### Content ####

The Pyramid project contains:


* ***\_\_init__.py*** is the main Pyramid file that configures the app and its routes.

* ***views.py*** is where all the SAML handling takes place.

* ***templates*** is the folder where Pyramid stores the templates of the project. It was implemented a ``layout.jinja2`` template that is extended by ``index.jinja2`` and ``attrs.jinja2``, the templates of our simple demo that shows messages, user attributes when available and login and logout links.

* ***saml*** is a folder that contains the 'certs' folder that could be used to store the X.509 public and private key, and the saml toolkit settings (``settings.json`` and ``advanced_settings.json``).


#### SP setup ####

The SAML Python Toolkit allows you to provide the settings info in 2 ways: settings files or define a setting dict. In ``demo_pyramid`` the first method is used.

In the ``views.py`` file we define the ``SAML_PATH``, which will target the ``saml`` folder. We require it in order to load the settings files.

First we need to edit the ``saml/settings.json``, configure the SP part and review the metadata of the IdP and complete the IdP info.  Later edit the ``saml/advanced_settings.json`` files and configure the how the toolkit will work. Check the settings section of this document if you have any doubt.

#### IdP setup ####

Once the SP is configured, the metadata of the SP is published at the ``/metadata`` url. Based on that info, configure the IdP.

#### How it works ####

1. First time you access to the main view (http://localhost:6543), you can select to login and return to the same view or login and be redirected to ``/?attrs`` (attrs view).

 2. When you click:

    2.1 in the first link, we access to ``/?sso`` (index view). An ``AuthNRequest`` is sent to the IdP, we authenticate at the IdP and then a Response is sent through the user's client to the SP, specifically the Assertion Consumer Service view: ``/?acs``. Notice that a ``RelayState`` parameter is set to the url that initiated the process, the index view.

    2.2 in the second link we access to ``/?attrs`` (attrs view), we will experience the same process described at 2.1 with the diference that as ``RelayState`` is set the ``attrs`` url.

 3. The SAML Response is processed in the ACS ``/?acs``, if the Response is not valid, the process stops here and a message is shown. Otherwise we are redirected to the ``RelayState`` view. a) ``/`` or b) ``/?attrs``

 4. We are logged in the app and the user attributes are showed. At this point, we can test the single log out functionality.

 The single log out functionality could be tested by 2 ways.

    5.1 SLO Initiated by SP. Click on the "logout" link at the SP, after that a Logout Request is sent to the IdP, the session at the IdP is closed and replies through the client to the SP with a Logout Response (sent to the Single Logout Service endpoint). The SLS endpoint /?sls of the SP process the Logout Response and if is valid, close the user session of the local app. Notice that the SLO Workflow starts and ends at the SP.

    5.2 SLO Initiated by IdP. In this case, the action takes place on the IdP side, the logout process is initiated at the IdP, sends a Logout Request to the SP (SLS endpoint, /?sls). The SLS endpoint of the SP process the Logout Request and if is valid, close the session of the user at the local app and send a Logout Response to the IdP (to the SLS endpoint of the IdP). The IdP receives the Logout Response, process it and close the session at of the IdP. Notice that the SLO Workflow starts and ends at the IdP.

Notice that all the SAML Requests and Responses are handled at a unique view (index) and how GET parameters are used to know the action that must be done.
