axmlparser
===========


A simple parser to parse Android XML file.


Usage
======

.. code-block:: shell

    ➜ apkinfo --help
    Usage: apkinfo [OPTIONS] FILENAME

    Options:
    -s, --silent  Don't print any debug or warning logs
    --help        Show this message and exit.

CLI :
====

.. code-block:: shell

    $ apkinfo ~/Downloads/com.hardcodedjoy.roboremo.15.apk
    APK: /home/chillaranand/Downloads/com.hardcodedjoy.roboremo.15.apk
    App name: RoboRemo
    Package: com.hardcodedjoy.roboremo
    Version name: 2.0.0
    Version code: 15
    Is it Signed: True
    Is it Signed with v1 Signatures: True
    Is it Signed with v2 Signatures: True
    Is it Signed with v3 Signatures: False



Python package :
================

.. code-block:: python

    from pyaxmlparser import APK


    apk = APK('/foo/bar.apk')
    print(apk.package)
    print(apk.version_name)
    print(apk.version_code)
    print(apk.icon_info)
    print(apk.icon_data)
    print(apk.application)
