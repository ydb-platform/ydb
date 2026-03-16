python-doipclient
#################

.. image:: https://github.com/jacobschaer/python-doipclient/actions/workflows/tests.yml/badge.svg?branch=main

doipclient is a pure Python 3 Diagnostic over IP (DoIP) client which can be used
for communicating with modern ECU's over automotive ethernet. It implements the
majority of ISO-13400 (2019) from the perspective of a short-lived synchronous
client. The primary use case is to serve as a transport layer implementation for
the `udsoncan <https://github.com/pylessard/python-udsoncan>`_ library. The code
is published under MIT license on GitHub (jacobschaer/python-doipclient).

Documentation
-------------

The documentation is available here : https://python-doipclient.readthedocs.io/

Requirements
------------

 - Python 3.6+

Installation
------------

using pip::

    pip install doipclient

Running Tests from source
-------------------------

using pytest::

    pip install pytest pytest-mock
    pytest

Example
-------
Updated version of udsoncan's example using `python_doip` instead of IsoTPSocketConnection

.. code-block:: python

   import SomeLib.SomeCar.SomeModel as MyCar

   import udsoncan
   from doipclient import DoIPClient
   from doipclient.connectors import DoIPClientUDSConnector
   from udsoncan.client import Client
   from udsoncan.exceptions import *
   from udsoncan.services import *
   
   udsoncan.setup_logging()
   
   ecu_ip = '127.0.0.1'
   ecu_logical_address = 0x00E0
   doip_client = DoIPClient(ecu_ip, ecu_logical_address)
   conn = DoIPClientUDSConnector(doip_client)
   with Client(conn, request_timeout=2, config=MyCar.config) as client:
      try:
         client.change_session(DiagnosticSessionControl.Session.extendedDiagnosticSession)  # integer with value of 3
         client.unlock_security_access(MyCar.debug_level)                                   # Fictive security level. Integer coming from fictive lib, let's say its value is 5
         client.write_data_by_identifier(udsoncan.DataIdentifier.VIN, 'ABC123456789')       # Standard ID for VIN is 0xF190. Codec is set in the client configuration
         print('Vehicle Identification Number successfully changed.')
         client.ecu_reset(ECUReset.ResetType.hardReset)                                     # HardReset = 0x01
      except NegativeResponseException as e:
         print('Server refused our request for service %s with code "%s" (0x%02x)' % (e.response.service.get_name(), e.response.code_name, e.response.code))
      except (InvalidResponseException, UnexpectedResponseException) as e:
         print('Server sent an invalid payload : %s' % e.response.original_payload)

      # Because we reset our UDS server, we must also reconnect/reactivate the DoIP socket
      # Alternatively, we could have used the auto_reconnect_tcp flag on the DoIPClient
      # Note: ECU's do not restart instantly, so you may need a sleep() before moving on
      doip_client.reconnect()
      client.tester_present()

   # Cleanup the DoIP Socket when we're done. Alternatively, we could have used the
   # close_connection flag on conn so that the udsoncan client would clean it up
   doip_client.close()

python-uds Support
------------------
The `python-uds <https://github.com/richClubb/python-uds>`_ can also be used
but requires a fork until the owner merges this PR
`Doip #63 <https://github.com/richClubb/python-uds/pull/63>`_. For now, to use
the port:

using pip::

    git clone https://github.com/jacobschaer/python-uds
    git checkout doip
    cd python-uds
    pip install .

Example:

.. code-block:: python

   from uds import Uds

   ecu = Uds(transportProtocol="DoIP", ecu_ip="192.168.1.1", ecu_logical_address=1)
   try:
       response = ecu.send([0x3E, 0x00])
       print(response)  # This should be [0x7E, 0x00]
   except:
       print("Send did not complete")