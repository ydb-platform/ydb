"""SoftWebauthnDevice usage for (yubico's) fido2 example webserver/application"""

from io import BytesIO

import pytest
from fido2 import cbor

from . import example_server
from soft_webauthn import SoftWebauthnDevice


@pytest.fixture
def client():
    """yield application test client as pytest fixture"""
    yield example_server.app.test_client()


def test_example_server_registration(client):  # pylint: disable=redefined-outer-name
    """Registration example"""

    # User holds an authenticator.
    #
    # NOTE: SoftWebauthnDevice emulates mixed client and authenticator behavior
    # and can be used for testing Webauthn/FIDO2 enabled applications during
    # continuous integration test-cases.
    device = SoftWebauthnDevice()

    # The browser starts registration process by requesting
    # publicKeyCredentialCreationOptions (pkcco) from the server/web
    # application/relying party (RP).
    #
    # NOTE: The transfer encoding and format is not in scope of Webauthn spec,
    # Yubico fido2 example server uses application/cbor.
    pkcco = cbor.decode(client.post('/api/register/begin').data)
    print('publicKeyCredentialCreationOptions: ', pkcco)

    # publicKeyCredentialCreationOptions object is passed from browser/client
    # to the authenticator. Authenticator will generate new credential and
    # return credential object (attestation).
    attestation = device.create(pkcco, 'https://localhost')
    print('new credential attestation: ', attestation)

    # Browser conveys the attestation data to the RP for registration.
    attestation_data = cbor.encode({
        'clientDataJSON': attestation['response']['clientDataJSON'],
        'attestationObject': attestation['response']['attestationObject']
    })
    raw_response = client.post(
        '/api/register/complete',
        input_stream=BytesIO(attestation_data),
        content_type='application/cbor'
    )
    registration_response = cbor.decode(raw_response.data)
    print('registration response:', registration_response)

    # After verification, RP stores attested credential data associated with
    # user account for later authentication.
    assert registration_response == {'status': 'OK'}


def test_example_server_authentication(client):  # pylint: disable=redefined-outer-name
    """Authentication example"""

    # Already registered credential is typicaly part of fixture test-case code.
    #
    # NOTE: the storage of the credential data on the RP side is not in scope
    # of Webauthn spec. Yubico example server uses module scoped variable.
    device = SoftWebauthnDevice()
    device.cred_init('localhost', b'randomhandle')
    example_server.credentials = [device.cred_as_attested()]

    # Browser starts authentication by requesting
    # publicKeyCredentialRequestOptions (pkcro) from the RP.
    pkcro = cbor.decode(client.post('/api/authenticate/begin').data)
    print('publicKeyCredentialRequestOptions: ', pkcro)

    # publicKeyCredentialRequestOptions object is passed to the authenticator,
    # which performs requester user verification and return credential object
    # (assertion).
    assertion = device.get(pkcro, 'https://localhost')
    print('credential assertion: ', assertion)

    # Browser conveys assertion data to the RP for authentication.
    assertion_data = cbor.encode({
        'credentialId': assertion['rawId'],
        'clientDataJSON': assertion['response']['clientDataJSON'],
        'authenticatorData': assertion['response']['authenticatorData'],
        'signature': assertion['response']['signature'],
        'userHandle': assertion['response']['userHandle']
    })
    raw_response = client.post(
        '/api/authenticate/complete',
        input_stream=BytesIO(assertion_data),
        content_type='application/cbor'
    )
    authentication_response = cbor.decode(raw_response.data)
    print('authentication response:', authentication_response)

    # RP will verify assertion and on success proceeds with user logon.
    assert authentication_response == {'status': 'OK'}
