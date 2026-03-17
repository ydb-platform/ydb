import sys

import pytest

if sys.version_info < (3, 11):
    pytest.skip(reason="required python 3.11+", allow_module_level=True)


def simple_response(code, message):
    async def command(connection, rest):
        connection.response(code, message)
        return True

    return command


AUTH_RESPONSE = simple_response("234", ":P")
PBSZ_RESPONSE = simple_response("200", ":P")
PROT_RESPONSE = simple_response("200", ":P")
OK_RESPONSE = simple_response("200", ":P")


@pytest.mark.asyncio
async def test_upgrade_to_tls(mocker, pair_factory):
    ssl_context = object()
    create_default_context = mocker.patch("aioftp.client.ssl.create_default_context", return_value=ssl_context)

    async with pair_factory(logged=False, do_quit=False) as pair:
        pair.server.commands_mapping["auth"] = AUTH_RESPONSE
        pair.server.commands_mapping["pbsz"] = PBSZ_RESPONSE
        pair.server.commands_mapping["prot"] = PROT_RESPONSE

        start_tls = mocker.patch.object(pair.client.stream, "start_tls")
        command_spy = mocker.spy(pair.client, "command")

        await pair.client.upgrade_to_tls()

    create_default_context.assert_called_once_with()
    start_tls.assert_called_once_with(sslcontext=ssl_context, server_hostname=pair.client.server_host)
    assert command_spy.call_count == 3
    command_spy.assert_has_calls(
        [
            mocker.call("AUTH TLS", "234"),
            mocker.call("PBSZ 0", "200"),
            mocker.call("PROT P", "200"),
        ],
    )


@pytest.mark.asyncio
async def test_upgrade_to_tls_custom_ssl_context(mocker, pair_factory):
    ssl_context = object()
    create_default_context = mocker.patch("aioftp.client.ssl.create_default_context")

    async with pair_factory(logged=False, do_quit=False) as pair:
        pair.server.commands_mapping["auth"] = AUTH_RESPONSE
        pair.server.commands_mapping["pbsz"] = PBSZ_RESPONSE
        pair.server.commands_mapping["prot"] = PROT_RESPONSE

        start_tls = mocker.patch.object(pair.client.stream, "start_tls")
        command_spy = mocker.spy(pair.client, "command")

        await pair.client.upgrade_to_tls(sslcontext=ssl_context)

    create_default_context.assert_not_called()
    start_tls.assert_called_once_with(sslcontext=ssl_context, server_hostname=pair.client.server_host)
    assert command_spy.call_count == 3
    command_spy.assert_has_calls(
        [
            mocker.call("AUTH TLS", "234"),
            mocker.call("PBSZ 0", "200"),
            mocker.call("PROT P", "200"),
        ],
    )


@pytest.mark.asyncio
async def test_upgrade_to_tls_when_logged_in(mocker, pair_factory):
    mocker.patch("aioftp.client.ssl.create_default_context")

    async with pair_factory(logged=False, do_quit=False) as pair:
        pair.server.commands_mapping["auth"] = AUTH_RESPONSE
        pair.server.commands_mapping["pbsz"] = OK_RESPONSE
        pair.server.commands_mapping["prot"] = OK_RESPONSE

        mocker.patch.object(pair.client.stream, "start_tls")
        command_spy = mocker.spy(pair.client, "command")

        await pair.client.upgrade_to_tls()

    command_spy.assert_has_calls(
        [
            mocker.call("AUTH TLS", "234"),
            mocker.call("PBSZ 0", "200"),
            mocker.call("PROT P", "200"),
        ],
    )
