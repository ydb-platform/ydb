import pytest

import aioftp


@pytest.mark.asyncio
async def test_permission_denied(
    pair_factory,
    Server,
    expect_codes_in_exception,
):
    s = Server(
        [
            aioftp.User(permissions=[aioftp.Permission(writable=False)]),
        ],
    )
    async with pair_factory(None, s) as pair:
        with expect_codes_in_exception("550"):
            await pair.client.make_directory("foo")


@pytest.mark.asyncio
async def test_permission_overriden(pair_factory, Server):
    s = Server(
        [
            aioftp.User(
                permissions=[
                    aioftp.Permission("/", writable=False),
                    aioftp.Permission("/foo"),
                ],
            ),
        ],
    )
    async with pair_factory(None, s) as pair:
        await pair.client.make_directory("foo")
        await pair.client.remove_directory("foo")
