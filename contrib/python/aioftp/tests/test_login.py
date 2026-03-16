import pytest

import aioftp


@pytest.mark.asyncio
async def test_client_list_override(pair_factory, expect_codes_in_exception):
    async with pair_factory(logged=False, do_quit=False) as pair:
        with expect_codes_in_exception("503"):
            await pair.client.get_current_directory()


@pytest.mark.asyncio
async def test_anonymous_login(pair_factory):
    async with pair_factory():
        pass


@pytest.mark.asyncio
async def test_login_with_login_data(pair_factory):
    async with pair_factory(logged=False) as pair:
        await pair.client.login("foo", "bar")


@pytest.mark.asyncio
async def test_login_with_login_and_no_password(pair_factory, Server):
    s = Server([aioftp.User("foo")])
    async with pair_factory(None, s, logged=False) as pair:
        await pair.client.login("foo")


@pytest.mark.asyncio
async def test_login_with_login_and_password(pair_factory, Server):
    s = Server([aioftp.User("foo", "bar")])
    async with pair_factory(None, s, logged=False) as pair:
        await pair.client.login("foo", "bar")


@pytest.mark.asyncio
async def test_login_with_login_and_password_no_such_user(
    pair_factory,
    Server,
    expect_codes_in_exception,
):
    s = Server([aioftp.User("foo", "bar")])
    async with pair_factory(None, s, logged=False) as pair:
        with expect_codes_in_exception("530"):
            await pair.client.login("fo", "bar")


@pytest.mark.asyncio
async def test_login_with_login_and_password_bad_password(
    pair_factory,
    Server,
    expect_codes_in_exception,
):
    s = Server([aioftp.User("foo", "bar")])
    async with pair_factory(None, s, logged=False) as pair:
        with expect_codes_in_exception("530"):
            await pair.client.login("foo", "baz")


@pytest.mark.asyncio
async def test_pass_after_login(
    pair_factory,
    Server,
    expect_codes_in_exception,
):
    s = Server([aioftp.User("foo", "bar")])
    async with pair_factory(None, s, logged=False) as pair:
        await pair.client.login("foo", "bar")
        with expect_codes_in_exception("503"):
            await pair.client.command("PASS baz", ("230", "33x"))
