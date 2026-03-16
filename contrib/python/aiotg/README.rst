aiotg
=====

Asynchronous Python API for building Telegram bots, featuring:

- Easy and declarative API
- Hassle-free setup - no need for SSL certificates or static IP
- Built-in support for analytics via chatbase.com
- Automatic handling of Telegram API throttling or timeouts

Install it with pip:

.. code:: sh

    pip install aiotg

Then you can create a new bot in few lines:

.. code:: python

    from aiotg import Bot, Chat

    bot = Bot(api_token="...")

    @bot.command(r"/echo (.+)")
    def echo(chat: Chat, match):
        return chat.reply(match.group(1))

    bot.run()

Now run it with a proper API\_TOKEN and it should reply to /echo commands.

.. note:: Type annotations are not required but will help your editor/IDE to provide code completion.

The example above looks like a normal synchronous code but it actually returns a coroutine.
If you want to make an external request (and that's what bots usually do) just use aiohttp and async/await syntax:

.. code:: python

    import aiohttp
    from aiotg import Bot, Chat

    bot = Bot(api_token="...")

    @bot.command("bitcoin")
    async def bitcoin(chat: Chat, match):
        url = "https://apiv2.bitcoinaverage.com/indices/global/ticker/BTCUSD"
        async with aiohttp.ClientSession() as session:
            response = await session.get(url)
            info = await response.json()
            await chat.send_text(str(info["averages"]["day"]))

    bot.run()


But what if you just want to write a quick integration and don't need to provide a conversational interface? We've got you covered!
You can send messages (or any other media) by constructing a Chat object with user_id or channel name. We even saved you some extra keystrokes by providing handy Channel constructors:

.. code:: python

    ...
    channel = bot.channel("@yourchannel")
    private = bot.private("1111111")

    async def greeter():
        await channel.send_text("Hello from channel!")
        await private.send_text("Why not greet personally?")
    ...


Examples
---------------

- `Async IO <https://github.com/szastupov/aiotg/blob/master/examples/async.py>`__
- `Send image <https://github.com/szastupov/aiotg/blob/master/examples/getimage.py>`__
- `Post to channel <https://github.com/szastupov/aiotg/blob/master/examples/post_to_channel.py>`__
- `Webhooks mode <https://github.com/szastupov/aiotg/blob/master/examples/webhook.py>`__
- `Sender id <https://github.com/szastupov/aiotg/blob/master/examples/whoami.py>`__

For a real world example, take a look at
`WhatisBot <https://github.com/szastupov/whatisbot/blob/master/main.py>`__ or `Music Catalog Bot <https://github.com/szastupov/musicbot>`__.

For more information on how to use the project, see the project's `documentation <http://szastupov.github.io/aiotg/index.html>`__.
