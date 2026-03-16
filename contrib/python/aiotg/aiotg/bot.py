import os
import re
import logging
import uuid
import asyncio
from urllib.parse import urlparse

import aiohttp
from aiohttp import web
import json

from .chat import Chat, Sender
from .reloader import run_with_reloader

__author__ = "Stepan Zastupov"
__copyright__ = "Copyright 2015-2017 Stepan Zastupov"
__license__ = "MIT"

API_URL = "https://api.telegram.org"
API_TIMEOUT = 60
RETRY_TIMEOUT = 30
RETRY_CODES = [429, 500, 502, 503, 504]

# Message types to be handled by bot.handle(...)
MESSAGE_TYPES = [
    "location",
    "photo",
    "document",
    "audio",
    "voice",
    "sticker",
    "contact",
    "venue",
    "video",
    "game",
    "delete_chat_photo",
    "new_chat_photo",
    "delete_chat_photo",
    "new_chat_member",
    "left_chat_member",
    "new_chat_title",
    "group_chat_created",
    "successful_payment",
]

# Update types for
MESSAGE_UPDATES = [
    "message",
    "edited_message",
    "channel_post",
    "edited_channel_post",
    "successful_payment",
]

logger = logging.getLogger("aiotg")


class Bot:
    """Telegram bot framework designed for asyncio

    :param str api_token: Telegram bot token, ask @BotFather for this
    :param int api_timeout: Timeout for long polling
    :param str name: Bot name
    :param callable json_serialize: JSON serializer function. (json.dumps by default)
    :param callable json_deserialize: JSON deserializer function. (json.loads by default)
    :param bool default_in_groups: Enables default callback in groups
    :param str proxy: Proxy URL to use for HTTP requests
    :param connector: Custom aiohttp connector
    """

    _running = False
    _offset = 0

    def __init__(
        self,
        api_token,
        api_timeout=API_TIMEOUT,
        name=None,
        json_serialize=json.dumps,
        json_deserialize=json.loads,
        default_in_groups=False,
        connector=None,
    ):
        self.api_token = api_token
        self.api_timeout = api_timeout
        self.name = name
        self.json_serialize = json_serialize
        self.json_deserialize = json_deserialize
        self.default_in_groups = default_in_groups
        self._session = None
        self._cleanups = []
        self._webhook_uuid = None
        self._connector = connector

        def no_handle(mt):
            return lambda chat, msg: logger.debug("no handle for %s", mt)

        # Init default handlers and callbacks
        self._handlers = {mt: no_handle(mt) for mt in MESSAGE_TYPES}
        self._commands = []
        self._callbacks = []
        self._inlines = []
        self._chosen_inline_result_callbacks = []
        self._checkouts = []
        self._default = lambda chat, message: None
        self._default_callback = lambda chat, cq: None
        self._default_inline = lambda iq: None
        self._default_chosen_inline_result_callback = lambda res: None

    async def loop(self):
        """
        Return bot's main loop as coroutine. Use with asyncio.

        :Example:

        >>> loop = asyncio.get_event_loop()
        >>> loop.run_until_complete(bot.loop())

        or

        >>> loop = asyncio.get_event_loop()
        >>> loop.create_task(bot.loop())
        """
        self._running = True
        while self._running:
            updates = await self.api_call(
                "getUpdates", offset=self._offset + 1, timeout=self.api_timeout
            )
            self._process_updates(updates)

    def run(self, debug=False, reload=None):
        """
        Convenience method for running bots in getUpdates mode

        :param bool debug: Enable debug logging and automatic reloading
        :param bool reload: Automatically reload bot on code change
        :Example:

        >>> if __name__ == '__main__':
        >>>     bot.run()

        """
        loop = asyncio.get_event_loop()

        logging.basicConfig(level=logging.DEBUG if debug else logging.INFO)

        if reload is None:
            reload = debug

        bot_loop = asyncio.ensure_future(self.loop())

        try:
            if reload:
                loop.run_until_complete(run_with_reloader(loop, bot_loop, self.stop))

            else:
                loop.run_until_complete(bot_loop)

        # User cancels
        except KeyboardInterrupt:
            logger.debug("User cancelled")
            bot_loop.cancel()
            self.stop()

        # Stop loop
        finally:
            for cleanup_action in self._cleanups:
                cleanup_action()
            loop.run_until_complete(self.session.close())

            logger.debug("Closing loop")
            loop.stop()
            loop.close()

    def run_webhook(self, webhook_url, **options):
        """
        Convenience method for running bots in webhook mode

        :Example:

        >>> if __name__ == '__main__':
        >>>     bot.run_webhook(webhook_url="https://yourserver.com/webhooktoken")

        Additional documentation on https://core.telegram.org/bots/api#setwebhook
        """
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.set_webhook(webhook_url, **options))
        if webhook_url:
            url = urlparse(webhook_url)
            app = self.create_webhook_app(url.path, loop)
            host = os.environ.get("HOST", "0.0.0.0")
            port = int(os.environ.get("PORT", 0)) or url.port

            app.on_cleanup.append(lambda _: self.session.close())
            for cleanup_action in self._cleanups:
                app.on_cleanup.append(cleanup_action)

            web.run_app(app, host=host, port=port)
        else:
            loop.run_until_complete(self.session.close())

    def stop_webhook(self):
        """
        Use to switch from Webhook to getUpdates mode
        """
        self.run_webhook(webhook_url="")

    def add_command(self, regexp, fn):
        """
        Manually register regexp based command
        """
        self._commands.append((regexp, fn))

    def command(self, regexp):
        """
        Register a new command

        :param str regexp: Regular expression matching the command to register

        :Example:

        >>> @bot.command(r"/echo (.+)")
        >>> def echo(chat, match):
        >>>     return chat.reply(match.group(1))
        """

        def decorator(fn):
            self.add_command(regexp, fn)
            return fn

        return decorator

    def default(self, callback):
        """
        Set callback for default command that is called on unrecognized
        commands for 1-to-1 chats
        If default_in_groups option is True, callback is called in groups too

        :Example:

        >>> @bot.default
        >>> def echo(chat, message):
        >>>     return chat.reply(message["text"])
        """
        self._default = callback
        return callback

    def add_inline(self, regexp, fn):
        """
        Manually register regexp based callback
        """
        self._inlines.append((regexp, fn))

    def inline(self, callback):
        """
        Set callback for inline queries

        :Example:

        >>> @bot.inline
        >>> def echo(iq):
        >>>     return iq.answer([
        >>>         {"type": "text", "title": "test", "id": "0"}
        >>>     ])

        >>> @bot.inline(r"myinline-(.+)")
        >>> def echo(chat, iq, match):
        >>>     return iq.answer([
        >>>         {"type": "text", "title": "test", "id": "0"}
        >>>     ])
        """
        if callable(callback):
            self._default_inline = callback
            return callback
        elif isinstance(callback, str):

            def decorator(fn):
                self.add_inline(callback, fn)
                return fn

            return decorator
        else:
            raise TypeError("str expected {} given".format(type(callback)))

    def add_chosen_inline_result_callback(self, regexp, fn):
        """
        Manually register regexp based callback for the ``chosen_inline_result`` updates
        """
        self._chosen_inline_result_callbacks.append((regexp, fn))

    def chosen_inline_result_callback(self, callback):
        """
        Set callback for ``chosen_inline_result`` updates

        :Example:

        >>> @bot.chosen_inline_result_callback
        >>> def inc_metric(iq):
        >>>     metrics[iq.result_id].inc()

        >>> @bot.chosen_inline_result_callback(r"myinline-(.+)")
        >>> def inc_metric(chat, iq, match):
        >>>     metrics[iq.result_id].inc()
        """
        if callable(callback):
            self._default_chosen_inline_result_callback = callback
            return callback
        elif isinstance(callback, str):

            def decorator(fn):
                self.add_chosen_inline_result_callback(callback, fn)
                return fn

            return decorator
        else:
            raise TypeError("str expected {} given".format(type(callback)))

    def add_callback(self, regexp, fn):
        """
        Manually register regexp based callback
        """
        self._callbacks.append((regexp, fn))

    def callback(self, callback):
        """
        Set callback for callback queries

        :Example:

        >>> @bot.callback
        >>> def echo(chat, cq):
        >>>     return cq.answer()

        >>> @bot.callback(r"buttonclick-(.+)")
        >>> def echo(chat, cq, match):
        >>>     return chat.reply(match.group(1))
        """
        if callable(callback):
            self._default_callback = callback
            return callback
        elif isinstance(callback, str):

            def decorator(fn):
                self.add_callback(callback, fn)
                return fn

            return decorator
        else:
            raise TypeError("str expected {} given".format(type(callback)))

    def add_checkout(self, regexp, fn):
        """
        Manually register regexp based checkout handler
        """
        self._checkouts.append((regexp, fn))

    def checkout(self, callback):
        if callable(callback):
            self._default_checkout = callback
        elif isinstance(callback, str):

            def decorator(fn):
                self.add_checkout(callback, fn)
                return fn

            return decorator
        else:
            raise TypeError("str expected {} given".format(type(callback)))

    def handle(self, msg_type):
        """
        Set handler for specific message type

        :Example:

        >>> @bot.handle("audio")
        >>> def handle(chat, audio):
        >>>     pass
        """

        def wrap(callback):
            self._handlers[msg_type] = callback
            return callback

        return wrap

    def channel(self, channel_name):
        """
        Construct a Chat object used to post to channel

        :param str channel_name: Channel name
        """
        return Chat(self, channel_name, "channel")

    def private(self, user_id):
        """
        Construct a Chat object used to post direct messages

        :param str user_id: User id
        """
        return Chat(self, user_id, "private")

    def group(self, group_id):
        """
        Construct a Chat object used to post group messages

        :param str group_id: Group chat id
        """
        return Chat(self, group_id, "group")

    def api_call(self, method, **params):
        """
        Call Telegram API.

        See https://core.telegram.org/bots/api for reference.

        :param str method: Telegram API method
        :param params: Arguments for the method call
        """
        coro = self._api_call(method, **params)
        # Explicitly ensure that API call is executed
        return asyncio.ensure_future(coro)

    async def _api_call(self, method, **params):
        url = "{0}/bot{1}/{2}".format(API_URL, self.api_token, method)
        logger.debug("api_call %s, %s", method, params)

        response = await self.session.post(url, data=params)

        if response.status == 200:
            return await response.json(loads=self.json_deserialize)
        elif response.status in RETRY_CODES:
            logger.info(
                "Server returned %d, retrying in %d sec.",
                response.status,
                RETRY_TIMEOUT,
            )
            await response.release()
            await asyncio.sleep(RETRY_TIMEOUT)
            return await self.api_call(method, **params)
        else:
            if response.headers["content-type"] == "application/json":
                json_resp = await response.json(loads=self.json_deserialize)
                err_msg = json_resp["description"]
            else:
                err_msg = await response.read()
            logger.error(err_msg)
            raise BotApiError(err_msg, response=response)

    async def get_me(self):
        """
        Returns basic information about the bot
        (see https://core.telegram.org/bots/api#getme)
        """
        json_result = await self.api_call("getMe")
        return json_result["result"]

    async def leave_chat(self, chat_id):
        """
        Use this method for your bot to leave a group, supergroup or channel.
        Returns True on success.

        :param int chat_id: Unique identifier for the target chat \
            or username of the target supergroup or channel \
            (in the format @channelusername)
        """
        json_result = await self.api_call("leaveChat", chat_id=chat_id)
        return json_result["result"]

    def send_message(self, chat_id, text, **options):
        """
        Send a text message to chat

        :param int chat_id: ID of the chat to send the message to
        :param str text: Text to send
        :param options: Additional sendMessage options
            (see https://core.telegram.org/bots/api#sendmessage)
        """
        return self.api_call("sendMessage", chat_id=chat_id, text=text, **options)

    def edit_message_text(self, chat_id, message_id, text, **options):
        """
        Edit a text message in a chat

        :param int chat_id: ID of the chat the message to edit is in
        :param int message_id: ID of the message to edit
        :param str text: Text to edit the message to
        :param options: Additional API options
        """
        return self.api_call(
            "editMessageText",
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            **options
        )

    def edit_message_reply_markup(self, chat_id, message_id, reply_markup, **options):
        """
        Edit a reply markup of message in a chat

        :param int chat_id: ID of the chat the message to edit is in
        :param int message_id: ID of the message to edit
        :param str reply_markup: New inline keyboard markup for the message
        :param options: Additional API options
        """
        return self.api_call(
            "editMessageReplyMarkup",
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=reply_markup,
            **options
        )

    async def get_file(self, file_id):
        """
        Get basic information about a file and prepare it for downloading.

        :param int file_id: File identifier to get information about
        :return: File object (see https://core.telegram.org/bots/api#file)
        """
        json = await self.api_call("getFile", file_id=file_id)
        return json["result"]

    def download_file(self, file_path, range=None):
        """
        Download a file from Telegram servers
        """
        headers = {"range": range} if range else None
        url = "{0}/file/bot{1}/{2}".format(API_URL, self.api_token, file_path)
        return self.session.get(url, headers=headers)

    def get_user_profile_photos(self, user_id, **options):
        """
        Get a list of profile pictures for a user

        :param int user_id: Unique identifier of the target user
        :param options: Additional getUserProfilePhotos options (see
            https://core.telegram.org/bots/api#getuserprofilephotos)
        """
        return self.api_call("getUserProfilePhotos", user_id=str(user_id), **options)

    def track(self, message, name="Message"):
        # TODO allow configuring custom tracking
        pass

    def stop(self):
        self._running = False

    async def webhook_handle(self, request):
        """
        aiohttp.web handle for processing web hooks

        :Example:

        >>> from aiohttp import web
        >>> app = web.Application()
        >>> app.router.add_route('/webhook')
        """

        if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != self._webhook_uuid:
            logger.warning("Probably, a malicious request! " + request)
            return web.Response(status=403)

        update = await request.json(loads=self.json_deserialize)
        self._process_update(update)
        return web.Response()

    def create_webhook_app(self, path, loop=None):
        """
        Shorthand for creating aiohttp.web.Application with registered webhook hanlde
        """
        app = web.Application(loop=loop)
        app.router.add_route("POST", path, self.webhook_handle)
        return app

    def set_webhook(self, webhook_url, **options):
        """
        Register you webhook url for Telegram service.

        A newly generated UUID will be used as a secret_token parameter
        if it's not specified explicitly
        """
        if "secret_token" not in options:
            options["secret_token"] = str(uuid.uuid4())
        self._webhook_uuid = options["secret_token"]
        return self.api_call("setWebhook", url=webhook_url, **options)

    def delete_webhook(self):
        """
        Tell Telegram to switch back to getUpdates mode
        """
        return self.api_call("deleteWebhook")

    def on_cleanup(self, action):
        """
        You can set an action that will be executed before closing the loop

        :param action: must be a simple callable without any arguments

        :Example:

        >>> bot.on_cleanup(lambda: [t.cancel() for t in tasks])
        """
        self._cleanups.append(action)

    @property
    def session(self):
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession(
                json_serialize=self.json_serialize, connector=self._connector
            )
        return self._session

    def _process_message(self, message):
        chat = Chat.from_message(self, message)

        for mt, func in self._handlers.items():
            if mt in message:
                self.track(message, mt)
                return func(chat, message[mt])

        if "text" not in message:
            return

        for patterns, handler in self._commands:
            m = re.search(patterns, message["text"], re.I)
            if m:
                self.track(message, handler.__name__)
                return handler(chat, m)

        # No match, run default if it's a 1to1 chat
        # However, if default_in_groups option is active, run default in any chat (not only 1to1)
        if not chat.is_group() or self.default_in_groups:
            self.track(message, "default")
            return self._default(chat, message)

    def _process_inline_query(self, query):
        iq = InlineQuery(self, query)

        for patterns, handler in self._inlines:
            match = re.search(patterns, query["query"], re.I)
            if match:
                return handler(iq, match)
        return self._default_inline(iq)

    def _process_chosen_inline_result(self, result):
        cir = ChosenInlineResult(self, result)
        for patterns, handler in self._chosen_inline_result_callbacks:
            match = re.search(patterns, result["query"], re.I)
            if match:
                return handler(cir, match)
        return self._default_chosen_inline_result_callback(cir)

    def _process_callback_query(self, query):
        chat = Chat.from_message(self, query["message"]) if "message" in query else None
        cq = CallbackQuery(self, query)
        for patterns, handler in self._callbacks:
            match = re.search(patterns, cq.data, re.I)
            if match:
                return handler(chat, cq, match)

        if chat and not chat.is_group() or self.default_in_groups:
            return self._default_callback(chat, cq)

    def _process_pre_checkout_query(self, query):
        pcq = PreCheckoutQuery(self, query)

        for patterns, handler in self._checkouts:
            match = re.search(patterns, pcq.invoice_payload, re.I)
            if match:
                return handler(pcq, match)
        return self._default_checkout(pcq)

    def _process_updates(self, updates):
        if not updates["ok"]:
            logger.error("getUpdates error: %s", updates.get("description"))
            return

        for update in updates["result"]:
            self._process_update(update)

    def _process_update(self, update):
        logger.debug("update %s", update)

        # Update offset
        self._offset = max(self._offset, update["update_id"])

        coro = None

        # Determine update type starting with message updates
        for ut in MESSAGE_UPDATES:
            if ut in update:
                coro = self._process_message(update[ut])
                break
        else:
            if "inline_query" in update:
                coro = self._process_inline_query(update["inline_query"])
            elif "callback_query" in update:
                coro = self._process_callback_query(update["callback_query"])
            elif "pre_checkout_query" in update:
                coro = self._process_pre_checkout_query(update["pre_checkout_query"])
            elif "chosen_inline_result" in update:
                coro = self._process_chosen_inline_result(
                    update["chosen_inline_result"]
                )
            else:
                logger.error("don't know how to handle update: %s", update)

        if coro:
            asyncio.ensure_future(coro)


class TgBot(Bot):
    def __init__(self, *args, **kwargs):
        logger.warning("TgBot is depricated, use Bot instead")
        super().__init__(*args, **kwargs)


class InlineQuery:
    """
    Incoming inline query
    See https://core.telegram.org/bots/api#inline-mode for details
    """

    def __init__(self, bot, src):
        self.bot = bot
        self.sender = Sender(src["from"])
        self.query_id = src["id"]
        self.query = src["query"]

    def answer(self, results, **options):
        return self.bot.api_call(
            "answerInlineQuery",
            inline_query_id=self.query_id,
            results=self.bot.json_serialize(results),
            **options
        )


class TgInlineQuery(InlineQuery):
    def __init__(self, *args, **kwargs):
        logger.warning("TgInlineQuery is depricated, use InlineQuery instead")
        super().__init__(*args, **kwargs)


class ChosenInlineResult:
    def __init__(self, bot, src):
        self.bot = bot
        self.sender = Sender(src["from"])
        self.result_id = src["result_id"]
        self.location = src.get("location")
        self.inline_message_id = src.get("inline_messages_id")
        self.query = src["query"]


class CallbackQuery:
    def __init__(self, bot, src):
        self.bot = bot
        self.query_id = src["id"]
        self.data = src["data"]
        self.src = src

    def answer(self, **options):
        return self.bot.api_call(
            "answerCallbackQuery", callback_query_id=self.query_id, **options
        )


class PreCheckoutQuery:
    def __init__(self, bot, src):
        self.bot = bot
        self.sender = Sender(src["from"])
        self.query_id = src["id"]
        self.currency = src["currency"]
        self.total_amount = src["total_amount"]
        self.invoice_payload = src["invoice_payload"]

    def answer(self, error_message=None, **options):
        return self.bot.api_call(
            "answerPreCheckoutQuery",
            pre_checkout_query_id=self.query_id,
            ok=error_message is None,
            error_message=error_message,
            **options
        )


class BotApiError(RuntimeError):
    def __init__(self, *args, response):
        super().__init__(*args)
        self.response = response
