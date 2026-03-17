from logging import getLogger

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest
from aiogram.types import (
    CallbackQuery,
    ContentType,
    FSInputFile,
    InputFile,
    InputMediaAnimation,
    InputMediaAudio,
    InputMediaDocument,
    InputMediaPhoto,
    InputMediaVideo,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    URLInputFile,
)

from aiogram_dialog.api.entities import (
    MediaAttachment,
    MediaId,
    NewMessage,
    OldMessage,
    ShowMode,
)
from aiogram_dialog.api.protocols import (
    MessageManagerProtocol,
    MessageNotModified,
)
from aiogram_dialog.utils import get_media_id

logger = getLogger(__name__)

SEND_METHODS = {
    ContentType.ANIMATION: "send_animation",
    ContentType.AUDIO: "send_audio",
    ContentType.DOCUMENT: "send_document",
    ContentType.PHOTO: "send_photo",
    ContentType.VIDEO: "send_video",
    ContentType.VIDEO_NOTE: "send_video_note",
    ContentType.DICE: "send_dice",
    ContentType.STICKER: "send_sticker",
    ContentType.VOICE: "send_voice",
}

INPUT_MEDIA_TYPES = {
    ContentType.ANIMATION: InputMediaAnimation,
    ContentType.DOCUMENT: InputMediaDocument,
    ContentType.AUDIO: InputMediaAudio,
    ContentType.PHOTO: InputMediaPhoto,
    ContentType.VIDEO: InputMediaVideo,
}

_INVALID_QUERY_ID_MSG = (
    "query is too old and response timeout expired or query id is invalid"
)


def _combine(sent_message: NewMessage, message_result: Message) -> OldMessage:
    media_id = get_media_id(message_result)
    return OldMessage(
        message_id=message_result.message_id,
        chat=message_result.chat,
        has_protected_content=message_result.has_protected_content,
        has_reply_keyboard=isinstance(
            sent_message.reply_markup, ReplyKeyboardMarkup,
        ),
        text=message_result.text,
        media_uniq_id=(media_id.file_unique_id if media_id else None),
        media_id=(media_id.file_id if media_id else None),
        business_connection_id=message_result.business_connection_id,
        content_type=message_result.content_type,
    )


class MessageManager(MessageManagerProtocol):
    async def answer_callback(
            self, bot: Bot, callback_query: CallbackQuery,
    ) -> None:
        try:
            await bot.answer_callback_query(
                callback_query_id=callback_query.id,
            )
        except TelegramAPIError as e:
            if _INVALID_QUERY_ID_MSG in e.message.lower():
                logger.warning("Cannot answer callback: %s", e)
            else:
                raise

    async def get_media_source(
            self, media: MediaAttachment, bot: Bot,
    ) -> InputFile | str:
        if media.file_id:
            return media.file_id.file_id
        if media.url:
            if media.use_pipe:
                return URLInputFile(media.url, bot=bot)
            return media.url
        else:
            return FSInputFile(media.path)

    def had_media(self, old_message: OldMessage) -> bool:
        return old_message.media_id is not None

    def need_media(self, new_message: NewMessage) -> bool:
        return bool(new_message.media)

    def had_reply_keyboard(self, old_message: OldMessage | None) -> bool:
        if not old_message:
            return False
        return old_message.has_reply_keyboard

    def need_reply_keyboard(self, new_message: NewMessage | None) -> bool:
        if not new_message:
            return False
        return isinstance(new_message.reply_markup, ReplyKeyboardMarkup)

    def had_voice(self, old_message: OldMessage) -> bool:
        return old_message.content_type == ContentType.VOICE

    def need_voice(self, new_message: NewMessage) -> bool:
        return (
            new_message.media is not None
            and new_message.media.type == ContentType.VOICE
        )

    def _message_changed(
            self, new_message: NewMessage, old_message: OldMessage,
    ) -> bool:
        if (
            (new_message.text != old_message.text) or
            # we cannot actually compare reply keyboards
            (new_message.reply_markup or old_message.has_reply_keyboard) or
            # we do not know if link preview changed
            new_message.link_preview_options or
            (
                bool(new_message.protect_content)
                != bool(old_message.has_protected_content)
            )
        ):
            return True

        if self.had_media(old_message) != self.need_media(new_message):
            return True
        if not self.need_media(new_message):
            return False
        old_media_id = MediaId(old_message.media_id, old_message.media_uniq_id)
        if new_message.media.file_id != old_media_id:
            return True

        return False

    def _can_edit(self, new_message: NewMessage,
                  old_message: OldMessage) -> bool:
        # we cannot edit message if media removed
        if self.had_media(old_message) and not self.need_media(new_message):
            return False
        # we cannot edit a message if there was voice
        if self.had_voice(old_message) or self.need_voice(new_message):
            return False
        return not (
            self.had_reply_keyboard(old_message)
            or self.need_reply_keyboard(new_message)
        )

    async def show_message(
            self, bot: Bot, new_message: NewMessage,
            old_message: OldMessage | None,
    ) -> OldMessage:
        if new_message.show_mode is ShowMode.NO_UPDATE:
            logger.debug("ShowMode is NO_UPDATE, skipping show")
            raise MessageNotModified("ShowMode is NO_UPDATE")
        if old_message and new_message.show_mode is ShowMode.DELETE_AND_SEND:
            logger.debug(
                "Delete and send new message, because: mode=%s",
                new_message.show_mode,
            )
            # optimize order not to blink
            if self.need_reply_keyboard(new_message):
                sent_message = await self.send_message(bot, new_message)
                await self.remove_message_safe(bot, old_message, new_message)
            else:
                await self.remove_message_safe(bot, old_message, new_message)
                sent_message = await self.send_message(bot, new_message)
            return _combine(new_message, sent_message)
        if not old_message or new_message.show_mode is ShowMode.SEND:
            logger.debug(
                "Send new message, because: mode=%s, has old_message=%s",
                new_message.show_mode,
                bool(old_message),
            )
            await self._remove_kbd(bot, old_message, new_message)
            return _combine(
                new_message,
                await self.send_message(bot, new_message),
            )

        if not self._message_changed(new_message, old_message):
            logger.debug("Message dit not change")
            # nothing changed: text, keyboard or media
            return old_message

        if not self._can_edit(new_message, old_message):
            await self.remove_message_safe(bot, old_message, new_message)
            return _combine(
                new_message,
                await self.send_message(bot, new_message),
            )
        return _combine(
            new_message,
            await self.edit_message_safe(bot, new_message, old_message),
        )

    # Clear
    async def remove_kbd(
            self,
            bot: Bot,
            show_mode: ShowMode,
            old_message: OldMessage | None,
    ) -> Message | None:
        if show_mode is ShowMode.NO_UPDATE:
            return None
        if show_mode is ShowMode.DELETE_AND_SEND and old_message:
            return await self.remove_message_safe(bot, old_message, None)
        return await self._remove_kbd(bot, old_message, None)

    async def _remove_kbd(
            self,
            bot: Bot,
            old_message: OldMessage | None,
            new_message: NewMessage | None,
    ) -> Message | None:
        if self.had_reply_keyboard(old_message):
            if not self.need_reply_keyboard(new_message):
                return await self.remove_reply_kbd(bot, old_message)
            return None
        else:
            return await self.remove_inline_kbd(bot, old_message)

    async def remove_inline_kbd(
            self, bot: Bot, old_message: OldMessage | None,
    ) -> Message | None:
        if not old_message:
            return None
        logger.debug("remove_inline_kbd in %s", old_message.chat)
        try:
            return await bot.edit_message_reply_markup(
                message_id=old_message.message_id,
                chat_id=old_message.chat.id,
                business_connection_id=old_message.business_connection_id,
            )
        except TelegramBadRequest as err:
            if "message is not modified" in err.message:
                pass  # nothing to remove
            elif "message can't be edited" in err.message:
                pass
            elif "message to edit not found" in err.message:
                pass
            elif "MESSAGE_ID_INVALID" in err.message:
                pass
            else:
                raise err

    async def remove_reply_kbd(
            self, bot: Bot, old_message: OldMessage | None,
    ) -> Message | None:
        if not old_message:
            return None
        logger.debug("remove_reply_kbd in %s", old_message.chat)
        return await self.send_text(
            bot=bot,
            new_message=NewMessage(
                chat=old_message.chat,
                text="...",
                reply_markup=ReplyKeyboardRemove(),
                business_connection_id=old_message.business_connection_id,
            ),
        )

    async def remove_message_safe(
            self,
            bot: Bot,
            old_message: OldMessage,
            new_message: NewMessage | None,
    ) -> None:
        if old_message.business_connection_id:
            await self._remove_kbd(bot, old_message, new_message)
            return
        try:
            await bot.delete_message(
                chat_id=old_message.chat.id,
                message_id=old_message.message_id,
            )
        except TelegramBadRequest as err:
            if "message to delete not found" in err.message:
                pass
            elif "message can't be deleted" in err.message:
                await self._remove_kbd(bot, old_message, new_message)
            else:
                raise

    # Edit
    async def edit_message_safe(
            self, bot: Bot, new_message: NewMessage, old_message: OldMessage,
    ) -> Message:
        try:
            return await self.edit_message(bot, new_message, old_message)
        except TelegramBadRequest as err:
            if "message is not modified" in err.message:
                raise MessageNotModified from err
            if (
                    "message can't be edited" in err.message or
                    "message to edit not found" in err.message
            ):
                return await self.send_message(bot, new_message)
            else:
                raise

    async def edit_message(
            self, bot: Bot, new_message: NewMessage, old_message: OldMessage,
    ) -> Message:
        if bool(old_message.has_protected_content) != \
           bool(new_message.protect_content):
            await self.remove_message_safe(bot, old_message, new_message)
            return await self.send_message(bot, new_message)

        if new_message.media:
            if (
                old_message.media_id is not None and
                new_message.media.file_id == old_message.media_id
            ):
                return await self.edit_caption(bot, new_message, old_message)
            return await self.edit_media(bot, new_message, old_message)
        else:
            return await self.edit_text(bot, new_message, old_message)

    async def edit_caption(
            self, bot: Bot, new_message: NewMessage, old_message: OldMessage,
    ) -> Message:
        logger.debug("edit_caption to %s", new_message.chat)
        return await bot.edit_message_caption(
            message_id=old_message.message_id,
            chat_id=old_message.chat.id,
            business_connection_id=new_message.business_connection_id,
            caption=new_message.text,
            reply_markup=new_message.reply_markup,
            parse_mode=new_message.parse_mode,
        )

    async def edit_text(
            self, bot: Bot, new_message: NewMessage, old_message: OldMessage,
    ) -> Message:
        logger.debug("edit_text to %s", new_message.chat)
        return await bot.edit_message_text(
            message_id=old_message.message_id,
            chat_id=old_message.chat.id,
            business_connection_id=new_message.business_connection_id,
            text=new_message.text,
            reply_markup=new_message.reply_markup,
            parse_mode=new_message.parse_mode,
            link_preview_options=new_message.link_preview_options,
        )

    async def edit_media(
            self, bot: Bot, new_message: NewMessage, old_message: OldMessage,
    ) -> Message:
        logger.debug(
            "edit_media to %s, media_id: %s",
            new_message.chat,
            new_message.media.file_id,
        )
        media = INPUT_MEDIA_TYPES[new_message.media.type](
            caption=new_message.text,
            reply_markup=new_message.reply_markup,
            parse_mode=new_message.parse_mode,
            media=await self.get_media_source(new_message.media, bot),
            **new_message.media.kwargs,
        )
        return await bot.edit_message_media(
            message_id=old_message.message_id,
            chat_id=old_message.chat.id,
            media=media,
            reply_markup=new_message.reply_markup,
        )

    # Send
    async def send_message(self, bot: Bot, new_message: NewMessage) -> Message:
        if new_message.media:
            return await self.send_media(bot, new_message)
        else:
            return await self.send_text(bot, new_message)

    async def send_text(self, bot: Bot, new_message: NewMessage) -> Message:
        logger.debug(
            "send_text to chat %s, thread %s, business_id %s",
            new_message.chat.id, new_message.thread_id,
            new_message.business_connection_id,
        )
        return await bot.send_message(
            new_message.chat.id,
            text=new_message.text,
            message_thread_id=new_message.thread_id,
            business_connection_id=new_message.business_connection_id,
            reply_markup=new_message.reply_markup,
            parse_mode=new_message.parse_mode,
            protect_content=new_message.protect_content,
            link_preview_options=new_message.link_preview_options,
        )

    async def send_media(self, bot: Bot, new_message: NewMessage) -> Message:
        logger.debug(
            "send_media to %s, media_id: %s",
            new_message.chat,
            new_message.media.file_id,
        )
        method = getattr(bot, SEND_METHODS[new_message.media.type], None)
        if not method:
            raise ValueError(
                f"ContentType {new_message.media.type} is not supported",
            )
        return await method(
            new_message.chat.id,
            await self.get_media_source(new_message.media, bot),
            message_thread_id=new_message.thread_id,
            business_connection_id=new_message.business_connection_id,
            caption=new_message.text,
            reply_markup=new_message.reply_markup,
            parse_mode=new_message.parse_mode,
            protect_content=new_message.protect_content,
            **new_message.media.kwargs,
        )
