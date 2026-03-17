import random
import string
import time
from dataclasses import dataclass, field

from aiogram.enums import ContentType
from aiogram.fsm.state import State

from aiogram_dialog.api.exceptions import DialogStackOverflow
from .access import AccessSettings
from .context import Context, Data

DEFAULT_STACK_ID = ""
GROUP_STACK_ID = "<->"
_STACK_LIMIT = 100
_ID_SYMS = string.digits + string.ascii_letters


def new_int_id() -> int:
    return int(time.time()) % 100000000 + random.randint(0, 99) * 100000000


def id_to_str(int_id: int) -> str:
    if not int_id:
        return _ID_SYMS[0]
    base = len(_ID_SYMS)
    res = ""
    while int_id:
        int_id, mod = divmod(int_id, base)
        res += _ID_SYMS[mod]
    return res


def new_id():
    return id_to_str(new_int_id())


@dataclass(unsafe_hash=True)
class Stack:
    _id: str = field(compare=True, default_factory=new_id)
    intents: list[str] = field(compare=False, default_factory=list)
    last_message_id: int | None = field(compare=False, default=None)
    last_reply_keyboard: bool = field(compare=False, default=False)
    last_media_id: str | None = field(compare=False, default=None)
    last_media_unique_id: str | None = field(compare=False, default=None)
    last_income_media_group_id: str | None = field(
        compare=False, default=None,
    )
    content_type: ContentType | None = field(compare=False, default=None)
    access_settings: AccessSettings | None = None
    has_protected_content: bool | None = field(compare=False, default=None)

    @property
    def id(self):
        return self._id

    def push(self, state: State, data: Data) -> Context:
        if len(self.intents) >= _STACK_LIMIT:
            raise DialogStackOverflow(
                f"Cannot open more dialogs in current stack. "
                f"Max count is {_STACK_LIMIT}",
            )
        context = Context(
            _intent_id=new_id(),
            _stack_id=self.id,
            state=state,
            start_data=data,
        )
        self.intents.append(context.id)
        return context

    def pop(self):
        return self.intents.pop()

    def last_intent_id(self):
        return self.intents[-1]

    def empty(self):
        return not self.intents

    def default(self):
        return self.id == DEFAULT_STACK_ID
