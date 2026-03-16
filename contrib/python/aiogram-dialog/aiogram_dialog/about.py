import importlib.metadata

from aiogram.fsm.state import State, StatesGroup

from aiogram_dialog.api.internal import TextWidget
from aiogram_dialog.dialog import Dialog
from aiogram_dialog.widgets.kbd import Cancel, Keyboard, Start
from aiogram_dialog.widgets.link_preview import LinkPreview
from aiogram_dialog.widgets.text import Const, Jinja
from aiogram_dialog.window import Window


class AiogramDialogStates(StatesGroup):
    ABOUT = State()


async def metadata_getter(**_kwargs) -> dict:
    metadata = importlib.metadata.metadata("aiogram-dialog")
    urls = [u.split(",", maxsplit=1) for u in metadata.get_all("Project-Url")]
    return {
        "metadata": metadata,
        "urls": urls,
    }


def about_dialog():
    return Dialog(
        Window(
            Jinja(
                "<b><u>{{metadata.Name}}</u></b> by @tishka17\n"
                "\n"
                "{{metadata.Summary}}\n"
                "\n"
                "<b>Version:</b> {{metadata.Version}}\n"
                "<b>Author:</b> {{metadata['Author-email']}}\n"
                "\n"
                "{% for name, url in urls%}"
                '<b>{{name}}:</b> <a href="{{url}}">{{url}}</a>\n'
                "{% endfor %}"
                "",
            ),
            LinkPreview(is_disabled=True),
            Cancel(Const("Ok")),
            getter=metadata_getter,
            preview_data=metadata_getter,
            state=AiogramDialogStates.ABOUT,
            parse_mode="html",
        ),
    )


DEFAULT_ABOUT_BTN_TEXT = Const("🗨️ About aiogram-dialog")


def about_aiogram_dialog_button(
        text: TextWidget = DEFAULT_ABOUT_BTN_TEXT,
) -> Keyboard:
    return Start(
        text=text,
        state=AiogramDialogStates.ABOUT,
        id="__aiogd_about__",
    )
