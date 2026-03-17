# Aiogram Dialog

[![PyPI version](https://badge.fury.io/py/aiogram-dialog.svg)](https://badge.fury.io/py/aiogram-dialog)
[![Doc](https://readthedocs.org/projects/aiogram-dialog/badge/?version=latest&style=flat)](https://aiogram-dialog.readthedocs.io)
[![downloads](https://img.shields.io/pypi/dm/aiogram_dialog.svg)](https://pypistats.org/packages/aiogram_dialog)
[![license](https://img.shields.io/github/license/Tishka17/aiogram_dialog.svg)](https://github.com/Tishka17/aiogram_dialog/blob/master/LICENSE)
[![license](https://img.shields.io/badge/💬-Telegram-blue)](https://t.me/aiogram_dialog)

#### Version status:
* v2.x - actual release, supports aiogram v3.x
* v1.x - old release, supports aiogram v2.x, critical fix only

### About 
`aiogram-dialog` is a framework for developing interactive messages and menus in your telegram bot like a normal GUI application.  
 
It is inspired by ideas of Android SDK and other tools.

Main ideas are:
* **split data retrieving, rendering and action processing** - you need nothing to do for showing same content after some actions, also you can show same data in multiple ways. 
* **reusable widgets**  - you can create calendar or multiselect at any point of your application without copy-pasting its internal logic  
* **limited scope of context** - any dialog keeps some data until closed, multiple opened dialogs process their data separately

Designing you bot with `aiogram-dialog` you **think about user**, what he sees and what he does. Then you split this vision into reusable parts and design your bot combining dialogs, widows and widgets. By this moment you can review interface and add your core logic. 

Many components are ready for use, but you can extend and add your own widgets and even core features. 

For more details see [documentation](https://aiogram-dialog.readthedocs.io) and [examples](example)

### Supported features:
* Rich text rendering using `format` function or `Jinja2` template engine. 
* Automatic message updating after user actions
* Multiple independent dialog stacks with own data storage and transitions
* Inline keyboard widgets like `SwitchTo`, `Start`, `Cancel` for state switching, `Calendar` for date selection and others. 
* Stateful widgets: `Checkbox`, `Multiselect`, `Counter`, `TextInput`. They record user actions and allow you to retrieve this data later. 
* Multiple buttons layouts including simple grouping (`Group`, `Column`), page scrolling (`ScrollingGroup`), repeating of same buttons for list of data (`ListGroup`). 
* Sending media (like photo or video) with fileid caching and handling switching to/from message with no media. 
* Different rules of transitions between windows/dialogs like keeping only one dialog on top of stack or force sending new message instead of updating one. 
* Offline HTML-preview for messages and transitions diagram. They can be used to check all states without emulating real use cases or exported for demonstration purposes. 


### Usage

Example below is suitable for aiogram_dialog v2.x and aiogram v3.x

#### Declaring Window

Each window consists of:

* Text widgets. Render text of message.
* Keyboard widgets. Render inline keyboard
* Media widget. Renders media if need
* Message handler. Called when user sends a message when window is shown
* Data getter functions (`getter=`). They load data from any source which can be used in text/keyboard
* State. Used when switching between windows

**Info:** always create `State` inside `StatesGroup`


```python
from aiogram.fsm.state import StatesGroup, State
from aiogram_dialog.widgets.text import Format, Const
from aiogram_dialog.widgets.kbd import Button
from aiogram_dialog import Window


class MySG(StatesGroup):
    main = State()


async def get_data(**kwargs):
    return {"name": "world"}


Window(
    Format("Hello, {name}!"),
    Button(Const("Empty button"), id="nothing"),
    state=MySG.main,
    getter=get_data,
)
```

### Declaring dialog

Window itself can do nothing, just prepares message. To use it you need dialog:

```python
from aiogram.fsm.state import StatesGroup, State
from aiogram_dialog import Dialog, Window


class MySG(StatesGroup):
    first = State()
    second = State()


dialog = Dialog(
    Window(..., state=MySG.first),
    Window(..., state=MySG.second),
)
```

> **Info:** All windows in a dialog MUST have states from then same `StatesGroup`

After creating a dialog you need to register it into the Dispatcher and set it up using the `setup_dialogs` function:

```python
from aiogram import Dispatcher
from aiogram_dialog import setup_dialogs

...
dp = Dispatcher(storage=storage)  # create as usual
dp.include_router(dialog)
setup_dialogs(dp)
```

Then start dialog when you are ready to use it. Dialog is started via `start` method of `DialogManager` instance. You
should provide corresponding state to switch into (usually it is state of first window in dialog).

For example in `/start` command handler:

```python
async def user_start(message: Message, dialog_manager: DialogManager):
    await dialog_manager.start(MySG.first, mode=StartMode.RESET_STACK)

dp.message.register(user_start, CommandStart())
```

> **Info:** Always set `mode=StartMode.RESET_STACK` in your top level start command. Otherwise, dialogs are stacked just as they do
on your mobile phone, so you can reach stackoverflow error
