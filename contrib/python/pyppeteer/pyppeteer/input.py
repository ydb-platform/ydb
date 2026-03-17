#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Keyboard and Mouse module."""

import asyncio
from typing import Any, Dict, TYPE_CHECKING

from pyppeteer.connection import CDPSession
from pyppeteer.errors import PyppeteerError
from pyppeteer.us_keyboard_layout import keyDefinitions
from pyppeteer.util import merge_dict

if TYPE_CHECKING:
    from typing import Set  # noqa: F401


class Keyboard(object):
    """Keyboard class provides as api for managing a virtual keyboard.

    The high level api is :meth:`type`, which takes raw characters and
    generate proper keydown, keypress/input, and keyup events on your page.

    For finer control, you can use :meth:`down`, :meth:`up`, and
    :meth:`sendCharacter` to manually fire events as if they were generated
    from a real keyboard.

    An example of holding down ``Shift`` in order to select and delete some
    text:

    .. code::

        await page.keyboard.type('Hello, World!')
        await page.keyboard.press('ArrowLeft')

        await page.keyboard.down('Shift')
        for i in ' World':
            await page.keyboard.press('ArrowLeft')
        await page.keyboard.up('Shift')

        await page.keyboard.press('Backspace')
        # Result text will end up saying 'Hello!'.

    An example of pressing ``A``:

    .. code::

        await page.keyboard.down('Shift')
        await page.keyboard.press('KeyA')
        await page.keyboard.up('Shift')
    """

    def __init__(self, client: CDPSession) -> None:
        self._client = client
        self._modifiers = 0
        self._pressedKeys: Set[str] = set()

    async def down(self, key: str, options: dict = None, **kwargs: Any
                   ) -> None:
        """Dispatch a ``keydown`` event with ``key``.

        If ``key`` is a single character and no modifier keys besides ``Shift``
        are being held down, and a ``keypress``/``input`` event will also
        generated. The ``text`` option can be specified to force an ``input``
        event to be generated.

        If ``key`` is a modifier key, like ``Shift``, ``Meta``, or ``Alt``,
        subsequent key presses will be sent with that modifier active. To
        release the modifier key, use :meth:`up` method.

        :arg str key: Name of key to press, such as ``ArrowLeft``.
        :arg dict options: Option can have ``text`` field, and if this option
            specified, generate an input event with this text.

        .. note::
            Modifier keys DO influence :meth:`down`. Holding down ``shift``
            will type the text in upper case.
        """
        options = merge_dict(options, kwargs)

        description = self._keyDescriptionForString(key)
        autoRepeat = description['code'] in self._pressedKeys
        self._pressedKeys.add(description['code'])
        self._modifiers |= self._modifierBit(description['key'])

        text = options.get('text')
        if text is None:
            text = description['text']

        await self._client.send('Input.dispatchKeyEvent', {
            'type': 'keyDown' if text else 'rawKeyDown',
            'modifiers': self._modifiers,
            'windowsVirtualKeyCode': description['keyCode'],
            'code': description['code'],
            'key': description['key'],
            'text': text,
            'unmodifiedText': text,
            'autoRepeat': autoRepeat,
            'location': description['location'],
            'isKeypad': description['location'] == 3,
        })

    def _modifierBit(self, key: str) -> int:
        if key == 'Alt':
            return 1
        if key == 'Control':
            return 2
        if key == 'Meta':
            return 4
        if key == 'Shift':
            return 8
        return 0

    def _keyDescriptionForString(self, keyString: str) -> Dict:  # noqa: C901
        shift = self._modifiers & 8
        description = {
            'key': '',
            'keyCode': 0,
            'code': '',
            'text': '',
            'location': 0,
        }

        definition: Dict = keyDefinitions.get(keyString)  # type: ignore
        if not definition:
            raise PyppeteerError(f'Unknown key: {keyString}')

        if 'key' in definition:
            description['key'] = definition['key']
        if shift and definition.get('shiftKey'):
            description['key'] = definition['shiftKey']

        if 'keyCode' in definition:
            description['keyCode'] = definition['keyCode']
        if shift and definition.get('shiftKeyCode'):
            description['keyCode'] = definition['shiftKeyCode']

        if 'code' in definition:
            description['code'] = definition['code']

        if 'location' in definition:
            description['location'] = definition['location']

        if len(description['key']) == 1:  # type: ignore
            description['text'] = description['key']

        if 'text' in definition:
            description['text'] = definition['text']
        if shift and definition.get('shiftText'):
            description['text'] = definition['shiftText']

        if self._modifiers & ~8:
            description['text'] = ''

        return description

    async def up(self, key: str) -> None:
        """Dispatch a ``keyup`` event of the ``key``.

        :arg str key: Name of key to release, such as ``ArrowLeft``.
        """
        description = self._keyDescriptionForString(key)

        self._modifiers &= ~self._modifierBit(description['key'])
        if description['code'] in self._pressedKeys:
            self._pressedKeys.remove(description['code'])
        await self._client.send('Input.dispatchKeyEvent', {
            'type': 'keyUp',
            'modifiers': self._modifiers,
            'key': description['key'],
            'windowsVirtualKeyCode': description['keyCode'],
            'code': description['code'],
            'location': description['location'],
        })

    async def sendCharacter(self, char: str) -> None:
        """Send character into the page.

        This method dispatches a ``keypress`` and ``input`` event. This does
        not send a ``keydown`` or ``keyup`` event.

        .. note::
            Modifier keys DO NOT effect :meth:`sendCharacter`. Holding down
            ``shift`` will not type the text in upper case.
        """
        await self._client.send('Input.insertText', {'text': char})

    async def type(self, text: str, options: Dict = None, **kwargs: Any
                   ) -> None:
        """Type characters into a focused element.

        This method sends ``keydown``, ``keypress``/``input``, and ``keyup``
        event for each character in the ``text``.

        To press a special key, like ``Control`` or ``ArrowDown``, use
        :meth:`press` method.

        :arg str text: Text to type into a focused element.
        :arg dict options: Options can have ``delay`` (int|float) field, which
          specifies time to wait between key presses in milliseconds. Defaults
          to 0.

        .. note::
            Modifier keys DO NOT effect :meth:`type`. Holding down ``shift``
            will not type the text in upper case.
        """
        options = merge_dict(options, kwargs)
        delay = options.get('delay', 0)
        for char in text:
            if char in keyDefinitions:
                await self.press(char, {'delay': delay})
            else:
                await self.sendCharacter(char)
            if delay:
                await asyncio.sleep(delay / 1000)

    async def press(self, key: str, options: Dict = None, **kwargs: Any
                    ) -> None:
        """Press ``key``.

        If ``key`` is a single character and no modifier keys besides
        ``Shift`` are being held down, a ``keypress``/``input`` event will also
        generated. The ``text`` option can be specified to force an input event
        to be generated.

        :arg str key: Name of key to press, such as ``ArrowLeft``.

        This method accepts the following options:

        * ``text`` (str): If specified, generates an input event with this
          text.
        * ``delay`` (int|float): Time to wait between ``keydown`` and
          ``keyup``. Defaults to 0.

        .. note::
            Modifier keys DO effect :meth:`press`. Holding down ``Shift`` will
            type the text in upper case.
        """
        options = merge_dict(options, kwargs)

        await self.down(key, options)
        if 'delay' in options:
            await asyncio.sleep(options['delay'] / 1000)
        await self.up(key)


class Mouse(object):
    """Mouse class.

    The :class:`Mouse` operates in main-frame CSS pixels relative to the
    top-left corner of the viewport.
    """

    def __init__(self, client: CDPSession, keyboard: Keyboard) -> None:
        self._client = client
        self._keyboard = keyboard
        self._x = 0.0
        self._y = 0.0
        self._button = 'none'

    async def move(self, x: float, y: float, options: dict = None,
                   **kwargs: Any) -> None:
        """Move mouse cursor (dispatches a ``mousemove`` event).

        Options can accepts ``steps`` (int) field. If this ``steps`` option
        specified, Sends intermediate ``mousemove`` events. Defaults to 1.
        """
        options = merge_dict(options, kwargs)
        fromX = self._x
        fromY = self._y
        self._x = x
        self._y = y
        steps = options.get('steps', 1)
        for i in range(1, steps + 1):
            x = round(fromX + (self._x - fromX) * (i / steps))
            y = round(fromY + (self._y - fromY) * (i / steps))
            await self._client.send('Input.dispatchMouseEvent', {
                'type': 'mouseMoved',
                'button': self._button,
                'x': x,
                'y': y,
                'modifiers': self._keyboard._modifiers,
            })

    async def click(self, x: float, y: float, options: dict = None,
                    **kwargs: Any) -> None:
        """Click button at (``x``, ``y``).

        Shortcut to :meth:`move`, :meth:`down`, and :meth:`up`.

        This method accepts the following options:

        * ``button`` (str): ``left``, ``right``, or ``middle``, defaults to
          ``left``.
        * ``clickCount`` (int): defaults to 1.
        * ``delay`` (int|float): Time to wait between ``mousedown`` and
          ``mouseup`` in milliseconds. Defaults to 0.
        """
        options = merge_dict(options, kwargs)
        await self.move(x, y)
        await self.down(options)
        if options and options.get('delay'):
            await asyncio.sleep(options.get('delay', 0) / 1000)
        await self.up(options)

    async def down(self, options: dict = None, **kwargs: Any) -> None:
        """Press down button (dispatches ``mousedown`` event).

        This method accepts the following options:

        * ``button`` (str): ``left``, ``right``, or ``middle``, defaults to
          ``left``.
        * ``clickCount`` (int): defaults to 1.
        """
        options = merge_dict(options, kwargs)
        self._button = options.get('button', 'left')
        await self._client.send('Input.dispatchMouseEvent', {
            'type': 'mousePressed',
            'button': self._button,
            'x': self._x,
            'y': self._y,
            'modifiers': self._keyboard._modifiers,
            'clickCount': options.get('clickCount') or 1,
        })

    async def up(self, options: dict = None, **kwargs: Any) -> None:
        """Release pressed button (dispatches ``mouseup`` event).

        This method accepts the following options:

        * ``button`` (str): ``left``, ``right``, or ``middle``, defaults to
          ``left``.
        * ``clickCount`` (int): defaults to 1.
        """
        options = merge_dict(options, kwargs)
        self._button = 'none'
        await self._client.send('Input.dispatchMouseEvent', {
            'type': 'mouseReleased',
            'button': options.get('button', 'left'),
            'x': self._x,
            'y': self._y,
            'modifiers': self._keyboard._modifiers,
            'clickCount': options.get('clickCount') or 1,
        })


class Touchscreen(object):
    """Touchscreen class."""

    def __init__(self, client: CDPSession, keyboard: Keyboard) -> None:
        """Make new touchscreen object."""
        self._client = client
        self._keyboard = keyboard

    async def tap(self, x: float, y: float) -> None:
        """Tap (``x``, ``y``).

        Dispatches a ``touchstart`` and ``touchend`` event.
        """
        touchPoints = [{'x': round(x), 'y': round(y)}]
        await self._client.send('Input.dispatchTouchEvent', {
            'type': 'touchStart',
            'touchPoints': touchPoints,
            'modifiers': self._keyboard._modifiers,
        })
        await self._client.send('Input.dispatchTouchEvent', {
            'type': 'touchEnd',
            'touchPoints': [],
            'modifiers': self._keyboard._modifiers,
        })
