"""Sub-module providing mouse event handling."""
# std imports
import re
from typing import Match


class MouseEvent:  # pylint: disable=too-many-instance-attributes
    """
    Mouse event with button, coordinates, and modifier information.

    A unified mouse event structure that supports both legacy and SGR mouse protocols. Provides a
    dynamic button property that returns human-readable button names like "LEFT", "SCROLL_UP",
    "CTRL_LEFT", etc.

    :ivar int button_value: Raw button number (0=left, 1=middle, 2=right, 64=scroll up, 65=scroll
        down, or higher for extended buttons).
    :ivar int x: Horizontal position (0-indexed, in cells or pixels depending on mode).
    :ivar int y: Vertical position (0-indexed, in cells or pixels depending on mode).
    :ivar bool released: True if this is a button release event.
    :ivar bool shift: True if Shift modifier is pressed.
    :ivar bool meta: True if Meta/Alt modifier is pressed.
    :ivar bool ctrl: True if Ctrl modifier is pressed.
    :ivar bool is_motion: True if motion is being reported (drag or all- motion mode).
    :ivar bool is_wheel: True if this is a scroll wheel event.
    """

    def __init__(self, button_value: int, x: int, y: int, released: bool,
                 shift: bool, meta: bool, ctrl: bool, is_motion: bool, is_wheel: bool):
        # pylint: disable=too-many-positional-arguments
        """
        Initialize a MouseEvent.

        :param int button_value: Raw button number.
        :param int x: Horizontal position.
        :param int y: Vertical position.
        :param bool released: Whether this is a button release event.
        :param bool shift: Whether Shift modifier is pressed.
        :param bool meta: Whether Meta/Alt modifier is pressed.
        :param bool ctrl: Whether Ctrl modifier is pressed.
        :param bool is_motion: Whether motion is being reported.
        :param bool is_wheel: Whether this is a scroll wheel event.
        """
        self.button_value = button_value
        self.x = x
        self.y = y
        self.released = released
        self.shift = shift
        self.meta = meta
        self.ctrl = ctrl
        self.is_motion = is_motion
        self.is_wheel = is_wheel

    def _get_base_button_name(self) -> str:
        """
        Get base button name without modifiers or state.

        :rtype: str
        :returns: Base button name like "LEFT", "MIDDLE", "RIGHT", or "BUTTON_6".
        """
        if self.button_value < 66:
            return {
                0: "LEFT",
                1: "MIDDLE",
                2: "RIGHT",
            }.get(self.button_value, '')
        # Extended buttons (button_value >= 66)
        return f"BUTTON_{self.button_value - 60}"

    @property
    def button(self) -> str:
        """
        Return human-readable button name.

        Generates button names that include modifiers, button type, motion/release state:
        - "LEFT", "MIDDLE", "RIGHT" for standard mouse buttons
        - "LEFT_RELEASED", "MIDDLE_RELEASED", "RIGHT_RELEASED" for button releases
        - "SCROLL_UP", "SCROLL_DOWN" for wheel events
        - "MOTION" for mouse movement with no button pressed
        - "LEFT_MOTION", "MIDDLE_MOTION", "RIGHT_MOTION" for drag events
        - "CTRL_LEFT", "SHIFT_SCROLL_UP", "CTRL_SHIFT_META_MOTION" with modifiers
        - "BUTTON_6", "BUTTON_7", etc. for extended mouse buttons

        :rtype: str
        :returns: Button name with modifiers, button type, and motion/release state.
        """
        button_name = ''

        # Add modifiers in order: ctrl, shift, meta
        for modifier in ('ctrl', 'shift', 'meta'):
            if getattr(self, modifier):
                button_name += f'{modifier.upper()}_'

        # Handle wheel events first (legacy uses button_value 0/1, SGR uses 64/65)
        if self.is_wheel:
            # Legacy wheel: button_value 0=up, 1=down
            # SGR wheel: button_value 64=up, 65=down
            if self.button_value in {0, 64}:
                button_name += "SCROLL_UP"
            elif self.button_value in {1, 65}:
                button_name += "SCROLL_DOWN"
            # Wheel events don't have motion or release variants in typical usage
            return button_name

        # Handle motion events specially
        if self.is_motion:
            # Motion with no button pressed (button_value=3 means no button in SGR motion)
            if self.button_value == 3:
                button_name += "MOTION"
            else:
                # Dragging with a specific button
                button_name += f"{self._get_base_button_name()}_MOTION"
        else:
            # Regular click or release events
            button_name += self._get_base_button_name()

            # Add release state (only for non-motion events)
            if self.released:
                button_name += "_RELEASED"

        return button_name

    def __repr__(self) -> str:
        """Return succinct representation showing only active attributes."""
        # Always show button_value, x, y
        parts = [f'button_value={self.button_value}', f'x={self.x}', f'y={self.y}']

        # Only show boolean flags when True
        for bool_name in ('released', 'shift', 'meta', 'ctrl', 'is_motion', 'is_wheel'):
            if getattr(self, bool_name):
                parts.append(f'{bool_name}=True')
        return f"MouseEvent({', '.join(parts)})"

    @classmethod
    def from_sgr_match(cls, match: Match[str]) -> 'MouseEvent':
        """
        Parse SGR mouse event from regex match.

        Handles both SGR (mode 1006) and SGR-Pixels (mode 1016) since they
        use identical wire formats: CSI < b;x;y m/M. The difference is semantic:
        - Mode 1006: coordinates represent character cell positions
        - Mode 1016: coordinates represent pixel positions
        Applications must interpret x,y coordinates based on which mode was enabled.

        The protocol sends 1-indexed coordinates (top-left is 1,1), but we convert
        to 0-indexed (top-left is 0,0) to match blessed's terminal movement functions.

        :param Match match: Regex match object with groups 'b', 'x', 'y', 'type'.
        :rtype: MouseEvent
        :returns: Parsed MouseEvent instance.
        """
        b = int(match.group('b'))
        x = int(match.group('x')) - 1  # Convert from 1-indexed to 0-indexed
        y = int(match.group('y')) - 1  # Convert from 1-indexed to 0-indexed
        event_type = match.group('type')

        released = event_type == 'm'

        # Extract modifiers from button code
        shift = bool(b & 4)
        meta = bool(b & 8)
        ctrl = bool(b & 16)

        # Extract motion/drag flags
        is_motion = bool(b & 32)

        # Check for wheel events (button 64-65) by masking modifiers
        # Wheel events: button & ~(shift|meta|ctrl|motion) gives base button
        base_button = b & ~(4 | 8 | 16 | 32)
        is_wheel = base_button in {64, 65}  # wheel up/down

        # Get base button (0-2 for left/middle/right, or 64-65 for wheel)
        button = b & 3 if not is_wheel else base_button

        return cls(
            button_value=button,
            x=x,
            y=y,
            released=released,
            shift=shift,
            meta=meta,
            ctrl=ctrl,
            is_motion=is_motion,
            is_wheel=is_wheel
        )

    @classmethod
    def from_legacy_match(cls, match: Match[str]) -> 'MouseEvent':
        """
        Parse legacy mouse event (X10/1000/1002/1003) from regex match.

        The protocol sends 1-indexed coordinates (top-left is 1,1), but we convert to 0-indexed
        (top-left is 0,0) to match blessed's terminal movement functions.

        :param Match match: Regex match object with groups 'cb', 'cx', 'cy'.
        :rtype: MouseEvent
        :returns: Parsed MouseEvent instance.
        """
        cb = ord(match.group('cb')) - 32
        cx = ord(match.group('cx')) - 32 - 1  # Convert from 1-indexed to 0-indexed
        cy = ord(match.group('cy')) - 32 - 1  # Convert from 1-indexed to 0-indexed

        # Extract button and modifiers from cb
        button = cb & 3
        released = button == 3
        if released:
            button = 0  # Release doesn't specify which button

        # Extract modifier flags
        shift = bool(cb & 4)
        meta = bool(cb & 8)
        ctrl = bool(cb & 16)

        # Extract motion/drag flags
        is_motion = bool(cb & 32)

        # Wheel events
        is_wheel = cb >= 64
        if is_wheel:
            button = cb - 64  # 0=wheel up, 1=wheel down

        return cls(
            button_value=button,
            x=cx,
            y=cy,
            released=released,
            shift=shift,
            meta=meta,
            ctrl=ctrl,
            is_motion=is_motion,
            is_wheel=is_wheel
        )


# Backwards compatibility aliases
MouseSGREvent = MouseEvent
MouseLegacyEvent = MouseEvent


# Mouse event patterns (shared across multiple DEC modes)
# SGR mouse format (modes 1006 and 1016): ESC [ < b ; x ; y M/m
# The optional '<' allows backward compatibility with non-standard implementations
RE_PATTERN_MOUSE_SGR = re.compile(r'\x1b\[<?(?P<b>\d+);(?P<x>\d+);(?P<y>\d+)(?P<type>[mM])')
# Legacy mouse format (modes 1000, 1002, 1003): ESC [ M cb cx cy
RE_PATTERN_MOUSE_LEGACY = re.compile(r'\x1b\[M(?P<cb>.)(?P<cx>.)(?P<cy>.)')


__all__ = ('MouseEvent', 'MouseSGREvent', 'MouseLegacyEvent',
           'RE_PATTERN_MOUSE_SGR', 'RE_PATTERN_MOUSE_LEGACY')
