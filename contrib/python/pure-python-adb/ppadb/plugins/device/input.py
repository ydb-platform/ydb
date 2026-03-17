from ppadb.plugins import Plugin


class Source:
    KEYBOARD = 'keyboard'
    MOUSE = 'mouse'
    JOYSTICK = 'joystick'
    TOUCHNAVIGATION = 'touchnavigation'
    TOUCHPAD = 'touchpad'
    TRACKBALL = 'trackball'
    DPAD = 'dpad'
    STYLUS = 'stylus'
    GAMEPAD = 'gamepad'
    touchscreen = 'touchscreen'


class Input(Plugin):
    def input_text(self, string):
        return self.shell('input text "{}"'.format(string))

    def input_keyevent(self, keycode, longpress=False):
        cmd = 'input keyevent {}'.format(keycode)
        if longpress:
            cmd += " --longpress"
        return self.shell(cmd)

    def input_tap(self, x, y):
        return self.shell("input tap {} {}".format(x, y))

    def input_swipe(self, start_x, start_y, end_x, end_y, duration):
        return self.shell("input swipe {} {} {} {} {}".format(
            start_x,
            start_y,
            end_x,
            end_y,
            duration
        ))

    def input_press(self):
        return self.shell("input press")

    def input_roll(self, dx, dy):
        return self.roll("roll {} {}".format(dx, dy))
