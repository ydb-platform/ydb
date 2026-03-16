from __future__ import print_function
import attr

VERSBOSE = False

def getoutput(s):
    try:
        import commands as subprocess # for PY2
    except ImportError:
        import subprocess
    if VERSBOSE:
        print(s)
    return subprocess.getoutput(s)

def system(s):
    import os
    if VERSBOSE:
        print(s)
    return os.system(s)

def strip_prefix (prefix, word):
    if word.startswith(prefix):
        return word[len(prefix):]
    return word

def uniq(it):
    return list(set(it))



@attr.s
class Window(object):
    id = attr.ib()
    desktop = attr.ib()
    pid = attr.ib()
    x = attr.ib()
    y = attr.ib()
    w = attr.ib()
    h = attr.ib()
    wm_class = attr.ib()
    host = attr.ib()
    wm_name = attr.ib()
    _wm_window_role = attr.ib(default=None)
    _wm_state = attr.ib(default=None)

    @property
    def wm_window_role(self):
        if self._wm_window_role is not None:
            return self._wm_window_role
        #
        out = getoutput('xprop -id %s WM_WINDOW_ROLE' % self.id)
        try:
            _, value = out.split(' = ')
        except ValueError:
            # probably xprop returned an error
            self._wm_window_role = ''
        else:
            self._wm_window_role = value.strip('"')
        return self._wm_window_role

    @property
    def wm_state (self):
        if self._wm_state is not None:
            return self._wm_state

        out = getoutput('xprop -id %s _NET_WM_STATE' % self.id)
        try:
            _, value = out.split(' = ')
        except ValueError:
            # probably xprop returned an error
            self._wm_state = []
        else:
            self._wm_state = [strip_prefix("_NET_WM_STATE_",s).lower()
                              for s in value.split(', ')]
        return self._wm_state


    @classmethod
    def list(cls):
        out = getoutput('wmctrl -l -G -p -x')
        windows = []
        for line in out.splitlines():
            parts = line.split(None, 9)
            parts = list(map(str.strip, parts))
            parts[1:7] = list(map(int, parts[1:7]))
            if len(parts) == 9: # title is missing
                parts.append('')
            elif len(parts) != 10:
                continue # something was wrong
            windows.append(cls(*parts))
        return windows

    @classmethod
    def list_classes(cls):
        return uniq([w.wm_class for w in cls.list()])

    @classmethod
    def list_names(cls):
        return uniq([w.name_class for w in cls.list()])

    @classmethod
    def list_roles(cls):
        return uniq([w.wm_window_role for w in cls.list()])

    @classmethod
    def by_name(cls, name):
        return [win for win in cls.list() if win.wm_name == name]

    @classmethod
    def by_name_endswith(cls, name):
        return [win for win in cls.list() if win.wm_name.endswith(name)]

    @classmethod
    def by_name_startswith(cls, name):
        return [win for win in cls.list() if win.wm_name.startswith(name)]

    @classmethod
    def by_role(cls, role):
        return [win for win in cls.list() if win.wm_window_role == role]

    @classmethod
    def by_class(cls, wm_class):
        return [win for win in cls.list() if win.wm_class == wm_class]

    @classmethod
    def by_id(cls, id):
        return [win for win in cls.list() if int(win.id, 16) == id]

    @classmethod
    def get_active(cls):
        out = getoutput("xprop -root _NET_ACTIVE_WINDOW")
        parts = out.split()
        try:
            id = int(parts[-1], 16)
        except ValueError:
            return None
        lst = cls.by_id(id)
        if not lst:
            return None
        assert len(lst) == 1
        return lst[0]

    def activate(self):
        system('wmctrl -id -a %s' % self.id)

    def resize_and_move(self, x=None, y=None, w=None, h=None):
        # XXX: the "move" part doesn't really work, it is affected by this:
        # https://askubuntu.com/questions/576604/what-causes-the-deviation-in-the-wmctrl-window-move-command
        if x is None:
            x = self.x
        if y is None:
            y = self.y
        if w is None:
            w = self.w
        if h is None:
            h = self.h
        mvarg = '0,%d,%d,%d,%d' % (x, y, w, h)
        system('wmctrl -i -r %s -e %s' % (self.id, mvarg))

    def resize(self, w=None, h=None):
        self.resize_and_move(w=w, h=h)

    def move(self, x=None, y=None):
        self.resize_and_move(x=x, y=y)

    def sticky(self):
        # -2 seems to work on kde plasma, not sure about the other WMs:
        # https://unix.stackexchange.com/questions/11893/command-to-move-a-window
        self.move_to_destktop(-2)

    def move_to_destktop(self, n):
        system('wmctrl -i -r %s -t %s' % (self.id, n))

    def set_geometry(self, geometry):
        dim, pos = geometry.split('+', 1)
        w, h = map(int, dim.split('x'))
        x, y = map(int, pos.split('+'))
        self.resize_and_move(x, y, w, h)

    def set_properties(self, properties):
        proparg = ",".join(properties)
        system('wmctrl -i -r %s -b %s' % (self.id, proparg))

    def set_decorations(self, v):
        try:
            # try to use gtk
            import gtk.gdk
            w = gtk.gdk.window_foreign_new(int(self.id, 16))
            w.set_decorations(v)
            gtk.gdk.window_process_all_updates()
            gtk.gdk.flush()
        except ImportError:
            # try to use my utilty
            op = int(bool(v)) # 1 or 0
            system("set-x11-decorations %s %s" % (self.id, op))

    def maximize(self, verb='add'):
        "verb can be 'add', 'remove' or 'toggle'"
        self.set_properties([verb, 'maximized_vert', 'maximized_horz'])

    def unmaximize(self):
        self.maximize('remove')


@attr.s
class Desktop(object):
    num = attr.ib()
    active = attr.ib()
    name = attr.ib()

    @classmethod
    def list(cls):
        out = getoutput('wmctrl -d')
        desktops = []
        for line in out.splitlines():
            parts = line.split(None, 9)
            parts = list(map(str.strip, parts))
            num = int(parts[0])
            active = parts[1] == '*'
            name = parts[-1]
            desktops.append(cls(num, active, name))
        return desktops

    @classmethod
    def get_active(cls):
        dlist = cls.list()
        for d in dlist:
            if d.active:
                return d
        # this should never happen, but who knows?
        return None

if __name__ == '__main__':
    class magic(object):
        def __getattr__(self, name):
            return name
    windows = Window.list()
    windows.sort(key=lambda w:w.wm_class)
    print('{s.id:<10} {s.x:>4} {s.y:>4} {s.w:>4} {s.h:>4} '
          '{s.class:>37} {s.role:>15} {s.name}'.format(s=magic()))
    for w in windows:
        print('{w.id:<10} {w.x:>4d} {w.y:>4d} {w.w:4d} {w.h:4d} '
              '{w.wm_class:>37} {w.wm_window_role:>15} {w.wm_name}'.format(w=w))
