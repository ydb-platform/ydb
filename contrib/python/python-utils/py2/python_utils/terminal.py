import os


def get_terminal_size():  # pragma: no cover
    '''Get the current size of your terminal

    Multiple returns are not always a good idea, but in this case it greatly
    simplifies the code so I believe it's justified. It's not the prettiest
    function but that's never really possible with cross-platform code.

    Returns:
        width, height: Two integers containing width and height
    '''

    try:
        # Default to 79 characters for IPython notebooks
        from IPython import get_ipython
        ipython = get_ipython()
        from ipykernel import zmqshell
        if isinstance(ipython, zmqshell.ZMQInteractiveShell):
            return 79, 24
    except Exception:  # pragma: no cover
        pass

    try:
        # This works for Python 3, but not Pypy3. Probably the best method if
        # it's supported so let's always try
        import shutil
        w, h = shutil.get_terminal_size()
        if w and h:
            # The off by one is needed due to progressbars in some cases, for
            # safety we'll always substract it.
            return w - 1, h
    except Exception:  # pragma: no cover
        pass

    try:
        w = int(os.environ.get('COLUMNS'))
        h = int(os.environ.get('LINES'))
        if w and h:
            return w, h
    except Exception:  # pragma: no cover
        pass

    try:
        import blessings
        terminal = blessings.Terminal()
        w = terminal.width
        h = terminal.height
        if w and h:
            return w, h
    except Exception:  # pragma: no cover
        pass

    try:
        w, h = _get_terminal_size_linux()
        if w and h:
            return w, h
    except Exception:  # pragma: no cover
        pass

    try:
        # Windows detection doesn't always work, let's try anyhow
        w, h = _get_terminal_size_windows()
        if w and h:
            return w, h
    except Exception:  # pragma: no cover
        pass

    try:
        # needed for window's python in cygwin's xterm!
        w, h = _get_terminal_size_tput()
        if w and h:
            return w, h
    except Exception:  # pragma: no cover
        pass

    return 79, 24


def _get_terminal_size_windows():  # pragma: no cover
    res = None
    try:
        from ctypes import windll, create_string_buffer

        # stdin handle is -10
        # stdout handle is -11
        # stderr handle is -12

        h = windll.kernel32.GetStdHandle(-12)
        csbi = create_string_buffer(22)
        res = windll.kernel32.GetConsoleScreenBufferInfo(h, csbi)
    except Exception:
        return None

    if res:
        import struct
        (_, _, _, _, _, left, top, right, bottom, _, _) = \
            struct.unpack("hhhhHhhhhhh", csbi.raw)
        w = right - left
        h = bottom - top
        return w, h
    else:
        return None


def _get_terminal_size_tput():  # pragma: no cover
    # get terminal width src: http://stackoverflow.com/questions/263890/
    try:
        import subprocess
        proc = subprocess.Popen(
            ['tput', 'cols'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        output = proc.communicate(input=None)
        w = int(output[0])
        proc = subprocess.Popen(
            ['tput', 'lines'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        output = proc.communicate(input=None)
        h = int(output[0])
        return w, h
    except Exception:
        return None


def _get_terminal_size_linux():  # pragma: no cover
    def ioctl_GWINSZ(fd):
        try:
            import fcntl
            import termios
            import struct
            size = struct.unpack(
                'hh', fcntl.ioctl(fd, termios.TIOCGWINSZ, '1234'))
        except Exception:
            return None
        return size

    size = ioctl_GWINSZ(0) or ioctl_GWINSZ(1) or ioctl_GWINSZ(2)

    if not size:
        try:
            fd = os.open(os.ctermid(), os.O_RDONLY)
            size = ioctl_GWINSZ(fd)
            os.close(fd)
        except Exception:
            pass
    if not size:
        try:
            size = os.environ['LINES'], os.environ['COLUMNS']
        except Exception:
            return None

    return int(size[1]), int(size[0])
