import sys
import termios
import tty


def getch() -> str:
    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)  # type: ignore
    try:
        tty.setraw(sys.stdin.fileno())  # type: ignore
        ch = sys.stdin.read(1)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)  # type: ignore

    return ch
