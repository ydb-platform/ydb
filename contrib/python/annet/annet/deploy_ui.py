import asyncio
import math
import os
import platform
import re
import sys
import tempfile
import textwrap
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

from contextlog import get_logger

from annet import text_term_format
from annet.deploy import ProgressBar
from annet.output import TextArgs


try:
    import curses
except ImportError:
    curses = None

uname = platform.uname()[0]
NCURSES_SIZE_T = 2**15 - 1
MIN_CONTENT_HEIGHT = 20


class AskConfirm:
    CUT_WARN_MSG = "WARNING: the text was cut because of curses limits."

    def __init__(
        self,
        text: str,
        text_type="diff",
        alternative_text: str = "",
        alternative_text_type: str = "diff",
        allow_force_yes: bool = False,
    ):
        self.text = [text, text_type]
        self.alternative_text = [alternative_text, alternative_text_type]
        self.color_to_curses: Dict[Optional[str], int] = {}
        self.lines: Dict[int, List[TextArgs]] = {}
        self.rows = 0
        self.cols = None
        self.top = 0
        self.left = 0
        self.pad: Optional["curses.window"] = None
        self.screen = None
        self.found_pos: dict[int, list[TextArgs]] = {}
        self.curses_lines = None
        self.debug_prompt = TextArgs("")
        self.page_position = TextArgs("")
        s_force = "/f" if allow_force_yes else ""
        self.prompt = [
            TextArgs("Execute these commands? [Y%s/q] (/ - search, a - patch/cmds)" % s_force, "blue", offset=0),
            self.page_position,
            self.debug_prompt,
        ]

    def _parse_text(self):
        txt = self.text[0]
        txt_split = txt.splitlines()
        # curses pad, который тут используется, имеет ограничение на количество линий
        if (len(txt_split) + 1) >= NCURSES_SIZE_T:  # +1 для того чтобы курсор можно было переместить на пустую строку
            del txt_split[NCURSES_SIZE_T - 3 :]
            txt_split.insert(0, self.CUT_WARN_MSG)
            txt_split.append(self.CUT_WARN_MSG)
            txt = "\n".join(txt_split)
        self.rows = len(txt_split)
        self.cols = max(len(line) for line in txt_split)
        res = text_term_format.curses_format(txt, self.text[1])
        self.lines = res

    def _update_search_pos(self, pattern: str) -> None:
        self.found_pos = {}
        if not pattern:
            return
        try:
            expr = re.compile(pattern)
        except Exception:
            return None
        lines = self.text[0].splitlines()
        for line_no, line in enumerate(lines):
            for match in re.finditer(expr, line):
                if line_no not in self.found_pos:
                    self.found_pos[line_no] = []
                self.found_pos[line_no].append(TextArgs(match.group(0), "highlight", match.start()))

    def _init_colors(self):
        self.color_to_curses = init_colors()

    def _init_pad(self):
        import curses

        with self._store_xy():
            self.pad = curses.newpad(self.rows + 1, self.cols)
            self.pad.keypad(True)  # accept arrow keys
            self._render_to_pad(self.lines)

    def _render_to_pad(self, lines: dict):
        """
        Рендерим данный на pad
        :param lines: словарь проиндексированный по номерам линий
        :return:
        """
        with self._store_xy():
            for line_no, line_data in sorted(lines.items()):
                line_pos_calc = 0
                for line_part in line_data:
                    if line_part.offset is not None:
                        line_pos = line_part.offset
                    else:
                        line_pos = line_pos_calc
                    if line_part.color:
                        self.pad.addstr(line_no, line_pos, line_part.text, self.color_to_curses[line_part.color])
                    else:
                        self.pad.addstr(line_no, line_pos, line_part.text)
                    line_pos_calc += len(line_part.text)

    def _add_prompt(self):
        for prompt_part in self.prompt:
            if not prompt_part:
                continue
            if prompt_part.offset is None:
                offset = 0
            else:
                offset = prompt_part.offset
            self.screen.addstr(self.curses_lines - 1, offset, prompt_part.text, self.color_to_curses[prompt_part.color])

    def _clear_prompt(self):
        with self._store_xy():
            self.screen.move(self.curses_lines - 1, 0)
            self.screen.clrtoeol()

    def show(self):
        self._add_prompt()
        self.screen.refresh()
        size = self.screen.getmaxyx()
        self.pad.refresh(self.top, self.left, 0, 0, size[0] - 2, size[1] - 2)

    @contextmanager
    def _store_xy(self):
        if self.pad is not None:
            current_y, current_x = self.pad.getyx()
            yield current_y, current_x
            max_y, max_x = self.pad.getmaxyx()
            current_y = min(max_y - 1, current_y)
            current_x = min(max_x - 1, current_x)

            self.pad.move(current_y, current_x)
        else:
            yield

    def search_next(self, prev=False):
        to = None
        current_y, current_x = self.pad.getyx()
        if prev:
            for line_index in sorted(self.found_pos, reverse=True):
                for text_args in self.found_pos[line_index]:
                    if line_index > current_y:
                        continue

                    if line_index < current_y or line_index == current_y and text_args.offset < current_x:
                        to = line_index, text_args.offset
                        break
                if to:
                    break
        else:
            for line_index in sorted([i for i in self.found_pos if i >= current_y]):
                for text_args in self.found_pos[line_index]:
                    if line_index > current_y or line_index == current_y and text_args.offset > current_x:
                        to = line_index, text_args.offset
                        break
                if to:
                    break
        if to:
            return to[0] - current_y, to[1] - current_x
        else:
            return 0, 0

    def _search_prompt(self) -> tuple[int, int]:
        import curses

        search_prompt = [TextArgs("Search: ", "green_bold", offset=0)]
        current_prompt = self.prompt
        self.prompt = search_prompt
        with self._store_xy():
            self._clear_prompt()
            self.show()
            curses.echo()
            expr = self.screen.getstr().decode()
            curses.noecho()
            self._update_search_pos(expr)
            self._parse_text()
            self._init_pad()
            # срендерем поверх pad слой с подстветкой
            self._render_to_pad(self.found_pos)
            y_offset, x_offset = self.search_next()
        self.prompt = current_prompt
        return y_offset, x_offset

    def _do_commands(self) -> str:
        import curses

        while True:
            self._clear_prompt()
            try:
                ch = self.pad.getch()
            except KeyboardInterrupt:
                return "n"
            max_y, max_x = self.screen.getmaxyx()
            _, pad_max_x = self.pad.getmaxyx()
            max_y -= 2  # prompt
            y_offset = 0
            x_offset = 0
            margin = 0
            y_delta = 0
            x_delta = 0

            y, x = self.pad.getyx()
            if ch == ord("q"):
                return "exit"
            elif ch in [ord("y"), ord("Y")]:
                return "y"
            elif ch in [ord("f"), ord("F")]:
                return "force-yes"
            elif ch == ord("a"):
                if self.alternative_text:
                    self.text, self.alternative_text = self.alternative_text, self.text
                self.screen.clear()
                self._parse_text()
                self._init_pad()
            elif ch == ord("d"):
                if self.debug_prompt.text == "":
                    self.debug_prompt.text = "init"
                else:
                    self.debug_prompt.text = ""
            elif ch == ord("n"):
                y_offset, x_offset = self.search_next()
                margin = 10
            elif ch == ord("N"):
                y_offset, x_offset = self.search_next(prev=True)
                margin = 10
            elif ch == ord("/"):
                y_offset, x_offset = self._search_prompt()
                margin = 10
            elif ch == curses.KEY_UP:
                y_offset = -1
            elif ch == curses.KEY_PPAGE:
                y_offset = -10
            elif ch == curses.KEY_HOME:
                y_offset = -len(self.lines)
            elif ch == curses.KEY_DOWN:
                y_offset = 1
            elif ch == curses.KEY_NPAGE:
                y_offset = 10
            elif ch == curses.KEY_END:
                y_offset = len(self.lines)
            elif ch == curses.KEY_LEFT:
                x_offset = -1
            elif ch == curses.KEY_RIGHT:
                x_offset = 1

            if y_offset or x_offset:
                y = max(0, y + y_offset)
                y = min(self.rows, y)
                x = max(0, x + x_offset)
                x = min(self.cols, x)

                y_delta = y - (self.top + max_y - margin)
                if y_delta > 0:
                    self.top += y_delta
                elif (y - margin) < self.top:
                    self.top = y

                self.top = min(self.top, len(self.lines) - max_y)

                x_delta = x - (self.left + max_x)
                if x_delta > 0:
                    self.left += x_delta
                elif x < self.left:
                    self.left = x

                x = min(x, pad_max_x - 1)
                self.pad.move(y, x)

            if self.debug_prompt.text != "":
                debug_line = "y=%s x=%s, x_delta=%s y_delta=%s top=%s, max_y=%s max_x=%s lines=%s" % (
                    y,
                    x,
                    x_delta,
                    y_delta,
                    self.top,
                    max_y,
                    max_x,
                    len(self.lines),
                )
                self.debug_prompt.text = debug_line
                self.debug_prompt.color = "green_bold"
                self.debug_prompt.offset = max_x - len(debug_line) - 1

            if self.debug_prompt.text == "":
                self.page_position.color = "highlight"
                self.page_position.text = "line %s/%s" % (y, len(self.lines))
                self.page_position.offset = max_x - len(self.page_position.text) - 1

            self.show()

    def loop(self) -> str:
        import curses

        res = ""
        old_cursor = None
        try:
            self.screen = curses.initscr()
            self.screen.leaveok(True)
            self.curses_lines = curses.LINES  # pylint: disable=maybe-no-member
            curses.start_color()
            curses.noecho()  # no echo key input
            curses.cbreak()  # input with no-enter keyed
            try:
                old_cursor = curses.curs_set(2)
            except Exception:
                pass
            self._init_colors()
            self._parse_text()
            self._init_pad()
            self.pad.move(0, 0)
            self.show()
            res = self._do_commands()
        except Exception as err:
            get_logger().exception("%s", err)
        finally:
            if old_cursor is not None:
                curses.curs_set(old_cursor)
            curses.nocbreak()
            curses.echo()
            curses.endwin()
        return res


def init_colors():
    import curses

    curses.init_pair(1, curses.COLOR_GREEN, curses.COLOR_BLACK)
    curses.init_pair(2, curses.COLOR_CYAN, curses.COLOR_BLACK)
    curses.init_pair(3, curses.COLOR_RED, curses.COLOR_BLACK)
    curses.init_pair(4, curses.COLOR_MAGENTA, curses.COLOR_BLACK)
    curses.init_pair(5, curses.COLOR_YELLOW, curses.COLOR_BLACK)
    curses.init_pair(6, curses.COLOR_BLUE, curses.COLOR_WHITE)
    curses.init_pair(7, curses.COLOR_RED, curses.COLOR_WHITE)
    curses.init_pair(8, curses.COLOR_BLACK, curses.COLOR_WHITE)
    curses.init_pair(9, curses.COLOR_CYAN, curses.COLOR_BLUE)
    return {
        "green": curses.color_pair(1),
        "green_bold": curses.color_pair(1) | curses.A_BOLD,
        "cyan": curses.color_pair(2),
        "red": curses.color_pair(3),
        "magenta": curses.color_pair(4),
        "yellow": curses.color_pair(5),
        "blue": curses.color_pair(6),
        "highlight": curses.color_pair(7),
        None: curses.color_pair(8),
        "cyan_blue": curses.color_pair(9),
    }


@dataclass
class Tile:
    win: "curses.window | None"
    content: list[list[TextArgs]]
    title: list[str]
    height: int
    width: int
    need_draw: bool = True
    total: int = 0
    iteration: int = 0


class UiState(Enum):
    INIT = "INIT"
    OK = "OK"
    WAIT_INPUT = "WAIT_INPUT"
    CLOSED = "CLOSED"


class TailMode(Enum):
    UNIFORM = "UNIFORM"  # display is split into several parts with content and headers
    ONE_CONTENT = "ONE_CONTENT"  # only one part displays content, others show headers
    NO_CONTENT = "NO_CONTENT"  # only headers


class ProgressBars(ProgressBar):
    def __init__(self, tiles_params: dict[str, dict[Any, Any]]):
        self.tiles_params = tiles_params
        self.mode: TailMode = TailMode.UNIFORM
        self.screen: "curses.window | None" = None
        self.tiles: dict[str, Tile] = {}
        self.offset = [0, 0]
        self.terminal_refresher_coro = None
        self.color_to_curses: dict[Optional[str], int] = {}
        self.state: UiState = UiState.INIT
        self.active_tile: int = len(tiles_params) - 1  # tiles with content have numbers from 1

        # context
        self.enter_ok = False
        self._new_stderr = None
        self._saved_stderr_fd = None
        self.progress_length = 10

    def __enter__(self):
        self.enter_ok = False
        sys.stderr.flush()
        self._new_stderr = tempfile.TemporaryFile()
        self._saved_stderr_fd = os.dup(sys.stderr.fileno())
        os.dup2(self._new_stderr.fileno(), sys.stderr.fileno())
        self.enter_ok = True
        try:
            self.init()
        except Exception:
            self.__exit__(*sys.exc_info())
            raise
        return self

    def __exit__(self, exc_type, exc_value, trace):
        try:
            self.stop_curses()
        except Exception:
            pass
        if self.enter_ok:
            sys.stderr.flush()
            self._new_stderr.seek(0)
            stderr_data = self._new_stderr.read().decode()
            os.dup2(self._saved_stderr_fd, sys.stderr.fileno())
            sys.stderr.write(stderr_data)

    def evaluate_mode(self, scree_size):
        height = scree_size[0] // len(self.tiles_params)
        if height > MIN_CONTENT_HEIGHT:
            return TailMode.UNIFORM
        if scree_size[0] - len(self.tiles_params) > MIN_CONTENT_HEIGHT:
            return TailMode.ONE_CONTENT
        if scree_size[0] // len(self.tiles_params) > 1:
            return TailMode.NO_CONTENT
        else:
            return TailMode.NO_CONTENT

    def make_tiles(self):
        import curses

        i = 0
        scree_size = self.screen.getmaxyx()
        scree_size = scree_size[0] - 1, scree_size[1]
        scree_offset = self.screen.getbegyx()
        mode = self.evaluate_mode(scree_size)
        tiles_count = len(self.tiles_params)
        max_height = scree_size[0]
        width = scree_size[1]
        begin_y = 0
        begin_x = 0
        tile_no = 0
        status_bar_win = curses.newwin(1, width, scree_size[0], 0)
        self.tiles["status:"] = Tile(
            win=status_bar_win,
            content=[],
            title=[""],
            height=1,
            width=width,
            need_draw=True,
        )
        max_tile_name_len = max(len(tile_name) for tile_name in self.tiles_params)

        for tile_name in self.tiles_params:
            win = None
            height = 0
            tile_no += 1

            if mode is TailMode.UNIFORM:
                height = int(scree_size[0] // len(self.tiles_params))  # TODO:остаток от деления прибавить к последнему
                win = curses.newwin(height, width, begin_y, begin_x)
            elif mode is TailMode.ONE_CONTENT:
                if i == self.active_tile:
                    height = scree_size[0] - tiles_count + 1
                else:
                    height = 1
                win = curses.newwin(height, width, begin_y, begin_x)
            elif mode is TailMode.NO_CONTENT:
                height = 1

                if tile_no < max_height:
                    begin_y = scree_offset[0] + begin_y
                    get_logger().debug(
                        "newwin height=%s, width=%s, begin_y=%s, begin_x=%s", height, width, begin_y, begin_x
                    )
                    win = curses.newwin(height, width, begin_y, begin_x)
                if tile_no == max_height:
                    left = len(self.tiles_params) - max_height + 1
                    self.tiles["dumb"] = Tile(
                        win=curses.newwin(height, width, begin_y, begin_x),
                        content=[],
                        title=["... and %s more" % left],
                        height=height,
                        width=width,
                        need_draw=True,
                    )

            self.tiles[tile_name] = Tile(
                win=win,
                content=[],
                title=[("{:<%s}" % (max_tile_name_len)).format(tile_name)],
                height=height,
                width=width,
                need_draw=True,
            )
            i += 1
            begin_y += height

    def _next_active_tile(self):
        self._set_active_tile((self.active_tile + 1) % len(self.tiles_params))

    def _prev_active_tile(self):
        self._set_active_tile((self.active_tile - 1) % len(self.tiles_params))

    def _set_active_tile(self, active_tile: int) -> None:
        self.active_tile = active_tile
        if self.mode is TailMode.ONE_CONTENT:
            return

        scree_size = self.screen.getmaxyx()
        scree_offset = self.screen.getbegyx()
        tiles_count = len(self.tiles_params)
        width = scree_size[1]
        begin_y = scree_offset[0]
        for n, dev in enumerate(self.tiles_params):
            tile = self.tiles[dev]
            if n == self.active_tile:
                height = scree_size[0] - tiles_count
            else:
                height = 1
            tile.height = height
            tile.win.resize(height, width)
            tile.win.mvwin(begin_y, scree_offset[1])
            begin_y += tile.height
            tile.need_draw = True
        self.refresh_all()

    def set_status(self):
        total = 0
        iteration = 0
        done_percent = 0
        for tile_name, tile in self.tiles.items():
            if tile_name == "status:":
                continue
            total += tile.total
            iteration += tile.iteration
        if total > 0 and iteration > 0:
            done_percent = float(iteration) / total * 100

        if done_percent == 100:
            msg = "All is done. Press q for exit."
        else:
            msg = "Deploying... %3.0f%%" % math.floor(done_percent)
        self.set_title("status:", msg)

    def start_terminal_refresher(self, max_refresh_rate=200):
        if self.terminal_refresher_coro:
            return
        self.terminal_refresher_coro = asyncio.ensure_future(self._terminal_refresher(max_refresh_rate))

    def stop_terminal_refresher(self):
        if not self.terminal_refresher_coro:
            return
        self.terminal_refresher_coro.cancel()
        self.terminal_refresher_coro = None
        self.refresh_all()

    async def _terminal_refresher(self, max_refresh_rate: int):
        sleep = 1 / max_refresh_rate
        try:
            while True:
                await asyncio.sleep(sleep)
                self.refresh_all()
        except asyncio.CancelledError:
            return

    def init(self):
        import curses

        self.screen = curses.initscr()
        self.screen.nodelay(True)  # getch() will be non-blocking
        curses.noecho()
        curses.cbreak()
        scree_size = self.screen.getmaxyx()
        get_logger().debug(
            "orig scree_size y=%s, x=%s offset=%s %s", scree_size[0], scree_size[1], self.offset[0], self.offset[1]
        )

        if self.offset[0] == 0:
            new_y = 0
            nlines = scree_size[0]
        elif self.offset[0] < 0:
            nlines = abs(self.offset[0])
            new_y = scree_size[0] - nlines
        else:
            nlines = self.offset[0]
            new_y = scree_size[0]

        if self.offset[1] == 0:
            new_x = 0
            ncols = scree_size[1]
        elif self.offset[1] < 0:
            ncols = abs(self.offset[1])
            new_x = scree_size[1] - ncols
        else:
            ncols = self.offset[1]
            new_x = scree_size[1]

        get_logger().debug("nlines=%s, ncols=%s, new_y=%s, new_x=%s", nlines, ncols, new_y, new_x)

        self.screen.mvwin(new_y, new_x)

        curses.curs_set(0)
        curses.start_color()
        self.color_to_curses = init_colors()
        self.state = UiState.OK
        self.make_tiles()
        self.progress_length = scree_size[1] // 3

    def stop_curses(self):
        import curses

        curses.curs_set(1)
        curses.nocbreak()
        curses.echo()
        curses.endwin()
        self.state = UiState.CLOSED

    def draw_content(self, tile_name):
        tile = self.tiles[tile_name]
        win = tile.win
        size = win.getmaxyx()
        margin = 1
        if (size[0] - 2 * margin) <= 0:
            return
        draw_lines_in_win(tile.content, win, color_to_curses=self.color_to_curses, margin=margin)

    def draw_title(self, tile_name):
        tile = self.tiles[tile_name]
        title = tile.title
        win = tile.win
        if not isinstance(title, (tuple, list)):
            title = [title]
        draw_lines_in_win([title], win, color_to_curses=self.color_to_curses, x_margin=1)

    def refresh(self, tile_name: str, noutrefresh: bool = False):
        # see noutrefresh in curses doc
        tile = self.tiles[tile_name]
        win = tile.win
        if not tile.need_draw or win is None:
            return
        win.clear()
        if tile.height > 1:
            win.border()
        self.draw_title(tile_name)
        self.draw_content(tile_name)
        if noutrefresh:
            win.noutrefresh()
        else:
            win.refresh()
        tile.need_draw = False

    def refresh_all(self):
        if self.state is UiState.CLOSED:
            return
        if self.state is UiState.OK:
            ch_list = self.get_pressed_keys()
            if "\t" in ch_list:  # Tab
                self._next_active_tile()
            if "\x1b[Z" in ch_list:  # shift-Tab
                self._prev_active_tile()
        self.screen.refresh()
        self.set_status()
        tile_name = None
        for tile_name in self.tiles:
            self.refresh(tile_name, True)
        if tile_name is not None:
            self.refresh(tile_name, False)

    def set_title(self, tile_name, title):
        tile = self.tiles[tile_name]
        # в 0 элементе хранится выровненный хостнейм
        title0 = tile.title[0]
        new_title = [title0, title]
        if new_title == tile.title:
            return
        tile.title = new_title
        tile.need_draw = True
        if not self.terminal_refresher_coro:
            self.refresh(tile_name)

    def set_content(self, tile_name: str, content: str):
        tile = self.tiles[tile_name]
        new_content = list(text_term_format.curses_format(content, "switch_out").values())
        if new_content == tile.content:
            return
        tile.need_draw = True
        tile.content = new_content
        if not self.terminal_refresher_coro:
            self.refresh(tile_name)

    def add_content(self, tile_name: str, content: str):
        tile = self.tiles[tile_name]
        tile.need_draw = True
        tile.content.extend(text_term_format.curses_format(content, "switch_out").values())
        if not self.terminal_refresher_coro:
            self.refresh(tile_name)

    def reset_content(self, tile_name: str):
        tile = self.tiles[tile_name]
        tile.need_draw = True
        tile.content = []
        if not self.terminal_refresher_coro:
            self.refresh(tile_name)

    def set_progress(
        self,
        tile_name: str,
        iteration: int,
        total: int,
        prefix="",
        suffix="",
        fill="█",
        error=False,
    ):
        """
        Call in a loop to create terminal progress bar
        @params:
            iteration   - Required  : current iteration (Int)
            total       - Required  : total iterations (Int)
            prefix      - Optional  : prefix string (Str)
            suffix      - Optional  : suffix string (Str)
            length      - Optional  : character length of bar (Int)
            fill        - Optional  : bar fill character (Str)
        """
        if uname == "FreeBSD":
            fill = "#"
        percent = "{0:.1f}".format(100 * (iteration / float(total)))
        filled_length = int(self.progress_length * iteration // total)
        bar = fill * filled_length + "-" * (self.progress_length - filled_length)
        res = "%s |%s| %s%% %s" % (prefix, bar, percent, suffix)
        tile = self.tiles[tile_name]
        tile.total = total
        tile.iteration = iteration
        if error:
            res = TextArgs(res, "red")
        else:
            res = TextArgs(res, "cyan")
        self.set_title(tile_name, res)

    def set_exception(self, tile_name: str, cmd_exc: str, last_cmd: str, progress_max: int, content: str = ""):
        suffix = "cmd error: %s %s" % (str(cmd_exc).strip().replace("\n", "--"), last_cmd)
        self.set_progress(tile_name, progress_max, progress_max, suffix=suffix, error=True)
        if content:
            self.add_content(tile_name, content)

    def get_pressed_keys(self):
        ch_list = ""
        while True:
            try:
                ch = self.screen.getkey()
                ch_list += ch
            except Exception:
                time.sleep(0.01)
                break
        return ch_list

    async def wait_for_exit(self):
        self.state = UiState.WAIT_INPUT
        while True:
            ch_list = self.get_pressed_keys()
            if ch_list:
                get_logger().debug("read ch %s", repr(ch_list))
                if "\t" in ch_list:  # Tab
                    self._next_active_tile()
                if "\x1b[Z" in ch_list:  # shift-Tab
                    self._prev_active_tile()
                if "q" in ch_list:
                    return
            else:
                await asyncio.sleep(0.001)


def draw_lines_in_win(
    lines: list[list[TextArgs]],
    win: "curses.window | None",
    color_to_curses: dict[Optional[str], int],
    margin: int = 0,
    x_margin: int = 0,
    y_margin: int = 0,
) -> None:
    if win is None:
        return
    max_y, max_x = win.getmaxyx()
    max_y -= 2 * (margin or y_margin)
    max_x -= 2 * (margin or x_margin)
    lines_count = len(lines)
    if lines_count > max_y:
        start_line = lines_count - max_y
    else:
        start_line = 0
    for line_no, line_data in enumerate(lines[start_line:]):
        line_pos_calc = 0
        for line_part in line_data:
            if not isinstance(line_part, TextArgs):
                line_part = TextArgs(line_part)
            if line_part.offset is not None:
                line_pos = line_part.offset
            else:
                line_pos = line_pos_calc
            y = (margin or y_margin) + line_no
            x = (margin or x_margin) + line_pos
            max_line_len = max_x - x + 1
            text = line_part.text[0:max_line_len]
            try:
                if line_part.color:
                    win.addstr(y, x, text, color_to_curses[line_part.color])
                else:
                    win.addstr(y, x, text)
            except Exception as exp:
                get_logger().error("y=%s, x=%s, text=%s %s", y, x, text, exp)
            line_pos_calc += len(text)
