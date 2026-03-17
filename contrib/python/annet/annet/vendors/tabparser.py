from __future__ import annotations

import dataclasses
import itertools
import json
import re
import textwrap
from collections import OrderedDict as odict
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any, Dict, Generic, Iterable, List, Optional, Tuple, TypeAlias

from annet.annlib.types import Op


if TYPE_CHECKING:
    from annet.annlib.patching import PatchTree


# =====
class ParserError(Exception):
    pass


# =====
class _CommentOrEmpty:
    pass


class BlockBegin:
    pass


class BlockEnd:
    pass


RowWithContext = Tuple[str, Optional[Dict[str, Any]]]


def block_wrapper(value: Any) -> Iterable[Any]:
    yield from iter((BlockBegin, value, BlockEnd))


@dataclasses.dataclass
class FormatterContext:
    parent: Optional["FormatterContext"] = None

    prev: Optional[RowWithContext] = None
    current: Optional[RowWithContext] = None
    next: Optional[RowWithContext] = None

    @property
    def level(self) -> int:
        if self.parent is None:
            return 0
        return self.parent.level + 1

    @property
    def row_prev(self) -> Optional[str]:
        return self.prev and self.prev[0]

    @property
    def row(self) -> Optional[str]:
        return self.current and self.current[0]

    @property
    def row_next(self) -> Optional[str]:
        return self.next and self.next[0]


class NotUniquePatch:
    def __init__(self):
        """In the case of comments, odict is not suitable: there may be several identical edit and exit"""
        self._items = []
        self._keys = set()

    def __setitem__(self, key, value):
        self._keys.add(key)
        self._items.append((key, value))

    def keys(self):
        return list(self)

    def items(self):
        return self._items

    def __contains__(self, item):
        return item in self._keys

    def __iter__(self):
        return iter(item[0] for item in self._items)

    def __bool__(self) -> bool:
        return bool(self._items)


# =====
class CommonFormatter:
    cmd_path_cls = odict

    def __init__(self, indent="  "):
        self._indent = indent
        self._block_begin = ""
        self._block_end = ""
        self._statement_end = ""

    def split(self, text: str):
        return list(filter(None, text.split("\n")))

    def join(self, config: PatchTree) -> str:
        return "\n".join(_filtered_block_marks(self._indent_blocks(self._blocks(config, is_patch=False))))

    def diff_generator(self, diff):
        yield from self._diff_lines(diff)

    def diff(self, diff):
        return list(self.diff_generator(diff))

    def patch(self, patch: "PatchTree") -> str:
        return "\n".join(_filtered_block_marks(self._indent_blocks(self._blocks(patch, is_patch=True))))

    def cmd_paths(self, patch: "PatchTree") -> odict:
        ret = self.cmd_path_cls()
        path = []
        for row, context in self.blocks_and_context(patch, is_patch=True):
            if row is BlockBegin:
                path.append(path[-1])
            elif row is BlockEnd:
                path.pop()
            else:
                if path:
                    path.pop()
                path.append(row)
                ret[tuple(path)] = context
        return ret

    def patch_plain(self, patch):
        return list(self.cmd_paths(patch).keys())

    def _diff_lines(self, diff, _level=0, _block_sign=None):
        sign_map = {
            Op.REMOVED: "-",
            Op.ADDED: "+",
            Op.MOVED: ">",
            Op.AFFECTED: " ",
        }
        for flag, row, children, _ in diff:
            sign = sign_map[flag]
            if not children:
                yield "%s %s%s" % (sign, self._indent * _level, row + self._statement_end)
            else:
                yield "%s %s%s" % (sign, self._indent * _level, row + self._block_begin)
                yield from self._diff_lines(children, _level + 1, sign)
        if _level > 0 and self._block_end and _block_sign is not None:
            yield "%s %s%s" % (_block_sign, self._indent * (_level - 1), self._block_end)

    def _indented_blocks(self, tree):
        return self._indent_blocks(self._blocks(tree, False))

    def _indent_blocks(self, blocks):
        _level = 0
        for row in blocks:
            if row is BlockBegin:
                _level += 1
            elif row is BlockEnd:
                _level -= 1
            else:
                row = self._indent * _level + row
            yield row

    def blocks_and_context(self, tree: "PatchTree", is_patch: bool, context: Optional[FormatterContext] = None):
        if context is None:
            context = FormatterContext()

        if is_patch:
            items = [(item.row, item.child, item.context) for item in tree.itms]
        else:
            items = [(row, child, {}) for row, child in tree.items()]

        n = len(items)
        for i in range(n):
            prev_row, prev_sub_config, prev_row_context = items[i - 1] if i > 0 else (None, None, None)
            row, sub_config, row_context = items[i]
            next_row, next_sub_config, next_row_context = items[i + 1] if i + 1 < n else (None, None, None)

            context.current = (row, row_context)
            context.prev = (prev_row, prev_row_context) if prev_row else None
            context.next = (next_row, next_row_context) if next_row else None

            yield row, row_context

            if sub_config or (is_patch and sub_config is not None):
                yield BlockBegin, None
                yield from self.blocks_and_context(sub_config, is_patch, context=FormatterContext(parent=context))
                yield BlockEnd, None

    def _blocks(self, tree: "PatchTree", is_patch: bool):
        for row, _context in self.blocks_and_context(tree, is_patch):
            yield row


class BlockExitFormatter(CommonFormatter):
    def __init__(self, block_exit, no_block_exit=(), indent="  "):
        super().__init__(indent)
        self._block_exit = block_exit
        self._no_block_exit = tuple(no_block_exit)

    def split_remove_spaces(self, text):
        # эта регулярка заменяет 2 и более пробела на один, но оставляет пробелы в начале линии
        text = re.sub(r"(?<=\S)\ {2,}(?=\S)", " ", text)
        res = super().split(text)
        return res

    def block_exit(self, context: Optional[FormatterContext]) -> Iterable[Any]:
        current = context and context.row
        if current and not current.startswith(self._no_block_exit):
            yield from block_wrapper(self._block_exit)

    def blocks_and_context(self, tree, is_patch, context: Optional[FormatterContext] = None):
        if context is None:
            context = FormatterContext()

        level = context.level
        block_level = level

        last_row_context = {}
        for row, row_context in super().blocks_and_context(tree, is_patch, context=context):
            yield row, row_context
            if row_context is not None:
                last_row_context = row_context

            if row is BlockBegin:
                block_level += 1
            elif row is BlockEnd:
                block_level -= 1

            if row is BlockEnd and block_level == level and is_patch:
                for exit_statement in filter(None, self.block_exit(context)):
                    yield exit_statement, last_row_context


class HuaweiPatch(NotUniquePatch):
    policy_end_blocks = ("end-list", "endif", "end-filter")

    def __setitem__(self, key, value):
        if not key:
            return

        if key not in self or (key[0].startswith("xpl") and key[-1] in self.policy_end_blocks):
            super().__setitem__(key, value)


class HuaweiFormatter(BlockExitFormatter):
    cmd_path_cls = HuaweiPatch

    def __init__(self, indent="  "):
        super().__init__(
            block_exit="quit",
            no_block_exit=[
                "rsa peer-public-key",
                "dsa peer-public-key",
                "public-key-code begin",
            ],
            indent=indent,
        )

    def split(self, text):
        # на старых прошивка наблюдается баг с двумя пробелами в этом месте в конфиге
        # например на VRP V100R006C00SPC500 + V100R006SPH003
        tree = self.split_remove_spaces(text)
        tree[:] = filter(lambda x: not str(x).strip().startswith(HuaweiPatch.policy_end_blocks), tree)
        return tree

    def block_exit(self, context: Optional[FormatterContext]):
        row = context and context.row or ""
        row_next = context and context.row_next
        parent_row = context and context.parent and context.parent.row or ""

        if row.startswith("xpl route-filter"):
            yield from block_wrapper("end-filter")
            return

        if row.startswith("xpl"):
            yield from block_wrapper("end-list")
            return

        if parent_row.startswith("xpl route-filter"):
            if (row.startswith(("if", "elseif")) and row.endswith("then")) and (
                not row_next or not row_next.startswith(("elseif", "else"))
            ):
                yield "endif"
            elif row == "else":
                yield "endif"
            return

        yield from super().block_exit(context)


class OptixtransFormatter(CommonFormatter):
    pass


class CiscoFormatter(BlockExitFormatter):
    def __init__(self, indent="  "):
        super().__init__("exit", indent)

    def _split_indent(self, line: str, indent: int, block_exit_strings: List[str]) -> Tuple[List[str], int]:
        """
        The small helper calculates indent shift based on block exit string.
        If configuration line has non-default block exit string it means that
        new subsection is started and indent should be increased.
        If configuration line exists in list of block exit strings,
        it means that subsection is finished and indent should be decreased

        Args:
            line: just configuration line
            indent: current indent
            block_exit_strings: list of previously seen block exit strings
        Returns:
            new indent and list of previously seen block exit strings
        """

        if line.strip() in block_exit_strings:
            indent -= 1
            block_exit_strings.remove(line.strip())
            return block_exit_strings, indent

        block_exit_wrapped = [v for v in self.block_exit(FormatterContext(current=(line.strip(), {})))]

        if not block_exit_wrapped or len(block_exit_wrapped) != 3:
            return block_exit_strings, indent
        if not isinstance(block_exit_wrapped[1], str):
            return block_exit_strings, indent
        if block_exit_wrapped[1] == self._block_exit:
            return block_exit_strings, indent

        indent += 1
        block_exit_strings.append(block_exit_wrapped[1])
        return block_exit_strings, indent

    def split(self, text):
        additional_indent = 0
        block_exit_strings = [self._block_exit]

        # hide banner content
        pattern = re.compile(r"((^banner [a-z-]+) \^C.*?\^C)", flags=re.MULTILINE | re.DOTALL)
        repl_map = {replace_str: banner_str for banner_str, replace_str in pattern.findall(text)}
        for replace_str, banner in repl_map.items():
            text = text.replace(banner, replace_str)

        tree = self.split_remove_spaces(text)
        for i, item in enumerate(tree):
            # fix incorrect indent for "exit-address-family"
            if item == " exit-address-family":
                item = "  exit-address-family"
            # fix incorrect indent for "class-map match.*"
            if i and tree[i - 1].startswith("class-map match"):
                if item.startswith("  description"):
                    item = item[1:]
            block_exit_strings, new_indent = self._split_indent(item, additional_indent, block_exit_strings)
            tree[i] = f"{' ' * additional_indent}{item}"
            additional_indent = new_indent

        # restore banner content in the `tree`
        for i, item in enumerate(tree):
            if item in repl_map:
                tree[i] = repl_map[item]

        return tree

    def block_exit(self, context: Optional[FormatterContext]) -> str:
        current = context and context.row or ""

        if current.startswith(("address-family")):
            yield from block_wrapper("exit-address-family")
        else:
            yield from super().block_exit(context)


class NexusFormatter(BlockExitFormatter):
    def __init__(self, indent="  "):
        super().__init__("exit", indent)

    def split(self, text):
        return self.split_remove_spaces(text)


class B4comFormatter(BlockExitFormatter):
    def __init__(self, indent="  "):
        super().__init__("exit", indent)

    def _fix_af_indentation(self, text: str) -> str:
        """Fixes the indentation of address-family blocks in B4COM configuration.
        This method ensures that lines within address-family blocks are indented correctly.
        It adds one space before each line inside the address-family block.
        For some reason, B4COM displays commands in address-family blocks without indentation,
        but this is not correct; they are part of the address-family context.

        Fixes this:
        address-family ipv6 unicast
        max-paths ebgp 48
        exit-address-family
        exit

        To this:
        address-family ipv6 unicast
         max-paths ebgp 48
         exit-address-family
        exit
        """
        result = []
        inside_af = False
        af_re = re.compile(r"^\s*address-family\s+.*")
        exit_af_re = re.compile(r"^\s*exit-address-family")

        for line in text.splitlines():
            if af_re.match(line):
                inside_af = True
                result.append(line)
                continue

            if exit_af_re.match(line):
                result.append(" " + line)
                inside_af = False
                continue

            if inside_af and line.strip():
                result.append(" " + line)
            else:
                result.append(line)

        return "\n".join(result)

    def split(self, text):
        text = self._fix_af_indentation(text)
        return self.split_remove_spaces(text)

    def block_exit(self, context: Optional[FormatterContext]) -> str:
        current = context and context.row or ""

        if current.startswith(("address-family")):
            yield from block_wrapper("exit-address-family")
        else:
            yield from super().block_exit(context)


class ArubaFormatter(BlockExitFormatter):
    def __init__(self, indent="  "):
        super().__init__("exit", indent)

    def split(self, text):
        return self.split_remove_spaces(text)


class AristaFormatter(BlockExitFormatter):
    def __init__(self, indent="  "):
        super().__init__("exit", indent)

    def split(self, text):
        return self.split_remove_spaces(text)


class AsrFormatter(BlockExitFormatter):
    def __init__(self, indent="  "):
        super().__init__("exit", indent)

    def split(self, text):
        policy_end_blocks = ("end-set", "endif", "end-policy")
        tree = self.split_remove_spaces(text)
        tree[:] = filter(lambda x: not x.endswith(policy_end_blocks), tree)
        return tree

    def block_exit(self, context: Optional[FormatterContext]) -> str:
        current = context and context.row or ""

        if current.startswith(("prefix-set", "as-path-set", "community-set")):
            yield from block_wrapper("end-set")
        elif current.startswith("if") and current.endswith("then"):
            yield from block_wrapper("endif")
        elif current.startswith("route-policy"):
            yield from block_wrapper("end-policy")
        else:
            yield from super().block_exit(context)


class JuniperFormatter(CommonFormatter):
    patch_set_prefix = "set"
    cmd_path_cls = NotUniquePatch

    @dataclasses.dataclass
    class Comment:
        begin = "/*"
        end = "*/"

        row: str
        comment: str

        def __post_init__(self):
            self.row = " ".join(map(lambda x: x.strip("\"'"), self.row.strip().split(" ")))
            self.comment = self.comment.strip()

        @classmethod
        def loads(cls, value: str):
            return cls(**json.loads(value.removeprefix(cls.begin).removesuffix(cls.end).strip()))

        def dumps(self):
            return json.dumps({"row": self.row, "comment": self.comment})

    def __init__(self, indent="    "):
        super().__init__(indent)
        self._block_begin = " {"
        self._block_end = "}"
        self._statement_end = ";"
        self._endofline_comment = "; ##"

        self._sub_regexs = (
            (re.compile(self._block_begin + r"\s*" + self._block_end + r"$"), ""),  # collapse empty blocks
            (re.compile(self._block_begin + r"\s*" + r"(\t# .+)?$"), ""),
            (re.compile(self._statement_end + r"$"), ""),
            (re.compile(r"\s*" + self._block_end + "(\t# .+)?$"), ""),
            (re.compile(self._endofline_comment + r".*$"), ""),
        )

    def sub_regexs(self, value: str) -> str:
        for regex, repl_line in self._sub_regexs:
            value = regex.sub(repl_line, value)
        return value

    def split(self, text: str) -> list[str]:
        comment_begin, comment_end = map(re.escape, (self.Comment.begin, self.Comment.end))
        comment_regexp = re.compile(rf"(\s+{comment_begin})((?:(?!{comment_end}).)*)({comment_end})")

        result = []
        lines = text.split("\n")
        for i, line in enumerate(lines):
            line = self.sub_regexs(line)
            if i + 1 < len(lines) and (m := comment_regexp.match(line)):
                line = f"{m.group(1)} {self.Comment(self.sub_regexs(lines[i + 1]), m.group(2)).dumps()} {m.group(3)}"
            result.append(line)

        return list(filter(None, result))

    def join(self, config):
        return "\n".join(_filtered_block_marks(self._formatted_blocks(self._indented_blocks(config))))

    def patch(self, patch):
        return "\n".join(" ".join(x) for x in self.cmd_paths(patch))

    def patch_plain(self, patch):
        return list(self.cmd_paths(patch).keys())

    def _blocks(self, tree: "PatchTree", is_patch: bool):
        for row in super()._blocks(tree, is_patch):
            if isinstance(row, str) and row.startswith(self.Comment.begin):
                yield f"{self.Comment.begin} {self.Comment.loads(row).comment} {self.Comment.end}"
            else:
                yield row

    def _formatted_blocks(self, blocks):
        level = 0
        line = None
        for new_line in blocks:
            if new_line is BlockBegin:
                level += 1
                if isinstance(line, str):
                    yield line + self._block_begin
            elif new_line is BlockEnd:
                level -= 1
                if isinstance(line, str):
                    yield line + ("" if line.endswith(self.Comment.end) else self._statement_end)
                yield self._indent * level + self._block_end
            elif isinstance(line, str):
                yield line + ("" if line.endswith(self.Comment.end) else self._statement_end)
            line = new_line
        if isinstance(line, str):
            yield line + self._statement_end

    def cmd_paths(self, patch, _prev=tuple()):
        commands = self.cmd_path_cls()

        for item in patch.itms:
            key, childs, context = item.row, item.child, item.context

            if childs:
                for k, v in self.cmd_paths(childs, (*_prev, key.strip())).items():
                    commands[k] = v
            else:
                if "comment" in context:
                    value = (
                        ""
                        if key.startswith("delete")
                        else key.removeprefix(self.Comment.begin).removesuffix(self.Comment.end).strip()
                    )
                    cmds = (
                        f"edit {' '.join(_prev)}",
                        " ".join(("annotate", context["row"].split(" ")[0], f'"{value}"')),
                        "exit",
                    )
                elif key.startswith("delete"):
                    cmds = (" ".join(("delete", *_prev, key.replace("delete", "", 1).strip())),)
                elif key.startswith("activate"):
                    cmds = (" ".join(("activate", *_prev, key.replace("activate", "", 1).strip())),)
                elif key.startswith("deactivate"):
                    cmds = (" ".join(("deactivate", *_prev, key.replace("deactivate", "", 1).strip())),)
                else:
                    cmds = (" ".join((self.patch_set_prefix, *_prev, key.strip())),)

                # Expanding [ a b c ] junipers list of arguments
                for cmd in cmds:
                    if matches := re.search(r"^(.*)\s+\[(.+)\]$", cmd):
                        for c in matches.group(2).split(" "):
                            if c.strip():
                                items = " ".join([matches.group(1), c])
                                commands[(items,)] = context
                    else:
                        commands[(cmd,)] = context

        return commands


class RibbonFormatter(JuniperFormatter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._endofline_comment = "; # SECRET-DATA"


class JuniperList:
    """
    Форматирует inline-листы в конфиге juniper
    """

    def __init__(self, *args, spaces=True, **kwargs):
        self._items = list(*args, **kwargs)
        self.spaces = spaces

    def __str__(self):
        if self.spaces:
            return "[ %s ]" % " ".join(str(_) for _ in self._items)
        else:
            return "[%s]" % " ".join(str(_) for _ in self._items)


class NokiaFormatter(JuniperFormatter):
    patch_set_prefix = "/configure"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._statement_end = ""
        self._endofline_comment = " ##"

    def split(self, text):
        ret = super().split(text)
        # NOCDEVDUTY-248 сдергиваем верхний configure-блок
        # NOCDEVDUTY-282 после configure {} блока могут идти еще блоки которые нам не нужны
        start, finish = None, None
        for i, line in enumerate(ret):
            if line.startswith("#"):
                continue
            # начало configure-блока
            if line == "configure":
                start = i + 1
            # любой после configure последующий блок на глобальном уровне
            elif len(line) == len(line.lstrip()):
                if start is not None and finish is None:
                    finish = i
        # Если configure-блока не было - то весь конфиг считаем configre'ом
        start = start if start is not None else 0
        finish = finish if finish is not None else len(ret)
        return ret[start:finish]

    def cmd_paths(self, patch, _prev=tuple()):
        commands = odict()
        for item in patch.itms:
            key, childs, context = item.row, item.child, item.context
            if childs:
                for k, v in self.cmd_paths(childs, (*_prev, key.strip())).items():
                    commands[k] = v
            else:
                if key.startswith("delete"):
                    cmd = " ".join((self.patch_set_prefix, "delete", *_prev, key.replace("delete", "", 1).strip()))
                else:
                    cmd = " ".join((self.patch_set_prefix, *_prev, key.strip()))
                # Expanding [ a b c ] junipers list of arguments
                matches = re.search(r"^(.*)\s+\[(.+)\]$", cmd)
                if matches:
                    for c in matches.group(2).split(" "):
                        if c.strip():
                            cmd = " ".join([matches.group(1), c])
                            commands[(cmd,)] = context
                else:
                    commands[(cmd,)] = context
        return commands


class RosFormatter(CommonFormatter):
    patch_set_prefix = "set "

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._block_begin = "/"

    def join(self, config):
        return "\n".join(_filtered_block_marks(self._formatted_blocks(self._indented_blocks(config))))

    def patch(self, patch):
        return "\n".join(" ".join(x) for x in self.cmd_paths(patch))

    def patch_plain(self, patch):
        return list(self.cmd_paths(patch).keys())

    def blocks_and_context(self, tree: "PatchTree", is_patch: bool, context: Optional[FormatterContext] = None):
        if is_patch:
            raise RuntimeError("Ros not supported blocks in patch")

        rows = []

        items = ((row, child, {}) for row, child in tree.items())
        for row, sub_config, row_context in items:
            rows.append((row, sub_config if sub_config else None, row_context))

        for sub_config, row_group in itertools.groupby(rows, lambda x: x[1]):
            if sub_config is None:
                blocks, leaf = [], context
                while leaf:
                    if leaf.current:
                        blocks.append(leaf.current[0])
                    leaf = leaf.parent

                yield " ".join(reversed(blocks)), None
                yield BlockBegin, None
                for row, _, row_context in row_group:
                    yield row, row_context
                yield BlockEnd, None
            else:
                for row, _, row_context in row_group:
                    yield from self.blocks_and_context(
                        sub_config, is_patch, context=FormatterContext(parent=context, current=(row, row_context))
                    )

    def _formatted_blocks(self, blocks):
        line = None
        for new_line in blocks:
            if new_line is BlockBegin:
                if isinstance(line, str):
                    yield self._block_begin + line.strip()
            elif isinstance(line, str):
                yield line
            line = new_line

    def _splitter_file(self, lines):
        filedesrc_re = re.compile(
            r"^\s+(?P<num>\d+)\s+name=\"(?P<name>[^\"]+)\"\s+type=\"(?P<type>[^\"]+)\""
            r"\s+(size=(?P<size>.*))?creation-time=(?P<time>.*?)(contents=(?P<content>.*)?)?$"
        )
        file_content_indent = re.compile(r"^\s{5}")
        out = []
        files = {}
        curfile = None
        for line in lines:
            match = filedesrc_re.search(line)
            if match:
                if match.group("type").strip() == ".txt file":
                    curfile = match.group("name")
                    files[curfile] = {"name": curfile, "contents": []}
                    if match.group("content"):
                        files[curfile]["contents"].append(match.group("content").strip())
            elif curfile and file_content_indent.match(line):
                files[curfile]["contents"].append(file_content_indent.sub("", line))
        for file in files.values():
            out.append(f"print file={file['name']}")
            if len(file["contents"]) > 0:
                text = "\n".join(file["contents"])
                out.append(f'set {file["name"]} contents="{text}"')
        return out

    def _splitter_user_ssh_keys(self, lines):
        keydescr_re = re.compile(r"user=(?P<user>\w+).*key-owner=(?P<owner>.*)$")
        out = []
        for line in lines:
            match = keydescr_re.search(line)
            if match:
                out.append(f"import public-key-file={match.group('owner')}.ssh_key.txt user={match.group('user')}")

        return out

    def split(self, text):
        split = []
        level = 0
        postj = {}
        curgroup = None
        for line in text.split("\n"):
            if line.startswith("/"):
                if curgroup:
                    for row in getattr(self, curgroup)(postj[curgroup]):
                        if level > 0:
                            row = row.strip()
                        split.append(self._indent * level + row)

                level = 0
                for group in line.split():
                    split.append(self._indent * level + group.replace("/", ""))
                    level += 1

                gpath = line.replace("/", "_splitter_").replace(" ", "_").replace("-", "_")
                if hasattr(self, gpath):
                    postj[gpath] = []
                    curgroup = gpath
                else:
                    curgroup = None
            else:
                row = line
                if curgroup:
                    postj[curgroup].append(row)
                else:
                    if level > 0:
                        row = line.strip()
                    split.append(self._indent * level + row)
        if curgroup:
            for row in getattr(self, curgroup)(postj[curgroup]):
                if level > 0:
                    row = row.strip()
                split.append(self._indent * level + row)
        return list(filter(None, split))

    def cmd_paths(self, patch, _prev=""):
        rm_regexs = (
            (re.compile(r"^add "), ""),
            (re.compile(r"^print file="), "name="),
        )

        commands = odict()
        for item in patch.itms:
            key, childs, context = item.row, item.child, item.context
            if childs:
                childs_prev = f"{_prev} {key.strip()}".lstrip()
                commands.update(self.cmd_paths(childs, _prev=childs_prev))
            else:
                if key.startswith("remove"):
                    find_cmd = key.removeprefix("remove").strip()
                    for regex, repl_line in rm_regexs:
                        find_cmd = regex.sub(repl_line, find_cmd)
                    cmd = f"/{_prev} remove [ find {find_cmd} ]"
                else:
                    cmd = f"/{_prev} {key}"
                commands[(cmd,)] = context

        return commands


# ====


def parse_to_tree(text: str, splitter: Callable[[str], Iterator[str]], comments: Iterable[str] = ("!", "#")):
    tree = odict()
    for stack in _stacked(splitter(text), tuple(comments)):
        local_tree = tree
        for key in stack:
            if key not in local_tree:
                local_tree[key] = odict()
            local_tree = local_tree[key]
    return tree


SimpleTree: TypeAlias = list[tuple[str, "SimpleTree"]]


def parse_to_tree_multi(
    text: str, splitter: Callable[[str], Iterator[str]], comments: Iterable[str] = ("!", "#")
) -> SimpleTree:
    tree = []
    for stack in _stacked(splitter(text), tuple(comments)):
        local_tree = tree
        for key in stack:
            if not local_tree or local_tree[-1][0] != key:
                local_tree.append((key, []))
            local_tree = local_tree[-1][1]
    return tree


# =====
def _stacked(lines: Iterator[str], comments: tuple[str, ...]) -> Iterator[tuple[str, ...]]:
    stack: list[str] = []
    for level, line in _stripped_indents(lines, comments):
        level += 1
        if level > len(stack):
            stack.append(line)
        elif level == len(stack):
            stack[-1] = line
        else:
            stack = stack[: level - 1] + [line]
        yield tuple(stack)


def _stripped_indents(lines: Iterator[str], comments: tuple[str, ...]) -> Iterator[tuple[int, str]]:
    indents: list[int] = []
    curr_level = 0
    g_level = None

    for number, (level, line) in enumerate(_parsed_indents(lines, comments), start=1):
        if isinstance(line, str):
            if g_level is None:
                g_level = level
            level = level - (g_level or 0)
            if level < 0:
                raise ParserError("Invalid top indentation: line %d: %s" % (number, line))

            if level > curr_level:
                indents.append(level - curr_level)
                curr_level += level - curr_level
            elif level < curr_level:
                while curr_level > level and len(indents):
                    curr_level -= indents.pop()
                if curr_level != level:
                    raise ParserError("Invalid top indentation: line %d: %s" % (number, line))

            yield (len(indents), line)

        elif line is BlockEnd:
            indents = []
            curr_level = 0
            g_level = None


def _parsed_indents(
    lines: Iterator[str], comments: tuple[str, ...]
) -> Iterator[tuple[int, str | type[BlockEnd] | type[_CommentOrEmpty]]]:
    for line in _filtered_lines(lines, comments):
        if isinstance(line, str):
            yield (_parse_indent(line), line.strip())
        else:
            yield (0, line)


def _filtered_lines(
    lines: Iterator[str], comments: tuple[str, ...]
) -> Iterator[str | type[BlockEnd] | type[_CommentOrEmpty]]:
    for line in lines:
        stripped = line.strip()
        # TODO Это для хуавей, так что хелпер нужно унести в Formatter
        if "#" in comments and line.startswith("#"):
            yield BlockEnd
        elif len(stripped) == 0 or stripped.startswith(comments):
            yield _CommentOrEmpty
        else:
            yield line


def _filtered_block_marks(blocks):
    return filter(lambda b: isinstance(b, str), blocks)


def _parse_indent(line: str) -> int:
    level = 0
    for ch in line:
        if ch in ("\t", " "):
            level += 1
        else:
            break
    return level
