# Copyright 2015 Oliver Cope
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from itertools import count
from collections import abc
import configparser
import os
import random
import re
import string
import sys
import unicodedata
import typing as t

from yoyo.config import CONFIG_EDITOR_KEY

try:
    import termios

except ImportError:
    try:
        from msvcrt import getwch as getch  # type: ignore
    except ImportError:
        # some non Windows environments don't have termios (google cloud)
        # running yoyo through the python sdk should not require `getch`
        pass

else:

    def getch():
        """
        Read a single character without echoing to the console and without
        having to wait for a newline.
        """
        fd = sys.stdin.fileno()
        saved_attributes = termios.tcgetattr(fd)
        try:
            attributes = termios.tcgetattr(fd)  # get a fresh copy!
            attributes[3] = attributes[3] & ~(termios.ICANON | termios.ECHO)  # type: ignore  # noqa: E501
            attributes[6][termios.VMIN] = 1  # type: ignore
            attributes[6][termios.VTIME] = 0  # type: ignore
            termios.tcsetattr(fd, termios.TCSANOW, attributes)

            a = sys.stdin.read(1)
        finally:
            # be sure to reset the attributes no matter what!
            termios.tcsetattr(fd, termios.TCSANOW, saved_attributes)
        return a


def prompt(prompt, options):
    """
    Display the given prompt and list of options and return the user selection.
    """

    while True:
        sys.stdout.write("%s [%s]: " % (prompt, options))
        sys.stdout.flush()
        ch = getch()
        if ch == os.linesep:
            ch = (
                [o.lower() for o in options if "A" <= o <= "Z"] + list(options.lower())
            )[0]
        print(ch)
        if ch.lower() not in options.lower():
            print("Invalid response, please try again!")
        else:
            break

    return ch.lower()


def confirm(s, default=None):
    options = "yn"
    if default:
        default = default.lower()
        if default == "y":
            options = "Yn"
        elif default == "n":
            options = "yN"
    return prompt(s, options) == "y"


def plural(quantity, one, plural):
    """
    >>> plural(1, '%d dead frog', '%d dead frogs')
    '1 dead frog'
    >>> plural(2, '%d dead frog', '%d dead frogs')
    '2 dead frogs'
    """
    if quantity == 1:
        return one.replace("%d", "%d" % quantity)
    return plural.replace("%d", "%d" % quantity)


def get_editor(config):
    """
    Return the user's preferred visual editor
    """
    try:
        return config.get("DEFAULT", CONFIG_EDITOR_KEY)
    except configparser.NoOptionError:
        pass
    for key in ["VISUAL", "EDITOR"]:
        editor = os.environ.get(key, None)
        if editor:
            return editor
    return "vi"


def get_random_string(length, chars=(string.ascii_letters + string.digits)):
    """
    Return a random string of ``length`` characters
    """
    rng = random.SystemRandom()
    return "".join(rng.choice(chars) for i in range(length))


def change_param_style(
    target_style: str,
    sql: str,
    bind_parameters: t.Optional[abc.Mapping[str, t.Any]]
) -> tuple[str, t.Union[abc.Mapping[str, t.Any], abc.Sequence[str]]]:
    """
    :param target_style: A DBAPI paramstyle value (eg 'qmark', 'format', etc)
    :param sql: An SQL str
    :param bind_parameters: A dict of bind parameters for the query

    :return: tuple of `(sql, bind_parameters)`. ``sql`` will be rewritten with
             the target paramstyle; ``bind_parameters`` will be a tuple or
             dict as required.
    """
    if target_style == "named":
        return sql, bind_parameters or {}
    positional = target_style in {"qmark", "numeric", "format"}
    if not bind_parameters:
        return (sql, (tuple() if positional else {}))

    _param_counter = count(1)

    def param_gen_qmark(name):
        return "?"

    def param_gen_numeric(name):
        return f":{next(_param_counter)}"

    def param_gen_format(name):
        return "%s"

    def param_gen_pyformat(name):
        return f"%({name})s"

    param_gen = {
        "qmark": param_gen_qmark,
        "numeric": param_gen_numeric,
        "format": param_gen_format,
        "pyformat": param_gen_pyformat,
    }[target_style]

    pattern = re.compile(
        # Don't match if preceded by backslash (an escape)
        # or ':' (an SQL cast, eg '::INT')
        r"(?<![:\\])"
        # one of the given bind_parameters
        r":("
        + "|".join(re.escape(k) for k in bind_parameters)
        + r")"
        # followed by a non-word char, or end of string
        r"(?=\W|$)"
    )

    transformed_sql = pattern.sub(lambda match: param_gen(match.group(1)), sql)
    if positional:
        positional_params = []
        for match in pattern.finditer(sql):
            param_name = match.group(1)
            positional_params.append(bind_parameters[param_name])
        return transformed_sql, tuple(positional_params)
    return transformed_sql, bind_parameters


def unidecode(s: str) -> str:
    """
    Return ``s`` with unicode diacritics removed.
    """
    combining = unicodedata.combining
    return "".join(c for c in unicodedata.normalize("NFD", s) if not combining(c))
