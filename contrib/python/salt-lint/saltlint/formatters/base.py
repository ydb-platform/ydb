# -*- coding: utf-8 -*-
# Copyright (c) 2013-2018 Will Thames <will@thames.id.au>
# Copyright (c) 2018 Ansible by Red Hat
# Modified work Copyright (c) 2020 Warpnet B.V.


class BaseFormatter(object):

    def __init__(self, colored=False):
        self.colored = colored

    def process(self, problems):
        for problem in problems:
            print(self.format(problem))

    def format(self, problem):
        raise NotImplementedError()

    def get_colors(self, use=True):
        """
        Return the colors as a dict, pass False to return the colors as empty
        strings.
        """
        colors = {
            "BLACK": "\033[0;30m",
            "DARK_GRAY": "\033[1;30m",
            "RED": "\033[0;31m",
            "LIGHT_RED": "\033[1;31m",
            "GREEN": "\033[0;32m",
            "LIGHT_GREEN": "\033[1;32m",
            "BLUE": "\033[0;34m",
            "LIGHT_BLUE": "\033[1;34m",
            "MAGENTA": "\033[0;35m",
            "LIGHT_MAGENTA": "\033[1;35m",
            "CYAN": "\033[0;36m",
            "LIGHT_CYAN": "\033[1;36m",
            "LIGHT_GRAY": "\033[0;37m",
            "WHITE": "\033[1;37m",
            "DEFAULT_COLOR": "\033[00m",
            "ENDC": "\033[0m",
        }

        if not use:
            for color in colors:
                colors[color] = ''

        return colors
