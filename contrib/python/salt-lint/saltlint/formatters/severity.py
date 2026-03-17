# -*- coding: utf-8 -*-
# Copyright (c) 2013-2018 Will Thames <will@thames.id.au>
# Copyright (c) 2018 Ansible by Red Hat
# Modified work Copyright (c) 2020 Warpnet B.V.

from saltlint.formatters.base import BaseFormatter


class SeverityFormatter(BaseFormatter):

    def format(self, problem):
        formatstr = "{0} {sev} {1}\n{2}:{3}\n{4}\n"

        color = self.get_colors(self.colored)
        return formatstr.format(
            '{0}[{1}]{2}'.format(color['RED'], problem.rule.id,
                                 color['ENDC']),
            '{0}{1}{2}'.format(color['LIGHT_RED'], problem.message,
                               color['ENDC']),
            '{0}{1}{2}'.format(color['BLUE'], problem.filename,
                               color['ENDC']),
            '{0}{1}{2}'.format(color['CYAN'], str(problem.linenumber),
                               color['ENDC']),
            '{0}{1}{2}'.format(color['MAGENTA'], problem.line, color['ENDC']),
            sev='{0}[{1}]{2}'.format(color['RED'], problem.rule.severity,
                                     color['ENDC'])
        )
