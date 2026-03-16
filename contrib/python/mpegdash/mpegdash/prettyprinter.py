# inspired by https://gist.github.com/sente/1083506
# The MIT License (MIT)

# Copyright (c) 2016 Stuart Powers

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import re


def pretty_print(xmlstr, indent='    ', line_break='\n'):
    # python2 doesn't have nonlocal
    current = [0]

    def indent_line(line):
        addition = 0

        if re.match(r'.+</\w[^>]*>$', line):
            # single line text element, don't change indentation
            addition = 0
        elif re.match(r'^</\w', line) and current[0] > 0:
            # end of element and have padding, decrement indentation by one
            current[0] -= 1
        elif re.match(r'^<\w[^>]*[^/]>.*$', line):
            # start of element, increment indentation by one
            addition = 1
        else:
            # single line element, don't change indentation
            addition = 0

        # update and store current indentation in outer function
        current[0] += addition

        # pad the line and return
        return (indent * (current[0] - addition)) + line

    # split the document into lines, indent each line, then rejoin lines
    return line_break.join(
        map(
            indent_line,
            re.sub('(>)(<)(/*)', r'\1\n\2\3', xmlstr).split('\n')
        )
    )
