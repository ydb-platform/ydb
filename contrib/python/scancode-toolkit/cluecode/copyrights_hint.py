# -*- coding: utf-8 -*-
#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

from datetime import datetime
import re

# A regex to match a string that may contain a copyright year.
# This is a year between 1960 and today prefixed and suffixed with
# either a white-space or some punctuation.

all_years = tuple(str(year) for year in range(1960, datetime.today().year))
years = r'[\(\.,\-\)\s]+(' + '|'.join(all_years) + r')([\(\.,\-\)\s]+|$)'

years = re.compile(years).findall

# Various copyright/copyleft signs tm, r etc: http://en.wikipedia.org/wiki/Copyright_symbol
# © U+00A9 COPYRIGHT SIGN
#  decimal: 169
#  HTML: &#169;
#  UTF-8: 0xC2 0xA9
#  block: Latin-1 Supplement
#  U+00A9 (169)
# visually similar: Ⓒ ⓒ
# 🄯 COPYLEFT SYMBOL
#  U+1F12F
# ℗ Sound recording copyright
#  HTML &#8471;
#  U+2117
# ® registered trademark
#  U+00AE (174)
# 🅪 Marque de commerce
#  U+1F16A
# ™ U+2122 TRADE MARK SIGN
#  decimal: 8482
#  HTML: &#8482;
#  UTF-8: 0xE2 0x84 0xA2
#  block: Letterlike Symbols
#  decomposition: <super> U+0054 U+004D
# Ⓜ  mask work


statement_markers = (
    u'©',
    u'(c)',
    u'&#169',
    u'&#xa9',
    u'169',
    u'xa9',
    u'u00a9',
    u'00a9',
    u'\251',
    u'copyr',
    u'copyl',
    u'copr',
    u'right',
    u'reserv',
    u'auth',
    u'devel',
    u'<s>',
    u'</s>',
    u'<s/>',
    u'by ',  # note the trailing space
)
'''
HTML Entity (decimal)     &#169;
HTML Entity (hex)     &#xa9;
HTML Entity (named)     &copy;
How to type in Microsoft Windows     Alt +00A9
Alt 0169
UTF-8 (hex)     0xC2 0xA9 (c2a9)
UTF-8 (binary)     11000010:10101001
UTF-16 (hex)     0x00A9 (00a9)
UTF-16 (decimal)     169
UTF-32 (hex)     0x000000A9 (00a9)
UTF-32 (decimal)     169
C/C++/Java source code     "\u00A9"
Python source code     u"\u00A9"
'''

end_of_statement = (
    u'rights reserve',
    u'right reserve',
    u'rights reserved',
    u'right reserved',
    u'right reserved.',
)

# others stuffs
'''
&reg;
&trade;
trad
regi
hawlfraint
AB
AG
AS
auth
co
code
commit
common
comp
contrib
copyl
copyr
Copr
corp
devel
found
GB
gmbh
grou
holder
inc
inria
Lab
left
llc
ltd
llp
maint
micro
modi
compan
forum
oth
pack
perm
proj
research
sa
team
tech
tm
univ
upstream
write
'''.split()
