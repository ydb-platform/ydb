import re
from collections import namedtuple

from ppadb.plugins import Plugin

Size = namedtuple("Size", [
    'width',
    'height'
])

class WM(Plugin):
    SIZE_RE = 'Physical size:\s([\d]+)x([\d]+)'
    def wm_size(self):
        result = self.shell("wm size")
        match = re.search(self.SIZE_RE, result)

        if match:
            return Size(int(match.group(1)), int(match.group(2)))
        else:
            return None

    def wm_density(self):
        result = self.shell("wm density | cut -d ' ' -f 3")
        if result:
            return int(int(result) / 160)
        else:
            return None
