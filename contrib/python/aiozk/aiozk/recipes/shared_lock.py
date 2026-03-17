from typing import ClassVar

from .base_lock import BaseLock
from .lock import Lock


class SharedLock(BaseLock):
    sub_recipes: ClassVar = {
        'reader_lock': (Lock, ['base_path', 'read_znode_label', 'read_blocked_by']),
        'writer_lock': (Lock, ['base_path', 'write_znode_label']),
    }

    def __init__(self, base_path):
        self.read_znode_label = 'read'
        self.write_znode_label = 'write'
        self.read_blocked_by = ('write',)
        super().__init__(base_path)
