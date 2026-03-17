from .reader import read_binary_uint8
from .varint import read_varint


class BlockStreamProfileInfo(object):
    def __init__(self):
        self.rows = 0
        self.blocks = 0
        self.bytes = 0
        self.applied_limit = False  # bool
        self.rows_before_limit = 0
        self.calculated_rows_before_limit = 0  # bool

        super(BlockStreamProfileInfo, self).__init__()

    def read(self, fin):
        self.rows = read_varint(fin)
        self.blocks = read_varint(fin)
        self.bytes = read_varint(fin)
        self.applied_limit = bool(read_binary_uint8(fin))
        self.rows_before_limit = read_varint(fin)
        self.calculated_rows_before_limit = bool(read_binary_uint8(fin))
