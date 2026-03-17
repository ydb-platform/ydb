from ..block import ColumnOrientedBlock, BlockInfo
from ..columns.service import read_column, write_column
from ..reader import read_binary_str
from ..varint import write_varint, read_varint
from ..writer import write_binary_str
from .. import defines


class BlockOutputStream(object):
    def __init__(self, fout, context):
        self.fout = fout
        self.context = context

        super(BlockOutputStream, self).__init__()

    def write(self, block):
        revision = self.context.server_info.revision
        if revision >= defines.DBMS_MIN_REVISION_WITH_BLOCK_INFO:
            block.info.write(self.fout)

        # We write transposed data.
        n_columns = block.num_columns
        n_rows = block.num_rows

        write_varint(n_columns, self.fout)
        write_varint(n_rows, self.fout)

        for i, (col_name, col_type) in enumerate(block.columns_with_types):
            write_binary_str(col_name, self.fout)
            write_binary_str(col_type, self.fout)

            if n_columns:
                try:
                    items = block.get_column_by_index(i)
                except IndexError:
                    raise ValueError('Different rows length')

                write_column(self.context, col_name, col_type, items,
                             self.fout, types_check=block.types_check)

        self.finalize()

    def finalize(self):
        self.fout.flush()


class BlockInputStream(object):
    def __init__(self, fin, context):
        self.fin = fin
        self.context = context

        super(BlockInputStream, self).__init__()

    def read(self):
        info = BlockInfo()

        revision = self.context.server_info.revision
        if revision >= defines.DBMS_MIN_REVISION_WITH_BLOCK_INFO:
            info.read(self.fin)

        n_columns = read_varint(self.fin)
        n_rows = read_varint(self.fin)

        data, names, types = [], [], []

        for i in range(n_columns):
            column_name = read_binary_str(self.fin)
            column_type = read_binary_str(self.fin)

            names.append(column_name)
            types.append(column_type)

            if n_rows:
                column = read_column(self.context, column_type, n_rows,
                                     self.fin)
                data.append(column)

        if self.context.client_settings['use_numpy']:
            from ..numpy.block import NumpyColumnOrientedBlock
            block_cls = NumpyColumnOrientedBlock
        else:
            block_cls = ColumnOrientedBlock

        block = block_cls(
            columns_with_types=list(zip(names, types)),
            data=data,
            info=info,
        )

        return block
