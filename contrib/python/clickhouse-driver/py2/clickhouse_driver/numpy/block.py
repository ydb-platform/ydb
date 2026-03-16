import numpy as np

from ..block import ColumnOrientedBlock


class NumpyColumnOrientedBlock(ColumnOrientedBlock):
    def transposed(self):
        return np.transpose(self.data)
