import numpy
from django.core.serializers.json import DjangoJSONEncoder


class DjangoNumpyJSONEncoder(DjangoJSONEncoder):
    int_types = (
        numpy.intp,
        numpy.intc,
        numpy.int8,
        numpy.int16,
        numpy.int32,
        numpy.int64,
        numpy.uint8,
        numpy.uint16,
        numpy.uint32,
        numpy.uint64,
    )

    float_types = (numpy.float64, numpy.float16, numpy.float32)

    def default(self, o):
        if isinstance(o, self.int_types):
            return int(o)
        elif isinstance(o, self.float_types):
            return float(o)
        elif isinstance(o, numpy.ndarray):
            return o.tolist()

        return super().default(o)
