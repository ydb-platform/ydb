# coding: utf-8

from __future__ import print_function, absolute_import, division, unicode_literals

try:
    import numpy
except:  # NOQA
    numpy = None


def Xtest_numpy():
    import srsly.ruamel_yaml

    if numpy is None:
        return
    data = numpy.arange(10)
    print("data", type(data), data)

    yaml_str = srsly.ruamel_yaml.dump(data)
    datb = srsly.ruamel_yaml.load(yaml_str)
    print("datb", type(datb), datb)

    print("\nYAML", yaml_str)
    assert data == datb
