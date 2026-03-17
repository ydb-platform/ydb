#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#
import os


class AbstractReader:
    maxMibSize = 10000000  # MIBs can't be that large
    fuzzyMatching = True  # try different file names while searching for MIB
    originalMatching = uppercaseMatching = lowcaseMatching = True
    exts = ["", os.path.extsep + "txt", os.path.extsep + "mib", os.path.extsep + "my"]
    exts.extend([x.upper() for x in exts if x])

    def set_options(self, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])
        return self

    def get_mib_variants(self, mibname, **options):
        filenames = []

        if self.originalMatching:
            filenames.append(mibname)

        if self.uppercaseMatching:
            filenames.append(mibname.upper())

        if self.lowcaseMatching:
            filenames.append(mibname.lower())

        if self.fuzzyMatching:
            part = filenames[-1].find("-mib")
            if part != -1:
                filenames.extend([x[:part] for x in filenames])
            else:
                suffixed = mibname + "-mib"
                filenames.append(suffixed.upper())
                filenames.append(suffixed.lower())

        return ((x, x + y) for x in filenames for y in options.get("exts", self.exts))

    def get_data(self, filename, **options):
        raise NotImplementedError()
