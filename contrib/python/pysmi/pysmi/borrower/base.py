#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#
from pysmi import debug, error
from pysmi.reader.base import AbstractReader


class AbstractBorrower:
    genTexts = False
    exts = ""
    _reader: "AbstractReader"

    def __init__(self, reader, genTexts=False):
        """Creates an instance of *Borrower* class.

        Args:
            reader: a *reader* object

        Keyword Args:
            genText: indicates whether this borrower should be looking
                     for transformed MIBs that include human-oriented texts
        """
        if genTexts is not None:
            self.genTexts = genTexts

        self._reader = reader

    def __str__(self):
        """Return a string representation of the instance."""
        return f"{self.__class__.__name__}{{{self._reader}, genTexts={self.genTexts}, exts={self.exts}}}"

    def set_options(self, **kwargs):
        self._reader.set_options(**kwargs)

        for k in kwargs:
            setattr(self, k, kwargs[k])

        return self

    def get_data(self, mibname, **options):
        if bool(options.get("genTexts")) != self.genTexts:
            debug.logger & debug.FLAG_BORROWER and debug.logger(
                f"skipping incompatible borrower {self} for file {mibname}"
            )
            raise error.PySmiFileNotFoundError(mibname=mibname, reader=self._reader)

        debug.logger & debug.FLAG_BORROWER and (
            debug.logger(f"trying to borrow file {mibname} from {self._reader}")
        )

        if "exts" not in options:
            options["exts"] = self.exts

        return self._reader.get_data(mibname, **options)
