# -*- coding: utf-8 -*-
""" """

from pyvisa.highlevel import VisaLibraryBase


class FalseVISALib(VisaLibraryBase):
    pass


WRAPPER_CLASS = FalseVISALib
