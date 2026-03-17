"""
Contains Python equivalents of the structures in CANLIB's canlib.h,
with some supporting functionality specific to Python.
"""

import ctypes


class BusStatistics(ctypes.Structure):
    """This structure is used with the method
    :meth:`~can.interfaces.kvaser.canlib.KvaserBus.get_stats`.
    """

    _fields_ = [
        ("m_stdData", ctypes.c_ulong),
        ("m_stdRemote", ctypes.c_ulong),
        ("m_extData", ctypes.c_ulong),
        ("m_extRemote", ctypes.c_ulong),
        ("m_errFrame", ctypes.c_ulong),
        ("m_busLoad", ctypes.c_ulong),
        ("m_overruns", ctypes.c_ulong),
    ]

    def __str__(self):
        return (
            f"std_data: {self.std_data}, "
            f"std_remote: {self.std_remote}, "
            f"ext_data: {self.ext_data}, "
            f"ext_remote: {self.ext_remote}, "
            f"err_frame: {self.err_frame}, "
            f"bus_load: {self.bus_load / 100.0:.1f}%, "
            f"overruns: {self.overruns}"
        )

    @property
    def std_data(self):
        """Number of received standard (11-bit identifiers) data frames."""
        return self.m_stdData

    @property
    def std_remote(self):
        """Number of received standard (11-bit identifiers) remote frames."""
        return self.m_stdRemote

    @property
    def ext_data(self):
        """Number of received extended (29-bit identifiers) data frames."""
        return self.m_extData

    @property
    def ext_remote(self):
        """Number of received extended (29-bit identifiers) remote frames."""
        return self.m_extRemote

    @property
    def err_frame(self):
        """Number of error frames."""
        return self.m_errFrame

    @property
    def bus_load(self):
        """The bus load, expressed as an integer in the interval 0 - 10000 representing 0.00% - 100.00% bus load."""
        return self.m_busLoad

    @property
    def overruns(self):
        """Number of overruns."""
        return self.m_overruns
