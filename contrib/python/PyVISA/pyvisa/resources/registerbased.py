# -*- coding: utf-8 -*-
"""High level wrapper for RegisterBased Instruments.

This file is part of PyVISA.

:copyright: 2014-2024 by PyVISA Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

from typing import Iterable, List

from .. import constants
from .resource import Resource


class RegisterBasedResource(Resource):
    """Base class for resources that use register based communication."""

    def read_memory(
        self,
        space: constants.AddressSpace,
        offset: int,
        width: constants.DataWidth,
        extended: bool = False,
    ) -> int:
        """Read a value from the specified memory space and offset.

        Parameters
        ----------
        space : constants.AddressSpace
            Specifies the address space from which to read.
        offset : int
            Offset (in bytes) of the address or register from which to read.
        width : Union[Literal[8, 16, 32, 64], constants.DataWidth]
            Number of bits to read (8, 16, 32 or 64).
        extended : bool, optional
            Use 64 bits offset independent of the platform.

        Returns
        -------
        data : int
            Data read from memory

        Raises
        ------
        ValueError
            Raised if an invalid width is specified.

        """
        return self.visalib.read_memory(self.session, space, offset, width, extended)[0]

    def write_memory(
        self,
        space: constants.AddressSpace,
        offset: int,
        data: int,
        width: constants.DataWidth,
        extended: bool = False,
    ) -> constants.StatusCode:
        """Write a value to the specified memory space and offset.

        Parameters
        ----------
        space : constants.AddressSpace
            Specifies the address space.
        offset : int
            Offset (in bytes) of the address or register from which to read.
        data : int
            Data to write to bus.
        width : Union[Literal[8, 16, 32, 64], constants.DataWidth]
            Number of bits to read.
        extended : bool, optional
            Use 64 bits offset independent of the platform, by default False.

        Returns
        -------
        constants.StatusCode
            Return value of the library call.

        Raises
        ------
        ValueError
            Raised if an invalid width is specified.

        """
        return self.visalib.write_memory(
            self.session, space, offset, data, width, extended
        )

    def move_in(
        self,
        space: constants.AddressSpace,
        offset: int,
        length: int,
        width: constants.DataWidth,
        extended: bool = False,
    ) -> List[int]:
        """Move a block of data to local memory from the given address space and offset.

        Corresponds to viMoveIn* functions of the VISA library.

        Parameters
        ----------
        space : constants.AddressSpace
            Address space from which to move the data.
        offset : int
            Offset (in bytes) of the address or register from which to read.
        length : int
             Number of elements to transfer, where the data width of
             the elements to transfer is identical to the source data width.
        width : Union[Literal[8, 16, 32, 64], constants.DataWidth]
            Number of bits to read per element.
        extended : bool, optional
            Use 64 bits offset independent of the platform, by default False.

        Returns
        -------
        data : List[int]
            Data read from the bus
        status_code : constants.StatusCode
            Return value of the library call.

        Raises
        ------
        ValueError
            Raised if an invalid width is specified.

        """
        return self.visalib.move_in(
            self.session, space, offset, length, width, extended
        )[0]

    def move_out(
        self,
        space: constants.AddressSpace,
        offset: int,
        length: int,
        data: Iterable[int],
        width: constants.DataWidth,
        extended: bool = False,
    ) -> constants.StatusCode:
        """Move a block of data from local memory to the given address space and offset.

        Corresponds to viMoveOut* functions of the VISA library.

        Parameters
        ----------
        space : constants.AddressSpace
            Address space into which move the data.
        offset : int
            Offset (in bytes) of the address or register from which to read.
        length : int
            Number of elements to transfer, where the data width of
            the elements to transfer is identical to the source data width.
        data : Iterable[int]
            Data to write to bus.
        width : Union[Literal[8, 16, 32, 64], constants.DataWidth]
            Number of bits to per element.
        extended : bool, optional
            Use 64 bits offset independent of the platform, by default False.

        Returns
        -------
        constants.StatusCode
            Return value of the library call.

        Raises
        ------
        ValueError
            Raised if an invalid width is specified.

        """
        return self.visalib.move_out(
            self.session, space, offset, length, data, width, extended
        )
