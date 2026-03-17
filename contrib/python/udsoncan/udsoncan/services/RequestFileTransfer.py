import struct
from udsoncan import DataFormatIdentifier, Filesize
from udsoncan.Request import Request
from udsoncan.Response import Response
from udsoncan.exceptions import *
from udsoncan.BaseService import BaseService, BaseSubfunction, BaseResponseData
from udsoncan.ResponseCode import ResponseCode

from typing import Optional, Union, cast


class RequestFileTransfer(BaseService):
    _sid = 0x38
    _use_subfunction = False

    supported_negative_response = [
        ResponseCode.IncorrectMessageLengthOrInvalidFormat,
        ResponseCode.ConditionsNotCorrect,
        ResponseCode.RequestOutOfRange,
        ResponseCode.UploadDownloadNotAccepted,
        ResponseCode.RequestSequenceError  # ResumeFile only
    ]

    class ModeOfOperation(BaseSubfunction):  # Not really a subfunction, but we wantto inherit the helpers in BaseSubfunction class
        """
        RequestFileTransfer Mode Of Operation (MOOP). Represent the action that can be done on the server filesystem.
        See ISO-14229:2013 Annex G
        """

        __pretty_name__ = 'mode of operation'

        AddFile = 1
        DeleteFile = 2
        ReplaceFile = 3
        ReadFile = 4
        ReadDir = 5
        ResumeFile = 6

    @classmethod
    def normalize_data_format_identifier(cls, dfi: Optional[DataFormatIdentifier]) -> DataFormatIdentifier:
        if dfi is None:
            dfi = DataFormatIdentifier()

        if not isinstance(dfi, DataFormatIdentifier):
            raise ValueError('dfi must be an instance of DataFormatIdentifier')

        return dfi

    class ResponseData(BaseResponseData):
        """
        .. data:: moop_echo (int)

                Request ModeOfOperation echoed back by the server

        .. data:: max_length (int)

                The MaxNumberOfBlockLength returned by the server. Represent the number of data bytes that should be included
                in each subsequent TransferData request excepted the last one that might be smaller. 

                Not set for a response to ``DeleteFile``.

        .. data:: dfi (DataFormatIdentifier)

                Request DataFormatIdentifier echoed back by the server.

                Not set for a response to ``DeleteFile``.
                Set to Compression=0, Encryption=0, when getting a response for ``ReadDir`` as specified by ISO-14229.

        .. data:: filesize (Filesize)

                Defines the size fo the file to be read in bytes, including its uncompressed and compressed size.

                Only set when performing a ``ReadFile`` request

        .. data:: dirinfo_length (int)

                Defines the size of the directory information to be read in bytes.

                Only set when performing a ``ReadDir`` request

        .. data:: fileposition (int)

                Defines the position of the at which the tester will resume downloading after an initial download is suspended.

                Only set when performing a ``ResumeFile`` request

        """
        __slots__ = 'moop_echo', 'max_length', 'dfi', 'filesize', 'dirinfo_length', 'fileposition'

        moop_echo: int
        max_length: Optional[int]
        dfi: Optional[DataFormatIdentifier]
        filesize: Optional[Filesize]
        dirinfo_length: Optional[int]
        fileposition: Optional[int]

        def __init__(self,
                     moop_echo: int,
                     max_length: Optional[int] = None,
                     dfi: Optional[DataFormatIdentifier] = None,
                     filesize: Optional[Filesize] = None,
                     dirinfo_length: Optional[int] = None,
                     fileposition: Optional[int] = None):
            super().__init__(RequestFileTransfer)
            self.moop_echo = moop_echo
            self.max_length = max_length
            self.dfi = dfi
            self.filesize = filesize
            self.dirinfo_length = dirinfo_length
            self.fileposition = fileposition

    class InterpretedResponse(Response):
        service_data: "RequestFileTransfer.ResponseData"

    @classmethod
    def make_request(cls,
                     moop: int,
                     path: str,
                     dfi: Optional[DataFormatIdentifier] = None,
                     filesize: Optional[Union[Filesize, int]] = None
                     ) -> Request:
        """
        Generates a request for RequestFileTransfer

        :param moop: Mode of operation. Can be AddFile(1), DeleteFile(2), ReplaceFile(3), ReadFile(4), ReadDir(5), ResumeFile(6). See :class:`RequestFileTransfer.ModeOfOperation<udsoncan.services.RequestFileTransfer.ModeOfOperation>`
        :type moop: int

        :param path: String representing the path to the target file or directory.
        :type path: string

        :param dfi: DataFormatIdentifier defining the compression and encryption scheme of the data.
                If not specified, the default value of 00 will be used, specifying no encryption and no compression.
                This value is only used when ModeOfOperation is : ``AddFile``, ``ReplaceFile``, ``ReadFile``, ``ResumeFile``
        :type dfi: :ref:`DataFormatIdentifier<DataFormatIdentifier>`

        :param filesize: The filesize of the file to write when ModeOfOperation is ``AddFile``, ``ReplaceFile`` or ``ResumeFile``.
            If filesize is an object of type :ref:`Filesize<Filesize>`, the uncompressed size and compressed size will be encoded on
            the minimum amount of bytes necessary, unless a ``width`` is explicitly defined. If no compressed size is given or filesize is an ``int``,
            then the compressed size will be set equal to the uncompressed size or the integer value given as specified by ISO-14229
        :type filesize: :ref:`Filesize<Filesize>` or int

        :raises ValueError: If parameters are out of range, missing or wrong type
        """
        if not isinstance(moop, int):
            raise ValueError('Mode of operation must be an integer')

        if moop not in [cls.ModeOfOperation.AddFile,
                        cls.ModeOfOperation.DeleteFile,
                        cls.ModeOfOperation.ReplaceFile,
                        cls.ModeOfOperation.ReadFile,
                        cls.ModeOfOperation.ReadDir,
                        cls.ModeOfOperation.ResumeFile]:
            raise ValueError("Mode of operation of %d is not a known mode" % moop)

        if not isinstance(path, str):
            raise ValueError('Given path must be a valid string')

        if len(path) <= 0:
            raise ValueError('Path must be a string longer than 0 character')

        path_ascii = path.encode('ascii')
        if len(path_ascii) > 0xFFFF:
            raise ValueError('Path length must be smaller or equal than 65535  bytes (16 bits) when encoded in ASCII')

        use_dfi = moop in [cls.ModeOfOperation.AddFile, cls.ModeOfOperation.ReplaceFile, cls.ModeOfOperation.ReadFile, cls.ModeOfOperation.ResumeFile]
        use_filesize = moop in [cls.ModeOfOperation.AddFile, cls.ModeOfOperation.ReplaceFile, cls.ModeOfOperation.ResumeFile]

        if use_dfi:
            dfi = cls.normalize_data_format_identifier(dfi)
        else:
            if dfi is not None:
                raise ValueError('DataFormatIdentifier is not needed with ModeOfOperation=%d' % moop)

        if use_filesize:
            if filesize is None:
                raise ValueError('A filesize must be given for this mode of operation')

            if isinstance(filesize, int):
                filesize = Filesize(filesize)

            if not isinstance(filesize, Filesize):
                raise ValueError('Given filesize must be a valid Filesize object or an integer')

            if filesize.uncompressed is None:
                raise ValueError('Filesize needs at least an Uncompressed file size')

            if filesize.compressed is None:
                filesize = Filesize(uncompressed=filesize.uncompressed, compressed=filesize.uncompressed, width=filesize.get_width())
        else:
            if filesize is not None:
                raise ValueError('Filesize is not needed with ModeOfOperation=%d' % moop)

        data = moop.to_bytes(1, 'big')
        data += len(path_ascii).to_bytes(2, 'big')
        data += path_ascii
        if use_dfi:
            assert dfi is not None  # mypy nitpick
            data += dfi.get_byte()
        if use_filesize:
            assert filesize is not None  # mypy nitpick
            data += filesize.get_width().to_bytes(1, 'big')
            data += filesize.get_uncompressed_bytes()
            data += filesize.get_compressed_bytes()

        request = Request(cls, data=data)
        return request

    @classmethod
    def interpret_response(cls, response: Response, tolerate_zero_padding: bool = True) -> InterpretedResponse:
        """
        Populates the response ``service_data`` property with an instance of :class:`RequestFileTransfer.ResponseData<udsoncan.services.RequestFileTransfer.ResponseData>`

        :param response: The received response to interpret
        :type response: :ref:`Response<Response>`

        :raises InvalidResponseException: If length of ``response.data`` is too short or payload does not respect ISO-14229 specifications
        :raises NotImplementedError: If the MaxNumberOfBlock or fileSizeUncompressedOrDirInfoLength value is encoded over more than 8 bytes.
        """
        if response.data is None:
            raise InvalidResponseException(response, "No data in response")

        if len(response.data) < 1:
            raise InvalidResponseException(response, 'Response payload must be at least 1 byte long')

        response.service_data = cls.ResponseData(
            moop_echo=int(response.data[0])
        )

        has_lfid = response.service_data.moop_echo in [cls.ModeOfOperation.AddFile, cls.ModeOfOperation.ResumeFile,
                                                       cls.ModeOfOperation.ReplaceFile, cls.ModeOfOperation.ReadFile, cls.ModeOfOperation.ReadDir]
        has_dfi = response.service_data.moop_echo in [cls.ModeOfOperation.AddFile, cls.ModeOfOperation.ResumeFile,
                                                      cls.ModeOfOperation.ReplaceFile, cls.ModeOfOperation.ReadFile, cls.ModeOfOperation.ReadDir]
        has_filesize_length = response.service_data.moop_echo in [cls.ModeOfOperation.ReadFile, cls.ModeOfOperation.ReadDir]
        has_uncompressed_filesize = response.service_data.moop_echo in [cls.ModeOfOperation.ReadFile, cls.ModeOfOperation.ReadDir]
        has_compressed_filesize = response.service_data.moop_echo in [cls.ModeOfOperation.ReadFile]
        has_fileposition = response.service_data.moop_echo in [cls.ModeOfOperation.ResumeFile]

        cursor = 1
        if has_lfid:
            if len(response.data) < 2:
                raise InvalidResponseException(
                    response, 'Response payload must be at least 2 byte long for Mode of operation %d' % response.service_data.moop_echo)
            lfid = int(response.data[1])
            cursor = 2

            if lfid > 8:
                raise NotImplementedError(
                    'This client does not support number bigger than %d bits, but MaxNumberOfBlock is encoded on %d bits' % ((8 * 8), (lfid * 8)))

            if lfid == 0:
                raise InvalidResponseException(response, 'Received a MaxNumberOfBlockLength of 0 which is impossible')

            if len(response.data) < 2 + lfid:
                raise InvalidResponseException(
                    response, 'Response payload says that MaxNumberOfBlock is encoded on %d bytes, but only %d bytes are present' % (lfid, (len(response.data) - 2)))

            todecode = bytearray(b'\x00\x00\x00\x00\x00\x00\x00\x00')
            for i in range(1, lfid + 1):
                todecode[-i] = response.data[cursor + lfid - i]
            response.service_data.max_length = struct.unpack('>q', todecode)[0]
            cursor += lfid

        if has_dfi:
            if len(response.data) < cursor + 1:
                raise InvalidResponseException(response, 'Missing DataFormatIdentifier in received response')

            dfi = DataFormatIdentifier.from_byte(response.data[cursor])
            response.service_data.dfi = dfi
            cursor += 1
            dfi_int = dfi.get_byte_as_int()

            if response.service_data.moop_echo == cls.ModeOfOperation.ReadDir and dfi_int != 0:
                raise InvalidResponseException(
                    response, 'DataFormatIdentifier for ReadDir can only be 0x00 as per ISO-14229, but its value was set to 0x%02x' % (dfi_int))

        if has_filesize_length:
            if len(response.data) < cursor + 2:
                raise InvalidResponseException(response, 'Missing or incomplete FileSizeOrDirInfoParameterLength in received response')
            fsodipl = struct.unpack('>H', response.data[cursor:cursor + 2])[0]
            cursor += 2

            if fsodipl > 8:
                raise NotImplementedError(
                    response, 'This client does not support number bigger than %d bits, but FileSizeOrDirInfoLength is encoded on %d bits' % ((8 * 8), (fsodipl * 8)))

            if fsodipl == 0:
                raise InvalidResponseException(response, 'Received a FileSizeOrDirInfoParameterLength of 0 which is impossible')

            if has_uncompressed_filesize:
                if len(response.data) < cursor + fsodipl:
                    raise InvalidResponseException(response, 'Missing or incomplete fileSizeUncompressedOrDirInfoLength in received response')

                todecode = bytearray(b'\x00\x00\x00\x00\x00\x00\x00\x00')
                for i in range(1, fsodipl + 1):
                    todecode[-i] = response.data[cursor + fsodipl - i]
                uncompressed_size = struct.unpack('>q', todecode)[0]
                cursor += fsodipl
            else:
                uncompressed_size = None

            if has_compressed_filesize:
                if len(response.data) < cursor + fsodipl:
                    raise InvalidResponseException(response, 'Missing or incomplete fileSizeCompressed in received response')

                todecode = bytearray(b'\x00\x00\x00\x00\x00\x00\x00\x00')
                for i in range(1, fsodipl + 1):
                    todecode[-i] = response.data[cursor + fsodipl - i]
                compressed_size = struct.unpack('>q', todecode)[0]
                cursor += fsodipl
            else:
                compressed_size = None

        if has_uncompressed_filesize and response.service_data.moop_echo == cls.ModeOfOperation.ReadDir:
            response.service_data.dirinfo_length = uncompressed_size
        else:
            if has_uncompressed_filesize or has_compressed_filesize:
                response.service_data.filesize = Filesize(uncompressed=uncompressed_size, compressed=compressed_size)

        if has_fileposition:
            fposl = 8  # standard has hardcoded number of bytes to 8
            if len(response.data) < cursor + fposl:
                raise InvalidResponseException(response, 'Missing or incomplete FilePosition in received response')

            todecode = bytearray(b'\x00\x00\x00\x00\x00\x00\x00\x00')
            for i in range(1, fposl + 1):
                todecode[-i] = response.data[cursor + fposl - i]

            response.service_data.fileposition = struct.unpack('>q', todecode)[0]
            cursor += fposl

        if len(response.data) > cursor:
            if response.data[cursor:] == b'\x00' * (len(response.data) - cursor) and tolerate_zero_padding:
                pass
            else:
                raise InvalidResponseException(response, 'Response payload has extra data that has no meaning')

        return cast(RequestFileTransfer.InterpretedResponse, response)
