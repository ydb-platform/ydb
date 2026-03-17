# -*- coding: utf-8 -*-
#
# Copyright (C) 2020 Radim Rehurek <radim@rare-technologies.com>
# Copyright (C) 2020 Nicolas Mitchell <ncls.mitchell@gmail.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
"""Implements file-like objects for reading and writing to/from Azure Blob Storage."""

import base64
import io
import logging

import smart_open.bytebuffer
import smart_open.constants

try:
    import azure.storage.blob
    import azure.core.exceptions
except ImportError:
    MISSING_DEPS = True

logger = logging.getLogger(__name__)

_BINARY_TYPES = (bytes, bytearray, memoryview)
"""Allowed binary buffer types for writing to the underlying Azure Blob Storage stream"""

SCHEME = "azure"
"""Supported scheme for Azure Blob Storage in smart_open endpoint URL"""

_DEFAULT_MIN_PART_SIZE = 64 * 1024**2
"""Default minimum part size for Azure Cloud Storage multipart uploads is 64MB"""

DEFAULT_BUFFER_SIZE = 4 * 1024**2
"""Default buffer size for working with Azure Blob Storage is 256MB
https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs
"""

DEFAULT_MAX_CONCURRENCY = 1
"""Default number of parallel connections with which to download."""


def parse_uri(uri_as_string):
    sr = smart_open.utils.safe_urlsplit(uri_as_string)
    assert sr.scheme == SCHEME
    first = sr.netloc
    second = sr.path.lstrip('/')

    # https://docs.microsoft.com/en-us/rest/api/storageservices/working-with-the-root-container
    if not second:
        container_id = '$root'
        blob_id = first
    else:
        container_id = first
        blob_id = second

    return dict(scheme=SCHEME, container_id=container_id, blob_id=blob_id)


def open_uri(uri, mode, transport_params):
    parsed_uri = parse_uri(uri)
    kwargs = smart_open.utils.check_kwargs(open, transport_params)
    return open(parsed_uri['container_id'], parsed_uri['blob_id'], mode, **kwargs)


def open(
        container_id,
        blob_id,
        mode,
        client=None,  # type: Union[azure.storage.blob.BlobServiceClient, azure.storage.blob.ContainerClient, azure.storage.blob.BlobClient] # noqa
        blob_kwargs=None,
        buffer_size=DEFAULT_BUFFER_SIZE,
        min_part_size=_DEFAULT_MIN_PART_SIZE,
        max_concurrency=DEFAULT_MAX_CONCURRENCY,
        ):
    """Open an Azure Blob Storage blob for reading or writing.

    Parameters
    ----------
    container_id: str
        The name of the container this object resides in.
    blob_id: str
        The name of the blob within the bucket.
    mode: str
        The mode for opening the object.  Must be either "rb" or "wb".
    client: azure.storage.blob.BlobServiceClient, ContainerClient, or BlobClient
        The Azure Blob Storage client to use when working with azure-storage-blob.
    blob_kwargs: dict, optional
        Additional parameters to pass to `BlobClient.commit_block_list`.
        For writing only.
    buffer_size: int, optional
        The buffer size to use when performing I/O. For reading only.
    min_part_size: int, optional
        The minimum part size for multipart uploads. For writing only.
    max_concurrency: int, optional
        The number of parallel connections with which to download. For reading only.

    """
    if not client:
        raise ValueError('you must specify the client to connect to Azure')

    if mode == smart_open.constants.READ_BINARY:
        return Reader(
            container_id,
            blob_id,
            client,
            buffer_size=buffer_size,
            line_terminator=smart_open.constants.BINARY_NEWLINE,
            max_concurrency=max_concurrency,
        )
    elif mode == smart_open.constants.WRITE_BINARY:
        return Writer(
            container_id,
            blob_id,
            client,
            blob_kwargs=blob_kwargs,
            min_part_size=min_part_size
        )
    else:
        raise NotImplementedError('Azure Blob Storage support for mode %r not implemented' % mode)


def _get_blob_client(client, container, blob):
    # type: (Union[azure.storage.blob.BlobServiceClient, azure.storage.blob.ContainerClient, azure.storage.blob.BlobClient], str, str) -> azure.storage.blob.BlobClient  # noqa
    """
    Return an Azure BlobClient starting with any of BlobServiceClient,
    ContainerClient, or BlobClient plus container name and blob name.
    """
    if hasattr(client, "get_container_client"):
        client = client.get_container_client(container)

    if hasattr(client, "container_name") and client.container_name != container:
        raise ValueError(
            "Client for %r doesn't match "
            "container %r" % (client.container_name, container)
        )

    if hasattr(client, "get_blob_client"):
        client = client.get_blob_client(blob)

    return client


class _RawReader(object):
    """Read an Azure Blob Storage file."""

    def __init__(self, blob, size, concurrency):
        # type: (azure.storage.blob.BlobClient, int, int) -> None
        self._blob = blob
        self._size = size
        self._position = 0
        self._concurrency = concurrency

    def seek(self, position):
        """Seek to the specified position (byte offset) in the Azure Blob Storage blob.

        :param int position: The byte offset from the beginning of the blob.

        Returns the position after seeking.
        """
        self._position = position
        return self._position

    def read(self, size=-1):
        if self._position >= self._size:
            return b''
        binary = self._download_blob_chunk(size)
        self._position += len(binary)
        return binary

    def _download_blob_chunk(self, size):
        if self._size == self._position:
            #
            # When reading, we can't seek to the first byte of an empty file.
            # Similarly, we can't seek past the last byte.  Do nothing here.
            #
            return b''
        elif size == -1:
            stream = self._blob.download_blob(offset=self._position, max_concurrency=self._concurrency)
        else:
            stream = self._blob.download_blob(
                offset=self._position, max_concurrency=self._concurrency, length=size)
        logging.debug('reading with a max concurrency of %d', self._concurrency)
        if isinstance(stream, azure.storage.blob.StorageStreamDownloader):
            binary = stream.readall()
        else:
            binary = stream.read()
        return binary


class Reader(io.BufferedIOBase):
    """Reads bytes from Azure Blob Storage.

    Implements the io.BufferedIOBase interface of the standard library.

    :raises azure.core.exceptions.ResourceNotFoundError: Raised when the blob to read from does not exist.
    """
    _blob = None  # so `closed` property works in case __init__ fails and __del__ is called

    def __init__(
            self,
            container,
            blob,
            client,  # type: Union[azure.storage.blob.BlobServiceClient, azure.storage.blob.ContainerClient, azure.storage.blob.BlobClient]  # noqa
            buffer_size=DEFAULT_BUFFER_SIZE,
            line_terminator=smart_open.constants.BINARY_NEWLINE,
            max_concurrency=DEFAULT_MAX_CONCURRENCY,
    ):
        self._container_name = container
        self._blob_name = blob

        # type: azure.storage.blob.BlobClient
        self._blob = _get_blob_client(client, container, blob)

        if self._blob is None:
            raise azure.core.exceptions.ResourceNotFoundError(
                'blob %s not found in %s' % (blob, container)
            )
        try:
            self._size = self._blob.get_blob_properties()['size']
        except KeyError:
            self._size = 0

        self._raw_reader = _RawReader(self._blob, self._size, max_concurrency)
        self._position = 0
        self._current_part = smart_open.bytebuffer.ByteBuffer(buffer_size)
        self._line_terminator = line_terminator

    #
    # Override some methods from io.IOBase.
    #
    def close(self):
        """Flush and close this stream."""
        logger.debug("close: called")
        if not self.closed:
            self._blob = None
            self._raw_reader = None

    @property
    def closed(self):
        return self._blob is None

    def readable(self):
        """Return True if the stream can be read from."""
        return True

    def seekable(self):
        """If False, seek(), tell() and truncate() will raise IOError.

        We offer only seek support, and no truncate support."""
        return True

    #
    # io.BufferedIOBase methods.
    #
    def detach(self):
        """Unsupported."""
        raise io.UnsupportedOperation

    def seek(self, offset, whence=smart_open.constants.WHENCE_START):
        """Seek to the specified position.

        :param int offset: The offset in bytes.
        :param int whence: Where the offset is from.

        Returns the position after seeking."""
        logger.debug('seeking to offset: %r whence: %r', offset, whence)
        if whence not in smart_open.constants.WHENCE_CHOICES:
            raise ValueError('invalid whence %i, expected one of %r' % (whence,
                                                                       smart_open.constants.WHENCE_CHOICES))

        if whence == smart_open.constants.WHENCE_START:
            new_position = offset
        elif whence == smart_open.constants.WHENCE_CURRENT:
            new_position = self._position + offset
        else:
            new_position = self._size + offset
        self._position = new_position
        self._raw_reader.seek(new_position)
        logger.debug('current_pos: %r', self._position)

        self._current_part.empty()
        return self._position

    def tell(self):
        """Return the current position within the file."""
        return self._position

    def truncate(self, size=None):
        """Unsupported."""
        raise io.UnsupportedOperation

    def read(self, size=-1):
        """Read up to size bytes from the object and return them."""
        if size == 0:
            return b''
        elif size < 0:
            self._position = self._size
            return self._read_from_buffer() + self._raw_reader.read()

        #
        # Return unused data first
        #
        if len(self._current_part) >= size:
            return self._read_from_buffer(size)

        if self._position == self._size:
            return self._read_from_buffer()

        self._fill_buffer(size)
        return self._read_from_buffer(size)

    def read1(self, size=-1):
        """This is the same as read()."""
        return self.read(size=size)

    def readinto(self, b):
        """Read up to len(b) bytes into b, and return the number of bytes read."""
        data = self.read(len(b))
        if not data:
            return 0
        b[:len(data)] = data
        return len(data)

    def readline(self, limit=-1):
        """Read up to and including the next newline.  Returns the bytes read."""
        if limit != -1:
            raise NotImplementedError('limits other than -1 not implemented yet')

        #
        # A single line may span multiple buffers.
        #
        line = io.BytesIO()
        while not (self._position == self._size and len(self._current_part) == 0):
            line_part = self._current_part.readline(self._line_terminator)
            line.write(line_part)
            self._position += len(line_part)

            if line_part.endswith(self._line_terminator):
                break
            else:
                self._fill_buffer()

        return line.getvalue()

    #
    # Internal methods.
    #
    def _read_from_buffer(self, size=-1):
        """Remove at most size bytes from our buffer and return them."""
        # logger.debug('reading %r bytes from %r byte-long buffer', size, len(self._current_part))
        size = size if size >= 0 else len(self._current_part)
        part = self._current_part.read(size)
        self._position += len(part)
        # logger.debug('part: %r', part)
        return part

    def _fill_buffer(self, size=-1):
        size = max(size, self._current_part._chunk_size)
        while len(self._current_part) < size and not self._position == self._size:
            bytes_read = self._current_part.fill(self._raw_reader)
            if bytes_read == 0:
                logger.debug('reached EOF while filling buffer')
                return True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __str__(self):
        return "(%s, %r, %r)" % (
            self.__class__.__name__,
            self._container_name,
            self._blob_name
        )

    def __repr__(self):
        return "%s(container=%r, blob=%r)" % (
            self.__class__.__name__,
            self._container_name,
            self._blob_name,
        )


class Writer(io.BufferedIOBase):
    """Writes bytes to Azure Blob Storage.

    Implements the io.BufferedIOBase interface of the standard library.
    """
    _blob = None  # so `closed` property works in case __init__ fails and __del__ is called

    def __init__(
            self,
            container,
            blob,
            client,  # type: Union[azure.storage.blob.BlobServiceClient, azure.storage.blob.ContainerClient, azure.storage.blob.BlobClient]  # noqa
            blob_kwargs=None,
            min_part_size=_DEFAULT_MIN_PART_SIZE,
    ):
        self._container_name = container
        self._blob_name = blob
        self._blob_kwargs = blob_kwargs or {}
        self._min_part_size = min_part_size
        self._total_size = 0
        self._total_parts = 0
        self._bytes_uploaded = 0
        self._current_part = io.BytesIO()
        self._block_list = []

        # type: azure.storage.blob.BlobClient
        self._blob = _get_blob_client(client, container, blob)

    def flush(self):
        pass

    def terminate(self):
        """Do not commit block list on abort.

        Uploaded (uncommitted) blocks will be garbage collected after 7 days.

        See also https://stackoverflow.com/a/69673084/5511061."""
        logger.debug('%s: terminating multipart upload', self)
        if not self.closed:
            self._block_list = []
            self._blob = None
        logger.debug('%s: terminated multipart upload', self)

    #
    # Override some methods from io.IOBase.
    #
    def close(self):
        logger.debug("close: called")
        if not self.closed:
            logger.debug('%s: completing multipart upload', self)
            if self._current_part.tell() > 0:
                self._upload_part()
            self._blob.commit_block_list(self._block_list, **self._blob_kwargs)
            self._block_list = []
            self._blob = None
            logger.debug('%s: completed multipart upload', self)

    @property
    def closed(self):
        return self._blob is None

    def writable(self):
        """Return True if the stream supports writing."""
        return True

    def seekable(self):
        """If False, seek(), tell() and truncate() will raise IOError.

        We offer only tell support, and no seek or truncate support."""
        return True

    def seek(self, offset, whence=smart_open.constants.WHENCE_START):
        """Unsupported."""
        raise io.UnsupportedOperation

    def truncate(self, size=None):
        """Unsupported."""
        raise io.UnsupportedOperation

    def tell(self):
        """Return the current stream position."""
        return self._total_size

    #
    # io.BufferedIOBase methods.
    #
    def detach(self):
        raise io.UnsupportedOperation("detach() not supported")

    def write(self, b):
        """Write the given bytes (binary string) to the Azure Blob Storage file.

        There's buffering happening under the covers, so this may not actually
        do any HTTP transfer right away."""

        if not isinstance(b, _BINARY_TYPES):
            raise TypeError("input must be one of %r, got: %r" % (_BINARY_TYPES, type(b)))

        self._current_part.write(b)
        self._total_size += len(b)

        if self._current_part.tell() >= self._min_part_size:
            self._upload_part()

        return len(b)

    def _upload_part(self):
        part_num = self._total_parts + 1
        content_length = self._current_part.tell()
        range_stop = self._bytes_uploaded + content_length - 1

        """  # noqa: E501
        block_id's must be base64 encoded, all the same length, and less than or equal to 64 bytes in size prior
        to encoding.
        https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobclient?view=azure-python#stage-block-block-id--data--length-none----kwargs-
        """
        zero_padded_part_num = str(part_num).zfill(64 // 2)
        block_id = base64.b64encode(zero_padded_part_num.encode())
        self._current_part.seek(0)
        self._blob.stage_block(block_id, self._current_part.read(content_length))
        self._block_list.append(azure.storage.blob.BlobBlock(block_id=block_id))

        logger.info(
            "uploading part #%i, %i bytes (total %.3fGB)",
            part_num, content_length, range_stop / 1024.0 ** 3,
        )

        self._total_parts += 1
        self._bytes_uploaded += content_length
        self._current_part = io.BytesIO(self._current_part.read())
        self._current_part.seek(0, io.SEEK_END)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.terminate()
        else:
            self.close()

    def __str__(self):
        return "(%s, %r, %r)" % (
            self.__class__.__name__,
            self._container_name,
            self._blob_name
        )

    def __repr__(self):
        return "%s(container=%r, blob=%r, min_part_size=%r)" % (
            self.__class__.__name__,
            self._container_name,
            self._blob_name,
            self._min_part_size
        )
