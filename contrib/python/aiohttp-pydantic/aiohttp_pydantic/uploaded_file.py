import json
from typing import List

from aiohttp import BodyPartReader, MultipartReader
from aiohttp.web_exceptions import HTTPBadRequest


class MultipartReadingError(Exception):
    """
    This error in a programming error.
    """


class StrictOrderedMultipartReader:

    def __init__(self, reader: MultipartReader, part_names: List[str]):
        self._reader = reader
        self._expected_part_names = part_names[::-1]

    async def next_part(self, part_name):
        # Check Programing Error
        try:
            expected_part_name = self._expected_part_names.pop()
        except IndexError:
            raise MultipartReadingError(
                f'Try to read a not expected part "{part_name}" in the multipart request'
            )

        if expected_part_name != part_name:
            if part_name in self._expected_part_names:
                raise MultipartReadingError(
                    f'Try to read part "{part_name}" before "{expected_part_name}" in the multipart request'
                )
            raise MultipartReadingError(
                f'Try to read a not expected part "{part_name}" in the multipart request'
            )

        # Validate multipart request contents.
        if (part := await self._reader.next()) is None:
            # raise ValueError()
            raise HTTPBadRequest(
                text=json.dumps(
                    [
                        {
                            "in": "body",
                            "loc": ["__root__"],
                            "msg": f'The required part named "{part_name}" is not provided in the multipart request',
                            "type": "type_error.multipart",
                        }
                    ]
                ),
                content_type="application/json",
            )

        if part.name != part_name:
            raise HTTPBadRequest(
                text=json.dumps(
                    [
                        {
                            "in": "body",
                            "loc": ["__root__"],
                            "msg": f'The expected part name is "{part_name}" but the provided part name is "{part.name}"',
                            "type": "type_error.multipart",
                        }
                    ]
                ),
                content_type="application/json",
            )

        # TODO check header ?
        # part.headers[CONTENT_TYPE]
        return part


class UploadedFile:

    def __init__(self, multipart_reader: StrictOrderedMultipartReader, part_name: str):
        self._multipart_reader = multipart_reader
        self._part_name = part_name
        self._part = None

    async def body_part_reader(self) -> BodyPartReader:
        if self._part is None:
            self._part = await self._multipart_reader.next_part(self._part_name)
        return self._part

    async def read(self, *, decode: bool = False) -> bytes:
        """
        Reads body part data.

        decode: Decodes data following by encoding
                method from Content-Encoding header. If it missed
                data remains untouched
        """
        return await (await self.body_part_reader()).read(decode=decode)

    async def read_chunk(self, size: int = BodyPartReader.chunk_size) -> bytes:
        """
        Reads body part content chunk of the specified size.

        size: chunk size
        """
        return await (await self.body_part_reader()).read_chunk(size)
