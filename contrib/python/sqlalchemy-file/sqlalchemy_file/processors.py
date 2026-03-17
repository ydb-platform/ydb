import mimetypes
from abc import abstractmethod
from tempfile import SpooledTemporaryFile
from typing import TYPE_CHECKING, Optional, Tuple

from sqlalchemy_file.helpers import INMEMORY_FILESIZE

if TYPE_CHECKING:
    from sqlalchemy_file.file import File


class Processor:
    """Interface that must be implemented by file processors.
    Can be used to add additional data to the stored file or change it.
    When file processors are run the file has already been stored.

    """

    @abstractmethod
    def process(
        self, file: "File", upload_storage: Optional[str] = None
    ) -> None:  # pragma: no cover
        """Should be overridden in inherited class
        Parameters:
            file: [File][sqlalchemy_file.file.File] object,
                Use file.original_content to access uploaded file
            upload_storage: pass this to
                [file.store_content()][sqlalchemy_file.file.File.store_content]
                to attach additional files to the original file.
        """


class ThumbnailGenerator(Processor):
    """Generate thumbnail from original content.

    The default thumbnail format and size are `PNG@128x128`, those can be changed
    by giving custom `thumbnail_size` and `thumbnail_format`

    !!! note
        ThumbnailGenerator will add additional data
        to the file object under the key `thumbnail`.
        These data will be store in database.

        Properties available in `thumbnail` attribute

        - **file_id:**        This is the ID of the uploaded thumbnail file
        - **upload_storage:** Name of the storage used to save the uploaded file
        - **path:**           This is a upload_storage/file_id path which can
                                be used with :meth:`StorageManager.get_file` to
                                retrieve the thumbnail file
        - **width**           This is the width of the thumbnail image
        - **height:**         This is the height of the thumbnail image
        - **url:**            Public url of the uploaded file provided
                                by libcloud method `Object.get_cdn_url()`

    Example:
        ```Python
        class Book(Base):
            __tablename__ = "book"

            id = Column(Integer, autoincrement=True, primary_key=True)
            title = Column(String(100), unique=True)
            cover = Column(ImageField(processors=[ThumbnailGenerator()]))
        ```

        ```Python
        def test_create_image_with_thumbnail(self, fake_image) -> None:
            with Session(engine) as session:
                from PIL import Image

                session.add(Book(title="Pointless Meetings", cover=fake_image))
                session.flush()
                book = session.execute(
                    select(Book).where(Book.title == "Pointless Meetings")
                ).scalar_one()
                assert book.cover["thumbnail"] is not None
                thumbnail = StorageManager.get_file(book.cover["thumbnail"]["path"])
                assert thumbnail is not None
                thumbnail = Image.open(thumbnail)
                assert max(thumbnail.width, thumbnail.height) == 128
                assert book.cover["thumbnail"]["width"] == thumbnail.width
                assert book.cover["thumbnail"]["height"] == thumbnail.height
        ```


    """

    def __init__(
        self,
        thumbnail_size: Tuple[int, int] = (128, 128),
        thumbnail_format: str = "PNG",
    ) -> None:
        super().__init__()
        self.thumbnail_size = thumbnail_size
        self.thumbnail_format = thumbnail_format

    def process(self, file: "File", upload_storage: Optional[str] = None) -> None:
        from PIL import Image  # type: ignore

        content = file.original_content
        img = Image.open(content)
        thumbnail = img.copy()
        thumbnail.thumbnail(self.thumbnail_size)
        output = SpooledTemporaryFile(INMEMORY_FILESIZE)
        thumbnail.save(output, self.thumbnail_format)
        output.seek(0)
        width, height, content_type = (
            thumbnail.width,
            thumbnail.height,
            f"image/{self.thumbnail_format}".lower(),
        )
        ext = mimetypes.guess_extension(content_type)
        extra = file.get("extra", {})
        metadata = extra.get("meta_data", {})
        metadata.update(
            {
                "filename": file["filename"] + f".thumbnail{width}x{height}{ext}",
                "content_type": content_type,
                "width": width,
                "height": height,
            }
        )
        extra.update({"content_type": content_type, "meta_data": metadata})
        stored_file = file.store_content(
            output,
            upload_storage=upload_storage,
            extra=extra,
            headers=file.get("headers", None),
        )
        file.update(
            {
                "thumbnail": {
                    "file_id": stored_file.name,
                    "width": width,
                    "height": height,
                    "upload_storage": upload_storage,
                    "path": f"{upload_storage}/{stored_file.name}",
                    "url": stored_file.get_cdn_url(),
                }
            }
        )
