from typing import Any, ClassVar, Dict, List, Optional, Set, Tuple, Type, Union

from sqlalchemy import event, inspect, orm, types
from sqlalchemy.engine import Connection, Dialect
from sqlalchemy.orm import ColumnProperty, Mapper, Session, SessionTransaction
from sqlalchemy.orm.attributes import get_history
from sqlalchemy_file.file import File
from sqlalchemy_file.helpers import flatmap
from sqlalchemy_file.mutable_list import MutableList
from sqlalchemy_file.processors import Processor, ThumbnailGenerator
from sqlalchemy_file.storage import StorageManager
from sqlalchemy_file.validators import ImageValidator, Validator


class FileField(types.TypeDecorator):  # type: ignore
    """Provides support for storing attachments to **SQLAlchemy** models.

    [FileField][sqlalchemy_file.types.FileField] can be used as a Column type to
    store files into the model. The actual file itself will be uploaded to a specific
    `libcloud.storage.base.Container`, and only the [File][sqlalchemy_file.file.File]
    information will be stored on the database as JSON.

    [FileField][sqlalchemy_file.types.FileField] is transaction aware, so it will delete
    every uploaded file whenever the transaction is rolled back and will
    delete any old file whenever the transaction is committed.

    You can save `str`, `bytes` or any python `file` object

    Each file will be validated by provided validators before being saved into
    associate storage `libcloud.storage.base.Container` and can go through different
    processors before being saved in the database.

    """

    impl = types.JSON
    cache_ok = False

    def __init__(
        self,
        *args: Tuple[Any],
        upload_storage: Optional[str] = None,
        validators: Optional[List[Validator]] = None,
        processors: Optional[List[Processor]] = None,
        upload_type: Type[File] = File,
        multiple: Optional[bool] = False,
        extra: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs: Dict[str, Any],
    ) -> None:
        """Parameters:
        upload_storage: storage to use
        validators: List of validators to apply
        processors: List of validators to apply
        upload_type: File class to use, could be
        used to set custom File class
        multiple: Use this to save multiple files
        extra: Extra attributes (driver specific)
        headers: Additional request headers,
        such as CORS headers. For example:
        headers = {'Access-Control-Allow-Origin': 'http://mozilla.com'}.
        """
        super().__init__(*args, **kwargs)
        if processors is None:
            processors = []
        if validators is None:
            validators = []
        self.upload_storage = upload_storage
        self.upload_type = upload_type
        self.multiple = multiple
        self.extra = extra
        self.headers = headers
        self.validators = validators
        self.processors = processors

    def process_bind_param(
        self, value: Any, dialect: Dialect
    ) -> Union[None, Dict[str, Any], List[Dict[str, Any]]]:
        if not value:
            return None
        if not self.multiple and not isinstance(
            value, self.upload_type
        ):  # pragma: no cover
            raise ValueError(f"Expected {self.upload_type}, received: {type(value)}")
        if self.multiple and not (
            isinstance(value, list)
            and all(isinstance(v, self.upload_type) for v in value)
        ):  # pragma: no cover
            raise ValueError(
                f"Expected MutableList[{self.upload_type}], received: {type(value)}"
            )
        return [v.encode() for v in value] if self.multiple else value.encode()

    def process_result_value(
        self, value: Any, dialect: Dialect
    ) -> Union[None, MutableList[File], File]:
        if value is None:
            return None
        if isinstance(value, dict):
            return (
                MutableList([self.upload_type.decode(value)])
                if self.multiple
                else self.upload_type.decode(value)
            )
        return MutableList([self.upload_type.decode(v) for v in value])


class ImageField(FileField):
    """Inherits all attributes and methods from [FileField][sqlalchemy_file.types.FileField],
    but also validates that the uploaded object is a valid image.
    """

    cache_ok = False

    def __init__(
        self,
        *args: Tuple[Any],
        upload_storage: Optional[str] = None,
        thumbnail_size: Optional[Tuple[int, int]] = None,
        image_validator: Optional[ImageValidator] = None,
        validators: Optional[List[Validator]] = None,
        processors: Optional[List[Processor]] = None,
        upload_type: Type[File] = File,
        multiple: Optional[bool] = False,
        extra: Optional[Dict[str, str]] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs: Dict[str, Any],
    ) -> None:
        """Parameters
        upload_storage: storage to use
        image_validator: ImageField use default image
        validator, Use this property to customize it.
        thumbnail_size: If set, a thumbnail will be generated
        from original image using [ThumbnailGenerator]
        [sqlalchemy_file.processors.ThumbnailGenerator]
        validators: List of additional validators to apply
        processors: List of validators to apply
        upload_type: File class to use, could be
        used to set custom File class
        multiple: Use this to save multiple files
        extra: Extra attributes (driver specific).
        """
        if validators is None:
            validators = []
        if image_validator is None:
            image_validator = ImageValidator()
        if thumbnail_size is not None:
            if processors is None:
                processors = []
            processors.append(ThumbnailGenerator(thumbnail_size))
        validators.append(image_validator)
        super().__init__(
            *args,
            upload_storage=upload_storage,
            validators=validators,
            processors=processors,
            upload_type=upload_type,
            multiple=multiple,
            extra=extra,
            headers=headers,
            **kwargs,
        )


class FileFieldSessionTracker:
    mapped_entities: ClassVar[Dict[Type[Any], List[str]]] = {}

    @classmethod
    def delete_files(cls, paths: Set[str], ctx: str) -> None:
        for path in paths:
            StorageManager.delete_file(path)

    @classmethod
    def clear_session(cls, session: Session) -> None:
        if hasattr(session, "_new_files"):
            del session._new_files
        if hasattr(session, "_old_files"):
            del session._old_files

    @classmethod
    def add_new_files_to_session(cls, session: Session, paths: List[str]) -> None:
        session._new_files = getattr(session, "_new_files", set())  # type: ignore
        session._new_files.update(paths)  # type: ignore

    @classmethod
    def add_old_files_to_session(cls, session: Session, paths: List[str]) -> None:
        session._old_files = getattr(session, "_old_files", set())  # type: ignore
        session._old_files.update(paths)  # type: ignore

    @classmethod
    def extract_files_from_history(cls, data: Union[Tuple[()], List[Any]]) -> List[str]:
        paths = []
        for item in data:
            if isinstance(item, list):
                paths.extend([f["files"] for f in item])
            elif isinstance(item, File):
                paths.append(item["files"])
        return flatmap(paths)

    @classmethod
    def _mapper_configured(cls, mapper: Mapper, class_: Any) -> None:  # type: ignore[type-arg]
        """Detect and listen all class with FileField Column."""
        for mapper_property in mapper.iterate_properties:
            if isinstance(mapper_property, ColumnProperty) and isinstance(
                mapper_property.columns[0].type, FileField
            ):
                assert (
                    len(mapper_property.columns) == 1
                ), "Multiple-column properties are not supported"
                if mapper_property.columns[0].type.multiple:
                    MutableList.associate_with_attribute(
                        getattr(class_, mapper_property.key)
                    )
                cls.mapped_entities.setdefault(class_, []).append(mapper_property.key)

    @classmethod
    def _after_configured(cls) -> None:
        for entity in cls.mapped_entities:
            event.listen(entity, "before_insert", cls._before_insert)
            event.listen(entity, "before_update", cls._before_update)
            event.listen(entity, "after_update", cls._after_update)
            event.listen(entity, "after_delete", cls._after_delete)

    @classmethod
    def _after_commit(cls, session: Session) -> None:
        """After commit, old files are automatically deleted."""
        cls.delete_files(getattr(session, "_old_files", set()), "after_commit")
        cls.clear_session(session)

    @classmethod
    def _after_soft_rollback(cls, session: Session, _: SessionTransaction) -> None:
        """After rollback, new files are automatically deleted."""
        cls.delete_files(getattr(session, "_new_files", set()), "after_soft_rollback")
        cls.clear_session(session)

    @classmethod
    def _after_delete(cls, mapper: Mapper, _: Connection, obj: Any) -> None:  # type: ignore[type-arg]
        """After delete mark all linked files as old in order to delete
        them when after session is committed.
        """
        tracked_columns: List[str] = cls.mapped_entities.get(mapper.class_, [])
        for key in tracked_columns:
            value = getattr(obj, key)
            if value is not None:
                cls.add_old_files_to_session(
                    inspect(obj).session,
                    flatmap(
                        [
                            f["files"]
                            for f in (value if isinstance(value, list) else [value])
                        ]
                    ),
                )

    @classmethod
    def _after_update(cls, mapper: Mapper, _: Connection, obj: Any) -> None:  # type: ignore[type-arg]
        """After update, mark all edited files as old
        in order to delete them when after session is committed.
        """
        tracked_columns: List[str] = cls.mapped_entities.get(mapper.class_, [])
        for key in tracked_columns:
            history = get_history(obj, key)
            cls.add_old_files_to_session(
                inspect(obj).session, cls.extract_files_from_history(history.deleted)
            )

    @classmethod
    def _before_update(cls, mapper: Mapper, _: Connection, obj: Any) -> None:  # type: ignore[type-arg]
        """Before updating values, validate and save files. For multiple fields,
        mark all removed files as old, as _removed attribute will be
        reinitialised after update.
        """
        session = inspect(obj).session
        tracked_columns: List[str] = cls.mapped_entities.get(mapper.class_, [])
        for key in tracked_columns:
            value = getattr(obj, key)
            if value is not None:
                changed, prepare_value = cls.prepare_file_attr(mapper, obj, key)
                if changed:
                    setattr(obj, key, prepare_value)
                    history = get_history(obj, key)
                    cls.add_new_files_to_session(
                        session, cls.extract_files_from_history(history.added)
                    )
                if isinstance(value, MutableList):
                    _removed = getattr(value, "_removed", ())
                    cls.add_old_files_to_session(
                        session, flatmap([f["files"] for f in _removed])
                    )

    @classmethod
    def _before_insert(cls, mapper: Mapper, _: Connection, obj: Any) -> None:  # type: ignore[type-arg]
        """Before inserting values, mark all created files as new. They will be
        automatically removed when session rollback.
        """
        tracked_columns: List[str] = cls.mapped_entities.get(mapper.class_, [])
        for key in tracked_columns:
            value = getattr(obj, key)
            if value is not None:
                setattr(obj, key, cls.prepare_file_attr(mapper, obj, key)[1])
                history = get_history(obj, key)
                cls.add_new_files_to_session(
                    inspect(obj).session, cls.extract_files_from_history(history.added)
                )

    @classmethod
    def prepare_file_attr(
        cls, mapper: Mapper, obj: Any, key: str  # type: ignore[type-arg]
    ) -> Tuple[bool, Union[File, List[File]]]:
        """Prepare file(s) for saving into the database by converting bytes or strings into
        File objects, applying validators, uploading the file(s) to the upload storage,
        and applying processors.

        Arguments:
            mapper (Mapper): The mapper instance associated with the object.
            obj (Any): The object containing the file attribute.
            key (str): The name of the file attribute.

        Returns:
            Tuple[bool, Union[File, List[File]]]: A tuple containing a boolean flag indicating
            whether the file(s) were changed, and the prepared file object(s). If the column type
            allows multiple files, the prepared file objects are returned as a list.
        """
        value = getattr(obj, key)

        """
        The `changed` flag becomes True in two cases:
        1. For a single-field attribute, when a new file is assigned, replacing the previous file.
        2. For a multiple-field attribute, when new file objects are added to the existing ones.
        """
        changed = False

        column_type = mapper.attrs.get(key).columns[0].type  # type: ignore[misc,union-attr]
        assert isinstance(column_type, FileField)
        upload_type = column_type.upload_type

        prepared_values: List[File] = []
        for v in value if isinstance(value, list) else [value]:
            if not isinstance(v, upload_type):
                v = upload_type(v)  # noqa: PLW2901
            if not getattr(v, "saved", False):
                changed = True
                v.apply_validators(column_type.validators, key)
            prepared_values.append(v)

        upload_storage = column_type.upload_storage or StorageManager.get_default()
        for value in prepared_values:
            if not getattr(value, "saved", False):
                if column_type.extra is not None and value.get("extra", None) is None:
                    value["extra"] = column_type.extra
                if (
                    column_type.headers is not None
                    and value.get("headers", None) is None
                ):
                    value["headers"] = column_type.headers
                value.save_to_storage(upload_storage)
                value.apply_processors(column_type.processors, upload_storage)
        return changed, (
            prepared_values if column_type.multiple else prepared_values[0]
        )

    @classmethod
    def setup(cls) -> None:
        event.listen(orm.Mapper, "mapper_configured", cls._mapper_configured)
        event.listen(orm.Mapper, "after_configured", cls._after_configured)
        event.listen(Session, "after_commit", cls._after_commit)
        event.listen(Session, "after_soft_rollback", cls._after_soft_rollback)


FileFieldSessionTracker.setup()
