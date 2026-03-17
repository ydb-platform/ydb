from __future__ import annotations

import warnings
from collections.abc import Mapping
from contextlib import suppress
from datetime import timezone
from functools import partial
from os.path import realpath
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, ForwardRef, Generic, TypeVar, cast
from uuid import NAMESPACE_DNS, uuid1, uuid3, uuid5

from typing_extensions import Literal, get_args

from polyfactory.exceptions import MissingDependencyException
from polyfactory.factories.base import BaseFactory, BuildContext
from polyfactory.factories.base import BuildContext as BaseBuildContext
from polyfactory.field_meta import Constraints, FieldMeta, Null
from polyfactory.utils.helpers import unwrap_new_type, unwrap_optional
from polyfactory.utils.model_coverage import CoverageContainer
from polyfactory.utils.normalize_type import normalize_type
from polyfactory.utils.predicates import is_annotated, is_optional, is_safe_subclass, is_union
from polyfactory.utils.types import NoneType
from polyfactory.value_generators.primitives import create_random_bytes

try:
    import pydantic
    from pydantic import (
        VERSION,
        AnyHttpUrl,
        AnyUrl,
        ByteSize,
        EmailStr,
        FutureDate,
        HttpUrl,
        IPvAnyAddress,
        IPvAnyInterface,
        IPvAnyNetwork,
        Json,
        NameEmail,
        NegativeFloat,
        NegativeInt,
        NonNegativeInt,
        NonPositiveFloat,
        PastDate,
        PaymentCardNumber,
        PositiveFloat,
        PositiveInt,
        SecretBytes,
        SecretStr,
        StrictBool,
        StrictBytes,
        StrictFloat,
        StrictInt,
        StrictStr,
    )
    from pydantic.fields import FieldInfo
except ImportError as e:
    msg = "pydantic is not installed"
    raise MissingDependencyException(msg) from e

try:
    # pydantic v1
    import pydantic as pydantic_v1
    from pydantic import BaseModel as BaseModelV1

    # Keep this import last to prevent warnings from pydantic if pydantic v2
    # is installed.
    from pydantic.color import Color
    from pydantic.fields import (  # type: ignore[attr-defined]
        DeferredType,  # pyright: ignore[attr-defined,reportAttributeAccessIssue]
        ModelField,  # pyright: ignore[attr-defined,reportAttributeAccessIssue]
        Undefined,  # pyright: ignore[attr-defined,reportAttributeAccessIssue]
    )

    # prevent unbound variable warnings
    BaseModelV2 = BaseModelV1
    UndefinedV2 = Undefined
except ImportError:
    # pydantic v2

    # v2 specific imports
    from pydantic import BaseModel as BaseModelV2
    from pydantic_core import PydanticUndefined as UndefinedV2
    from pydantic_core import to_json

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".*Pydantic V1.*", category=UserWarning)
        import pydantic.v1 as pydantic_v1  # type: ignore[no-redef]
        from pydantic.v1 import BaseModel as BaseModelV1  # type: ignore[assignment]
        from pydantic.v1.color import Color  # type: ignore[assignment]
        from pydantic.v1.fields import DeferredType, ModelField, Undefined


if TYPE_CHECKING:
    from collections import abc
    from collections.abc import Iterable, Mapping, Sequence
    from typing import Callable

    from typing_extensions import NotRequired, TypeGuard

    from pydantic import BaseModel

T = TypeVar("T", bound="BaseModel")

_IS_PYDANTIC_V1 = VERSION.startswith("1")


class PydanticBuildContext(BaseBuildContext):
    factory_use_construct: bool


class PydanticConstraints(Constraints):
    """Metadata regarding a Pydantic type constraints, if any"""

    json: NotRequired[bool]


class PydanticFieldMeta(FieldMeta):
    """Field meta subclass capable of handling pydantic ModelFields"""

    def __init__(
        self,
        *,
        name: str,
        annotation: type,
        default: Any = ...,
        children: list[FieldMeta] | None = None,
        constraints: PydanticConstraints | None = None,
        examples: list[Any] | None = None,
        required: bool = True,
    ) -> None:
        super().__init__(
            name=name,
            annotation=annotation,
            default=default,
            children=children,
            constraints=constraints,
            required=required,
        )
        self.examples = examples

    @classmethod
    def from_field_info(
        cls,
        field_name: str,
        field_info: FieldInfo,
        use_alias: bool,
    ) -> PydanticFieldMeta:
        """Create an instance from a pydantic field info.

        :param field_name: The name of the field.
        :param field_info: A pydantic FieldInfo instance.
        :param use_alias: Whether to use the field alias.

        :returns: A PydanticFieldMeta instance.
        """
        field_info = FieldInfo.merge_field_infos(
            field_info,
            FieldInfo.from_annotation(normalize_type(field_info.annotation)),
            alias=field_info.alias,
        )

        if callable(field_info.default_factory):
            default_value = field_info.default_factory
        else:
            default_value = field_info.default if field_info.default is not UndefinedV2 else Null

        annotation = unwrap_new_type(field_info.annotation)
        children: list[FieldMeta,] | None = None
        name = field_info.alias if field_info.alias and use_alias else field_name

        constraints: PydanticConstraints
        # pydantic v2 does not always propagate metadata for Union types
        if is_union(annotation):
            constraints = {}
            children = []

            # create a child for each of the possible union values
            for arg in get_args(annotation):
                # don't add the NoneType in an optional to the list of children
                if arg is NoneType:
                    continue
                child_field_info = FieldInfo.from_annotation(arg)
                merged_field_info = FieldInfo.merge_field_infos(field_info, child_field_info)

                children.append(
                    # recurse for each element of the union
                    cls.from_field_info(
                        # this is a fake field name, but it makes it possible to debug which type variant
                        # is the source of an exception downstream
                        field_name=field_name,
                        field_info=merged_field_info,
                        use_alias=use_alias,
                    ),
                )
        else:
            metadata, is_json = [], False
            for m in field_info.metadata:
                if not is_json and isinstance(m, Json):  # type: ignore[misc]
                    is_json = True
                elif m is not None:
                    metadata.append(m)

            constraints = cast(
                "PydanticConstraints",
                cls.parse_constraints(metadata=metadata) if metadata else {},
            )

            if "url" in constraints:
                # pydantic uses a sentinel value for url constraints
                annotation = str

            if is_json:
                constraints["json"] = True

        result = super().from_type(
            annotation=annotation,
            children=children,
            constraints=cast("Constraints", {k: v for k, v in constraints.items() if v is not None}) or None,
            default=default_value,
            name=name,
        )
        result.examples = field_info.examples
        return result

    @classmethod
    def from_model_field(
        cls,
        model_field: ModelField,  # pyright: ignore[reportGeneralTypeIssues]
        use_alias: bool,
        required: bool = True,
    ) -> PydanticFieldMeta:
        """Create an instance from a pydantic model field.
        :param model_field: A pydantic ModelField.
        :param use_alias: Whether to use the field alias.

        :returns: A PydanticFieldMeta instance.

        """
        if model_field.default is not Undefined:
            default_value = model_field.default
        elif callable(model_field.default_factory):
            default_value = model_field.default_factory()
        else:
            default_value = model_field.default if model_field.default is not Undefined else Null

        name = model_field.alias if model_field.alias and use_alias else model_field.name

        outer_type = unwrap_new_type(model_field.outer_type_)
        annotation = (
            model_field.outer_type_
            if isinstance(model_field.annotation, (DeferredType, ForwardRef))
            else unwrap_new_type(model_field.annotation)
        )

        # In pydantic v1, we need to check if the annotation is directly annotated to properly extract constraints
        # from the metadata, as v1 doesn't automatically propagate constraints like v2 does
        annotation_constraints: Constraints = {}
        if is_annotated(model_field.annotation):
            annotation_metadata = cls.get_constraints_metadata(model_field.annotation)
            annotation_constraints = cls.parse_constraints(annotation_metadata) if annotation_metadata else {}

        field_info_constraints = {
            "ge": getattr(outer_type, "ge", model_field.field_info.ge),
            "gt": getattr(outer_type, "gt", model_field.field_info.gt),
            "le": getattr(outer_type, "le", model_field.field_info.le),
            "lt": getattr(outer_type, "lt", model_field.field_info.lt),
            "min_length": (
                getattr(outer_type, "min_length", model_field.field_info.min_length)
                or getattr(outer_type, "min_items", model_field.field_info.min_items)
            ),
            "max_length": (
                getattr(outer_type, "max_length", model_field.field_info.max_length)
                or getattr(outer_type, "max_items", model_field.field_info.max_items)
            ),
            "pattern": getattr(outer_type, "regex", model_field.field_info.regex),
            "unique_items": getattr(outer_type, "unique_items", model_field.field_info.unique_items),
            "decimal_places": getattr(outer_type, "decimal_places", None),
            "max_digits": getattr(outer_type, "max_digits", None),
            "multiple_of": getattr(outer_type, "multiple_of", None),
            "upper_case": getattr(outer_type, "to_upper", None),
            "lower_case": getattr(outer_type, "to_lower", None),
            "item_type": getattr(outer_type, "item_type", None),
        }

        constraints = cast("Constraints", {**field_info_constraints, **annotation_constraints})

        # pydantic v1 has constraints set for these values, but we generate them using faker
        if unwrap_optional(annotation) in (
            AnyUrl,
            AnyHttpUrl,
            HttpUrl,
            pydantic_v1.AnyUrl,
            pydantic_v1.AnyHttpUrl,
            pydantic_v1.HttpUrl,
            pydantic_v1.KafkaDsn,
            pydantic_v1.PostgresDsn,
            pydantic_v1.RedisDsn,
            pydantic_v1.AmqpDsn,
            pydantic_v1.EmailStr,
            pydantic_v1.NameEmail,
        ):
            constraints = {}

        if model_field.field_info.const and (
            default_value is None or isinstance(default_value, (int, bool, str, bytes))
        ):
            annotation = Literal[default_value]

        children: list[FieldMeta] = []

        # Refer #412.
        args = get_args(model_field.annotation)
        if is_optional(model_field.annotation) and len(args) == 2:  # noqa: PLR2004
            child_annotation = args[0] if args[0] is not NoneType else args[1]
            children.append(PydanticFieldMeta.from_type(child_annotation))
        elif model_field.key_field or model_field.sub_fields:
            fields_to_iterate = (
                ([model_field.key_field, *model_field.sub_fields])
                if model_field.key_field is not None
                else model_field.sub_fields
            )
            children.extend(
                PydanticFieldMeta.from_model_field(
                    model_field=arg,
                    use_alias=use_alias,
                )
                for arg in fields_to_iterate
            )

        examples = None

        return PydanticFieldMeta(
            name=name,
            annotation=annotation,  # pyright: ignore[reportArgumentType]
            children=children or None,
            default=default_value,
            constraints=cast("PydanticConstraints", {k: v for k, v in constraints.items() if v is not None}) or None,
            examples=examples,
            required=required,
        )

    if not _IS_PYDANTIC_V1:

        @classmethod
        def get_constraints_metadata(cls, annotation: Any) -> Sequence[Any]:
            metadata = []
            for m in super().get_constraints_metadata(annotation):
                if isinstance(m, FieldInfo):
                    metadata.extend(m.metadata)
                else:
                    metadata.append(m)

            return metadata


class ModelFactory(Generic[T], BaseFactory[T]):
    """Base factory for pydantic models"""

    __forward_ref_resolution_type_mapping__: ClassVar[Mapping[str, type]] = {}
    __is_base_factory__ = True
    __use_examples__: ClassVar[bool] = False  # for backwards compatibility
    """
    Flag indicating whether to use a random example, if provided (Pydantic >=V2)

    Example code::

        class Payment(BaseModel):
            amount: int = Field(0)
            currency: str = Field(examples=['USD', 'EUR', 'INR'])

        class PaymentFactory(ModelFactory[Payment]):
            __use_examples__ = True

    >>> payment = PaymentFactory.build()
    >>> payment
    Payment(amount=120, currency="EUR")
    """
    __by_name__: ClassVar[bool] = False
    """
    Flag indicating whether to use model_validate with by_name parameter (Pydantic V2 only)

    This helps handle validation aliases automatically without requiring users to modify their model configurations.

    Example code::

        class MyModel(BaseModel):
            field_a: str = Field(..., validation_alias="special_field_a")

        class MyFactory(ModelFactory[MyModel]):
            __by_name__ = True

    >>> instance = MyFactory.build(field_a="test")
    >>> instance.field_a
    "test"
    """
    if not _IS_PYDANTIC_V1:
        __forward_references__: ClassVar[dict[str, Any]] = {
            # Resolve to str to avoid recursive issues
            "JsonValue": str,
        }

    __config_keys__ = (
        *BaseFactory.__config_keys__,
        "__use_examples__",
        "__by_name__",
    )

    @classmethod
    def _init_model(cls) -> None:
        super()._init_model()

        model = getattr(cls, "__model__", None)
        if model is None:
            return

        if _is_pydantic_v1_model(model) and hasattr(cls.__model__, "update_forward_refs"):
            with suppress(NameError):  # pragma: no cover
                cls.__model__.update_forward_refs(**cls.__forward_ref_resolution_type_mapping__)

        if _is_pydantic_v2_model(model):
            model.model_rebuild()

    @classmethod
    def is_supported_type(cls, value: Any) -> TypeGuard[type[T]]:
        """Determine whether the given value is supported by the factory.

        :param value: An arbitrary value.
        :returns: A typeguard
        """

        return _is_pydantic_v1_model(value) or _is_pydantic_v2_model(value)

    @classmethod
    def get_model_fields(cls) -> list["FieldMeta"]:
        """Retrieve a list of fields from the factory's model.


        :returns: A list of field MetaData instances.

        """
        if "_fields_metadata" not in cls.__dict__:
            if _is_pydantic_v1_model(cls.__model__):
                cls._fields_metadata = [
                    PydanticFieldMeta.from_model_field(
                        field,
                        use_alias=not cls.__model__.__config__.allow_population_by_field_name,  # type: ignore[attr-defined]
                    )
                    for field in cls.__model__.__fields__.values()
                ]
            else:
                use_alias = cls.__model__.model_config.get("validate_by_name", False) or cls.__model__.model_config.get(
                    "populate_by_name", False
                )
                cls._fields_metadata = [
                    PydanticFieldMeta.from_field_info(
                        field_info=field_info,
                        field_name=field_name,
                        use_alias=not use_alias,
                    )
                    for field_name, field_info in cls.__model__.model_fields.items()  # pyright: ignore[reportGeneralTypeIssues]
                ]
        return cls._fields_metadata

    @classmethod
    def get_constrained_field_value(
        cls,
        annotation: Any,
        field_meta: FieldMeta,
        field_build_parameters: Any | None = None,
        build_context: BuildContext | None = None,
    ) -> Any:
        constraints = cast("PydanticConstraints", field_meta.constraints)

        if constraints.pop("json", None):
            value = cls.get_field_value(
                field_meta, field_build_parameters=field_build_parameters, build_context=build_context
            )
            return to_json(value)  # pyright: ignore[reportPossiblyUnboundVariable]

        return super().get_constrained_field_value(
            annotation, field_meta, field_build_parameters=field_build_parameters, build_context=build_context
        )

    @classmethod
    def get_field_value(
        cls,
        field_meta: FieldMeta,
        field_build_parameters: Any | None = None,
        build_context: BuildContext | None = None,
    ) -> Any:
        """Return a value from examples if exists, else random value.

        :param field_meta: FieldMeta instance.
        :param field_build_parameters: Any build parameters passed to the factory as kwarg values.
        :param build_context: BuildContext data for current build.

        :returns: An arbitrary value.

        """
        result: Any

        field_meta = cast("PydanticFieldMeta", field_meta)

        if cls.__use_examples__ and field_meta.examples:
            result = cls.__random__.choice(field_meta.examples)
        else:
            result = super().get_field_value(
                field_meta=field_meta, field_build_parameters=field_build_parameters, build_context=build_context
            )
        return result

    @classmethod
    def build(
        cls,
        factory_use_construct: bool = False,
        **kwargs: Any,
    ) -> T:
        """Build an instance of the factory's __model__

        :param factory_use_construct: A boolean that determines whether validations will be made when instantiating the
                model. This is supported only for pydantic models.
        :param kwargs: Any kwargs. If field_meta names are set in kwargs, their values will be used.

        :returns: An instance of type T.

        """

        if "_build_context" not in kwargs:
            kwargs["_build_context"] = PydanticBuildContext(
                seen_models=set(),
                factory_use_construct=factory_use_construct,
            )

        processed_kwargs = cls.process_kwargs(**kwargs)

        return cls._create_model(kwargs["_build_context"], **processed_kwargs)

    @classmethod
    def _get_build_context(cls, build_context: BaseBuildContext | PydanticBuildContext | None) -> PydanticBuildContext:
        """Return a PydanticBuildContext instance. If build_context is None, return a new PydanticBuildContext.

        :returns: PydanticBuildContext

        """
        build_context = cast("PydanticBuildContext", super()._get_build_context(build_context))
        if build_context.get("factory_use_construct") is None:
            build_context["factory_use_construct"] = False

        return build_context

    @classmethod
    def _create_model(cls, _build_context: PydanticBuildContext, **kwargs: Any) -> T:
        """Create an instance of the factory's __model__

        :param _build_context: BuildContext instance.
        :param kwargs: Model kwargs.

        :returns: An instance of type T.

        """
        if _build_context.get("factory_use_construct"):
            if _is_pydantic_v1_model(cls.__model__):
                return cls.__model__.construct(**kwargs)  # type: ignore[return-value]
            return cls.__model__.model_construct(**kwargs)

        # Use model_validate with by_name for Pydantic v2 models when requested
        if cls.__by_name__ and _is_pydantic_v2_model(cls.__model__):
            return cls.__model__.model_validate(kwargs, by_name=True)  # type: ignore[return-value]

        return cls.__model__(**kwargs)

    @classmethod
    def coverage(cls, factory_use_construct: bool = False, **kwargs: Any) -> abc.Iterator[T]:
        """Build a batch of the factory's Meta.model with full coverage of the sub-types of the model.

        :param factory_use_construct: A boolean that determines whether validations will be made when instantiating the
                model. This is supported only for pydantic models.
        :param kwargs: Any kwargs. If field_meta names are set in kwargs, their values will be used.

        :returns: A iterator of instances of type T.

        """

        if "_build_context" not in kwargs:
            kwargs["_build_context"] = PydanticBuildContext(
                seen_models=set(),
                factory_use_construct=factory_use_construct,
            )

        for data in cls.process_kwargs_coverage(**kwargs):
            yield cls._create_model(_build_context=kwargs["_build_context"], **data)

    @classmethod
    def is_custom_root_field(cls, field_meta: FieldMeta) -> bool:
        """Determine whether the field is a custom root field.

        :param field_meta: FieldMeta instance.

        :returns: A boolean determining whether the field is a custom root.

        """
        return field_meta.name == "__root__"

    @classmethod
    def should_set_field_value(cls, field_meta: FieldMeta, **kwargs: Any) -> bool:
        """Determine whether to set a value for a given field_name.
        This is an override of BaseFactory.should_set_field_value.

        :param field_meta: FieldMeta instance.
        :param kwargs: Any kwargs passed to the factory.

        :returns: A boolean determining whether a value should be set for the given field_meta.

        """
        return field_meta.name not in kwargs and (
            not field_meta.name.startswith("_") or cls.is_custom_root_field(field_meta)
        )

    @classmethod
    def get_provider_map(cls) -> dict[Any, Callable[[], Any]]:
        mapping: dict[Any, Callable[[], Any]] = {
            ByteSize: cls.__faker__.pyint,
            PositiveInt: cls.__faker__.pyint,
            NegativeFloat: lambda: cls.__random__.uniform(-100, -1),
            NegativeInt: lambda: cls.__faker__.pyint() * -1,
            PositiveFloat: cls.__faker__.pyint,
            NonPositiveFloat: lambda: cls.__random__.uniform(-100, 0),
            NonNegativeInt: cls.__faker__.pyint,
            StrictInt: cls.__faker__.pyint,
            StrictBool: cls.__faker__.pybool,
            StrictBytes: lambda: create_random_bytes(cls.__random__),
            StrictFloat: cls.__faker__.pyfloat,
            StrictStr: cls.__faker__.pystr,
            EmailStr: cls.__faker__.free_email,
            NameEmail: cls.__faker__.free_email,
            Json: cls.__faker__.json,
            PaymentCardNumber: cls.__faker__.credit_card_number,
            AnyUrl: cls.__faker__.url,
            AnyHttpUrl: cls.__faker__.url,
            HttpUrl: cls.__faker__.url,
            SecretBytes: lambda: create_random_bytes(cls.__random__),
            SecretStr: cls.__faker__.pystr,
            IPvAnyAddress: cls.__faker__.ipv4,
            IPvAnyInterface: cls.__faker__.ipv4,
            IPvAnyNetwork: lambda: cls.__faker__.ipv4(network=True),
            PastDate: cls.__faker__.past_date,
            FutureDate: cls.__faker__.future_date,
        }

        # v1 only values
        mapping.update(
            {
                pydantic_v1.AnyUrl: cls.__faker__.url,
                pydantic_v1.AnyHttpUrl: cls.__faker__.url,
                pydantic_v1.HttpUrl: cls.__faker__.url,
                pydantic_v1.PyObject: lambda: "decimal.Decimal",
                pydantic_v1.AmqpDsn: lambda: "amqps://example.com",
                pydantic_v1.KafkaDsn: lambda: "kafka://localhost:9092",
                pydantic_v1.PostgresDsn: lambda: "postgresql://user@localhost",
                pydantic_v1.RedisDsn: lambda: "redis://localhost:6379/0",
                pydantic_v1.FilePath: lambda: Path(realpath(__file__)),
                pydantic_v1.DirectoryPath: lambda: Path(realpath(__file__)).parent,
                pydantic_v1.UUID1: uuid1,
                pydantic_v1.UUID3: lambda: uuid3(NAMESPACE_DNS, cls.__faker__.pystr()),
                pydantic_v1.UUID4: cls.__faker__.uuid4,
                pydantic_v1.UUID5: lambda: uuid5(NAMESPACE_DNS, cls.__faker__.pystr()),
                Color: cls.__faker__.hex_color,  # pyright: ignore[reportGeneralTypeIssues]
                pydantic_v1.EmailStr: cls.__faker__.free_email,
                pydantic_v1.NameEmail: cls.__faker__.free_email,
            },
        )

        if not _IS_PYDANTIC_V1:
            mapping.update(
                {
                    # pydantic v2 specific types
                    pydantic.PastDatetime: cls.__faker__.past_datetime,
                    pydantic.FutureDatetime: cls.__faker__.future_datetime,
                    pydantic.AwareDatetime: partial(cls.__faker__.date_time, timezone.utc),
                    pydantic.NaiveDatetime: cls.__faker__.date_time,
                    pydantic.networks.AmqpDsn: lambda: "amqps://example.com",
                    pydantic.networks.KafkaDsn: lambda: "kafka://localhost:9092",
                    pydantic.networks.PostgresDsn: lambda: "postgresql://user@localhost",
                    pydantic.networks.RedisDsn: lambda: "redis://localhost:6379/0",
                    pydantic.networks.MongoDsn: lambda: "mongodb://mongodb0.example.com:27017",
                    pydantic.networks.MariaDBDsn: lambda: "mariadb://example.com:3306",
                    pydantic.networks.CockroachDsn: lambda: "cockroachdb://example.com:5432",
                    pydantic.networks.MySQLDsn: lambda: "mysql://example.com:5432",
                },
            )

        mapping.update(super().get_provider_map())
        return mapping

    @classmethod
    def get_field_value_coverage(
        cls,
        field_meta: FieldMeta,
        field_build_parameters: Any | None = None,
        build_context: BuildContext | None = None,
    ) -> Iterable[Any]:
        """Return a field value on the subclass if existing, otherwise returns a mock value.

        :param field_meta: FieldMeta instance.
        :param field_build_parameters: Any build parameters passed to the factory as kwarg values.
        :param build_context: BuildContext data for current build.

        :returns: An iterable of values.

        """
        if cls.is_ignored_type(field_meta.annotation):
            return

        if cls.__use_examples__:
            examples = getattr(field_meta, "examples", None) or []
            if len(examples) > 0:
                yield CoverageContainer(examples)
                return

        yield from super().get_field_value_coverage(field_meta, field_build_parameters, build_context)


def _is_pydantic_v1_model(model: Any) -> TypeGuard[BaseModelV1]:
    return is_safe_subclass(model, BaseModelV1)


def _is_pydantic_v2_model(model: Any) -> TypeGuard[BaseModelV2]:  # pyright: ignore[reportInvalidTypeForm]
    return not _IS_PYDANTIC_V1 and is_safe_subclass(model, BaseModelV2)
