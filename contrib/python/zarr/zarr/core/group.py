from __future__ import annotations

import asyncio
import itertools
import json
import logging
import unicodedata
import warnings
from collections import defaultdict
from dataclasses import asdict, dataclass, field, fields, replace
from itertools import accumulate
from typing import TYPE_CHECKING, Literal, TypeVar, assert_never, cast, overload

import numpy as np
import numpy.typing as npt
from typing_extensions import deprecated

import zarr.api.asynchronous as async_api
from zarr.abc.metadata import Metadata
from zarr.abc.store import Store, set_or_delete
from zarr.core._info import GroupInfo
from zarr.core.array import (
    DEFAULT_FILL_VALUE,
    Array,
    AsyncArray,
    CompressorLike,
    CompressorsLike,
    FiltersLike,
    SerializerLike,
    ShardsLike,
    _parse_deprecated_compressor,
    create_array,
)
from zarr.core.attributes import Attributes
from zarr.core.buffer import default_buffer_prototype
from zarr.core.common import (
    JSON,
    ZARR_JSON,
    ZARRAY_JSON,
    ZATTRS_JSON,
    ZGROUP_JSON,
    ZMETADATA_V2_JSON,
    DimensionNames,
    NodeType,
    ShapeLike,
    ZarrFormat,
    parse_shapelike,
)
from zarr.core.config import config
from zarr.core.metadata import ArrayV2Metadata, ArrayV3Metadata
from zarr.core.metadata.io import save_metadata
from zarr.core.sync import SyncMixin, sync
from zarr.errors import (
    ContainsArrayError,
    ContainsGroupError,
    GroupNotFoundError,
    MetadataValidationError,
    ZarrDeprecationWarning,
    ZarrUserWarning,
)
from zarr.storage import StoreLike, StorePath
from zarr.storage._common import ensure_no_existing_node, make_store_path
from zarr.storage._utils import _join_paths, _normalize_path_keys, normalize_path

if TYPE_CHECKING:
    from collections.abc import (
        AsyncGenerator,
        AsyncIterator,
        Coroutine,
        Generator,
        Iterable,
        Iterator,
        Mapping,
    )
    from typing import Any

    from zarr.core.array_spec import ArrayConfigLike
    from zarr.core.buffer import Buffer, BufferPrototype
    from zarr.core.chunk_key_encodings import ChunkKeyEncodingLike
    from zarr.core.common import MemoryOrder
    from zarr.core.dtype import ZDTypeLike
    from zarr.types import AnyArray, AnyAsyncArray, ArrayV2, ArrayV3, AsyncArrayV2, AsyncArrayV3

logger = logging.getLogger("zarr.group")

DefaultT = TypeVar("DefaultT")


def parse_zarr_format(data: Any) -> ZarrFormat:
    """Parse the zarr_format field from metadata."""
    if data in (2, 3):
        return cast("ZarrFormat", data)
    msg = f"Invalid zarr_format. Expected one of 2 or 3. Got {data}."
    raise ValueError(msg)


def parse_node_type(data: Any) -> NodeType:
    """Parse the node_type field from metadata."""
    if data in ("array", "group"):
        return cast("Literal['array', 'group']", data)
    msg = f"Invalid value for 'node_type'. Expected 'array' or 'group'. Got '{data}'."
    raise MetadataValidationError(msg)


# todo: convert None to empty dict
def parse_attributes(data: Any) -> dict[str, Any]:
    """Parse the attributes field from metadata."""
    if data is None:
        return {}
    elif isinstance(data, dict) and all(isinstance(k, str) for k in data):
        return data
    msg = f"Expected dict with string keys. Got {type(data)} instead."
    raise TypeError(msg)


@overload
def _parse_async_node(node: AsyncArrayV3) -> ArrayV3: ...


@overload
def _parse_async_node(node: AsyncArrayV2) -> ArrayV2: ...


@overload
def _parse_async_node(node: AsyncGroup) -> Group: ...


def _parse_async_node(
    node: AnyAsyncArray | AsyncGroup,
) -> AnyArray | Group:
    """Wrap an AsyncArray in an Array, or an AsyncGroup in a Group."""
    if isinstance(node, AsyncArray):
        return Array(node)
    elif isinstance(node, AsyncGroup):
        return Group(node)
    else:
        raise TypeError(f"Unknown node type, got {type(node)}")


@dataclass(frozen=True)
class ConsolidatedMetadata:
    """
    Consolidated Metadata for this Group.

    This stores the metadata of child nodes below this group. Any child groups
    will have their consolidated metadata set appropriately.
    """

    metadata: dict[str, ArrayV2Metadata | ArrayV3Metadata | GroupMetadata]
    kind: Literal["inline"] = "inline"
    must_understand: Literal[False] = False

    def to_dict(self) -> dict[str, JSON]:
        return {
            "kind": self.kind,
            "must_understand": self.must_understand,
            "metadata": {
                k: v.to_dict()
                for k, v in sorted(
                    self.flattened_metadata.items(),
                    key=lambda item: (
                        item[0].count("/"),
                        unicodedata.normalize("NFKC", item[0]).casefold(),
                    ),
                )
            },
        }

    @classmethod
    def from_dict(cls, data: dict[str, JSON]) -> ConsolidatedMetadata:
        data = dict(data)

        kind = data.get("kind")
        if kind != "inline":
            raise ValueError(f"Consolidated metadata kind='{kind}' is not supported.")

        raw_metadata = data.get("metadata")
        if not isinstance(raw_metadata, dict):
            raise TypeError(f"Unexpected type for 'metadata': {type(raw_metadata)}")

        metadata: dict[str, ArrayV2Metadata | ArrayV3Metadata | GroupMetadata] = {}
        if raw_metadata:
            for k, v in raw_metadata.items():
                if not isinstance(v, dict):
                    raise TypeError(
                        f"Invalid value for metadata items. key='{k}', type='{type(v).__name__}'"
                    )

                # zarr_format is present in v2 and v3.
                zarr_format = parse_zarr_format(v["zarr_format"])

                if zarr_format == 3:
                    node_type = parse_node_type(v.get("node_type", None))
                    if node_type == "group":
                        metadata[k] = GroupMetadata.from_dict(v)
                    elif node_type == "array":
                        metadata[k] = ArrayV3Metadata.from_dict(v)
                    else:
                        assert_never(node_type)
                elif zarr_format == 2:
                    if "shape" in v:
                        metadata[k] = ArrayV2Metadata.from_dict(v)
                    else:
                        metadata[k] = GroupMetadata.from_dict(v)
                else:
                    assert_never(zarr_format)

            cls._flat_to_nested(metadata)

        return cls(metadata=metadata)

    @staticmethod
    def _flat_to_nested(
        metadata: dict[str, ArrayV2Metadata | ArrayV3Metadata | GroupMetadata],
    ) -> None:
        """
        Convert a flat metadata representation to a nested one.

        Notes
        -----
        Flat metadata is used when persisting the consolidated metadata. The keys
        include the full path, not just the node name. The key prefixes can be
        used to determine which nodes are children of which other nodes.

        Nested metadata is used in-memory. The outermost level will only have the
        *immediate* children of the Group. All nested child groups will be stored
        under the consolidated metadata of their immediate parent.
        """
        # We have a flat mapping from {k: v} where the keys include the *full*
        # path segment:
        #  {
        #    "/a/b": { group_metadata },
        #    "/a/b/array-0": { array_metadata },
        #    "/a/b/array-1": { array_metadata },
        #  }
        #
        # We want to reorganize the metadata such that each Group contains the
        # array metadata of its immediate children.
        # In the example, the group at `/a/b` will have consolidated metadata
        # for its children `array-0` and `array-1`.
        #
        # metadata = dict(metadata)

        keys = sorted(metadata, key=lambda k: k.count("/"))
        grouped = {
            k: list(v) for k, v in itertools.groupby(keys, key=lambda k: k.rsplit("/", 1)[0])
        }

        # we go top down and directly manipulate metadata.
        for key, children_keys in grouped.items():
            # key is a key like "a", "a/b", "a/b/c"
            # The basic idea is to find the immediate parent (so "", "a", or "a/b")
            # and update that node's consolidated metadata to include the metadata
            # in children_keys
            *prefixes, name = key.split("/")
            parent = metadata

            while prefixes:
                # e.g. a/b/c has a parent "a/b". Walk through to get
                # metadata["a"]["b"]
                part = prefixes.pop(0)
                # we can assume that parent[part] here is a group
                # otherwise we wouldn't have a node with this `part` prefix.
                # We can also assume that the parent node will have consolidated metadata,
                # because we're walking top to bottom.
                parent = parent[part].consolidated_metadata.metadata  # type: ignore[union-attr]

            node = parent[name]
            children_keys = list(children_keys)

            if isinstance(node, ArrayV2Metadata | ArrayV3Metadata):
                # These are already present, either thanks to being an array in the
                # root, or by being collected as a child in the else clause
                continue
            children_keys = list(children_keys)
            # We pop from metadata, since we're *moving* this under group
            children = {
                child_key.split("/")[-1]: metadata.pop(child_key)
                for child_key in children_keys
                if child_key != key
            }
            parent[name] = replace(
                node, consolidated_metadata=ConsolidatedMetadata(metadata=children)
            )

    @property
    def flattened_metadata(self) -> dict[str, ArrayV2Metadata | ArrayV3Metadata | GroupMetadata]:
        """
        Return the flattened representation of Consolidated Metadata.

        The returned dictionary will have a key for each child node in the hierarchy
        under this group. Under the default (nested) representation available through
        ``self.metadata``, the dictionary only contains keys for immediate children.

        The keys of the dictionary will include the full path to a child node from
        the current group, where segments are joined by ``/``.

        Examples
        --------
        ```python
        from zarr.core.group import ConsolidatedMetadata, GroupMetadata
        cm = ConsolidatedMetadata(
            metadata={
                "group-0": GroupMetadata(
                    consolidated_metadata=ConsolidatedMetadata(
                        {
                            "group-0-0": GroupMetadata(),
                        }
                    )
                ),
                "group-1": GroupMetadata(),
            }
        )
        # {'group-0': GroupMetadata(attributes={}, zarr_format=3, consolidated_metadata=None, node_type='group'),
        #  'group-0/group-0-0': GroupMetadata(attributes={}, zarr_format=3, consolidated_metadata=None, node_type='group'),
        #  'group-1': GroupMetadata(attributes={}, zarr_format=3, consolidated_metadata=None, node_type='group')}
        ```
        """
        metadata = {}

        def flatten(
            key: str, group: GroupMetadata | ArrayV2Metadata | ArrayV3Metadata
        ) -> dict[str, ArrayV2Metadata | ArrayV3Metadata | GroupMetadata]:
            children: dict[str, ArrayV2Metadata | ArrayV3Metadata | GroupMetadata] = {}
            if isinstance(group, ArrayV2Metadata | ArrayV3Metadata):
                children[key] = group
            else:
                if group.consolidated_metadata and group.consolidated_metadata.metadata is not None:
                    children[key] = replace(
                        group, consolidated_metadata=ConsolidatedMetadata(metadata={})
                    )
                    for name, val in group.consolidated_metadata.metadata.items():
                        full_key = f"{key}/{name}"
                        if isinstance(val, GroupMetadata):
                            children.update(flatten(full_key, val))
                        else:
                            children[full_key] = val
                else:
                    children[key] = replace(group, consolidated_metadata=None)
            return children

        for k, v in self.metadata.items():
            metadata.update(flatten(k, v))

        return metadata


@dataclass(frozen=True)
class GroupMetadata(Metadata):
    """
    Metadata for a Group.
    """

    attributes: dict[str, Any] = field(default_factory=dict)
    zarr_format: ZarrFormat = 3
    consolidated_metadata: ConsolidatedMetadata | None = None
    node_type: Literal["group"] = field(default="group", init=False)

    def to_buffer_dict(self, prototype: BufferPrototype) -> dict[str, Buffer]:
        json_indent = config.get("json_indent")
        if self.zarr_format == 3:
            return {
                ZARR_JSON: prototype.buffer.from_bytes(
                    json.dumps(self.to_dict(), indent=json_indent, allow_nan=True).encode()
                )
            }
        else:
            items = {
                ZGROUP_JSON: prototype.buffer.from_bytes(
                    json.dumps({"zarr_format": self.zarr_format}, indent=json_indent).encode()
                ),
                ZATTRS_JSON: prototype.buffer.from_bytes(
                    json.dumps(self.attributes, indent=json_indent, allow_nan=True).encode()
                ),
            }
            if self.consolidated_metadata:
                d = {
                    ZGROUP_JSON: {"zarr_format": self.zarr_format},
                    ZATTRS_JSON: self.attributes,
                }
                consolidated_metadata = self.consolidated_metadata.to_dict()["metadata"]
                assert isinstance(consolidated_metadata, dict)
                for k, v in consolidated_metadata.items():
                    attrs = v.pop("attributes", {})
                    d[f"{k}/{ZATTRS_JSON}"] = attrs
                    if "shape" in v:
                        # it's an array
                        d[f"{k}/{ZARRAY_JSON}"] = v
                    else:
                        d[f"{k}/{ZGROUP_JSON}"] = {
                            "zarr_format": self.zarr_format,
                            "consolidated_metadata": {
                                "metadata": {},
                                "must_understand": False,
                                "kind": "inline",
                            },
                        }

                items[ZMETADATA_V2_JSON] = prototype.buffer.from_bytes(
                    json.dumps(
                        {"metadata": d, "zarr_consolidated_format": 1}, allow_nan=True
                    ).encode()
                )

            return items

    def __init__(
        self,
        attributes: dict[str, Any] | None = None,
        zarr_format: ZarrFormat = 3,
        consolidated_metadata: ConsolidatedMetadata | None = None,
    ) -> None:
        attributes_parsed = parse_attributes(attributes)
        zarr_format_parsed = parse_zarr_format(zarr_format)

        object.__setattr__(self, "attributes", attributes_parsed)
        object.__setattr__(self, "zarr_format", zarr_format_parsed)
        object.__setattr__(self, "consolidated_metadata", consolidated_metadata)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> GroupMetadata:
        data = dict(data)
        assert data.pop("node_type", None) in ("group", None)
        consolidated_metadata = data.pop("consolidated_metadata", None)
        if consolidated_metadata:
            data["consolidated_metadata"] = ConsolidatedMetadata.from_dict(consolidated_metadata)

        zarr_format = data.get("zarr_format")
        if zarr_format == 2 or zarr_format is None:
            # zarr v2 allowed arbitrary keys here.
            # We don't want the GroupMetadata constructor to fail just because someone put an
            # extra key in the metadata.
            expected = {x.name for x in fields(cls)}
            data = {k: v for k, v in data.items() if k in expected}

        return cls(**data)

    def to_dict(self) -> dict[str, Any]:
        result = asdict(replace(self, consolidated_metadata=None))
        if self.consolidated_metadata is not None:
            result["consolidated_metadata"] = self.consolidated_metadata.to_dict()
        else:
            # Leave consolidated metadata unset if it's None
            result.pop("consolidated_metadata")
        return result


@dataclass(frozen=True)
class ImplicitGroupMarker(GroupMetadata):
    """
    Marker for an implicit group. Instances of this class are only used in the context of group
    creation as a placeholder to represent groups that should only be created if they do not
    already exist in storage
    """


@dataclass(frozen=True)
class AsyncGroup:
    """
    Asynchronous Group object.
    """

    metadata: GroupMetadata
    store_path: StorePath

    # TODO: make this correct and work
    # TODO: ensure that this can be bound properly to subclass of AsyncGroup

    @classmethod
    async def from_store(
        cls,
        store: StoreLike,
        *,
        attributes: dict[str, Any] | None = None,
        overwrite: bool = False,
        zarr_format: ZarrFormat = 3,
    ) -> AsyncGroup:
        store_path = await make_store_path(store)

        if overwrite:
            if store_path.store.supports_deletes:
                await store_path.delete_dir()
            else:
                await ensure_no_existing_node(store_path, zarr_format=zarr_format)
        else:
            await ensure_no_existing_node(store_path, zarr_format=zarr_format)
        attributes = attributes or {}
        group = cls(
            metadata=GroupMetadata(attributes=attributes, zarr_format=zarr_format),
            store_path=store_path,
        )
        await group._save_metadata(ensure_parents=True)
        return group

    @classmethod
    async def open(
        cls,
        store: StoreLike,
        zarr_format: ZarrFormat | None = 3,
        use_consolidated: bool | str | None = None,
    ) -> AsyncGroup:
        """Open a new AsyncGroup

        Parameters
        ----------
        store : StoreLike
        zarr_format : {2, 3}, optional
        use_consolidated : bool or str, default None
            Whether to use consolidated metadata.

            By default, consolidated metadata is used if it's present in the
            store (in the ``zarr.json`` for Zarr format 3 and in the ``.zmetadata`` file
            for Zarr format 2) and the Store supports it.

            To explicitly require consolidated metadata, set ``use_consolidated=True``.
            In this case, if the Store doesn't support consolidation or consolidated metadata is
            not found, a ``ValueError`` exception is raised.

            To explicitly *not* use consolidated metadata, set ``use_consolidated=False``,
            which will fall back to using the regular, non consolidated metadata.

            Zarr format 2 allowed configuring the key storing the consolidated metadata
            (``.zmetadata`` by default). Specify the custom key as ``use_consolidated``
            to load consolidated metadata from a non-default key.
        """
        store_path = await make_store_path(store)
        if not store_path.store.supports_consolidated_metadata:
            # Fail if consolidated metadata was requested but the Store doesn't support it
            if use_consolidated:
                store_name = type(store_path.store).__name__
                raise ValueError(
                    f"The Zarr store in use ({store_name}) doesn't support consolidated metadata."
                )

            # if use_consolidated was None (optional), the Store dictates it doesn't want consolidation
            use_consolidated = False

        consolidated_key = ZMETADATA_V2_JSON

        if (zarr_format == 2 or zarr_format is None) and isinstance(use_consolidated, str):
            consolidated_key = use_consolidated

        if zarr_format == 2:
            paths = [store_path / ZGROUP_JSON, store_path / ZATTRS_JSON]
            if use_consolidated or use_consolidated is None:
                paths.append(store_path / consolidated_key)

            zgroup_bytes, zattrs_bytes, *rest = await asyncio.gather(
                *[path.get() for path in paths]
            )
            if zgroup_bytes is None:
                raise FileNotFoundError(store_path)

            if use_consolidated or use_consolidated is None:
                maybe_consolidated_metadata_bytes = rest[0]

            else:
                maybe_consolidated_metadata_bytes = None

        elif zarr_format == 3:
            zarr_json_bytes = await (store_path / ZARR_JSON).get()
            if zarr_json_bytes is None:
                raise FileNotFoundError(store_path)
        elif zarr_format is None:
            (
                zarr_json_bytes,
                zgroup_bytes,
                zattrs_bytes,
                maybe_consolidated_metadata_bytes,
            ) = await asyncio.gather(
                (store_path / ZARR_JSON).get(),
                (store_path / ZGROUP_JSON).get(),
                (store_path / ZATTRS_JSON).get(),
                (store_path / str(consolidated_key)).get(),
            )
            if zarr_json_bytes is not None and zgroup_bytes is not None:
                # warn and favor v3
                msg = f"Both zarr.json (Zarr format 3) and .zgroup (Zarr format 2) metadata objects exist at {store_path}. Zarr format 3 will be used."
                warnings.warn(msg, category=ZarrUserWarning, stacklevel=1)
            if zarr_json_bytes is None and zgroup_bytes is None:
                raise FileNotFoundError(
                    f"could not find zarr.json or .zgroup objects in {store_path}"
                )
            # set zarr_format based on which keys were found
            if zarr_json_bytes is not None:
                zarr_format = 3
            else:
                zarr_format = 2
        else:
            msg = f"Invalid value for 'zarr_format'. Expected 2, 3, or None. Got '{zarr_format}'."  # type: ignore[unreachable]
            raise MetadataValidationError(msg)

        if zarr_format == 2:
            # this is checked above, asserting here for mypy
            assert zgroup_bytes is not None

            if use_consolidated and maybe_consolidated_metadata_bytes is None:
                # the user requested consolidated metadata, but it was missing
                raise ValueError(consolidated_key)

            elif use_consolidated is False:
                # the user explicitly opted out of consolidated_metadata.
                # Discard anything we might have read.
                maybe_consolidated_metadata_bytes = None

            return cls._from_bytes_v2(
                store_path, zgroup_bytes, zattrs_bytes, maybe_consolidated_metadata_bytes
            )
        else:
            # V3 groups are comprised of a zarr.json object
            assert zarr_json_bytes is not None
            if not isinstance(use_consolidated, bool | None):
                raise TypeError("use_consolidated must be a bool or None for Zarr format 3.")

            return cls._from_bytes_v3(
                store_path,
                zarr_json_bytes,
                use_consolidated=use_consolidated,
            )

    @classmethod
    def _from_bytes_v2(
        cls,
        store_path: StorePath,
        zgroup_bytes: Buffer,
        zattrs_bytes: Buffer | None,
        consolidated_metadata_bytes: Buffer | None,
    ) -> AsyncGroup:
        # V2 groups are comprised of a .zgroup and .zattrs objects
        zgroup = json.loads(zgroup_bytes.to_bytes())
        zattrs = json.loads(zattrs_bytes.to_bytes()) if zattrs_bytes is not None else {}
        group_metadata = {**zgroup, "attributes": zattrs}

        if consolidated_metadata_bytes is not None:
            v2_consolidated_metadata = json.loads(consolidated_metadata_bytes.to_bytes())
            v2_consolidated_metadata = v2_consolidated_metadata["metadata"]
            # We already read zattrs and zgroup. Should we ignore these?
            v2_consolidated_metadata.pop(".zattrs", None)
            v2_consolidated_metadata.pop(".zgroup", None)

            consolidated_metadata: defaultdict[str, dict[str, Any]] = defaultdict(dict)

            # keys like air/.zarray, air/.zattrs
            for k, v in v2_consolidated_metadata.items():
                path, kind = k.rsplit("/.", 1)

                if kind == "zarray":
                    consolidated_metadata[path].update(v)
                elif kind == "zattrs":
                    consolidated_metadata[path]["attributes"] = v
                elif kind == "zgroup":
                    consolidated_metadata[path].update(v)
                else:
                    raise ValueError(f"Invalid file type '{kind}' at path '{path}")

            group_metadata["consolidated_metadata"] = {
                "metadata": dict(consolidated_metadata),
                "kind": "inline",
                "must_understand": False,
            }

        return cls.from_dict(store_path, group_metadata)

    @classmethod
    def _from_bytes_v3(
        cls,
        store_path: StorePath,
        zarr_json_bytes: Buffer,
        use_consolidated: bool | None,
    ) -> AsyncGroup:
        group_metadata = json.loads(zarr_json_bytes.to_bytes())
        if use_consolidated and group_metadata.get("consolidated_metadata") is None:
            msg = f"Consolidated metadata requested with 'use_consolidated=True' but not found in '{store_path.path}'."
            raise ValueError(msg)

        elif use_consolidated is False:
            # Drop consolidated metadata if it's there.
            group_metadata.pop("consolidated_metadata", None)

        return cls.from_dict(store_path, group_metadata)

    @classmethod
    def from_dict(
        cls,
        store_path: StorePath,
        data: dict[str, Any],
    ) -> AsyncGroup:
        node_type = data.pop("node_type", None)
        if node_type == "array":
            msg = f"An array already exists in store {store_path.store} at path {store_path.path}."
            raise ContainsArrayError(msg)
        elif node_type not in ("group", None):
            msg = f"Node type in metadata ({node_type}) is not 'group'"
            raise GroupNotFoundError(msg)
        return cls(
            metadata=GroupMetadata.from_dict(data),
            store_path=store_path,
        )

    async def setitem(self, key: str, value: Any) -> None:
        """
        Fastpath for creating a new array
        New arrays will be created with default array settings for the array type.

        Parameters
        ----------
        key : str
            Array name
        value : array-like
            Array data
        """
        path = self.store_path / key
        await async_api.save_array(
            store=path, arr=value, zarr_format=self.metadata.zarr_format, overwrite=True
        )

    async def getitem(
        self,
        key: str,
    ) -> AnyAsyncArray | AsyncGroup:
        """
        Get a subarray or subgroup from the group.

        Parameters
        ----------
        key : str
            Array or group name

        Returns
        -------
        AsyncArray or AsyncGroup
        """
        store_path = self.store_path / key
        logger.debug("key=%s, store_path=%s", key, store_path)

        # Consolidated metadata lets us avoid some I/O operations so try that first.
        if self.metadata.consolidated_metadata is not None:
            return self._getitem_consolidated(store_path, key, prefix=self.name)
        try:
            return await get_node(
                store=store_path.store, path=store_path.path, zarr_format=self.metadata.zarr_format
            )
        except FileNotFoundError as e:
            raise KeyError(key) from e

    def _getitem_consolidated(
        self, store_path: StorePath, key: str, prefix: str
    ) -> AnyAsyncArray | AsyncGroup:
        # getitem, in the special case where we have consolidated metadata.
        # Note that this is a regular def (non async) function.
        # This shouldn't do any additional I/O.

        # the caller needs to verify this!
        assert self.metadata.consolidated_metadata is not None

        # we support nested getitems like group/subgroup/array
        indexers = normalize_path(key).split("/")
        indexers.reverse()
        metadata: ArrayV2Metadata | ArrayV3Metadata | GroupMetadata = self.metadata

        while indexers:
            indexer = indexers.pop()
            if isinstance(metadata, ArrayV2Metadata | ArrayV3Metadata):
                # we've indexed into an array with group["array/subarray"]. Invalid.
                raise KeyError(key)
            if metadata.consolidated_metadata is None:
                # we've indexed into a group without consolidated metadata.
                # This isn't normal; typically, consolidated metadata
                # will include explicit markers for when there are no child
                # nodes as metadata={}.
                # We have some freedom in exactly how we interpret this case.
                # For now, we treat None as the same as {}, i.e. we don't
                # have any children.
                raise KeyError(key)
            try:
                metadata = metadata.consolidated_metadata.metadata[indexer]
            except KeyError as e:
                # The Group Metadata has consolidated metadata, but the key
                # isn't present. We trust this to mean that the key isn't in
                # the hierarchy, and *don't* fall back to checking the store.
                msg = f"'{key}' not found in consolidated metadata."
                raise KeyError(msg) from e

        # update store_path to ensure that AsyncArray/Group.name is correct
        if prefix != "/":
            key = "/".join([prefix.lstrip("/"), key])
        store_path = StorePath(store=store_path.store, path=key)

        if isinstance(metadata, GroupMetadata):
            return AsyncGroup(metadata=metadata, store_path=store_path)
        else:
            return AsyncArray(metadata=metadata, store_path=store_path)

    async def delitem(self, key: str) -> None:
        """Delete a group member.

        Parameters
        ----------
        key : str
            Array or group name
        """
        store_path = self.store_path / key

        await store_path.delete_dir()
        if self.metadata.consolidated_metadata:
            self.metadata.consolidated_metadata.metadata.pop(key, None)
            await self._save_metadata()

    async def get(
        self, key: str, default: DefaultT | None = None
    ) -> AnyAsyncArray | AsyncGroup | DefaultT | None:
        """Obtain a group member, returning default if not found.

        Parameters
        ----------
        key : str
            Group member name.
        default : object
            Default value to return if key is not found (default: None).

        Returns
        -------
        object
            Group member (AsyncArray or AsyncGroup) or default if not found.
        """
        try:
            return await self.getitem(key)
        except KeyError:
            return default

    async def _save_metadata(self, ensure_parents: bool = False) -> None:
        await save_metadata(self.store_path, self.metadata, ensure_parents=ensure_parents)

    @property
    def path(self) -> str:
        """Storage path."""
        return self.store_path.path

    @property
    def name(self) -> str:
        """Group name following h5py convention."""
        if self.path:
            # follow h5py convention: add leading slash
            name = self.path
            if name[0] != "/":
                name = "/" + name
            return name
        return "/"

    @property
    def basename(self) -> str:
        """Final component of name."""
        return self.name.split("/")[-1]

    @property
    def attrs(self) -> dict[str, Any]:
        return self.metadata.attributes

    @property
    def info(self) -> Any:
        """
        Return a visual representation of the statically known information about a group.

        Note that this doesn't include dynamic information, like the number of child
        Groups or Arrays.

        Returns
        -------
        GroupInfo

        Related
        -------
        [zarr.AsyncGroup.info_complete][]
            All information about a group, including dynamic information
        """

        if self.metadata.consolidated_metadata:
            members = list(self.metadata.consolidated_metadata.flattened_metadata.values())
        else:
            members = None
        return self._info(members=members)

    async def info_complete(self) -> Any:
        """
        Return all the information for a group.

        This includes dynamic information like the number
        of child Groups or Arrays. If this group doesn't contain consolidated
        metadata then this will need to read from the backing Store.

        Returns
        -------
        GroupInfo

        Related
        -------
        [zarr.AsyncGroup.info][]
        """
        members = [x[1].metadata async for x in self.members(max_depth=None)]
        return self._info(members=members)

    def _info(
        self, members: list[ArrayV2Metadata | ArrayV3Metadata | GroupMetadata] | None = None
    ) -> Any:
        kwargs = {}
        if members is not None:
            kwargs["_count_members"] = len(members)
            count_arrays = 0
            count_groups = 0
            for member in members:
                if isinstance(member, GroupMetadata):
                    count_groups += 1
                else:
                    count_arrays += 1
            kwargs["_count_arrays"] = count_arrays
            kwargs["_count_groups"] = count_groups

        return GroupInfo(
            _name=self.store_path.path,
            _read_only=self.read_only,
            _store_type=type(self.store_path.store).__name__,
            _zarr_format=self.metadata.zarr_format,
            # maybe do a typeddict
            **kwargs,  # type: ignore[arg-type]
        )

    @property
    def store(self) -> Store:
        return self.store_path.store

    @property
    def read_only(self) -> bool:
        # Backwards compatibility for 2.x
        return self.store_path.read_only

    @property
    def synchronizer(self) -> None:
        # Backwards compatibility for 2.x
        # Not implemented in 3.x yet.
        return None

    async def create_group(
        self,
        name: str,
        *,
        overwrite: bool = False,
        attributes: dict[str, Any] | None = None,
    ) -> AsyncGroup:
        """Create a sub-group.

        Parameters
        ----------
        name : str
            Group name.
        overwrite : bool, optional
            If True, do not raise an error if the group already exists.
        attributes : dict, optional
            Group attributes.

        Returns
        -------
        g : AsyncGroup
        """
        attributes = attributes or {}
        return await type(self).from_store(
            self.store_path / name,
            attributes=attributes,
            overwrite=overwrite,
            zarr_format=self.metadata.zarr_format,
        )

    async def require_group(self, name: str, overwrite: bool = False) -> AsyncGroup:
        """Obtain a sub-group, creating one if it doesn't exist.

        Parameters
        ----------
        name : str
            Group name.
        overwrite : bool, optional
            Overwrite any existing group with given `name` if present.

        Returns
        -------
        g : AsyncGroup
        """
        if overwrite:
            # TODO: check that overwrite=True errors if an array exists where the group is being created
            grp = await self.create_group(name, overwrite=True)
        else:
            try:
                item: AsyncGroup | AnyAsyncArray = await self.getitem(name)
                if not isinstance(item, AsyncGroup):
                    raise TypeError(
                        f"Incompatible object ({item.__class__.__name__}) already exists"
                    )
                assert isinstance(item, AsyncGroup)  # make mypy happy
                grp = item
            except KeyError:
                grp = await self.create_group(name)
        return grp

    async def require_groups(self, *names: str) -> tuple[AsyncGroup, ...]:
        """Convenience method to require multiple groups in a single call.

        Parameters
        ----------
        *names : str
            Group names.

        Returns
        -------
        Tuple[AsyncGroup, ...]
        """
        if not names:
            return ()
        return tuple(await asyncio.gather(*(self.require_group(name) for name in names)))

    async def create_array(
        self,
        name: str,
        *,
        shape: ShapeLike | None = None,
        dtype: ZDTypeLike | None = None,
        data: np.ndarray[Any, np.dtype[Any]] | None = None,
        chunks: tuple[int, ...] | Literal["auto"] = "auto",
        shards: ShardsLike | None = None,
        filters: FiltersLike = "auto",
        compressors: CompressorsLike = "auto",
        compressor: CompressorLike = "auto",
        serializer: SerializerLike = "auto",
        fill_value: Any | None = DEFAULT_FILL_VALUE,
        order: MemoryOrder | None = None,
        attributes: dict[str, JSON] | None = None,
        chunk_key_encoding: ChunkKeyEncodingLike | None = None,
        dimension_names: DimensionNames = None,
        storage_options: dict[str, Any] | None = None,
        overwrite: bool = False,
        config: ArrayConfigLike | None = None,
        write_data: bool = True,
    ) -> AnyAsyncArray:
        """Create an array within this group.

        This method lightly wraps [zarr.core.array.create_array][].

        Parameters
        ----------
        name : str
            The name of the array relative to the group. If ``path`` is ``None``, the array will be located
            at the root of the store.
        shape : tuple[int, ...]
            Shape of the array.
        dtype : npt.DTypeLike
            Data type of the array.
        chunks : tuple[int, ...], optional
            Chunk shape of the array.
            If not specified, default are guessed based on the shape and dtype.
        shards : tuple[int, ...], optional
            Shard shape of the array. The default value of ``None`` results in no sharding at all.
        filters : Iterable[Codec] | Literal["auto"], optional
            Iterable of filters to apply to each chunk of the array, in order, before serializing that
            chunk to bytes.

            For Zarr format 3, a "filter" is a codec that takes an array and returns an array,
            and these values must be instances of [`zarr.abc.codec.ArrayArrayCodec`][], or a
            dict representations of [`zarr.abc.codec.ArrayArrayCodec`][].

            For Zarr format 2, a "filter" can be any numcodecs codec; you should ensure that the
            the order if your filters is consistent with the behavior of each filter.

            The default value of ``"auto"`` instructs Zarr to use a default used based on the data
            type of the array and the Zarr format specified. For all data types in Zarr V3, and most
            data types in Zarr V2, the default filters are empty. The only cases where default filters
            are not empty is when the Zarr format is 2, and the data type is a variable-length data type like
            [`zarr.dtype.VariableLengthUTF8`][] or [`zarr.dtype.VariableLengthUTF8`][]. In these cases,
            the default filters contains a single element which is a codec specific to that particular data type.

            To create an array with no filters, provide an empty iterable or the value ``None``.
        compressors : Iterable[Codec], optional
            List of compressors to apply to the array. Compressors are applied in order, and after any
            filters are applied (if any are specified) and the data is serialized into bytes.

            For Zarr format 3, a "compressor" is a codec that takes a bytestream, and
            returns another bytestream. Multiple compressors my be provided for Zarr format 3.
            If no ``compressors`` are provided, a default set of compressors will be used.
            These defaults can be changed by modifying the value of ``array.v3_default_compressors``
            in [`zarr.config`][zarr.config].
            Use ``None`` to omit default compressors.

            For Zarr format 2, a "compressor" can be any numcodecs codec. Only a single compressor may
            be provided for Zarr format 2.
            If no ``compressor`` is provided, a default compressor will be used.
            in [`zarr.config`][zarr.config].
            Use ``None`` to omit the default compressor.
        compressor : Codec, optional
            Deprecated in favor of ``compressors``.
        serializer : dict[str, JSON] | ArrayBytesCodec, optional
            Array-to-bytes codec to use for encoding the array data.
            Zarr format 3 only. Zarr format 2 arrays use implicit array-to-bytes conversion.
            If no ``serializer`` is provided, a default serializer will be used.
            These defaults can be changed by modifying the value of ``array.v3_default_serializer``
            in [`zarr.config`][zarr.config].
        fill_value : Any, optional
            Fill value for the array.
        order : {"C", "F"}, optional
            The memory of the array (default is "C").
            For Zarr format 2, this parameter sets the memory order of the array.
            For Zarr format 3, this parameter is deprecated, because memory order
            is a runtime parameter for Zarr format 3 arrays. The recommended way to specify the memory
            order for Zarr format 3 arrays is via the ``config`` parameter, e.g. ``{'config': 'C'}``.
            If no ``order`` is provided, a default order will be used.
            This default can be changed by modifying the value of ``array.order`` in [`zarr.config`][zarr.config].
        attributes : dict, optional
            Attributes for the array.
        chunk_key_encoding : ChunkKeyEncoding, optional
            A specification of how the chunk keys are represented in storage.
            For Zarr format 3, the default is ``{"name": "default", "separator": "/"}}``.
            For Zarr format 2, the default is ``{"name": "v2", "separator": "."}}``.
        dimension_names : Iterable[str], optional
            The names of the dimensions (default is None).
            Zarr format 3 only. Zarr format 2 arrays should not use this parameter.
        storage_options : dict, optional
            If using an fsspec URL to create the store, these will be passed to the backend implementation.
            Ignored otherwise.
        overwrite : bool, default False
            Whether to overwrite an array with the same name in the store, if one exists.
        config : ArrayConfig or ArrayConfigLike, optional
            Runtime configuration for the array.
        write_data : bool
            If a pre-existing array-like object was provided to this function via the ``data`` parameter
            then ``write_data`` determines whether the values in that array-like object should be
            written to the Zarr array created by this function. If ``write_data`` is ``False``, then the
            array will be left empty.

        Returns
        -------
        AsyncArray

        """
        compressors = _parse_deprecated_compressor(
            compressor, compressors, zarr_format=self.metadata.zarr_format
        )
        return await create_array(
            store=self.store_path,
            name=name,
            shape=shape,
            dtype=dtype,
            data=data,
            chunks=chunks,
            shards=shards,
            filters=filters,
            compressors=compressors,
            serializer=serializer,
            fill_value=fill_value,
            order=order,
            zarr_format=self.metadata.zarr_format,
            attributes=attributes,
            chunk_key_encoding=chunk_key_encoding,
            dimension_names=dimension_names,
            storage_options=storage_options,
            overwrite=overwrite,
            config=config,
            write_data=write_data,
        )

    @deprecated("Use AsyncGroup.create_array instead.", category=ZarrDeprecationWarning)
    async def create_dataset(self, name: str, *, shape: ShapeLike, **kwargs: Any) -> AnyAsyncArray:
        """Create an array.

        !!! warning "Deprecated"
            `AsyncGroup.create_dataset()` is deprecated since v3.0.0 and will be removed in v3.1.0.
            Use `AsyncGroup.create_array` instead.

        Arrays are known as "datasets" in HDF5 terminology. For compatibility
        with h5py, Zarr groups also implement the [zarr.AsyncGroup.require_dataset][] method.

        Parameters
        ----------
        name : str
            Array name.
        **kwargs : dict
            Additional arguments passed to [zarr.AsyncGroup.create_array][].

        Returns
        -------
        a : AsyncArray
        """
        data = kwargs.pop("data", None)
        # create_dataset in zarr 2.x requires shape but not dtype if data is
        # provided. Allow this configuration by inferring dtype from data if
        # necessary and passing it to create_array
        if "dtype" not in kwargs and data is not None:
            kwargs["dtype"] = data.dtype
        array = await self.create_array(name, shape=shape, **kwargs)
        if data is not None:
            await array.setitem(slice(None), data)
        return array

    @deprecated("Use AsyncGroup.require_array instead.", category=ZarrDeprecationWarning)
    async def require_dataset(
        self,
        name: str,
        *,
        shape: tuple[int, ...],
        dtype: npt.DTypeLike = None,
        exact: bool = False,
        **kwargs: Any,
    ) -> AnyAsyncArray:
        """Obtain an array, creating if it doesn't exist.

        !!! warning "Deprecated"
            `AsyncGroup.require_dataset()` is deprecated since v3.0.0 and will be removed in v3.1.0.
            Use `AsyncGroup.require_dataset` instead.

        Arrays are known as "datasets" in HDF5 terminology. For compatibility
        with h5py, Zarr groups also implement the [zarr.AsyncGroup.create_dataset][] method.

        Other `kwargs` are as per [zarr.AsyncGroup.create_dataset][].

        Parameters
        ----------
        name : str
            Array name.
        shape : int or tuple of ints
            Array shape.
        dtype : str or dtype, optional
            NumPy dtype.
        exact : bool, optional
            If True, require `dtype` to match exactly. If false, require
            `dtype` can be cast from array dtype.

        Returns
        -------
        a : AsyncArray
        """
        return await self.require_array(name, shape=shape, dtype=dtype, exact=exact, **kwargs)

    async def require_array(
        self,
        name: str,
        *,
        shape: ShapeLike,
        dtype: npt.DTypeLike = None,
        exact: bool = False,
        **kwargs: Any,
    ) -> AnyAsyncArray:
        """Obtain an array, creating if it doesn't exist.

        Other `kwargs` are as per [zarr.AsyncGroup.create_dataset][].

        Parameters
        ----------
        name : str
            Array name.
        shape : int or tuple of ints
            Array shape.
        dtype : str or dtype, optional
            NumPy dtype.
        exact : bool, optional
            If True, require `dtype` to match exactly. If false, require
            `dtype` can be cast from array dtype.

        Returns
        -------
        a : AsyncArray
        """
        try:
            ds = await self.getitem(name)
            if not isinstance(ds, AsyncArray):
                raise TypeError(f"Incompatible object ({ds.__class__.__name__}) already exists")

            shape = parse_shapelike(shape)
            if shape != ds.shape:
                raise TypeError(f"Incompatible shape ({ds.shape} vs {shape})")

            dtype = np.dtype(dtype)
            if exact:
                if ds.dtype != dtype:
                    raise TypeError(f"Incompatible dtype ({ds.dtype} vs {dtype})")
            else:
                if not np.can_cast(ds.dtype, dtype):
                    raise TypeError(f"Incompatible dtype ({ds.dtype} vs {dtype})")
        except KeyError:
            ds = await self.create_array(name, shape=shape, dtype=dtype, **kwargs)

        return ds

    async def update_attributes(self, new_attributes: dict[str, Any]) -> AsyncGroup:
        """Update group attributes.

        Parameters
        ----------
        new_attributes : dict
            New attributes to set on the group.

        Returns
        -------
        self : AsyncGroup
        """
        self.metadata.attributes.update(new_attributes)

        # Write new metadata
        await self._save_metadata()

        return self

    def __repr__(self) -> str:
        return f"<AsyncGroup {self.store_path}>"

    async def nmembers(
        self,
        max_depth: int | None = 0,
    ) -> int:
        """Count the number of members in this group.

        Parameters
        ----------
        max_depth : int, default 0
            The maximum number of levels of the hierarchy to include. By
            default, (``max_depth=0``) only immediate children are included. Set
            ``max_depth=None`` to include all nodes, and some positive integer
            to consider children within that many levels of the root Group.

        Returns
        -------
        count : int
        """
        # check if we can use consolidated metadata, which requires that we have non-None
        # consolidated metadata at all points in the hierarchy.
        if self.metadata.consolidated_metadata is not None:
            if max_depth is not None and max_depth < 0:
                raise ValueError(f"max_depth must be None or >= 0. Got '{max_depth}' instead")
            if max_depth is None:
                return len(self.metadata.consolidated_metadata.flattened_metadata)
            else:
                return len(
                    [
                        x
                        for x in self.metadata.consolidated_metadata.flattened_metadata
                        if x.count("/") <= max_depth
                    ]
                )
        # TODO: consider using aioitertools.builtins.sum for this
        # return await aioitertools.builtins.sum((1 async for _ in self.members()), start=0)
        n = 0
        async for _ in self.members(max_depth=max_depth):
            n += 1
        return n

    async def members(
        self,
        max_depth: int | None = 0,
        *,
        use_consolidated_for_children: bool = True,
    ) -> AsyncGenerator[
        tuple[str, AnyAsyncArray | AsyncGroup],
        None,
    ]:
        """
        Returns an AsyncGenerator over the arrays and groups contained in this group.
        This method requires that `store_path.store` supports directory listing.

        The results are not guaranteed to be ordered.

        Parameters
        ----------
        max_depth : int, default 0
            The maximum number of levels of the hierarchy to include. By
            default, (``max_depth=0``) only immediate children are included. Set
            ``max_depth=None`` to include all nodes, and some positive integer
            to consider children within that many levels of the root Group.
        use_consolidated_for_children : bool, default True
            Whether to use the consolidated metadata of child groups loaded
            from the store. Note that this only affects groups loaded from the
            store. If the current Group already has consolidated metadata, it
            will always be used.

        Returns
        -------
        path:
            A string giving the path to the target, relative to the Group ``self``.
        value: AsyncArray or AsyncGroup
            The AsyncArray or AsyncGroup that is a child of ``self``.
        """
        if max_depth is not None and max_depth < 0:
            raise ValueError(f"max_depth must be None or >= 0. Got '{max_depth}' instead")
        async for item in self._members(
            max_depth=max_depth, use_consolidated_for_children=use_consolidated_for_children
        ):
            yield item

    def _members_consolidated(
        self, max_depth: int | None, prefix: str = ""
    ) -> Generator[
        tuple[str, AnyAsyncArray | AsyncGroup],
        None,
    ]:
        consolidated_metadata = self.metadata.consolidated_metadata

        do_recursion = max_depth is None or max_depth > 0

        # we kind of just want the top-level keys.
        if consolidated_metadata is not None:
            for key in consolidated_metadata.metadata:
                obj = self._getitem_consolidated(
                    self.store_path, key, prefix=self.name
                )  # Metadata -> Group/Array
                key = f"{prefix}/{key}".lstrip("/")
                yield key, obj

                if do_recursion and isinstance(obj, AsyncGroup):
                    if max_depth is None:
                        new_depth = None
                    else:
                        new_depth = max_depth - 1
                    yield from obj._members_consolidated(new_depth, prefix=key)

    async def _members(
        self, max_depth: int | None, *, use_consolidated_for_children: bool = True
    ) -> AsyncGenerator[tuple[str, AnyAsyncArray | AsyncGroup], None]:
        skip_keys: tuple[str, ...]
        if self.metadata.zarr_format == 2:
            skip_keys = (".zattrs", ".zgroup", ".zarray", ".zmetadata")
        elif self.metadata.zarr_format == 3:
            skip_keys = ("zarr.json",)
        else:
            raise ValueError(f"Unknown Zarr format: {self.metadata.zarr_format}")

        if self.metadata.consolidated_metadata is not None:
            members = self._members_consolidated(max_depth=max_depth)
            for member in members:
                yield member
            return

        if not self.store_path.store.supports_listing:
            msg = (
                f"The store associated with this group ({type(self.store_path.store)}) "
                "does not support listing, "
                "specifically via the `list_dir` method. "
                "This function requires a store that supports listing."
            )

            raise ValueError(msg)
        # enforce a concurrency limit by passing a semaphore to all the recursive functions
        semaphore = asyncio.Semaphore(config.get("async.concurrency"))
        async for member in _iter_members_deep(
            self,
            max_depth=max_depth,
            skip_keys=skip_keys,
            semaphore=semaphore,
            use_consolidated_for_children=use_consolidated_for_children,
        ):
            yield member

    async def create_hierarchy(
        self,
        nodes: dict[str, ArrayV2Metadata | ArrayV3Metadata | GroupMetadata],
        *,
        overwrite: bool = False,
    ) -> AsyncIterator[tuple[str, AsyncGroup | AnyAsyncArray]]:
        """
        Create a hierarchy of arrays or groups rooted at this group.

        This function will parse its input to ensure that the hierarchy is complete. Any implicit groups
        will be inserted as needed. For example, an input like
        ```{'a/b': GroupMetadata}``` will be parsed to
        ```{'': GroupMetadata, 'a': GroupMetadata, 'b': Groupmetadata}```.

        Explicitly specifying a root group, e.g. with ``nodes = {'': GroupMetadata()}`` is an error
        because this group instance is the root group.

        After input parsing, this function then creates all the nodes in the hierarchy concurrently.

        Arrays and Groups are yielded in the order they are created. This order is not stable and
        should not be relied on.

        Parameters
        ----------
        nodes : dict[str, GroupMetadata | ArrayV3Metadata | ArrayV2Metadata]
            A dictionary defining the hierarchy. The keys are the paths of the nodes in the hierarchy,
            relative to the path of the group. The values are instances of ``GroupMetadata`` or ``ArrayMetadata``. Note that
            all values must have the same ``zarr_format`` as the parent group -- it is an error to mix zarr versions in the
            same hierarchy.

            Leading "/" characters from keys will be removed.
        overwrite : bool
            Whether to overwrite existing nodes. Defaults to ``False``, in which case an error is
            raised instead of overwriting an existing array or group.

            This function will not erase an existing group unless that group is explicitly named in
            ``nodes``. If ``nodes`` defines implicit groups, e.g. ``{`'a/b/c': GroupMetadata}``, and a
            group already exists at path ``a``, then this function will leave the group at ``a`` as-is.

        Yields
        ------
            tuple[str, AsyncArray | AsyncGroup].
        """
        # check that all the nodes have the same zarr_format as Self
        prefix = self.path
        nodes_parsed = {}
        for key, value in nodes.items():
            if value.zarr_format != self.metadata.zarr_format:
                msg = (
                    "The zarr_format of the nodes must be the same as the parent group. "
                    f"The node at {key} has zarr_format {value.zarr_format}, but the parent group"
                    f" has zarr_format {self.metadata.zarr_format}."
                )
                raise ValueError(msg)
            if normalize_path(key) == "":
                msg = (
                    "The input defines a root node, but a root node already exists, namely this Group instance."
                    "It is an error to use this method to create a root node. "
                    "Remove the root node from the input dict, or use a function like "
                    "create_rooted_hierarchy to create a rooted hierarchy."
                )
                raise ValueError(msg)
            else:
                nodes_parsed[_join_paths([prefix, key])] = value

        async for key, node in create_hierarchy(
            store=self.store,
            nodes=nodes_parsed,
            overwrite=overwrite,
        ):
            if prefix == "":
                out_key = key
            else:
                out_key = key.removeprefix(prefix + "/")
            yield out_key, node

    async def keys(self) -> AsyncGenerator[str, None]:
        """Iterate over member names."""
        async for key, _ in self.members():
            yield key

    async def contains(self, member: str) -> bool:
        """Check if a member exists in the group.

        Parameters
        ----------
        member : str
            Member name.

        Returns
        -------
        bool
        """
        # TODO: this can be made more efficient.
        try:
            await self.getitem(member)
        except KeyError:
            return False
        else:
            return True

    async def groups(self) -> AsyncGenerator[tuple[str, AsyncGroup], None]:
        """Iterate over subgroups."""
        async for name, value in self.members():
            if isinstance(value, AsyncGroup):
                yield name, value

    async def group_keys(self) -> AsyncGenerator[str, None]:
        """Iterate over group names."""
        async for key, _ in self.groups():
            yield key

    async def group_values(self) -> AsyncGenerator[AsyncGroup, None]:
        """Iterate over group values."""
        async for _, group in self.groups():
            yield group

    async def arrays(
        self,
    ) -> AsyncGenerator[tuple[str, AnyAsyncArray], None]:
        """Iterate over arrays."""
        async for key, value in self.members():
            if isinstance(value, AsyncArray):
                yield key, value

    async def array_keys(self) -> AsyncGenerator[str, None]:
        """Iterate over array names."""
        async for key, _ in self.arrays():
            yield key

    async def array_values(
        self,
    ) -> AsyncGenerator[AnyAsyncArray, None]:
        """Iterate over array values."""
        async for _, array in self.arrays():
            yield array

    async def tree(self, expand: bool | None = None, level: int | None = None) -> Any:
        """
        Return a tree-like representation of a hierarchy.

        This requires the optional ``rich`` dependency.

        Parameters
        ----------
        expand : bool, optional
            This keyword is not yet supported. A NotImplementedError is raised if
            it's used.
        level : int, optional
            The maximum depth below this Group to display in the tree.

        Returns
        -------
        TreeRepr
            A pretty-printable object displaying the hierarchy.
        """
        from zarr.core._tree import group_tree_async

        if expand is not None:
            raise NotImplementedError("'expand' is not yet implemented.")
        return await group_tree_async(self, max_depth=level)

    async def empty(self, *, name: str, shape: tuple[int, ...], **kwargs: Any) -> AnyAsyncArray:
        """Create an empty array with the specified shape in this Group. The contents will
        be filled with the array's fill value or zeros if no fill value is provided.

        Parameters
        ----------
        name : str
            Name of the array.
        shape : int or tuple of int
            Shape of the empty array.
        **kwargs
            Keyword arguments passed to [zarr.api.asynchronous.create][].

        Notes
        -----
        The contents of an empty Zarr array are not defined. On attempting to
        retrieve data from an empty Zarr array, any values may be returned,
        and these are not guaranteed to be stable from one access to the next.
        """
        return await async_api.empty(shape=shape, store=self.store_path, path=name, **kwargs)

    async def zeros(self, *, name: str, shape: tuple[int, ...], **kwargs: Any) -> AnyAsyncArray:
        """Create an array, with zero being used as the default value for uninitialized portions of the array.

        Parameters
        ----------
        name : str
            Name of the array.
        shape : int or tuple of int
            Shape of the empty array.
        **kwargs
            Keyword arguments passed to [zarr.api.asynchronous.create][].

        Returns
        -------
        AsyncArray
            The new array.
        """
        return await async_api.zeros(shape=shape, store=self.store_path, path=name, **kwargs)

    async def ones(self, *, name: str, shape: tuple[int, ...], **kwargs: Any) -> AnyAsyncArray:
        """Create an array, with one being used as the default value for uninitialized portions of the array.

        Parameters
        ----------
        name : str
            Name of the array.
        shape : int or tuple of int
            Shape of the empty array.
        **kwargs
            Keyword arguments passed to [zarr.api.asynchronous.create][].

        Returns
        -------
        AsyncArray
            The new array.
        """
        return await async_api.ones(shape=shape, store=self.store_path, path=name, **kwargs)

    async def full(
        self, *, name: str, shape: tuple[int, ...], fill_value: Any | None, **kwargs: Any
    ) -> AnyAsyncArray:
        """Create an array, with "fill_value" being used as the default value for uninitialized portions of the array.

        Parameters
        ----------
        name : str
            Name of the array.
        shape : int or tuple of int
            Shape of the empty array.
        fill_value : scalar
            Value to fill the array with.
        **kwargs
            Keyword arguments passed to [zarr.api.asynchronous.create][].

        Returns
        -------
        AsyncArray
            The new array.
        """
        return await async_api.full(
            shape=shape,
            fill_value=fill_value,
            store=self.store_path,
            path=name,
            **kwargs,
        )

    async def empty_like(
        self, *, name: str, data: async_api.ArrayLike, **kwargs: Any
    ) -> AnyAsyncArray:
        """Create an empty sub-array like `data`. The contents will be filled with
        the array's fill value or zeros if no fill value is provided.

        Parameters
        ----------
        name : str
            Name of the array.
        data : array-like
            The array to create an empty array like.
        **kwargs
            Keyword arguments passed to [zarr.api.asynchronous.create][].

        Returns
        -------
        AsyncArray
            The new array.
        """
        return await async_api.empty_like(a=data, store=self.store_path, path=name, **kwargs)

    async def zeros_like(
        self, *, name: str, data: async_api.ArrayLike, **kwargs: Any
    ) -> AnyAsyncArray:
        """Create a sub-array of zeros like `data`.

        Parameters
        ----------
        name : str
            Name of the array.
        data : array-like
            The array to create the new array like.
        **kwargs
            Keyword arguments passed to [zarr.api.asynchronous.create][].

        Returns
        -------
        AsyncArray
            The new array.
        """
        return await async_api.zeros_like(a=data, store=self.store_path, path=name, **kwargs)

    async def ones_like(
        self, *, name: str, data: async_api.ArrayLike, **kwargs: Any
    ) -> AnyAsyncArray:
        """Create a sub-array of ones like `data`.

        Parameters
        ----------
        name : str
            Name of the array.
        data : array-like
            The array to create the new array like.
        **kwargs
            Keyword arguments passed to [zarr.api.asynchronous.create][].

        Returns
        -------
        AsyncArray
            The new array.
        """
        return await async_api.ones_like(a=data, store=self.store_path, path=name, **kwargs)

    async def full_like(
        self, *, name: str, data: async_api.ArrayLike, **kwargs: Any
    ) -> AnyAsyncArray:
        """Create a sub-array like `data` filled with the `fill_value` of `data` .

        Parameters
        ----------
        name : str
            Name of the array.
        data : array-like
            The array to create the new array like.
        **kwargs
            Keyword arguments passed to [zarr.api.asynchronous.create][].

        Returns
        -------
        AsyncArray
            The new array.
        """
        return await async_api.full_like(a=data, store=self.store_path, path=name, **kwargs)

    async def move(self, source: str, dest: str) -> None:
        """Move a sub-group or sub-array from one path to another.

        Notes
        -----
        Not implemented
        """
        raise NotImplementedError


@dataclass(frozen=True)
class Group(SyncMixin):
    """
    A Zarr group.
    """

    _async_group: AsyncGroup

    @classmethod
    def from_store(
        cls,
        store: StoreLike,
        *,
        attributes: dict[str, Any] | None = None,
        zarr_format: ZarrFormat = 3,
        overwrite: bool = False,
    ) -> Group:
        """Instantiate a group from an initialized store.

        Parameters
        ----------
        store : StoreLike
            StoreLike containing the Group. See the
            [storage documentation in the user guide][user-guide-store-like]
            for a description of all valid StoreLike values.
        attributes : dict, optional
            A dictionary of JSON-serializable values with user-defined attributes.
        zarr_format : {2, 3}, optional
            Zarr storage format version.
        overwrite : bool, optional
            If True, do not raise an error if the group already exists.

        Returns
        -------
        Group
            Group instantiated from the store.

        Raises
        ------
        ContainsArrayError, ContainsGroupError, ContainsArrayAndGroupError
        """
        attributes = attributes or {}
        obj = sync(
            AsyncGroup.from_store(
                store,
                attributes=attributes,
                overwrite=overwrite,
                zarr_format=zarr_format,
            ),
        )

        return cls(obj)

    @classmethod
    def open(
        cls,
        store: StoreLike,
        zarr_format: ZarrFormat | None = 3,
    ) -> Group:
        """Open a group from an initialized store.

        Parameters
        ----------
        store : StoreLike
            Store containing the Group. See the
            [storage documentation in the user guide][user-guide-store-like]
            for a description of all valid StoreLike values.
        zarr_format : {2, 3, None}, optional
            Zarr storage format version.

        Returns
        -------
        Group
            Group instantiated from the store.
        """
        obj = sync(AsyncGroup.open(store, zarr_format=zarr_format))
        return cls(obj)

    def __getitem__(self, path: str) -> AnyArray | Group:
        """Obtain a group member.

        Parameters
        ----------
        path : str
            Group member name.

        Returns
        -------
        Array | Group
            Group member (Array or Group) at the specified key

        Examples
        --------
        ```python
        import zarr
        from zarr.core.group import Group
        group = Group.from_store(zarr.storage.MemoryStore())
        group.create_array(name="subarray", shape=(10,), chunks=(10,), dtype="float64")
        group.create_group(name="subgroup").create_array(name="subarray", shape=(10,), chunks=(10,), dtype="float64")
        group["subarray"]
        # <Array memory://... shape=(10,) dtype=float64>
        group["subgroup"]
        # <Group memory://...>
        group["subgroup"]["subarray"]
        # <Array memory://... shape=(10,) dtype=float64>
        ```

        """
        obj = self._sync(self._async_group.getitem(path))
        if isinstance(obj, AsyncArray):
            return Array(obj)
        else:
            return Group(obj)

    def get(self, path: str, default: DefaultT | None = None) -> AnyArray | Group | DefaultT | None:
        """Obtain a group member, returning default if not found.

        Parameters
        ----------
        path : str
            Group member name.
        default : object
            Default value to return if key is not found (default: None).

        Returns
        -------
        object
            Group member (Array or Group) or default if not found.

        Examples
        --------
        ```python
        import zarr
        from zarr.core.group import Group
        group = Group.from_store(zarr.storage.MemoryStore())
        group.create_array(name="subarray", shape=(10,), chunks=(10,), dtype="float64")
        group.create_group(name="subgroup")
        group.get("subarray")
        # <Array memory://... shape=(10,) dtype=float64>
        group.get("subgroup")
        # <Group memory://...>
        group.get("nonexistent", None)
        # None
        ```

        """
        try:
            return self[path]
        except KeyError:
            return default

    def __delitem__(self, key: str) -> None:
        """Delete a group member.

        Parameters
        ----------
        key : str
            Group member name.

        Examples
        --------
        >>> import zarr
        >>> group = Group.from_store(zarr.storage.MemoryStore()
        >>> group.create_array(name="subarray", shape=(10,), chunks=(10,))
        >>> del group["subarray"]
        >>> "subarray" in group
        False
        """
        self._sync(self._async_group.delitem(key))

    def __iter__(self) -> Iterator[str]:
        """Return an iterator over group member names.
        Examples
        --------
        >>> import zarr
        >>> g1 = zarr.group()
        >>> g2 = g1.create_group('foo')
        >>> g3 = g1.create_group('bar')
        >>> d1 = g1.create_array('baz', shape=(10,), chunks=(10,))
        >>> d2 = g1.create_array('quux', shape=(10,), chunks=(10,))
        >>> for name in g1:
        ...     print(name)
        baz
        bar
        foo
        quux
        """
        yield from self.keys()

    def __len__(self) -> int:
        """Number of members."""
        return self.nmembers()

    def __setitem__(self, key: str, value: Any) -> None:
        """Fastpath for creating a new array.

        New arrays will be created using default settings for the array type.
        If you need to create an array with custom settings, use the `create_array` method.

        Parameters
        ----------
        key : str
            Array name.
        value : Any
            Array data.

        Examples
        --------
        >>> import zarr
        >>> group = zarr.group()
        >>> group["foo"] = zarr.zeros((10,))
        >>> group["foo"]
        <Array memory://132270269438272/foo shape=(10,) dtype=float64>
        """
        self._sync(self._async_group.setitem(key, value))

    def __repr__(self) -> str:
        return f"<Group {self.store_path}>"

    async def update_attributes_async(self, new_attributes: dict[str, Any]) -> Group:
        """Update the attributes of this group.

        Examples
        --------
        >>> import zarr
        >>> group = zarr.group()
        >>> await group.update_attributes_async({"foo": "bar"})
        >>> group.attrs.asdict()
        {'foo': 'bar'}
        """
        new_metadata = replace(self.metadata, attributes=new_attributes)

        # Write new metadata
        to_save = new_metadata.to_buffer_dict(default_buffer_prototype())
        awaitables = [set_or_delete(self.store_path / key, value) for key, value in to_save.items()]
        await asyncio.gather(*awaitables)

        async_group = replace(self._async_group, metadata=new_metadata)
        return replace(self, _async_group=async_group)

    @property
    def store_path(self) -> StorePath:
        """Path-like interface for the Store."""
        return self._async_group.store_path

    @property
    def metadata(self) -> GroupMetadata:
        """Group metadata."""
        return self._async_group.metadata

    @property
    def path(self) -> str:
        """Storage path."""
        return self._async_group.path

    @property
    def name(self) -> str:
        """Group name following h5py convention."""
        return self._async_group.name

    @property
    def basename(self) -> str:
        """Final component of name."""
        return self._async_group.basename

    @property
    def attrs(self) -> Attributes:
        """Attributes of this Group"""
        return Attributes(self)

    @property
    def info(self) -> Any:
        """
        Return the statically known information for a group.

        Returns
        -------
        GroupInfo

        Related
        -------
        [zarr.Group.info_complete][]
            All information about a group, including dynamic information
            like the children members.
        """
        return self._async_group.info

    def info_complete(self) -> Any:
        """
        Return information for a group.

        If this group doesn't contain consolidated metadata then
        this will need to read from the backing Store.

        Returns
        -------
        GroupInfo

        Related
        -------
        [zarr.Group.info][]
        """
        return self._sync(self._async_group.info_complete())

    @property
    def store(self) -> Store:
        # Backwards compatibility for 2.x
        return self._async_group.store

    @property
    def read_only(self) -> bool:
        # Backwards compatibility for 2.x
        return self._async_group.read_only

    @property
    def synchronizer(self) -> None:
        # Backwards compatibility for 2.x
        # Not implemented in 3.x yet.
        return self._async_group.synchronizer

    def update_attributes(self, new_attributes: dict[str, Any]) -> Group:
        """Update the attributes of this group.

        Examples
        --------
        >>> import zarr
        >>> group = zarr.group()
        >>> group.update_attributes({"foo": "bar"})
        >>> group.attrs.asdict()
        {'foo': 'bar'}
        """
        self._sync(self._async_group.update_attributes(new_attributes))
        return self

    def nmembers(self, max_depth: int | None = 0) -> int:
        """Count the number of members in this group.

        Parameters
        ----------
        max_depth : int, default 0
            The maximum number of levels of the hierarchy to include. By
            default, (``max_depth=0``) only immediate children are included. Set
            ``max_depth=None`` to include all nodes, and some positive integer
            to consider children within that many levels of the root Group.

        Returns
        -------
        count : int
        """

        return self._sync(self._async_group.nmembers(max_depth=max_depth))

    def members(
        self, max_depth: int | None = 0, *, use_consolidated_for_children: bool = True
    ) -> tuple[tuple[str, AnyArray | Group], ...]:
        """
        Returns an AsyncGenerator over the arrays and groups contained in this group.
        This method requires that `store_path.store` supports directory listing.

        The results are not guaranteed to be ordered.

        Parameters
        ----------
        max_depth : int, default 0
            The maximum number of levels of the hierarchy to include. By
            default, (``max_depth=0``) only immediate children are included. Set
            ``max_depth=None`` to include all nodes, and some positive integer
            to consider children within that many levels of the root Group.
        use_consolidated_for_children : bool, default True
            Whether to use the consolidated metadata of child groups loaded
            from the store. Note that this only affects groups loaded from the
            store. If the current Group already has consolidated metadata, it
            will always be used.

        Returns
        -------
        path:
            A string giving the path to the target, relative to the Group ``self``.
        value: AsyncArray or AsyncGroup
            The AsyncArray or AsyncGroup that is a child of ``self``.
        """
        _members = self._sync_iter(self._async_group.members(max_depth=max_depth))

        return tuple((kv[0], _parse_async_node(kv[1])) for kv in _members)

    def create_hierarchy(
        self,
        nodes: dict[str, ArrayV2Metadata | ArrayV3Metadata | GroupMetadata],
        *,
        overwrite: bool = False,
    ) -> Iterator[tuple[str, Group | AnyArray]]:
        """
        Create a hierarchy of arrays or groups rooted at this group.

        This function will parse its input to ensure that the hierarchy is complete. Any implicit groups
        will be inserted as needed. For example, an input like
        ```{'a/b': GroupMetadata}``` will be parsed to
        ```{'': GroupMetadata, 'a': GroupMetadata, 'b': Groupmetadata}```.

        Explicitly specifying a root group, e.g. with ``nodes = {'': GroupMetadata()}`` is an error
        because this group instance is the root group.

        After input parsing, this function then creates all the nodes in the hierarchy concurrently.

        Arrays and Groups are yielded in the order they are created. This order is not stable and
        should not be relied on.

        Parameters
        ----------
        nodes : dict[str, GroupMetadata | ArrayV3Metadata | ArrayV2Metadata]
            A dictionary defining the hierarchy. The keys are the paths of the nodes in the hierarchy,
            relative to the path of the group. The values are instances of ``GroupMetadata`` or ``ArrayMetadata``. Note that
            all values must have the same ``zarr_format`` as the parent group -- it is an error to mix zarr versions in the
            same hierarchy.

            Leading "/" characters from keys will be removed.
        overwrite : bool
            Whether to overwrite existing nodes. Defaults to ``False``, in which case an error is
            raised instead of overwriting an existing array or group.

            This function will not erase an existing group unless that group is explicitly named in
            ``nodes``. If ``nodes`` defines implicit groups, e.g. ``{`'a/b/c': GroupMetadata}``, and a
            group already exists at path ``a``, then this function will leave the group at ``a`` as-is.

        Yields
        ------
            tuple[str, Array | Group].

        Examples
        --------
        >>> import zarr
        >>> from zarr.core.group import GroupMetadata
        >>> root = zarr.create_group(store={})
        >>> for key, val in root.create_hierarchy({'a/b/c': GroupMetadata()}):
        ...   print(key, val)
        ...
        <AsyncGroup memory://123209880766144/a>
        <AsyncGroup memory://123209880766144/a/b/c>
        <AsyncGroup memory://123209880766144/a/b>
        """
        for key, node in self._sync_iter(
            self._async_group.create_hierarchy(nodes, overwrite=overwrite)
        ):
            yield (key, _parse_async_node(node))

    def keys(self) -> Generator[str, None]:
        """Return an iterator over group member names.

        Examples
        --------
        >>> import zarr
        >>> g1 = zarr.group()
        >>> g2 = g1.create_group('foo')
        >>> g3 = g1.create_group('bar')
        >>> d1 = g1.create_array('baz', shape=(10,), chunks=(10,))
        >>> d2 = g1.create_array('quux', shape=(10,), chunks=(10,))
        >>> for name in g1.keys():
        ...     print(name)
        baz
        bar
        foo
        quux
        """
        yield from self._sync_iter(self._async_group.keys())

    def __contains__(self, member: str) -> bool:
        """Test for group membership.

        Examples
        --------
        >>> import zarr
        >>> g1 = zarr.group()
        >>> g2 = g1.create_group('foo')
        >>> d1 = g1.create_array('bar', shape=(10,), chunks=(10,))
        >>> 'foo' in g1
        True
        >>> 'bar' in g1
        True
        >>> 'baz' in g1
        False

        """
        return self._sync(self._async_group.contains(member))

    def groups(self) -> Generator[tuple[str, Group], None]:
        """Return the sub-groups of this group as a generator of (name, group) pairs.

        Examples
        --------
        >>> import zarr
        >>> group = zarr.group()
        >>> group.create_group("subgroup")
        >>> for name, subgroup in group.groups():
        ...     print(name, subgroup)
        subgroup <Group memory://132270269438272/subgroup>
        """
        for name, async_group in self._sync_iter(self._async_group.groups()):
            yield name, Group(async_group)

    def group_keys(self) -> Generator[str, None]:
        """Return an iterator over group member names.

        Examples
        --------
        >>> import zarr
        >>> group = zarr.group()
        >>> group.create_group("subgroup")
        >>> for name in group.group_keys():
        ...     print(name)
        subgroup
        """
        for name, _ in self.groups():
            yield name

    def group_values(self) -> Generator[Group, None]:
        """Return an iterator over group members.

        Examples
        --------
        >>> import zarr
        >>> group = zarr.group()
        >>> group.create_group("subgroup")
        >>> for subgroup in group.group_values():
        ...     print(subgroup)
        <Group memory://132270269438272/subgroup>
        """
        for _, group in self.groups():
            yield group

    def arrays(self) -> Generator[tuple[str, AnyArray], None]:
        """Return the sub-arrays of this group as a generator of (name, array) pairs

        Examples
        --------
        >>> import zarr
        >>> group = zarr.group()
        >>> group.create_array("subarray", shape=(10,), chunks=(10,))
        >>> for name, subarray in group.arrays():
        ...     print(name, subarray)
        subarray <Array memory://140198565357056/subarray shape=(10,) dtype=float64>
        """
        for name, async_array in self._sync_iter(self._async_group.arrays()):
            yield name, Array(async_array)

    def array_keys(self) -> Generator[str, None]:
        """Return an iterator over group member names.

        Examples
        --------
        >>> import zarr
        >>> group = zarr.group()
        >>> group.create_array("subarray", shape=(10,), chunks=(10,))
        >>> for name in group.array_keys():
        ...     print(name)
        subarray
        """

        for name, _ in self.arrays():
            yield name

    def array_values(self) -> Generator[AnyArray, None]:
        """Return an iterator over group members.

        Examples
        --------
        >>> import zarr
        >>> group = zarr.group()
        >>> group.create_array("subarray", shape=(10,), chunks=(10,))
        >>> for subarray in group.array_values():
        ...     print(subarray)
        <Array memory://140198565357056/subarray shape=(10,) dtype=float64>
        """
        for _, array in self.arrays():
            yield array

    def tree(self, expand: bool | None = None, level: int | None = None) -> Any:
        """
        Return a tree-like representation of a hierarchy.

        This requires the optional ``rich`` dependency.

        Parameters
        ----------
        expand : bool, optional
            This keyword is not yet supported. A NotImplementedError is raised if
            it's used.
        level : int, optional
            The maximum depth below this Group to display in the tree.

        Returns
        -------
        TreeRepr
            A pretty-printable object displaying the hierarchy.
        """
        return self._sync(self._async_group.tree(expand=expand, level=level))

    def create_group(self, name: str, **kwargs: Any) -> Group:
        """Create a sub-group.

        Parameters
        ----------
        name : str
            Name of the new subgroup.

        Returns
        -------
        Group

        Examples
        --------
        >>> import zarr
        >>> group = zarr.group()
        >>> subgroup = group.create_group("subgroup")
        >>> subgroup
        <Group memory://132270269438272/subgroup>
        """
        return Group(self._sync(self._async_group.create_group(name, **kwargs)))

    def require_group(self, name: str, **kwargs: Any) -> Group:
        """Obtain a sub-group, creating one if it doesn't exist.

        Parameters
        ----------
        name : str
            Group name.

        Returns
        -------
        g : Group
        """
        return Group(self._sync(self._async_group.require_group(name, **kwargs)))

    def require_groups(self, *names: str) -> tuple[Group, ...]:
        """Convenience method to require multiple groups in a single call.

        Parameters
        ----------
        *names : str
            Group names.

        Returns
        -------
        groups : tuple of Groups
        """
        return tuple(map(Group, self._sync(self._async_group.require_groups(*names))))

    def create(
        self,
        name: str,
        *,
        shape: ShapeLike | None = None,
        dtype: ZDTypeLike | None = None,
        data: np.ndarray[Any, np.dtype[Any]] | None = None,
        chunks: tuple[int, ...] | Literal["auto"] = "auto",
        shards: ShardsLike | None = None,
        filters: FiltersLike = "auto",
        compressors: CompressorsLike = "auto",
        compressor: CompressorLike = "auto",
        serializer: SerializerLike = "auto",
        fill_value: Any | None = DEFAULT_FILL_VALUE,
        order: MemoryOrder | None = None,
        attributes: dict[str, JSON] | None = None,
        chunk_key_encoding: ChunkKeyEncodingLike | None = None,
        dimension_names: DimensionNames = None,
        storage_options: dict[str, Any] | None = None,
        overwrite: bool = False,
        config: ArrayConfigLike | None = None,
        write_data: bool = True,
    ) -> AnyArray:
        """Create an array within this group.

        This method lightly wraps [`zarr.core.array.create_array`][].

        Parameters
        ----------
        name : str
            The name of the array relative to the group. If ``path`` is ``None``, the array will be located
            at the root of the store.
        shape : ShapeLike, optional
            Shape of the array. Must be ``None`` if ``data`` is provided.
        dtype : npt.DTypeLike | None
            Data type of the array. Must be ``None`` if ``data`` is provided.
        data : Array-like data to use for initializing the array. If this parameter is provided, the
            ``shape`` and ``dtype`` parameters must be ``None``.
        chunks : tuple[int, ...], optional
            Chunk shape of the array.
            If not specified, default are guessed based on the shape and dtype.
        shards : tuple[int, ...], optional
            Shard shape of the array. The default value of ``None`` results in no sharding at all.
        filters : Iterable[Codec] | Literal["auto"], optional
            Iterable of filters to apply to each chunk of the array, in order, before serializing that
            chunk to bytes.

            For Zarr format 3, a "filter" is a codec that takes an array and returns an array,
            and these values must be instances of [`zarr.abc.codec.ArrayArrayCodec`][], or a
            dict representations of [`zarr.abc.codec.ArrayArrayCodec`][].

            For Zarr format 2, a "filter" can be any numcodecs codec; you should ensure that the
            the order if your filters is consistent with the behavior of each filter.

            The default value of ``"auto"`` instructs Zarr to use a default used based on the data
            type of the array and the Zarr format specified. For all data types in Zarr V3, and most
            data types in Zarr V2, the default filters are empty. The only cases where default filters
            are not empty is when the Zarr format is 2, and the data type is a variable-length data type like
            [`zarr.dtype.VariableLengthUTF8`][] or [`zarr.dtype.VariableLengthUTF8`][]. In these cases,
            the default filters contains a single element which is a codec specific to that particular data type.

            To create an array with no filters, provide an empty iterable or the value ``None``.
        compressors : Iterable[Codec], optional
            List of compressors to apply to the array. Compressors are applied in order, and after any
            filters are applied (if any are specified) and the data is serialized into bytes.

            For Zarr format 3, a "compressor" is a codec that takes a bytestream, and
            returns another bytestream. Multiple compressors my be provided for Zarr format 3.
            If no ``compressors`` are provided, a default set of compressors will be used.
            These defaults can be changed by modifying the value of ``array.v3_default_compressors``
            in [`zarr.config`][].
            Use ``None`` to omit default compressors.

            For Zarr format 2, a "compressor" can be any numcodecs codec. Only a single compressor may
            be provided for Zarr format 2.
            If no ``compressor`` is provided, a default compressor will be used.
            in [`zarr.config`][].
            Use ``None`` to omit the default compressor.
        compressor : Codec, optional
            Deprecated in favor of ``compressors``.
        serializer : dict[str, JSON] | ArrayBytesCodec, optional
            Array-to-bytes codec to use for encoding the array data.
            Zarr format 3 only. Zarr format 2 arrays use implicit array-to-bytes conversion.
            If no ``serializer`` is provided, a default serializer will be used.
            These defaults can be changed by modifying the value of ``array.v3_default_serializer``
            in [`zarr.config`][].
        fill_value : Any, optional
            Fill value for the array.
        order : {"C", "F"}, optional
            The memory of the array (default is "C").
            For Zarr format 2, this parameter sets the memory order of the array.
            For Zarr format 3, this parameter is deprecated, because memory order
            is a runtime parameter for Zarr format 3 arrays. The recommended way to specify the memory
            order for Zarr format 3 arrays is via the ``config`` parameter, e.g. ``{'config': 'C'}``.
            If no ``order`` is provided, a default order will be used.
            This default can be changed by modifying the value of ``array.order`` in [`zarr.config`][].
        attributes : dict, optional
            Attributes for the array.
        chunk_key_encoding : ChunkKeyEncoding, optional
            A specification of how the chunk keys are represented in storage.
            For Zarr format 3, the default is ``{"name": "default", "separator": "/"}}``.
            For Zarr format 2, the default is ``{"name": "v2", "separator": "."}}``.
        dimension_names : Iterable[str], optional
            The names of the dimensions (default is None).
            Zarr format 3 only. Zarr format 2 arrays should not use this parameter.
        storage_options : dict, optional
            If using an fsspec URL to create the store, these will be passed to the backend implementation.
            Ignored otherwise.
        overwrite : bool, default False
            Whether to overwrite an array with the same name in the store, if one exists.
        config : ArrayConfig or ArrayConfigLike, optional
            Runtime configuration for the array.
        write_data : bool
            If a pre-existing array-like object was provided to this function via the ``data`` parameter
            then ``write_data`` determines whether the values in that array-like object should be
            written to the Zarr array created by this function. If ``write_data`` is ``False``, then the
            array will be left empty.

        Returns
        -------
        AsyncArray
        """
        return self.create_array(
            name,
            shape=shape,
            dtype=dtype,
            data=data,
            chunks=chunks,
            shards=shards,
            filters=filters,
            compressors=compressors,
            compressor=compressor,
            serializer=serializer,
            fill_value=fill_value,
            order=order,
            attributes=attributes,
            chunk_key_encoding=chunk_key_encoding,
            dimension_names=dimension_names,
            storage_options=storage_options,
            overwrite=overwrite,
            config=config,
            write_data=write_data,
        )

    def create_array(
        self,
        name: str,
        *,
        shape: ShapeLike | None = None,
        dtype: ZDTypeLike | None = None,
        data: np.ndarray[Any, np.dtype[Any]] | None = None,
        chunks: tuple[int, ...] | Literal["auto"] = "auto",
        shards: ShardsLike | None = None,
        filters: FiltersLike = "auto",
        compressors: CompressorsLike = "auto",
        compressor: CompressorLike = "auto",
        serializer: SerializerLike = "auto",
        fill_value: Any | None = DEFAULT_FILL_VALUE,
        order: MemoryOrder | None = None,
        attributes: dict[str, JSON] | None = None,
        chunk_key_encoding: ChunkKeyEncodingLike | None = None,
        dimension_names: DimensionNames = None,
        storage_options: dict[str, Any] | None = None,
        overwrite: bool = False,
        config: ArrayConfigLike | None = None,
        write_data: bool = True,
    ) -> AnyArray:
        """Create an array within this group.

        This method lightly wraps [zarr.core.array.create_array][].

        Parameters
        ----------
        name : str
            The name of the array relative to the group. If ``path`` is ``None``, the array will be located
            at the root of the store.
        shape : ShapeLike, optional
            Shape of the array. Must be ``None`` if ``data`` is provided.
        dtype : npt.DTypeLike | None
            Data type of the array. Must be ``None`` if ``data`` is provided.
        data : Array-like data to use for initializing the array. If this parameter is provided, the
            ``shape`` and ``dtype`` parameters must be ``None``.
        chunks : tuple[int, ...], optional
            Chunk shape of the array.
            If not specified, default are guessed based on the shape and dtype.
        shards : tuple[int, ...], optional
            Shard shape of the array. The default value of ``None`` results in no sharding at all.
        filters : Iterable[Codec] | Literal["auto"], optional
            Iterable of filters to apply to each chunk of the array, in order, before serializing that
            chunk to bytes.

            For Zarr format 3, a "filter" is a codec that takes an array and returns an array,
            and these values must be instances of [`zarr.abc.codec.ArrayArrayCodec`][], or a
            dict representations of [`zarr.abc.codec.ArrayArrayCodec`][].

            For Zarr format 2, a "filter" can be any numcodecs codec; you should ensure that the
            the order if your filters is consistent with the behavior of each filter.

            The default value of ``"auto"`` instructs Zarr to use a default used based on the data
            type of the array and the Zarr format specified. For all data types in Zarr V3, and most
            data types in Zarr V2, the default filters are empty. The only cases where default filters
            are not empty is when the Zarr format is 2, and the data type is a variable-length data type like
            [`zarr.dtype.VariableLengthUTF8`][] or [`zarr.dtype.VariableLengthUTF8`][]. In these cases,
            the default filters contains a single element which is a codec specific to that particular data type.

            To create an array with no filters, provide an empty iterable or the value ``None``.
        compressors : Iterable[Codec], optional
            List of compressors to apply to the array. Compressors are applied in order, and after any
            filters are applied (if any are specified) and the data is serialized into bytes.

            For Zarr format 3, a "compressor" is a codec that takes a bytestream, and
            returns another bytestream. Multiple compressors my be provided for Zarr format 3.
            If no ``compressors`` are provided, a default set of compressors will be used.
            These defaults can be changed by modifying the value of ``array.v3_default_compressors``
            in [`zarr.config`][zarr.config].
            Use ``None`` to omit default compressors.

            For Zarr format 2, a "compressor" can be any numcodecs codec. Only a single compressor may
            be provided for Zarr format 2.
            If no ``compressor`` is provided, a default compressor will be used.
            in [`zarr.config`][zarr.config].
            Use ``None`` to omit the default compressor.
        compressor : Codec, optional
            Deprecated in favor of ``compressors``.
        serializer : dict[str, JSON] | ArrayBytesCodec, optional
            Array-to-bytes codec to use for encoding the array data.
            Zarr format 3 only. Zarr format 2 arrays use implicit array-to-bytes conversion.
            If no ``serializer`` is provided, a default serializer will be used.
            These defaults can be changed by modifying the value of ``array.v3_default_serializer``
            in [`zarr.config`][zarr.config].
        fill_value : Any, optional
            Fill value for the array.
        order : {"C", "F"}, optional
            The memory of the array (default is "C").
            For Zarr format 2, this parameter sets the memory order of the array.
            For Zarr format 3, this parameter is deprecated, because memory order
            is a runtime parameter for Zarr format 3 arrays. The recommended way to specify the memory
            order for Zarr format 3 arrays is via the ``config`` parameter, e.g. ``{'config': 'C'}``.
            If no ``order`` is provided, a default order will be used.
            This default can be changed by modifying the value of ``array.order`` in [`zarr.config`][zarr.config].
        attributes : dict, optional
            Attributes for the array.
        chunk_key_encoding : ChunkKeyEncoding, optional
            A specification of how the chunk keys are represented in storage.
            For Zarr format 3, the default is ``{"name": "default", "separator": "/"}}``.
            For Zarr format 2, the default is ``{"name": "v2", "separator": "."}}``.
        dimension_names : Iterable[str], optional
            The names of the dimensions (default is None).
            Zarr format 3 only. Zarr format 2 arrays should not use this parameter.
        storage_options : dict, optional
            If using an fsspec URL to create the store, these will be passed to the backend implementation.
            Ignored otherwise.
        overwrite : bool, default False
            Whether to overwrite an array with the same name in the store, if one exists.
        config : ArrayConfig or ArrayConfigLike, optional
            Runtime configuration for the array.
        write_data : bool
            If a pre-existing array-like object was provided to this function via the ``data`` parameter
            then ``write_data`` determines whether the values in that array-like object should be
            written to the Zarr array created by this function. If ``write_data`` is ``False``, then the
            array will be left empty.

        Returns
        -------
        AsyncArray
        """
        compressors = _parse_deprecated_compressor(
            compressor, compressors, zarr_format=self.metadata.zarr_format
        )
        return Array(
            self._sync(
                self._async_group.create_array(
                    name=name,
                    shape=shape,
                    dtype=dtype,
                    data=data,
                    chunks=chunks,
                    shards=shards,
                    fill_value=fill_value,
                    attributes=attributes,
                    chunk_key_encoding=chunk_key_encoding,
                    compressors=compressors,
                    serializer=serializer,
                    dimension_names=dimension_names,
                    order=order,
                    filters=filters,
                    overwrite=overwrite,
                    storage_options=storage_options,
                    config=config,
                    write_data=write_data,
                )
            )
        )

    @deprecated("Use Group.create_array instead.", category=ZarrDeprecationWarning)
    def create_dataset(self, name: str, **kwargs: Any) -> AnyArray:
        """Create an array.

        !!! warning "Deprecated"
            `Group.create_dataset()` is deprecated since v3.0.0 and will be removed in v3.1.0.
            Use `Group.create_array` instead.


        Arrays are known as "datasets" in HDF5 terminology. For compatibility
        with h5py, Zarr groups also implement the [zarr.Group.require_dataset][] method.

        Parameters
        ----------
        name : str
            Array name.
        **kwargs : dict
            Additional arguments passed to [zarr.Group.create_array][]

        Returns
        -------
        a : Array
        """
        return Array(self._sync(self._async_group.create_dataset(name, **kwargs)))

    @deprecated("Use Group.require_array instead.", category=ZarrDeprecationWarning)
    def require_dataset(self, name: str, *, shape: ShapeLike, **kwargs: Any) -> AnyArray:
        """Obtain an array, creating if it doesn't exist.

        !!! warning "Deprecated"
            `Group.require_dataset()` is deprecated since v3.0.0 and will be removed in v3.1.0.
            Use `Group.require_array` instead.

        Arrays are known as "datasets" in HDF5 terminology. For compatibility
        with h5py, Zarr groups also implement the [zarr.Group.create_dataset][] method.

        Other `kwargs` are as per [zarr.Group.create_dataset][].

        Parameters
        ----------
        name : str
            Array name.
        **kwargs :
            See [zarr.Group.create_dataset][].

        Returns
        -------
        a : Array
        """
        return Array(self._sync(self._async_group.require_array(name, shape=shape, **kwargs)))

    def require_array(self, name: str, *, shape: ShapeLike, **kwargs: Any) -> AnyArray:
        """Obtain an array, creating if it doesn't exist.

        Other `kwargs` are as per [zarr.Group.create_array][].

        Parameters
        ----------
        name : str
            Array name.
        **kwargs :
            See [zarr.Group.create_array][].

        Returns
        -------
        a : Array
        """
        return Array(self._sync(self._async_group.require_array(name, shape=shape, **kwargs)))

    def empty(self, *, name: str, shape: tuple[int, ...], **kwargs: Any) -> AnyArray:
        """Create an empty array with the specified shape in this Group. The contents will be filled with
        the array's fill value or zeros if no fill value is provided.

        Parameters
        ----------
        name : str
            Name of the array.
        shape : int or tuple of int
            Shape of the empty array.
        **kwargs
            Keyword arguments passed to [zarr.api.asynchronous.create][].

        Notes
        -----
        The contents of an empty Zarr array are not defined. On attempting to
        retrieve data from an empty Zarr array, any values may be returned,
        and these are not guaranteed to be stable from one access to the next.
        """
        return Array(self._sync(self._async_group.empty(name=name, shape=shape, **kwargs)))

    def zeros(self, *, name: str, shape: tuple[int, ...], **kwargs: Any) -> AnyArray:
        """Create an array, with zero being used as the default value for uninitialized portions of the array.

        Parameters
        ----------
        name : str
            Name of the array.
        shape : int or tuple of int
            Shape of the empty array.
        **kwargs
            Keyword arguments passed to [zarr.api.asynchronous.create][].

        Returns
        -------
        Array
            The new array.
        """
        return Array(self._sync(self._async_group.zeros(name=name, shape=shape, **kwargs)))

    def ones(self, *, name: str, shape: tuple[int, ...], **kwargs: Any) -> AnyArray:
        """Create an array, with one being used as the default value for uninitialized portions of the array.

        Parameters
        ----------
        name : str
            Name of the array.
        shape : int or tuple of int
            Shape of the empty array.
        **kwargs
            Keyword arguments passed to [zarr.api.asynchronous.create][].

        Returns
        -------
        Array
            The new array.
        """
        return Array(self._sync(self._async_group.ones(name=name, shape=shape, **kwargs)))

    def full(
        self, *, name: str, shape: tuple[int, ...], fill_value: Any | None, **kwargs: Any
    ) -> AnyArray:
        """Create an array, with "fill_value" being used as the default value for uninitialized portions of the array.

        Parameters
        ----------
        name : str
            Name of the array.
        shape : int or tuple of int
            Shape of the empty array.
        fill_value : scalar
            Value to fill the array with.
        **kwargs
            Keyword arguments passed to [zarr.api.asynchronous.create][].

        Returns
        -------
        Array
            The new array.
        """
        return Array(
            self._sync(
                self._async_group.full(name=name, shape=shape, fill_value=fill_value, **kwargs)
            )
        )

    def empty_like(self, *, name: str, data: async_api.ArrayLike, **kwargs: Any) -> AnyArray:
        """Create an empty sub-array like `data`. The contents will be filled
        with the array's fill value or zeros if no fill value is provided.

        Parameters
        ----------
        name : str
            Name of the array.
        data : array-like
            The array to create an empty array like.
        **kwargs
            Keyword arguments passed to [zarr.api.asynchronous.create][].

        Returns
        -------
        Array
            The new array.

        Notes
        -----
        The contents of an empty Zarr array are not defined. On attempting to
        retrieve data from an empty Zarr array, any values may be returned,
        and these are not guaranteed to be stable from one access to the next.
        """
        return Array(self._sync(self._async_group.empty_like(name=name, data=data, **kwargs)))

    def zeros_like(self, *, name: str, data: async_api.ArrayLike, **kwargs: Any) -> AnyArray:
        """Create a sub-array of zeros like `data`.

        Parameters
        ----------
        name : str
            Name of the array.
        data : array-like
            The array to create the new array like.
        **kwargs
            Keyword arguments passed to [zarr.api.asynchronous.create][].

        Returns
        -------
        Array
            The new array.
        """

        return Array(self._sync(self._async_group.zeros_like(name=name, data=data, **kwargs)))

    def ones_like(self, *, name: str, data: async_api.ArrayLike, **kwargs: Any) -> AnyArray:
        """Create a sub-array of ones like `data`.

        Parameters
        ----------
        name : str
            Name of the array.
        data : array-like
            The array to create the new array like.
        **kwargs
            Keyword arguments passed to [zarr.api.asynchronous.create][].

        Returns
        -------
        Array
            The new array.
        """
        return Array(self._sync(self._async_group.ones_like(name=name, data=data, **kwargs)))

    def full_like(self, *, name: str, data: async_api.ArrayLike, **kwargs: Any) -> AnyArray:
        """Create a sub-array like `data` filled with the `fill_value` of `data` .

        Parameters
        ----------
        name : str
            Name of the array.
        data : array-like
            The array to create the new array like.
        **kwargs
            Keyword arguments passed to [zarr.api.asynchronous.create][].

        Returns
        -------
        Array
            The new array.
        """
        return Array(self._sync(self._async_group.full_like(name=name, data=data, **kwargs)))

    def move(self, source: str, dest: str) -> None:
        """Move a sub-group or sub-array from one path to another.

        Notes
        -----
        Not implemented
        """
        return self._sync(self._async_group.move(source, dest))

    @deprecated("Use Group.create_array instead.", category=ZarrDeprecationWarning)
    def array(
        self,
        name: str,
        *,
        shape: ShapeLike,
        dtype: npt.DTypeLike,
        chunks: tuple[int, ...] | Literal["auto"] = "auto",
        shards: tuple[int, ...] | Literal["auto"] | None = None,
        filters: FiltersLike = "auto",
        compressors: CompressorsLike = "auto",
        compressor: CompressorLike = None,
        serializer: SerializerLike = "auto",
        fill_value: Any | None = DEFAULT_FILL_VALUE,
        order: MemoryOrder | None = None,
        attributes: dict[str, JSON] | None = None,
        chunk_key_encoding: ChunkKeyEncodingLike | None = None,
        dimension_names: DimensionNames = None,
        storage_options: dict[str, Any] | None = None,
        overwrite: bool = False,
        config: ArrayConfigLike | None = None,
        data: npt.ArrayLike | None = None,
    ) -> AnyArray:
        """Create an array within this group.

        !!! warning "Deprecated"
            `Group.array()` is deprecated since v3.0.0 and will be removed in a future release.
            Use `Group.create_array` instead.

        This method lightly wraps [zarr.core.array.create_array][].

        Parameters
        ----------
        name : str
            The name of the array relative to the group. If ``path`` is ``None``, the array will be located
            at the root of the store.
        shape : tuple[int, ...]
            Shape of the array.
        dtype : npt.DTypeLike
            Data type of the array.
        chunks : tuple[int, ...], optional
            Chunk shape of the array.
            If not specified, default are guessed based on the shape and dtype.
        shards : tuple[int, ...], optional
            Shard shape of the array. The default value of ``None`` results in no sharding at all.
        filters : Iterable[Codec] | Literal["auto"], optional
            Iterable of filters to apply to each chunk of the array, in order, before serializing that
            chunk to bytes.

            For Zarr format 3, a "filter" is a codec that takes an array and returns an array,
            and these values must be instances of [`zarr.abc.codec.ArrayArrayCodec`][], or a
            dict representations of [`zarr.abc.codec.ArrayArrayCodec`][].

            For Zarr format 2, a "filter" can be any numcodecs codec; you should ensure that the
            the order if your filters is consistent with the behavior of each filter.

            The default value of ``"auto"`` instructs Zarr to use a default used based on the data
            type of the array and the Zarr format specified. For all data types in Zarr V3, and most
            data types in Zarr V2, the default filters are empty. The only cases where default filters
            are not empty is when the Zarr format is 2, and the data type is a variable-length data type like
            [`zarr.dtype.VariableLengthUTF8`][] or [`zarr.dtype.VariableLengthUTF8`][]. In these cases,
            the default filters contains a single element which is a codec specific to that particular data type.

            To create an array with no filters, provide an empty iterable or the value ``None``.
        compressors : Iterable[Codec], optional
            List of compressors to apply to the array. Compressors are applied in order, and after any
            filters are applied (if any are specified) and the data is serialized into bytes.

            For Zarr format 3, a "compressor" is a codec that takes a bytestream, and
            returns another bytestream. Multiple compressors my be provided for Zarr format 3.
            If no ``compressors`` are provided, a default set of compressors will be used.
            These defaults can be changed by modifying the value of ``array.v3_default_compressors``
            in [`zarr.config`][zarr.config].
            Use ``None`` to omit default compressors.

            For Zarr format 2, a "compressor" can be any numcodecs codec. Only a single compressor may
            be provided for Zarr format 2.
            If no ``compressor`` is provided, a default compressor will be used.
            in [`zarr.config`][zarr.config].
            Use ``None`` to omit the default compressor.
        compressor : Codec, optional
            Deprecated in favor of ``compressors``.
        serializer : dict[str, JSON] | ArrayBytesCodec, optional
            Array-to-bytes codec to use for encoding the array data.
            Zarr format 3 only. Zarr format 2 arrays use implicit array-to-bytes conversion.
            If no ``serializer`` is provided, a default serializer will be used.
            These defaults can be changed by modifying the value of ``array.v3_default_serializer``
            in [`zarr.config`][zarr.config].
        fill_value : Any, optional
            Fill value for the array.
        order : {"C", "F"}, optional
            The memory of the array (default is "C").
            For Zarr format 2, this parameter sets the memory order of the array.
            For Zarr format 3, this parameter is deprecated, because memory order
            is a runtime parameter for Zarr format 3 arrays. The recommended way to specify the memory
            order for Zarr format 3 arrays is via the ``config`` parameter, e.g. ``{'config': 'C'}``.
            If no ``order`` is provided, a default order will be used.
            This default can be changed by modifying the value of ``array.order`` in [`zarr.config`][zarr.config].
        attributes : dict, optional
            Attributes for the array.
        chunk_key_encoding : ChunkKeyEncoding, optional
            A specification of how the chunk keys are represented in storage.
            For Zarr format 3, the default is ``{"name": "default", "separator": "/"}}``.
            For Zarr format 2, the default is ``{"name": "v2", "separator": "."}}``.
        dimension_names : Iterable[str], optional
            The names of the dimensions (default is None).
            Zarr format 3 only. Zarr format 2 arrays should not use this parameter.
        storage_options : dict, optional
            If using an fsspec URL to create the store, these will be passed to the backend implementation.
            Ignored otherwise.
        overwrite : bool, default False
            Whether to overwrite an array with the same name in the store, if one exists.
        config : ArrayConfig or ArrayConfigLike, optional
            Runtime configuration for the array.
        data : array_like
            The data to fill the array with.

        Returns
        -------
        AsyncArray
        """
        compressors = _parse_deprecated_compressor(compressor, compressors)
        return Array(
            self._sync(
                self._async_group.create_dataset(
                    name=name,
                    shape=shape,
                    dtype=dtype,
                    chunks=chunks,
                    shards=shards,
                    fill_value=fill_value,
                    attributes=attributes,
                    chunk_key_encoding=chunk_key_encoding,
                    compressors=compressors,
                    serializer=serializer,
                    dimension_names=dimension_names,
                    order=order,
                    filters=filters,
                    overwrite=overwrite,
                    storage_options=storage_options,
                    config=config,
                    data=data,
                )
            )
        )


async def create_hierarchy(
    *,
    store: Store,
    nodes: dict[str, GroupMetadata | ArrayV2Metadata | ArrayV3Metadata],
    overwrite: bool = False,
) -> AsyncIterator[tuple[str, AsyncGroup | AnyAsyncArray]]:
    """
    Create a complete zarr hierarchy from a collection of metadata objects.

    This function will parse its input to ensure that the hierarchy is complete. Any implicit groups
    will be inserted as needed. For example, an input like
    ```{'a/b': GroupMetadata}``` will be parsed to
    ```{'': GroupMetadata, 'a': GroupMetadata, 'b': Groupmetadata}```

    After input parsing, this function then creates all the nodes in the hierarchy concurrently.

    Arrays and Groups are yielded in the order they are created. This order is not stable and
    should not be relied on.

        Parameters
    ----------
    store : Store
        The storage backend to use.
    nodes : dict[str, GroupMetadata | ArrayV3Metadata | ArrayV2Metadata]
        A dictionary defining the hierarchy. The keys are the paths of the nodes in the hierarchy,
        relative to the root of the ``Store``. The root of the store can be specified with the empty
        string ``''``. The values are instances of ``GroupMetadata`` or ``ArrayMetadata``. Note that
        all values must have the same ``zarr_format`` -- it is an error to mix zarr versions in the
        same hierarchy.

        Leading "/" characters from keys will be removed.
    overwrite : bool
        Whether to overwrite existing nodes. Defaults to ``False``, in which case an error is
        raised instead of overwriting an existing array or group.

        This function will not erase an existing group unless that group is explicitly named in
        ``nodes``. If ``nodes`` defines implicit groups, e.g. ``{`'a/b/c': GroupMetadata}``, and a
        group already exists at path ``a``, then this function will leave the group at ``a`` as-is.

    Yields
    ------
    tuple[str, AsyncGroup | AsyncArray]
        This function yields (path, node) pairs, in the order the nodes were created.

    Examples
    --------
    >>> from zarr.api.asynchronous import create_hierarchy
    >>> from zarr.storage import MemoryStore
    >>> from zarr.core.group import GroupMetadata
    >>> import asyncio
    >>> store = MemoryStore()
    >>> nodes = {'a': GroupMetadata(attributes={'name': 'leaf'})}
    >>> async def run():
        ... print(dict([x async for x in create_hierarchy(store=store, nodes=nodes)]))
    >>> asyncio.run(run())
    # {'a': <AsyncGroup memory://140345143770112/a>, '': <AsyncGroup memory://140345143770112>}
    """
    # normalize the keys to be valid paths
    nodes_normed_keys = _normalize_path_keys(nodes)

    # ensure that all nodes have the same zarr_format, and add implicit groups as needed
    nodes_parsed = _parse_hierarchy_dict(data=nodes_normed_keys)
    redundant_implicit_groups = []

    # empty hierarchies should be a no-op
    if len(nodes_parsed) > 0:
        # figure out which zarr format we are using
        zarr_format = next(iter(nodes_parsed.values())).zarr_format

        # check which implicit groups will require materialization
        implicit_group_keys = tuple(
            filter(lambda k: isinstance(nodes_parsed[k], ImplicitGroupMarker), nodes_parsed)
        )
        # read potential group metadata for each implicit group
        maybe_extant_group_coros = (
            _read_group_metadata(store, k, zarr_format=zarr_format) for k in implicit_group_keys
        )
        maybe_extant_groups = await asyncio.gather(
            *maybe_extant_group_coros, return_exceptions=True
        )

        for key, value in zip(implicit_group_keys, maybe_extant_groups, strict=True):
            if isinstance(value, BaseException):
                if isinstance(value, FileNotFoundError):
                    # this is fine -- there was no group there, so we will create one
                    pass
                else:
                    raise value
            else:
                # a loop exists already at ``key``, so we can avoid creating anything there
                redundant_implicit_groups.append(key)

        if overwrite:
            # we will remove any nodes that collide with arrays and non-implicit groups defined in
            # nodes

            # track the keys of nodes we need to delete
            to_delete_keys = []
            to_delete_keys.extend(
                [k for k, v in nodes_parsed.items() if k not in implicit_group_keys]
            )
            await asyncio.gather(*(store.delete_dir(key) for key in to_delete_keys))
        else:
            # This type is long.
            coros: (
                Generator[Coroutine[Any, Any, ArrayV2Metadata | GroupMetadata], None, None]
                | Generator[Coroutine[Any, Any, ArrayV3Metadata | GroupMetadata], None, None]
            )
            if zarr_format == 2:
                coros = (_read_metadata_v2(store=store, path=key) for key in nodes_parsed)
            elif zarr_format == 3:
                coros = (_read_metadata_v3(store=store, path=key) for key in nodes_parsed)
            else:  # pragma: no cover
                raise ValueError(f"Invalid zarr_format: {zarr_format}")  # pragma: no cover

            extant_node_query = dict(
                zip(
                    nodes_parsed.keys(),
                    await asyncio.gather(*coros, return_exceptions=True),
                    strict=False,
                )
            )
            # iterate over the existing arrays / groups and figure out which of them conflict
            # with the arrays / groups we want to create
            for key, extant_node in extant_node_query.items():
                proposed_node = nodes_parsed[key]
                if isinstance(extant_node, BaseException):
                    if isinstance(extant_node, FileNotFoundError):
                        # ignore FileNotFoundError, because they represent nodes we can safely create
                        pass
                    else:
                        # Any other exception is a real error
                        raise extant_node
                else:
                    # this is a node that already exists, but a node with the same key was specified
                    #  in nodes_parsed.
                    if isinstance(extant_node, GroupMetadata):
                        # a group already exists where we want to create a group
                        if isinstance(proposed_node, ImplicitGroupMarker):
                            # we have proposed an implicit group, which is OK -- we will just skip
                            # creating this particular metadata document
                            redundant_implicit_groups.append(key)
                        else:
                            # we have proposed an explicit group, which is an error, given that a
                            # group already exists.
                            msg = f"A group exists in store {store!r} at path {key!r}."
                            raise ContainsGroupError(msg)
                    elif isinstance(extant_node, ArrayV2Metadata | ArrayV3Metadata):
                        # we are trying to overwrite an existing array. this is an error.
                        msg = f"An array exists in store {store!r} at path {key!r}."
                        raise ContainsArrayError(msg)

    nodes_explicit: dict[str, GroupMetadata | ArrayV2Metadata | ArrayV3Metadata] = {}

    for k, v in nodes_parsed.items():
        if k not in redundant_implicit_groups:
            if isinstance(v, ImplicitGroupMarker):
                nodes_explicit[k] = GroupMetadata(zarr_format=v.zarr_format)
            else:
                nodes_explicit[k] = v

    async for key, node in create_nodes(store=store, nodes=nodes_explicit):
        yield key, node


async def create_nodes(
    *,
    store: Store,
    nodes: dict[str, GroupMetadata | ArrayV2Metadata | ArrayV3Metadata],
) -> AsyncIterator[tuple[str, AsyncGroup | AnyAsyncArray]]:
    """Create a collection of arrays and / or groups concurrently.

    Note: no attempt is made to validate that these arrays and / or groups collectively form a
    valid Zarr hierarchy. It is the responsibility of the caller of this function to ensure that
    the ``nodes`` parameter satisfies any correctness constraints.

    Parameters
    ----------
    store : Store
        The storage backend to use.
    nodes : dict[str, GroupMetadata | ArrayV3Metadata | ArrayV2Metadata]
        A dictionary defining the hierarchy. The keys are the paths of the nodes
        in the hierarchy, and the values are the metadata of the nodes. The
        metadata must be either an instance of GroupMetadata, ArrayV3Metadata
        or ArrayV2Metadata.

    Yields
    ------
    AsyncGroup | AsyncArray
        The created nodes in the order they are created.
    """

    # Note: the only way to alter this value is via the config. If that's undesirable for some reason,
    # then we should consider adding a keyword argument this this function
    semaphore = asyncio.Semaphore(config.get("async.concurrency"))
    create_tasks: list[Coroutine[None, None, str]] = []

    for key, value in nodes.items():
        # make the key absolute
        create_tasks.extend(_persist_metadata(store, key, value, semaphore=semaphore))

    created_object_keys = []

    for coro in asyncio.as_completed(create_tasks):
        created_key = await coro
        # we need this to track which metadata documents were written so that we can yield a
        # complete v2 Array / Group class after both .zattrs and the metadata JSON was created.
        created_object_keys.append(created_key)

        # get the node name from the object key
        if len(created_key.split("/")) == 1:
            # this is the root node
            meta_out = nodes[""]
            node_name = ""
        else:
            # turn "foo/<anything>" into "foo"
            node_name = created_key[: created_key.rfind("/")]
            meta_out = nodes[node_name]
        if meta_out.zarr_format == 3:
            yield node_name, _build_node(store=store, path=node_name, metadata=meta_out)
        else:
            # For zarr v2
            # we only want to yield when both the metadata and attributes are created
            # so we track which keys have been created, and wait for both the meta key and
            # the attrs key to be created before yielding back the AsyncArray / AsyncGroup

            attrs_done = _join_paths([node_name, ZATTRS_JSON]) in created_object_keys

            if isinstance(meta_out, GroupMetadata):
                meta_done = _join_paths([node_name, ZGROUP_JSON]) in created_object_keys
            else:
                meta_done = _join_paths([node_name, ZARRAY_JSON]) in created_object_keys

            if meta_done and attrs_done:
                yield node_name, _build_node(store=store, path=node_name, metadata=meta_out)

            continue


def _get_roots(
    data: Iterable[str],
) -> tuple[str, ...]:
    """
    Return the keys of the root(s) of the hierarchy. A root is a key with the fewest number of
    path segments.
    """
    if "" in data:
        return ("",)
    keys_split = sorted((key.split("/") for key in data), key=len)
    groups: defaultdict[int, list[str]] = defaultdict(list)
    for key_split in keys_split:
        groups[len(key_split)].append("/".join(key_split))
    return tuple(groups[min(groups.keys())])


def _parse_hierarchy_dict(
    *,
    data: Mapping[str, ImplicitGroupMarker | GroupMetadata | ArrayV2Metadata | ArrayV3Metadata],
) -> dict[str, ImplicitGroupMarker | GroupMetadata | ArrayV2Metadata | ArrayV3Metadata]:
    """
    Take an input with type Mapping[str, ArrayMetadata | GroupMetadata] and parse it into
     a dict of str: node pairs that models a valid, complete Zarr hierarchy.

    If the input represents a complete Zarr hierarchy, i.e. one with no implicit groups,
    then return a dict with the exact same data as the input.

    Otherwise, return a dict derived from the input with GroupMetadata inserted as needed to make
    the hierarchy complete.

    For example, an input of {'a/b': ArrayMetadata} is incomplete, because it references two
    groups (the root group '' and a group at 'a') that are not specified in the input. Applying this function
    to that input will result in a return value of
    {'': GroupMetadata, 'a': GroupMetadata, 'a/b': ArrayMetadata}, i.e. the implied groups
    were added.

    The input is also checked for the following conditions; an error is raised if any are violated:

    - No arrays can contain group or arrays (i.e., all arrays must be leaf nodes).
    - All arrays and groups must have the same ``zarr_format`` value.

    This function ensures that the input is transformed into a specification of a complete and valid
    Zarr hierarchy.
    """

    # ensure that all nodes have the same zarr format
    data_purified = _ensure_consistent_zarr_format(data)

    # ensure that keys are normalized to zarr paths
    data_normed_keys = _normalize_path_keys(data_purified)

    # insert an implicit root group if a root was not specified
    # but not if an empty dict was provided, because any empty hierarchy has no nodes
    if len(data_normed_keys) > 0 and "" not in data_normed_keys:
        z_format = next(iter(data_normed_keys.values())).zarr_format
        data_normed_keys = data_normed_keys | {"": ImplicitGroupMarker(zarr_format=z_format)}

    out: dict[str, GroupMetadata | ArrayV2Metadata | ArrayV3Metadata] = {**data_normed_keys}

    for k, v in data_normed_keys.items():
        key_split = k.split("/")

        # get every parent path
        *subpaths, _ = accumulate(key_split, lambda a, b: _join_paths([a, b]))

        for subpath in subpaths:
            # If a component is not already in the output dict, add ImplicitGroupMetadata
            if subpath not in out:
                out[subpath] = ImplicitGroupMarker(zarr_format=v.zarr_format)
            else:
                if not isinstance(out[subpath], GroupMetadata | ImplicitGroupMarker):
                    msg = (
                        f"The node at {subpath} contains other nodes, but it is not a Zarr group. "
                        "This is invalid. Only Zarr groups can contain other nodes."
                    )
                    raise ValueError(msg)
    return out


def _ensure_consistent_zarr_format(
    data: Mapping[str, GroupMetadata | ArrayV2Metadata | ArrayV3Metadata],
) -> Mapping[str, GroupMetadata | ArrayV2Metadata] | Mapping[str, GroupMetadata | ArrayV3Metadata]:
    """
    Ensure that all values of the input dict have the same zarr format. If any do not,
    then a value error is raised.
    """
    observed_zarr_formats: dict[ZarrFormat, list[str]] = {2: [], 3: []}

    for k, v in data.items():
        observed_zarr_formats[v.zarr_format].append(k)

    if len(observed_zarr_formats[2]) > 0 and len(observed_zarr_formats[3]) > 0:
        msg = (
            "Got data with both Zarr v2 and Zarr v3 nodes, which is invalid. "
            f"The following keys map to Zarr v2 nodes: {observed_zarr_formats.get(2)}. "
            f"The following keys map to Zarr v3 nodes: {observed_zarr_formats.get(3)}."
            "Ensure that all nodes have the same Zarr format."
        )
        raise ValueError(msg)

    return cast(
        "Mapping[str, GroupMetadata | ArrayV2Metadata] | Mapping[str, GroupMetadata | ArrayV3Metadata]",
        data,
    )


async def _getitem_semaphore(
    node: AsyncGroup, key: str, semaphore: asyncio.Semaphore | None
) -> AnyAsyncArray | AsyncGroup:
    """
    Wrap Group.getitem with an optional semaphore.

    If the semaphore parameter is an
    asyncio.Semaphore instance, then the getitem operation is performed inside an async context
    manager provided by that semaphore. If the semaphore parameter is None, then getitem is invoked
    without a context manager.
    """
    if semaphore is not None:
        async with semaphore:
            return await node.getitem(key)
    else:
        return await node.getitem(key)


async def _iter_members(
    node: AsyncGroup,
    skip_keys: tuple[str, ...],
    semaphore: asyncio.Semaphore | None,
) -> AsyncGenerator[tuple[str, AnyAsyncArray | AsyncGroup], None]:
    """
    Iterate over the arrays and groups contained in a group.

    Parameters
    ----------
    node : AsyncGroup
        The group to traverse.
    skip_keys : tuple[str, ...]
        A tuple of keys to skip when iterating over the possible members of the group.
    semaphore : asyncio.Semaphore | None
        An optional semaphore to use for concurrency control.

    Yields
    ------
    tuple[str, AnyAsyncArray | AsyncGroup]
    """

    # retrieve keys from storage
    keys = [key async for key in node.store.list_dir(node.path)]
    keys_filtered = tuple(filter(lambda v: v not in skip_keys, keys))

    node_tasks = tuple(
        asyncio.create_task(_getitem_semaphore(node, key, semaphore), name=key)
        for key in keys_filtered
    )

    for fetched_node_coro in asyncio.as_completed(node_tasks):
        try:
            fetched_node = await fetched_node_coro
        except KeyError as e:
            # keyerror is raised when `key` names an object (in the object storage sense),
            # as opposed to a prefix, in the store under the prefix associated with this group
            # in which case `key` cannot be the name of a sub-array or sub-group.
            warnings.warn(
                f"Object at {e.args[0]} is not recognized as a component of a Zarr hierarchy.",
                ZarrUserWarning,
                stacklevel=1,
            )
            continue
        match fetched_node:
            case AsyncArray() | AsyncGroup():
                yield fetched_node.basename, fetched_node
            case _:
                raise ValueError(f"Unexpected type: {type(fetched_node)}")


async def _iter_members_deep(
    group: AsyncGroup,
    *,
    max_depth: int | None,
    skip_keys: tuple[str, ...],
    semaphore: asyncio.Semaphore | None = None,
    use_consolidated_for_children: bool = True,
) -> AsyncGenerator[tuple[str, AnyAsyncArray | AsyncGroup], None]:
    """
    Iterate over the arrays and groups contained in a group, and optionally the
    arrays and groups contained in those groups.

    Parameters
    ----------
    group : AsyncGroup
        The group to traverse.
    max_depth : int | None
        The maximum depth of recursion.
    skip_keys : tuple[str, ...]
        A tuple of keys to skip when iterating over the possible members of the group.
    semaphore : asyncio.Semaphore | None
        An optional semaphore to use for concurrency control.
    use_consolidated_for_children : bool, default True
        Whether to use the consolidated metadata of child groups loaded
        from the store. Note that this only affects groups loaded from the
        store. If the current Group already has consolidated metadata, it
        will always be used.

    Yields
    ------
    tuple[str, AnyAsyncArray | AsyncGroup]
    """

    to_recurse = {}
    do_recursion = max_depth is None or max_depth > 0

    if max_depth is None:
        new_depth = None
    else:
        new_depth = max_depth - 1
    async for name, node in _iter_members(group, skip_keys=skip_keys, semaphore=semaphore):
        is_group = isinstance(node, AsyncGroup)
        if (
            is_group
            and not use_consolidated_for_children
            and node.metadata.consolidated_metadata is not None
        ):
            node = cast("AsyncGroup", node)
            # We've decided not to trust consolidated metadata at this point, because we're
            # reconsolidating the metadata, for example.
            node = replace(node, metadata=replace(node.metadata, consolidated_metadata=None))
        yield name, node
        if is_group and do_recursion:
            node = cast("AsyncGroup", node)
            to_recurse[name] = _iter_members_deep(
                node, max_depth=new_depth, skip_keys=skip_keys, semaphore=semaphore
            )

    for prefix, subgroup_iter in to_recurse.items():
        async for name, node in subgroup_iter:
            key = f"{prefix}/{name}".lstrip("/")
            yield key, node


async def _read_metadata_v3(store: Store, path: str) -> ArrayV3Metadata | GroupMetadata:
    """
    Given a store_path, return ArrayV3Metadata or GroupMetadata defined by the metadata
    document stored at store_path.path / zarr.json. If no such document is found, raise a
    FileNotFoundError.
    """
    zarr_json_bytes = await store.get(
        _join_paths([path, ZARR_JSON]), prototype=default_buffer_prototype()
    )
    if zarr_json_bytes is None:
        raise FileNotFoundError(path)
    else:
        zarr_json = json.loads(zarr_json_bytes.to_bytes())
        return _build_metadata_v3(zarr_json)


async def _read_metadata_v2(store: Store, path: str) -> ArrayV2Metadata | GroupMetadata:
    """
    Given a store_path, return ArrayV2Metadata or GroupMetadata defined by the metadata
    document stored at store_path.path / (.zgroup | .zarray). If no such document is found,
    raise a FileNotFoundError.
    """
    # TODO: consider first fetching array metadata, and only fetching group metadata when we don't
    # find an array
    zarray_bytes, zgroup_bytes, zattrs_bytes = await asyncio.gather(
        store.get(_join_paths([path, ZARRAY_JSON]), prototype=default_buffer_prototype()),
        store.get(_join_paths([path, ZGROUP_JSON]), prototype=default_buffer_prototype()),
        store.get(_join_paths([path, ZATTRS_JSON]), prototype=default_buffer_prototype()),
    )

    if zattrs_bytes is None:
        zattrs = {}
    else:
        zattrs = json.loads(zattrs_bytes.to_bytes())

    # TODO: decide how to handle finding both array and group metadata. The spec does not seem to
    # consider this situation. A practical approach would be to ignore that combination, and only
    # return the array metadata.
    if zarray_bytes is not None:
        zmeta = json.loads(zarray_bytes.to_bytes())
    else:
        if zgroup_bytes is None:
            # neither .zarray or .zgroup were found results in KeyError
            raise FileNotFoundError(path)
        else:
            zmeta = json.loads(zgroup_bytes.to_bytes())

    return _build_metadata_v2(zmeta, zattrs)


async def _read_group_metadata_v2(store: Store, path: str) -> GroupMetadata:
    """
    Read group metadata or error
    """
    meta = await _read_metadata_v2(store=store, path=path)
    if not isinstance(meta, GroupMetadata):
        raise FileNotFoundError(f"Group metadata was not found in {store} at {path}")
    return meta


async def _read_group_metadata_v3(store: Store, path: str) -> GroupMetadata:
    """
    Read group metadata or error
    """
    meta = await _read_metadata_v3(store=store, path=path)
    if not isinstance(meta, GroupMetadata):
        raise FileNotFoundError(f"Group metadata was not found in {store} at {path}")
    return meta


async def _read_group_metadata(
    store: Store, path: str, *, zarr_format: ZarrFormat
) -> GroupMetadata:
    if zarr_format == 2:
        return await _read_group_metadata_v2(store=store, path=path)
    return await _read_group_metadata_v3(store=store, path=path)


def _build_metadata_v3(zarr_json: dict[str, JSON]) -> ArrayV3Metadata | GroupMetadata:
    """
    Convert a dict representation of Zarr V3 metadata into the corresponding metadata class.
    """
    if "node_type" not in zarr_json:
        msg = "Required key 'node_type' is missing from the provided metadata document."
        raise MetadataValidationError(msg)
    match zarr_json:
        case {"node_type": "array"}:
            return ArrayV3Metadata.from_dict(zarr_json)
        case {"node_type": "group"}:
            return GroupMetadata.from_dict(zarr_json)
        case _:  # pragma: no cover
            raise ValueError(
                "invalid value for `node_type` key in metadata document"
            )  # pragma: no cover


def _build_metadata_v2(
    zarr_json: dict[str, JSON], attrs_json: dict[str, JSON]
) -> ArrayV2Metadata | GroupMetadata:
    """
    Convert a dict representation of Zarr V2 metadata into the corresponding metadata class.
    """
    match zarr_json:
        case {"shape": _}:
            return ArrayV2Metadata.from_dict(zarr_json | {"attributes": attrs_json})
        case _:  # pragma: no cover
            return GroupMetadata.from_dict(zarr_json | {"attributes": attrs_json})


@overload
def _build_node(*, store: Store, path: str, metadata: ArrayV2Metadata) -> AsyncArrayV2: ...


@overload
def _build_node(*, store: Store, path: str, metadata: ArrayV3Metadata) -> AsyncArrayV3: ...


@overload
def _build_node(*, store: Store, path: str, metadata: GroupMetadata) -> AsyncGroup: ...


def _build_node(
    *, store: Store, path: str, metadata: ArrayV3Metadata | ArrayV2Metadata | GroupMetadata
) -> AnyAsyncArray | AsyncGroup:
    """
    Take a metadata object and return a node (AsyncArray or AsyncGroup).
    """
    store_path = StorePath(store=store, path=path)
    match metadata:
        case ArrayV2Metadata() | ArrayV3Metadata():
            return AsyncArray(metadata, store_path=store_path)
        case GroupMetadata():
            return AsyncGroup(metadata, store_path=store_path)
        case _:  # pragma: no cover
            raise ValueError(f"Unexpected metadata type: {type(metadata)}")  # pragma: no cover


async def _get_node_v2(store: Store, path: str) -> AsyncArrayV2 | AsyncGroup:
    """
    Read a Zarr v2 AsyncArray or AsyncGroup from a path in a Store.

    Parameters
    ----------
    store : Store
        The store-like object to read from.
    path : str
        The path to the node to read.

    Returns
    -------
    AsyncArray | AsyncGroup
    """
    metadata = await _read_metadata_v2(store=store, path=path)
    return _build_node(store=store, path=path, metadata=metadata)


async def _get_node_v3(store: Store, path: str) -> AsyncArrayV3 | AsyncGroup:
    """
    Read a Zarr v3 AsyncArray or AsyncGroup from a path in a Store.

    Parameters
    ----------
    store : Store
        The store-like object to read from.
    path : str
        The path to the node to read.

    Returns
    -------
    AsyncArray | AsyncGroup
    """
    metadata = await _read_metadata_v3(store=store, path=path)
    return _build_node(store=store, path=path, metadata=metadata)


async def get_node(store: Store, path: str, zarr_format: ZarrFormat) -> AnyAsyncArray | AsyncGroup:
    """
    Get an AsyncArray or AsyncGroup from a path in a Store.

    Parameters
    ----------
    store : Store
        The store-like object to read from.
    path : str
        The path to the node to read.
    zarr_format : {2, 3}
        The zarr format of the node to read.

    Returns
    -------
    AsyncArray | AsyncGroup
    """

    match zarr_format:
        case 2:
            return await _get_node_v2(store=store, path=path)
        case 3:
            return await _get_node_v3(store=store, path=path)
        case _:  # pragma: no cover
            raise ValueError(f"Unexpected zarr format: {zarr_format}")  # pragma: no cover


async def _set_return_key(
    *, store: Store, key: str, value: Buffer, semaphore: asyncio.Semaphore | None = None
) -> str:
    """
    Write a value to storage at the given key. The key is returned.
    Useful when saving values via routines that return results in execution order,
    like asyncio.as_completed, because in this case we need to know which key was saved in order
    to yield the right object to the caller.

    Parameters
    ----------
    store : Store
        The store to save the value to.
    key : str
        The key to save the value to.
    value : Buffer
        The value to save.
    semaphore : asyncio.Semaphore | None
        An optional semaphore to use to limit the number of concurrent writes.
    """

    if semaphore is not None:
        async with semaphore:
            await store.set(key, value)
    else:
        await store.set(key, value)
    return key


def _persist_metadata(
    store: Store,
    path: str,
    metadata: ArrayV2Metadata | ArrayV3Metadata | GroupMetadata,
    semaphore: asyncio.Semaphore | None = None,
) -> tuple[Coroutine[None, None, str], ...]:
    """
    Prepare to save a metadata document to storage, returning a tuple of coroutines that must be awaited.
    """

    to_save = metadata.to_buffer_dict(default_buffer_prototype())
    return tuple(
        _set_return_key(store=store, key=_join_paths([path, key]), value=value, semaphore=semaphore)
        for key, value in to_save.items()
    )


async def create_rooted_hierarchy(
    *,
    store: Store,
    nodes: dict[str, GroupMetadata | ArrayV2Metadata | ArrayV3Metadata],
    overwrite: bool = False,
) -> AsyncGroup | AnyAsyncArray:
    """
    Create an ``AsyncGroup`` or ``AsyncArray`` from a store and a dict of metadata documents.
    This function ensures that its input contains a specification of a root node,
    calls ``create_hierarchy`` to create nodes, and returns the root node of the hierarchy.
    """
    roots = _get_roots(nodes.keys())
    if len(roots) != 1:
        msg = (
            "The input does not specify a root node. "
            "This function can only create hierarchies that contain a root node, which is "
            "defined as a group that is ancestral to all the other arrays and "
            "groups in the hierarchy, or a single array."
        )
        raise ValueError(msg)
    else:
        root_key = roots[0]

    nodes_created = [
        x async for x in create_hierarchy(store=store, nodes=nodes, overwrite=overwrite)
    ]
    return dict(nodes_created)[root_key]
