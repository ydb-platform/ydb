import builtins
import functools
from collections.abc import Callable
from typing import Any, TypeVar, cast

import hypothesis.extra.numpy as npst
import hypothesis.strategies as st
import numpy as np
from hypothesis import assume, note
from hypothesis.stateful import (
    RuleBasedStateMachine,
    initialize,
    invariant,
    precondition,
    rule,
)
from hypothesis.strategies import DataObject

import zarr
from zarr import Array
from zarr.abc.store import Store
from zarr.codecs.bytes import BytesCodec
from zarr.core.buffer import Buffer, BufferPrototype, cpu, default_buffer_prototype
from zarr.core.sync import SyncMixin
from zarr.storage import LocalStore, MemoryStore
from zarr.testing.strategies import (
    basic_indices,
    chunk_paths,
    dimension_names,
    key_ranges,
    node_names,
    np_array_and_chunks,
    orthogonal_indices,
)
from zarr.testing.strategies import keys as zarr_keys

MAX_BINARY_SIZE = 100

F = TypeVar("F", bound=Callable[..., Any])


def with_frequency(frequency: float) -> Callable[[F], F]:
    """This needs to be deterministic for hypothesis replaying"""

    def decorator(func: F) -> F:
        counter_attr = f"__{func.__name__}_counter"

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        @precondition
        def frequency_check(f: Any) -> Any:
            if not hasattr(f, counter_attr):
                setattr(f, counter_attr, 0)

            current_count = getattr(f, counter_attr) + 1
            setattr(f, counter_attr, current_count)

            return (current_count * frequency) % 1.0 >= (1.0 - frequency)

        return cast(F, frequency_check(wrapper))

    return decorator


def split_prefix_name(path: str) -> tuple[str, str]:
    split = path.rsplit("/", maxsplit=1)
    if len(split) > 1:
        prefix, name = split
    else:
        prefix = ""
        (name,) = split
    return prefix, name


class ZarrHierarchyStateMachine(SyncMixin, RuleBasedStateMachine):
    """
    This state machine models operations that modify a zarr store's
    hierarchy. That is, user actions that modify arrays/groups as well
    as list operations. It is intended to be used by external stores, and
    compares their results to a MemoryStore that is assumed to be perfect.
    """

    def __init__(self, store: Store) -> None:
        super().__init__()

        self.store = store

        self.model = MemoryStore()
        zarr.group(store=self.model)

        # Track state of the hierarchy, these should contain fully qualified paths
        self.all_groups: set[str] = set()
        self.all_arrays: set[str] = set()

    @initialize()
    def init_store(self) -> None:
        # This lets us reuse the fixture provided store.
        self._sync(self.store.clear())
        zarr.group(store=self.store)

    def can_add(self, path: str) -> bool:
        return path not in self.all_groups and path not in self.all_arrays

    # -------------------- store operations -----------------------
    @rule(name=node_names, data=st.data())
    def add_group(self, name: str, data: DataObject) -> None:
        # Handle possible case-insensitive file systems (e.g. MacOS)
        if isinstance(self.store, LocalStore):
            name = name.lower()
        if self.all_groups:
            parent = data.draw(st.sampled_from(sorted(self.all_groups)), label="Group parent")
        else:
            parent = ""
        path = f"{parent}/{name}".lstrip("/")
        assume(self.can_add(path))
        note(f"Adding group: path='{path}'")
        self.all_groups.add(path)
        zarr.group(store=self.store, path=path)
        zarr.group(store=self.model, path=path)

    @rule(data=st.data(), name=node_names, array_and_chunks=np_array_and_chunks())
    def add_array(
        self,
        data: DataObject,
        name: str,
        array_and_chunks: tuple[np.ndarray[Any, Any], tuple[int, ...]],
    ) -> None:
        # Handle possible case-insensitive file systems (e.g. MacOS)
        if isinstance(self.store, LocalStore):
            name = name.lower()
        array, chunks = array_and_chunks
        fill_value = data.draw(npst.from_dtype(array.dtype))
        if self.all_groups:
            parent = data.draw(st.sampled_from(sorted(self.all_groups)), label="Array parent")
        else:
            parent = ""
        # TODO: support creating deeper paths
        # TODO: support overwriting potentially by just skipping `self.can_add`
        path = f"{parent}/{name}".lstrip("/")
        assume(self.can_add(path))
        note(f"Adding array:  path='{path}'  shape={array.shape}  chunks={chunks}")
        for store in [self.store, self.model]:
            zarr.array(
                array,
                chunks=chunks,
                path=path,
                store=store,
                fill_value=fill_value,
                zarr_format=3,
                dimension_names=data.draw(
                    dimension_names(ndim=array.ndim), label="dimension names"
                ),
                # Chose bytes codec to avoid wasting time compressing the data being written
                codecs=[BytesCodec()],
            )
        self.all_arrays.add(path)

    @rule()
    @with_frequency(0.25)
    def clear(self) -> None:
        note("clearing")
        import zarr

        self._sync(self.store.clear())
        self._sync(self.model.clear())

        assert self._sync(self.store.is_empty("/"))
        assert self._sync(self.model.is_empty("/"))

        self.all_groups.clear()
        self.all_arrays.clear()

        zarr.group(store=self.store)
        zarr.group(store=self.model)

        # TODO: MemoryStore is broken?
        # assert not self._sync(self.store.is_empty("/"))
        # assert not self._sync(self.model.is_empty("/"))

    def draw_directory(self, data: DataObject) -> str:
        group_st = st.sampled_from(sorted(self.all_groups)) if self.all_groups else st.nothing()
        array_st = st.sampled_from(sorted(self.all_arrays)) if self.all_arrays else st.nothing()
        array_or_group = data.draw(st.one_of(group_st, array_st))
        if data.draw(st.booleans()) and array_or_group in self.all_arrays:
            arr = zarr.open_array(path=array_or_group, store=self.model)
            path = data.draw(
                st.one_of(
                    st.sampled_from([array_or_group]),
                    chunk_paths(ndim=arr.ndim, numblocks=arr.cdata_shape).map(
                        lambda x: f"{array_or_group}/c/"
                    ),
                )
            )
        else:
            path = array_or_group
        return path

    @precondition(lambda self: bool(self.all_groups))
    @rule(data=st.data())
    def check_list_dir(self, data: DataObject) -> None:
        path = self.draw_directory(data)
        note(f"list_dir for {path=!r}")
        # Consider .list_dir("path/to/array") for an array with a single chunk.
        # The MemoryStore model will return `"c", "zarr.json"` only if the chunk exists
        # If that chunk was deleted, then `"c"` is not returned.
        # LocalStore will not have this behaviour :/
        # There are similar consistency issues with delete_dir("/path/to/array/c/0/0")
        assume(not isinstance(self.store, LocalStore))
        model_ls = sorted(self._sync_iter(self.model.list_dir(path)))
        store_ls = sorted(self._sync_iter(self.store.list_dir(path)))
        assert model_ls == store_ls, (model_ls, store_ls)

    @precondition(lambda self: bool(self.all_arrays))
    @rule(data=st.data())
    def delete_chunk(self, data: DataObject) -> None:
        array = data.draw(st.sampled_from(sorted(self.all_arrays)))
        arr = zarr.open_array(path=array, store=self.model)
        chunk_path = data.draw(chunk_paths(ndim=arr.ndim, numblocks=arr.cdata_shape, subset=False))
        path = f"{array}/c/{chunk_path}"
        note(f"deleting chunk {path=!r}")
        self._sync(self.model.delete(path))
        self._sync(self.store.delete(path))

    @precondition(lambda self: bool(self.all_arrays))
    @rule(data=st.data())
    def check_array(self, data: DataObject) -> None:
        path = data.draw(st.sampled_from(sorted(self.all_arrays)))
        actual = zarr.open_array(self.store, path=path)[:]
        expected = zarr.open_array(self.model, path=path)[:]
        np.testing.assert_equal(actual, expected)

    @precondition(lambda self: bool(self.all_arrays))
    @rule(data=st.data())
    def overwrite_array_basic_indexing(self, data: DataObject) -> None:
        array = data.draw(st.sampled_from(sorted(self.all_arrays)))
        model_array = zarr.open_array(path=array, store=self.model)
        store_array = zarr.open_array(path=array, store=self.store)
        slicer = data.draw(basic_indices(shape=model_array.shape))
        note(f"overwriting array with basic indexer: {slicer=}")
        new_data = data.draw(
            npst.arrays(shape=np.shape(model_array[slicer]), dtype=model_array.dtype)
        )
        model_array[slicer] = new_data
        store_array[slicer] = new_data

    @precondition(lambda self: bool(self.all_arrays))
    @rule(data=st.data())
    def overwrite_array_orthogonal_indexing(self, data: DataObject) -> None:
        array = data.draw(st.sampled_from(sorted(self.all_arrays)))
        model_array = zarr.open_array(path=array, store=self.model)
        store_array = zarr.open_array(path=array, store=self.store)
        indexer, _ = data.draw(orthogonal_indices(shape=model_array.shape))
        note(f"overwriting array orthogonal {indexer=}")
        new_data = data.draw(
            npst.arrays(shape=model_array.oindex[indexer].shape, dtype=model_array.dtype)  # type: ignore[union-attr]
        )
        model_array.oindex[indexer] = new_data
        store_array.oindex[indexer] = new_data

    @precondition(lambda self: bool(self.all_arrays))
    @rule(data=st.data())
    def resize_array(self, data: DataObject) -> None:
        array = data.draw(st.sampled_from(sorted(self.all_arrays)))
        model_array = zarr.open_array(path=array, store=self.model)
        store_array = zarr.open_array(path=array, store=self.store)
        ndim = model_array.ndim
        new_shape = tuple(
            0 if oldsize == 0 else newsize
            for newsize, oldsize in zip(
                data.draw(npst.array_shapes(max_dims=ndim, min_dims=ndim, min_side=0)),
                model_array.shape,
                strict=True,
            )
        )

        note(f"resizing array from {model_array.shape} to {new_shape}")
        model_array.resize(new_shape)
        store_array.resize(new_shape)

    @precondition(lambda self: bool(self.all_arrays) or bool(self.all_groups))
    @rule(data=st.data())
    def delete_dir(self, data: DataObject) -> None:
        path = self.draw_directory(data)
        note(f"delete_dir with {path=!r}")
        self._sync(self.model.delete_dir(path))
        self._sync(self.store.delete_dir(path))

        matches = set()
        for node in self.all_groups | self.all_arrays:
            if node.startswith(path):
                matches.add(node)
        self.all_groups = self.all_groups - matches
        self.all_arrays = self.all_arrays - matches

    # @precondition(lambda self: bool(self.all_groups))
    # @precondition(lambda self: bool(self.all_arrays))
    # @rule(data=st.data())
    # def move_array(self, data):
    #     array_path = data.draw(st.sampled_from(self.all_arrays), label="Array move source")
    #     to_group = data.draw(st.sampled_from(self.all_groups), label="Array move destination")

    #     # fixme renaming to self?
    #     array_name = os.path.basename(array_path)
    #     assume(self.model.can_add(to_group, array_name))
    #     new_path = f"{to_group}/{array_name}".lstrip("/")
    #     note(f"moving array '{array_path}' -> '{new_path}'")
    #     self.model.rename(array_path, new_path)
    #     self.repo.store.rename(array_path, new_path)

    # @precondition(lambda self: len(self.all_groups) >= 2)
    # @rule(data=st.data())
    # def move_group(self, data):
    #     from_group = data.draw(st.sampled_from(self.all_groups), label="Group move source")
    #     to_group = data.draw(st.sampled_from(self.all_groups), label="Group move destination")
    #     assume(not to_group.startswith(from_group))

    #     from_group_name = os.path.basename(from_group)
    #     assume(self.model.can_add(to_group, from_group_name))
    #     # fixme renaming to self?
    #     new_path = f"{to_group}/{from_group_name}".lstrip("/")
    #     note(f"moving group '{from_group}' -> '{new_path}'")
    #     self.model.rename(from_group, new_path)
    #     self.repo.store.rename(from_group, new_path)

    @precondition(lambda self: self.store.supports_deletes)
    @precondition(lambda self: len(self.all_arrays) >= 1)
    @rule(data=st.data())
    def delete_array_using_del(self, data: DataObject) -> None:
        array_path = data.draw(
            st.sampled_from(sorted(self.all_arrays)), label="Array deletion target"
        )
        prefix, array_name = split_prefix_name(array_path)
        note(f"Deleting array '{array_path}' ({prefix=!r}, {array_name=!r}) using del")
        for store in [self.model, self.store]:
            group = zarr.open_group(path=prefix, store=store)
            group[array_name]  # check that it exists
            del group[array_name]
        self.all_arrays.remove(array_path)

    @precondition(lambda self: self.store.supports_deletes)
    @precondition(lambda self: len(self.all_groups) >= 2)  # fixme don't delete root
    @rule(data=st.data())
    def delete_group_using_del(self, data: DataObject) -> None:
        # ensure that we don't include the root group in the list of member names that we try
        # to delete
        member_names = tuple(filter(lambda v: "/" in v, sorted(self.all_groups)))
        group_path = data.draw(st.sampled_from(member_names), label="Group deletion target")
        prefix, group_name = split_prefix_name(group_path)
        note(f"Deleting group '{group_path=!r}', {prefix=!r}, {group_name=!r} using delete")
        members = zarr.open_group(store=self.model, path=group_path).members(max_depth=None)
        for _, obj in members:
            if isinstance(obj, Array):
                self.all_arrays.remove(obj.path)
            else:
                self.all_groups.remove(obj.path)
        for store in [self.store, self.model]:
            group = zarr.open_group(store=store, path=prefix)
            group[group_name]  # check that it exists
            del group[group_name]
        if group_path != "/":
            # The root group is always present
            self.all_groups.remove(group_path)

    # # --------------- assertions -----------------
    # def check_group_arrays(self, group):
    #     # note(f"Checking arrays of '{group}'")
    #     g1 = self.model.get_group(group)
    #     g2 = zarr.open_group(path=group, mode="r", store=self.repo.store)
    #     model_arrays = sorted(g1.arrays(), key=itemgetter(0))
    #     our_arrays = sorted(g2.arrays(), key=itemgetter(0))
    #     for (n1, a1), (n2, a2) in zip_longest(model_arrays, our_arrays):
    #         assert n1 == n2
    #         assert_array_equal(a1, a2)

    # def check_subgroups(self, group_path):
    #     g1 = self.model.get_group(group_path)
    #     g2 = zarr.open_group(path=group_path, mode="r", store=self.repo.store)
    #     g1_children = [name for (name, _) in g1.groups()]
    #     g2_children = [name for (name, _) in g2.groups()]
    #     # note(f"Checking {len(g1_children)} subgroups of group '{group_path}'")
    #     assert g1_children == g2_children

    # def check_list_prefix_from_group(self, group):
    #     prefix = f"meta/root/{group}"
    #     model_list = sorted(self.model.list_prefix(prefix))
    #     al_list = sorted(self.repo.store.list_prefix(prefix))
    #     # note(f"Checking {len(model_list)} keys under '{prefix}'")
    #     assert model_list == al_list

    #     prefix = f"data/root/{group}"
    #     model_list = sorted(self.model.list_prefix(prefix))
    #     al_list = sorted(self.repo.store.list_prefix(prefix))
    #     # note(f"Checking {len(model_list)} keys under '{prefix}'")
    #     assert model_list == al_list

    # @precondition(lambda self: self.model.is_persistent_session())
    # @rule(data=st.data())
    # def check_group_path(self, data):
    #     t0 = time.time()
    #     group = data.draw(st.sampled_from(self.all_groups))
    #     self.check_list_prefix_from_group(group)
    #     self.check_subgroups(group)
    #     self.check_group_arrays(group)
    #     t1 = time.time()
    #     note(f"Checks took {t1 - t0} sec.")
    @invariant()
    def check_list_prefix_from_root(self) -> None:
        model_list = self._sync_iter(self.model.list_prefix(""))
        store_list = self._sync_iter(self.store.list_prefix(""))
        note(f"Checking {len(model_list)} expected keys vs {len(store_list)} actual keys")
        assert sorted(model_list) == sorted(store_list), (
            sorted(model_list),
            sorted(store_list),
        )

        # check that our internal state matches that of the store and model
        assert all(f"{path}/zarr.json" in model_list for path in self.all_groups | self.all_arrays)
        assert all(f"{path}/zarr.json" in store_list for path in self.all_groups | self.all_arrays)


class SyncStoreWrapper(zarr.core.sync.SyncMixin):
    def __init__(self, store: Store) -> None:
        """Synchronous Store wrapper

        This class holds synchronous methods that map to async methods of Store classes.
        The synchronous wrapper is needed because hypothesis' stateful testing infra does
        not support asyncio so we redefine sync versions of the Store API.
        https://github.com/HypothesisWorks/hypothesis/issues/3712#issuecomment-1668999041
        """
        self.store = store

    @property
    def read_only(self) -> bool:
        return self.store.read_only

    def set(self, key: str, data_buffer: Buffer) -> None:
        return self._sync(self.store.set(key, data_buffer))

    def list(self) -> builtins.list[str]:
        return self._sync_iter(self.store.list())

    def get(self, key: str, prototype: BufferPrototype) -> Buffer | None:
        return self._sync(self.store.get(key, prototype=prototype))

    def get_partial_values(
        self, key_ranges: builtins.list[Any], prototype: BufferPrototype
    ) -> builtins.list[Buffer | None]:
        return self._sync(self.store.get_partial_values(prototype=prototype, key_ranges=key_ranges))

    def delete(self, path: str) -> None:
        return self._sync(self.store.delete(path))

    def is_empty(self, prefix: str) -> bool:
        return self._sync(self.store.is_empty(prefix=prefix))

    def clear(self) -> None:
        return self._sync(self.store.clear())

    def exists(self, key: str) -> bool:
        return self._sync(self.store.exists(key))

    def list_dir(self, prefix: str) -> None:
        raise NotImplementedError

    def list_prefix(self, prefix: str) -> None:
        raise NotImplementedError

    @property
    def supports_listing(self) -> bool:
        return self.store.supports_listing

    @property
    def supports_writes(self) -> bool:
        return self.store.supports_writes

    @property
    def supports_deletes(self) -> bool:
        return self.store.supports_deletes


class ZarrStoreStateMachine(RuleBasedStateMachine):
    """ "
    Zarr store state machine

        This is a subclass of a Hypothesis RuleBasedStateMachine.
        It is testing a framework to ensure that the state of a Zarr store matches
        an expected state after a set of random operations. It contains a store
        (currently, a Zarr MemoryStore) and a model, a simplified version of a
        zarr store (in this case, a dict). It also contains rules which represent
        actions that can be applied to a zarr store. Rules apply an action to both
        the store and the model, and invariants assert that the state of the model
        is equal to the state of the store. Hypothesis then generates sequences of
        rules, running invariants after each rule. It raises an error if a sequence
        produces discontinuity between state of the model and state of the store
        (ie. an invariant is violated).
        https://hypothesis.readthedocs.io/en/latest/stateful.html
    """

    def __init__(self, store: Store) -> None:
        super().__init__()
        self.model: dict[str, Buffer] = {}
        self.store = SyncStoreWrapper(store)
        self.prototype = default_buffer_prototype()

    @initialize()
    def init_store(self) -> None:
        self.store.clear()

    @rule(key=zarr_keys(), data=st.binary(min_size=0, max_size=MAX_BINARY_SIZE))
    def set(self, key: str, data: bytes) -> None:
        note(f"(set) Setting {key!r} with {data!r}")
        assert not self.store.read_only
        data_buf = cpu.Buffer.from_bytes(data)
        self.store.set(key, data_buf)
        self.model[key] = data_buf

    @precondition(lambda self: len(self.model.keys()) > 0)
    @rule(key=zarr_keys(), data=st.data())
    def get(self, key: str, data: DataObject) -> None:
        key = data.draw(
            st.sampled_from(sorted(self.model.keys()))
        )  # hypothesis wants to sample from sorted list
        note("(get)")
        store_value = self.store.get(key, self.prototype)
        # to bytes here necessary because data_buf set to model in set()
        assert self.model[key] == store_value

    @rule(key=zarr_keys(), data=st.data())
    def get_invalid_zarr_keys(self, key: str, data: DataObject) -> None:
        note("(get_invalid)")
        assume(key not in self.model)
        assert self.store.get(key, self.prototype) is None

    @precondition(lambda self: len(self.model.keys()) > 0)
    @rule(data=st.data())
    def get_partial_values(self, data: DataObject) -> None:
        key_range = data.draw(
            key_ranges(keys=st.sampled_from(sorted(self.model.keys())), max_size=MAX_BINARY_SIZE)
        )
        note(f"(get partial) {key_range=}")
        obs_maybe = self.store.get_partial_values(key_range, self.prototype)
        observed = []

        for obs in obs_maybe:
            assert obs is not None
            observed.append(obs.to_bytes())

        model_vals_ls = []

        for key, byte_range in key_range:
            start = byte_range.start
            stop = byte_range.end
            model_vals_ls.append(self.model[key][start:stop])

        assert all(
            obs == exp.to_bytes() for obs, exp in zip(observed, model_vals_ls, strict=True)
        ), (
            observed,
            model_vals_ls,
        )

    @precondition(lambda self: self.store.supports_deletes)
    @precondition(lambda self: len(self.model.keys()) > 0)
    @rule(data=st.data())
    def delete(self, data: DataObject) -> None:
        key = data.draw(st.sampled_from(sorted(self.model.keys())))
        note(f"(delete) Deleting {key=}")

        self.store.delete(key)
        del self.model[key]

    @rule()
    def clear(self) -> None:
        assert not self.store.read_only
        note("(clear)")
        self.store.clear()
        self.model.clear()

        assert self.store.is_empty("")

        assert len(self.model.keys()) == len(list(self.store.list())) == 0

    @rule()
    # Local store can be non-empty when there are subdirectories but no files
    @precondition(lambda self: not isinstance(self.store.store, LocalStore))
    def is_empty(self) -> None:
        note("(is_empty)")

        # make sure they either both are or both aren't empty (same state)
        assert self.store.is_empty("") == (not self.model)

    @rule(key=zarr_keys())
    def exists(self, key: str) -> None:
        note("(exists)")

        assert self.store.exists(key) == (key in self.model)

    @invariant()
    def check_paths_equal(self) -> None:
        note("Checking that paths are equal")
        paths = sorted(self.store.list())

        assert sorted(self.model.keys()) == paths

    @invariant()
    def check_vals_equal(self) -> None:
        note("Checking values equal")
        for key, val in self.model.items():
            store_item = self.store.get(key, self.prototype)
            assert val == store_item

    @invariant()
    def check_num_zarr_keys_equal(self) -> None:
        note("check num zarr_keys equal")

        assert len(self.model) == len(list(self.store.list()))

    @invariant()
    def check_zarr_keys(self) -> None:
        keys = list(self.store.list())

        if not keys:
            assert self.store.is_empty("") is True

        else:
            assert self.store.is_empty("") is False

            for key in keys:
                assert self.store.exists(key) is True
        note("checking keys / exists / empty")
