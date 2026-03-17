import contextlib
import copy
import functools
import threading
from contextvars import ContextVar
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
    Union,
    cast,
)

import srsly

from .backends import CupyOps, NumpyOps, Ops, ParamServer, get_current_ops
from .optimizers import Optimizer  # noqa: F401
from .shims import Shim
from .types import FloatsXd
from .util import (
    DATA_VALIDATION,
    convert_recursive,
    is_xp_array,
    partial,
    validate_fwd_input_output,
)

InT = TypeVar("InT")
OutT = TypeVar("OutT")
SelfT = TypeVar("SelfT", bound="Model")

context_operators: ContextVar[dict] = ContextVar("context_operators", default={})


def empty_init(model: "Model", *args, **kwargs) -> "Model":
    return model


class Model(Generic[InT, OutT]):
    """Class for implementing Thinc models and layers."""

    global_id: int = 0
    global_id_lock: threading.Lock = threading.Lock()
    _context_operators = context_operators

    name: str
    ops: Ops
    id: int
    _func: Callable
    init: Callable
    _params: ParamServer
    _dims: Dict[str, Optional[int]]
    _layers: List["Model"]
    _shims: List[Shim]
    _attrs: Dict[str, Any]
    _has_params: Dict[str, Optional[bool]]

    # This "locks" the class, so we get an error if you try to assign to
    # an unexpected variable.
    __slots__ = [
        "name",
        "id",
        "ops",
        "_func",
        "init",
        "_params",
        "_dims",
        "_attrs",
        "_refs",
        "_layers",
        "_shims",
        "_has_params",
    ]

    def __init__(
        self,
        name: str,
        forward: Callable,
        *,
        init: Optional[Callable] = None,
        dims: Dict[str, Optional[int]] = {},
        params: Dict[str, Optional[FloatsXd]] = {},
        layers: Sequence["Model"] = [],
        shims: List[Shim] = [],
        attrs: Dict[str, Any] = {},
        refs: Dict[str, Optional["Model"]] = {},
        ops: Optional[Union[NumpyOps, CupyOps]] = None,
    ):
        """Initialize a new model."""
        self.name = name
        if init is None:
            init = partial(empty_init, self)
        # Assign to callable attrs: https://github.com/python/mypy/issues/2427
        setattr(self, "_func", forward)
        setattr(self, "init", init)
        self.ops = ops if ops is not None else get_current_ops()
        self._params = ParamServer()
        self._dims = dict(dims)
        self._attrs = dict(attrs)
        self._refs = dict(refs)
        self._layers = list(layers)
        self._shims = list(shims)
        # Take care to increment the base class here! It needs to be unique
        # across all models.
        with Model.global_id_lock:
            Model.global_id += 1
            self.id = Model.global_id
        self._has_params = {}
        for name, value in params.items():
            self._has_params[name] = None
            if value is not None:
                self.set_param(name, value)

    @property
    def layers(self) -> List["Model"]:
        """A list of child layers of the model. You can append to it to add
        layers but not reassign it.
        """
        return self._layers

    @property
    def shims(self) -> List[Shim]:
        return self._shims

    @property
    def attrs(self) -> Dict[str, Any]:
        """A dict of the model's attrs. You can write to it to update attrs but
        not reassign it.
        """
        return self._attrs

    @property
    def param_names(self) -> Tuple[str, ...]:
        """Get the names of registered parameter (including unset)."""
        return tuple(self._has_params.keys())

    @property
    def grad_names(self) -> Tuple[str, ...]:
        """Get the names of parameters with registered gradients (including unset)."""
        return tuple([name for name in self.param_names if self.has_grad(name)])

    @property
    def dim_names(self) -> Tuple[str, ...]:
        """Get the names of registered dimensions (including unset)."""
        return tuple(self._dims.keys())

    @property
    def ref_names(self) -> Tuple[str, ...]:
        """Get the names of registered node references (including unset)."""
        return tuple(self._refs.keys())

    @classmethod
    @contextlib.contextmanager
    def define_operators(cls, operators: Dict[str, Callable]):
        """Bind arbitrary binary functions to Python operators, for use in any
        `Model` instance. Can (and should) be used as a contextmanager.

        EXAMPLE:
            with Model.define_operators({">>": chain}):
                model = Relu(512) >> Relu(512) >> Softmax()
        """
        token = cls._context_operators.set(dict(operators))
        yield
        cls._context_operators.reset(token)

    def has_dim(self, name: str) -> Optional[bool]:
        """Check whether the model has a dimension of a given name. If the
        dimension is registered but the value is unset, returns None.
        """
        if name not in self._dims:
            return False
        elif self._dims[name] is not None:
            return True
        else:
            return None

    def get_dim(self, name: str) -> int:
        """Retrieve the value of a dimension of the given name."""
        if name not in self._dims:
            raise KeyError(f"Cannot get dimension '{name}' for model '{self.name}'")
        value = self._dims[name]
        if value is None:
            err = f"Cannot get dimension '{name}' for model '{self.name}': value unset"
            raise ValueError(err)
        else:
            return value

    def set_dim(self, name: str, value: int, *, force: bool = False) -> None:
        """Set a value for a dimension."""
        if name not in self._dims:
            raise KeyError(
                f"Cannot set unknown dimension '{name}' for model '{self.name}'."
            )
        old_value = self._dims[name]
        has_params = any(bool(y) for x, y in self._has_params.items())
        invalid_change = (old_value is not None and old_value != value) and (
            not force or force and has_params
        )
        if invalid_change:
            err = f"Attempt to change dimension '{name}' for model '{self.name}' from {old_value} to {value}"
            raise ValueError(err)
        self._dims[name] = value

    def maybe_get_dim(self, name: str) -> Optional[int]:
        """Retrieve the value of a dimension of the given name, or None."""
        return self.get_dim(name) if self.has_dim(name) else None

    def has_param(self, name: str) -> Optional[bool]:
        """Check whether the model has a weights parameter of the given name.

        Returns None if the parameter is registered but currently unset.
        """
        if name not in self._has_params:
            return False
        elif self._has_params[name] is not None:
            return True
        else:
            return None

    def get_param(self, name: str) -> FloatsXd:
        """Retrieve a weights parameter by name."""
        if name not in self._has_params:
            raise KeyError(f"Unknown param: '{name}' for model '{self.name}'.")
        if not self._params.has_param(self.id, name):
            raise KeyError(
                f"Parameter '{name}' for model '{self.name}' has not been allocated yet."
            )
        return self._params.get_param(self.id, name)

    def maybe_get_param(self, name: str) -> Optional[FloatsXd]:
        """Retrieve a weights parameter by name, or None."""
        return self.get_param(name) if self.has_param(name) else None

    def set_param(self, name: str, value: Optional[FloatsXd]) -> None:
        """Set a weights parameter's value."""
        if value is None:
            self._has_params[name] = None
        else:
            self._params.set_param(self.id, name, value)
            self._has_params[name] = True

    def has_grad(self, name: str) -> bool:
        """Check whether the model has a non-zero gradient for a parameter."""
        return self._params.has_grad(self.id, name)

    def get_grad(self, name: str) -> FloatsXd:
        """Get a gradient from the model."""
        return self._params.get_grad(self.id, name)

    def set_grad(self, name: str, value: FloatsXd) -> None:
        """Set a gradient value for the model."""
        self._params.set_grad(self.id, name, value)

    def maybe_get_grad(self, name: str) -> Optional[FloatsXd]:
        """Retrieve a gradient by name, or None."""
        return self.get_grad(name) if self.has_grad(name) else None

    def inc_grad(self, name: str, value: FloatsXd) -> None:
        """Increment the gradient of a parameter by a value."""
        self._params.inc_grad(self.id, name, value)

    def has_ref(self, name: str) -> Optional[bool]:
        """Check whether the model has a reference of a given name. If the
        reference is registered but the value is unset, returns None.
        """
        if name not in self._refs:
            return False
        elif self._refs[name] is not None:
            return True
        else:
            return None

    def get_ref(self, name: str) -> "Model":
        """Retrieve the value of a reference of the given name."""
        if name not in self._refs:
            raise KeyError(f"Cannot get reference '{name}' for model '{self.name}'.")
        value = self._refs[name]
        if value is None:
            err = f"Cannot get reference '{name}' for model '{self.name}': value unset."
            raise ValueError(err)
        else:
            return value

    def maybe_get_ref(self, name: str) -> Optional["Model"]:
        """Retrieve the value of a reference if it exists, or None."""
        return self.get_ref(name) if self.has_ref(name) else None

    def set_ref(self, name: str, value: Optional["Model"]) -> None:
        """Set a value for a reference."""
        if value is None:
            self._refs[name] = value
        elif value in self.walk():
            self._refs[name] = value
        else:
            raise ValueError("Cannot add reference to node not in tree.")

    def __call__(self, X: InT, is_train: bool) -> Tuple[OutT, Callable]:
        """Call the model's `forward` function, returning the output and a
        callback to compute the gradients via backpropagation."""
        return self._func(self, X, is_train=is_train)

    def initialize(self, X: Optional[InT] = None, Y: Optional[OutT] = None) -> "Model":
        """Finish initialization of the model, optionally providing a batch of
        example input and output data to perform shape inference."""
        if DATA_VALIDATION.get():
            validate_fwd_input_output(self.name, self._func, X, Y)
        if self.init is not None:
            self.init(self, X=X, Y=Y)
        return self

    def begin_update(self, X: InT) -> Tuple[OutT, Callable[[OutT], InT]]:
        """Run the model over a batch of data, returning the output and a
        callback to complete the backward pass. A tuple (Y, finish_update),
        where Y is a batch of output data, and finish_update is a callback that
        takes the gradient with respect to the output and an optimizer function,
        and returns the gradient with respect to the input.
        """
        return self._func(self, X, is_train=True)

    def predict(self, X: InT) -> OutT:
        """Call the model's `forward` function with `is_train=False`, and return
        only the output, instead of the `(output, callback)` tuple.
        """
        return self._func(self, X, is_train=False)[0]

    def finish_update(self, optimizer: Optimizer) -> None:
        """Update parameters with current gradients. The optimizer is called
        with each parameter and gradient of the model.
        """
        for node in self.walk():
            for shim in node.shims:
                shim.finish_update(optimizer)
        for node in self.walk():
            for name in node.param_names:
                if node.has_grad(name):
                    param, grad = optimizer(
                        (node.id, name), node.get_param(name), node.get_grad(name)
                    )
                    node.set_param(name, param)

    @contextlib.contextmanager
    def use_params(self, params: Dict[Tuple[int, str], FloatsXd]):
        """Context manager to temporarily set the model's parameters to
        specified values. The params are a dictionary keyed by model IDs, whose
        values are arrays of weight values.
        """
        backup = {}
        for name in self.param_names:
            key = (self.id, name)
            if key in params:
                backup[name] = self.get_param(name)
                self.set_param(name, params[key])

        with contextlib.ExitStack() as stack:
            for layer in self.layers:
                stack.enter_context(layer.use_params(params))
            for shim in self.shims:
                stack.enter_context(shim.use_params(params))
            yield
        if backup:
            for name, param in backup.items():
                self.set_param(name, param)

    def walk(self, *, order: str = "bfs") -> Iterable["Model"]:
        """Iterate out layers of the model.

        Nodes are returned in breadth-first order by default. Other possible
        orders are "dfs_pre" (depth-first search in preorder) and "dfs_post"
        (depth-first search in postorder)."""
        if order == "bfs":
            return self._walk_bfs()
        elif order == "dfs_pre":
            return self._walk_dfs(post_order=False)
        elif order == "dfs_post":
            return self._walk_dfs(post_order=True)
        else:
            raise ValueError("Invalid order, must be one of: bfs, dfs_pre, dfs_post")

    def _walk_bfs(self) -> Iterable["Model"]:
        """Iterate out layers of the model, breadth-first."""
        queue = [self]
        seen: Set[int] = set()
        for node in queue:
            if id(node) in seen:
                continue
            seen.add(id(node))
            yield node
            queue.extend(node.layers)

    def _walk_dfs(self, post_order: bool = False) -> Iterable["Model"]:
        """Iterate out layers of the model, depth-first."""
        seen: Dict[int, Iterator["Model"]] = dict()
        stack = [self]
        seen[id(self)] = iter(self.layers)
        if not post_order:
            yield self

        while stack:
            try:
                next_child = next(seen[id(stack[-1])])
                if not id(next_child) in seen:
                    if not post_order:
                        yield next_child

                    stack.append(next_child)
                    seen[id(next_child)] = iter(next_child.layers)
            except StopIteration:
                if post_order:
                    yield stack[-1]
                stack.pop()

    def remove_node(self, node: "Model") -> None:
        """Remove a node from all layers lists, and then update references.
        References that no longer point to a node within the tree will be set
        to `None`. For instance, let's say a node has its grandchild as a reference.
        If the child is removed, the grandchild reference will be left dangling,
        so will be set to None.
        """
        for child in list(self.walk()):
            while node in child.layers:
                child.layers.remove(node)
        tree = set(self.walk())
        for node in tree:
            for name in node.ref_names:
                ref = node.get_ref(name)
                if ref is not None and ref not in tree:
                    node.set_ref(name, None)

    def replace_callbacks(
        self, forward: Callable, *, init: Optional[Callable] = None
    ) -> None:
        setattr(self, "_func", forward)
        setattr(self, "init", init)

    def replace_node(self, old: "Model", new: "Model") -> bool:
        """Replace a node anywhere it occurs within the model. Returns a boolean
        indicating whether the replacement was made."""
        seen = False

        # We need to replace nodes in topological order of the transposed graph
        # to ensure that a node's dependencies are processed before the node.
        # This is equivalent to a post-order traversal of the original graph.
        for node in list(self.walk(order="dfs_post")):
            if node is old:
                seen = True
            else:
                node._layers = [
                    new if layer is old else layer for layer in node._layers
                ]
                for name in node.ref_names:
                    if node.get_ref(name) is old:
                        node.set_ref(name, new)

        return seen

    def get_gradients(self) -> Dict[Tuple[int, str], Tuple[FloatsXd, FloatsXd]]:
        """Get non-zero gradients of the model's parameters, as a dictionary
        keyed by the parameter ID. The values are (weights, gradients) tuples.
        """
        gradients = {}
        for node in self.walk():
            for name in node.grad_names:
                param = node.get_param(name)
                grad = node.get_grad(name)
                gradients[(node.id, name)] = (param, grad)
        return gradients

    def copy(self: SelfT) -> SelfT:
        """
        Create a copy of the model, its attributes, and its parameters. Any child
        layers will also be deep-copied. The copy will receive a distinct `model.id`
        value.
        """
        return self._copy()

    def _copy(
        self: SelfT, seen: Optional[Dict[int, Union["Model", Shim]]] = None
    ) -> SelfT:
        if seen is None:
            seen = {}
        params = {}
        for name in self.param_names:
            params[name] = self.get_param(name) if self.has_param(name) else None

        copied_layers: List[Model] = []
        for layer in self.layers:
            if id(layer) in seen:
                copied_layers.append(cast(Model, seen[id(layer)]))
            else:
                copied_layer = layer._copy(seen)
                seen[id(layer)] = copied_layer
                copied_layers.append(copied_layer)

        copied_shims = []
        for shim in self.shims:
            if id(shim) in seen:
                copied_shims.append(cast(Shim, seen[id(shim)]))
            else:
                copied_shim = shim.copy()
                seen[id(shim)] = copied_shim
                copied_shims.append(copied_shim)

        copied: Model[InT, OutT] = Model(
            self.name,
            self._func,
            init=self.init,
            params=copy.deepcopy(params),
            dims=copy.deepcopy(self._dims),
            attrs=copy.deepcopy(self._attrs),
            layers=copied_layers,
            shims=copied_shims,
        )
        for name in self.grad_names:
            copied.set_grad(name, self.get_grad(name).copy())
        return cast(SelfT, copied)

    def to_gpu(self, gpu_id: int) -> None:  # pragma: no cover
        """Transfer the model to a given GPU device."""
        import cupy.cuda.device

        with cupy.cuda.device.Device(gpu_id):
            self._to_ops(CupyOps())

    def to_cpu(self) -> None:  # pragma: no cover
        """Transfer the model to CPU."""
        self._to_ops(NumpyOps())

    def _to_ops(self, ops: Ops) -> None:  # pragma: no cover
        """Common method for to_cpu/to_gpu."""
        for node in self.walk():
            node.ops = ops
            for name in node.param_names:
                if node.has_param(name):
                    node.set_param(name, ops.asarray_f(node.get_param(name)))
                if node.has_grad(name):
                    node.set_grad(name, ops.asarray_f(node.get_grad(name)))
            for shim in node.shims:
                shim.to_device(ops.device_type, ops.device_id)

    def to_bytes(self) -> bytes:
        """Serialize the model to a bytes representation. Models are usually
        serialized using msgpack, so you should be able to call msgpack.loads()
        on the data and get back a dictionary with the contents.

        Serialization should round-trip identically, i.e. the same bytes should
        result from loading and serializing a model.
        """
        msg = self.to_dict()
        to_numpy_le = partial(self.ops.to_numpy, byte_order="<")
        msg = convert_recursive(is_xp_array, to_numpy_le, msg)
        return srsly.msgpack_dumps(msg)

    def to_disk(self, path: Union[Path, str]) -> None:
        """Serialize the model to disk. Most models will serialize to a single
        file, which should just be the bytes contents of model.to_bytes().
        """
        path = Path(path) if isinstance(path, str) else path
        with path.open("wb") as file_:
            file_.write(self.to_bytes())

    def to_dict(self) -> Dict:
        """Serialize the model to a dict representation.

        Serialization should round-trip identically, i.e. the same dict should
        result from loading and serializing a model.
        """
        # We separate out like this to make it easier to read the data in chunks.
        # The shims might have large weights, while the nodes data will be
        # small. The attrs are probably not very large, but could be.
        # The lists are aligned, and refer to the order of self.walk().
        msg: Dict[str, List] = {"nodes": [], "attrs": [], "params": [], "shims": []}
        nodes = list(self.walk())
        # Serialize references by their index into the flattened tree.
        # This is the main reason we can't accept out-of-tree references:
        # we'd have no way to serialize/deserialize them.
        node_to_i: Dict[int, Optional[int]]
        node_to_i = {node.id: i for i, node in enumerate(nodes)}
        for i, node in enumerate(nodes):
            refs: Dict[str, Optional[int]] = {}
            invalid_refs: List[str] = []
            for name in node.ref_names:
                if not node.has_ref(name):
                    refs[name] = None
                else:
                    ref = node.get_ref(name)
                    if ref.id in node_to_i:
                        refs[name] = node_to_i[ref.id]
                    else:
                        invalid_refs.append(name)
            if invalid_refs:
                raise ValueError(f"Cannot get references: {invalid_refs}")
            dims = {}
            for dim in node.dim_names:
                dims[dim] = node.get_dim(dim) if node.has_dim(dim) else None
            msg["nodes"].append(
                {"index": i, "name": node.name, "dims": dims, "refs": refs}
            )
        for node in nodes:
            attrs = {}
            for name, value in node.attrs.items():
                try:
                    attrs[name] = serialize_attr(value, value, name, node)
                except TypeError:
                    continue
            msg["attrs"].append(attrs)
        for node in nodes:
            msg["shims"].append([shim.to_bytes() for shim in node.shims])
        for node in nodes:
            params: Dict[str, Optional[FloatsXd]] = {}
            for name in node.param_names:
                if node.has_param(name):
                    params[name] = cast(Optional[FloatsXd], node.get_param(name))
                else:
                    params[name] = None
            msg["params"].append(params)
        return msg

    def from_bytes(self, bytes_data: bytes) -> "Model":
        """Deserialize the model from a bytes representation. Models are usually
        serialized using msgpack, so you should be able to call msgpack.loads()
        on the data and get back a dictionary with the contents.

        Serialization should round-trip identically, i.e. the same bytes should
        result from loading and serializing a model.
        """
        msg = srsly.msgpack_loads(bytes_data)
        msg = convert_recursive(is_xp_array, self.ops.asarray, msg)
        return self.from_dict(msg)

    def from_disk(self, path: Union[Path, str]) -> "Model":
        """Deserialize the model from disk. Most models will serialize to a single
        file, which should just be the bytes contents of model.to_bytes().
        """
        path = Path(path) if isinstance(path, str) else path
        with path.open("rb") as file_:
            bytes_data = file_.read()
        return self.from_bytes(bytes_data)

    def from_dict(self, msg: Dict) -> "Model":
        if "nodes" not in msg.keys():  # pragma: no cover
            err = "Trying to read a Model that was created with an incompatible version of Thinc"
            raise ValueError(err)
        nodes = list(self.walk())
        if len(msg["nodes"]) != len(nodes):
            raise ValueError("Cannot deserialize model: mismatched structure")
        for i, node in enumerate(nodes):
            info = msg["nodes"][i]
            node.name = info["name"]
            for dim, value in info["dims"].items():
                if value is not None:
                    node.set_dim(dim, value)
            for ref, ref_index in info["refs"].items():
                if ref_index is None:
                    node.set_ref(ref, None)
                else:
                    node.set_ref(ref, nodes[ref_index])
            for attr, value in msg["attrs"][i].items():
                default_value = node.attrs.get(attr)
                loaded_value = deserialize_attr(default_value, value, attr, node)
                node.attrs[attr] = loaded_value
            for param_name, value in msg["params"][i].items():
                if value is not None:
                    value = node.ops.asarray(value).copy()
                node.set_param(param_name, value)
            for i, shim_bytes in enumerate(msg["shims"][i]):
                node.shims[i].from_bytes(shim_bytes)
        return self

    def can_from_disk(self, path: Union[Path, str], *, strict: bool = True) -> bool:
        """Check whether serialized data on disk is compatible with the model.
        If 'strict', the function returns False if the model has an attribute
        already loaded that would be changed.
        """
        path = Path(path) if isinstance(path, str) else path
        if path.is_dir() or not path.exists():
            return False
        with path.open("rb") as file_:
            bytes_data = file_.read()
        return self.can_from_bytes(bytes_data, strict=strict)

    def can_from_bytes(self, bytes_data: bytes, *, strict: bool = True) -> bool:
        """Check whether the bytes data is compatible with the model. If 'strict',
        the function returns False if the model has an attribute already loaded
        that would be changed.
        """
        try:
            msg = srsly.msgpack_loads(bytes_data)
        except ValueError:
            return False
        return self.can_from_dict(msg, strict=strict)

    def can_from_dict(self, msg: Dict, *, strict: bool = True) -> bool:
        """Check whether a dictionary is compatible with the model.
        If 'strict', the function returns False if the model has an attribute
        already loaded that would be changed.
        """
        if "nodes" not in msg.keys():
            return False
        nodes = list(self.walk())
        if len(msg["nodes"]) != len(nodes):
            return False

        for i, node in enumerate(nodes):
            info = msg["nodes"][i]
            if strict and info["name"] != node.name:
                return False
            if len(msg["shims"][i]) != len(node.shims):
                # TODO: The shims should have a check for this too, but
                # for now we just check if the lengths match.
                return False
            for dim, value in info["dims"].items():
                has_dim = node.has_dim(dim)
                if has_dim is False:
                    return False
                elif has_dim and node.get_dim(dim) != value:
                    return False
            for param_name, value in msg["params"][i].items():
                has_param = node.has_param(param_name)
                if has_param is False:
                    return False
                elif has_param and value is not None:
                    param = node.get_param(param_name)
                    if param.shape != value.shape:
                        return False
            if strict:
                for attr, value in msg["attrs"][i].items():
                    if attr in node.attrs:
                        try:

                            serialized = serialize_attr(
                                node.attrs[attr], node.attrs[attr], attr, node
                            )
                        except TypeError:
                            continue
                        if serialized != value:
                            return False
        return True

    def __add__(self, other: Any) -> "Model":
        """Apply the function bound to the '+' operator."""
        if "+" not in self._context_operators.get():
            raise TypeError("Undefined operator: +")
        return self._context_operators.get()["+"](self, other)

    def __sub__(self, other: Any) -> "Model":
        """Apply the function bound to the '-' operator."""
        if "-" not in self._context_operators.get():
            raise TypeError("Undefined operator: -")
        return self._context_operators.get()["-"](self, other)

    def __mul__(self, other: Any) -> "Model":
        """Apply the function bound to the '*' operator."""
        if "*" not in self._context_operators.get():
            raise TypeError("Undefined operator: *")
        return self._context_operators.get()["*"](self, other)

    def __matmul__(self, other: Any) -> "Model":
        """Apply the function bound to the '@' operator."""
        if "@" not in self._context_operators.get():
            raise TypeError("Undefined operator: @")
        return self._context_operators.get()["@"](self, other)

    def __div__(self, other: Any) -> "Model":  # pragma: no cover
        """Apply the function bound to the '/' operator."""
        if "/" not in self._context_operators.get():
            raise TypeError("Undefined operator: /")
        return self._context_operators.get()["/"](self, other)

    def __truediv__(self, other: Any) -> "Model":
        """Apply the function bound to the '/' operator."""
        if "/" not in self._context_operators.get():
            raise TypeError("Undefined operator: /")
        return self._context_operators.get()["/"](self, other)

    def __floordiv__(self, other: Any) -> "Model":
        """Apply the function bound to the '//' operator."""
        if "//" not in self._context_operators.get():
            raise TypeError("Undefined operator: //")
        return self._context_operators.get()["//"](self, other)

    def __mod__(self, other: Any) -> "Model":
        """Apply the function bound to the '%' operator."""
        if "%" not in self._context_operators.get():
            raise TypeError("Undefined operator: %")
        return self._context_operators.get()["%"](self, other)

    def __pow__(self, other: Any, **kwargs) -> "Model":
        """Apply the function bound to the '**' operator."""
        if "**" not in self._context_operators.get():
            raise TypeError("Undefined operator: **")
        return self._context_operators.get()["**"](self, other)

    def __lshift__(self, other: Any) -> "Model":
        """Apply the function bound to the '<<' operator."""
        if "<<" not in self._context_operators.get():
            raise TypeError("Undefined operator: <<")
        return self._context_operators.get()["<<"](self, other)

    def __rshift__(self, other: Any) -> "Model":
        """Apply the function bound to the '>>' operator."""
        if ">>" not in self._context_operators.get():
            raise TypeError("Undefined operator: >>")
        return self._context_operators.get()[">>"](self, other)

    def __and__(self, other: Any) -> "Model":
        """Apply the function bound to the '&' operator."""
        if "&" not in self._context_operators.get():
            raise TypeError("Undefined operator: &")
        return self._context_operators.get()["&"](self, other)

    def __xor__(self, other: Any) -> "Model":
        """Apply the function bound to the '^' operator."""
        if "^" not in self._context_operators.get():
            raise TypeError("Undefined operator: ^")
        return self._context_operators.get()["^"](self, other)

    def __or__(self, other: Any) -> "Model":
        """Apply the function bound to the '|' operator."""
        if "|" not in self._context_operators.get():
            raise TypeError("Undefined operator: |")
        return self._context_operators.get()["|"](self, other)


@functools.singledispatch
def serialize_attr(_: Any, value: Any, name: str, model: Model) -> bytes:
    """Serialize an attribute value (defaults to msgpack). You can register
    custom serializers using the @serialize_attr.register decorator with the
    type to serialize, e.g.: @serialize_attr.register(MyCustomObject).
    """
    return srsly.msgpack_dumps(value)


@functools.singledispatch
def deserialize_attr(_: Any, value: Any, name: str, model: Model) -> Any:
    """Deserialize an attribute value (defaults to msgpack). You can register
    custom deserializers using the @deserialize_attr.register decorator with the
    type to deserialize, e.g.: @deserialize_attr.register(MyCustomObject).
    """
    return srsly.msgpack_loads(value)


_ModelT = TypeVar("_ModelT", bound=Model)


def change_attr_values(model: _ModelT, mapping: Dict[str, Dict[str, Any]]) -> _ModelT:
    """Walk over the model's nodes, changing the value of attributes using the
    provided mapping, which maps node names to attr names to attr values.
    """
    for node in model.walk():
        if node.name in mapping:
            attrs = mapping[node.name]
            for attr, value in attrs.items():
                if attr in node.attrs:
                    node.attrs[attr] = value
    return model


def set_dropout_rate(model: _ModelT, drop: float, attrs=["dropout_rate"]) -> _ModelT:
    """Walk over the model's nodes, setting the dropout rate. You can specify
    one or more attribute names, by default it looks for ["dropout_rate"].
    """
    for node in model.walk():
        for attr in attrs:
            if attr in node.attrs:
                node.attrs[attr] = drop
    return model


def wrap_model_recursive(model: Model, wrapper: Callable[[Model], _ModelT]) -> _ModelT:
    """Recursively wrap a model and its submodules. The model is updated
    in-place."""
    for node in list(model.walk()):
        model.replace_node(node, wrapper(node))

    return wrapper(model)


__all__ = [
    "Model",
    "serialize_attr",
    "deserialize_attr",
    "change_attr_values",
    "set_dropout_rate",
    "wrap_model_recursive",
]
