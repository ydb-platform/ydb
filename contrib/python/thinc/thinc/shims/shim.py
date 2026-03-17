import contextlib
import copy
import threading
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple, Union


class Shim:  # pragma: no cover
    """Define a basic interface for external models. Users can create subclasses
    of 'shim' to wrap external libraries. We provide shims for PyTorch.

    The Thinc Model class treats Shim objects as a sort of special type of
    sublayer: it knows they're not actual Thinc Model instances, but it also
    knows to talk to the shim instances when doing things like using transferring
    between devices, loading in parameters, optimization. It also knows Shim
    objects need to be serialized and deserialized with to/from bytes/disk,
    rather than expecting that they'll be msgpack-serializable.
    """

    global_id: int = 0
    global_id_lock: threading.Lock = threading.Lock()
    cfg: Dict
    _model: Any
    _optimizer: Optional[Any]

    def __init__(self, model: Any, config=None, optimizer: Any = None):
        with Shim.global_id_lock:
            Shim.global_id += 1
            self.id = Shim.global_id

        self.cfg = dict(config) if config is not None else {}
        self._model = model
        self._optimizer = optimizer

    def __call__(self, inputs, is_train: bool) -> Tuple[Any, Callable[..., Any]]:
        raise NotImplementedError

    def predict(self, fwd_args: Any) -> Any:
        Y, backprop = self(fwd_args, is_train=False)
        return Y

    def begin_update(self, fwd_args: Any) -> Tuple[Any, Callable[..., Any]]:
        return self(fwd_args, is_train=True)

    def finish_update(self, optimizer):
        raise NotImplementedError

    @contextlib.contextmanager
    def use_params(self, params):
        yield

    def copy(self):
        return copy.deepcopy(self)

    def to_device(self, device_type: str, device_id: int):
        raise NotImplementedError

    def to_disk(self, path: Union[str, Path]):
        bytes_data = self.to_bytes()
        path = Path(path) if isinstance(path, str) else path
        with path.open("wb") as file_:
            file_.write(bytes_data)

    def from_disk(self, path: Union[str, Path]) -> "Shim":
        path = Path(path) if isinstance(path, str) else path
        with path.open("rb") as file_:
            bytes_data = file_.read()
        return self.from_bytes(bytes_data)

    def to_bytes(self):
        raise NotImplementedError

    def from_bytes(self, data) -> "Shim":
        raise NotImplementedError
