from typing import Any, Dict, Optional, Tuple

from ..types import FloatsXd
from ..util import get_array_module

KeyT = Tuple[int, str]


class ParamServer:
    """Serve parameters for a single process."""

    _params: Dict[KeyT, FloatsXd] = {}
    _grads: Dict[KeyT, FloatsXd] = {}
    proxy: Optional[Any]

    def __init__(
        self,
        params: Dict[KeyT, FloatsXd] = {},
        grads: Dict[KeyT, FloatsXd] = {},
        *,
        proxy=None
    ):
        self._params = dict(params)
        self._grads = dict(grads)
        # Allow a 'proxy' to be provided to support remote parameters. This
        # is experimental, it's the mechanism we use in the Ray integration.
        self.proxy = proxy

    @property
    def param_keys(self) -> Tuple[KeyT, ...]:
        """Get the names of registered parameter (including unset)."""
        return tuple(self._params.keys())

    @property
    def grad_keys(self) -> Tuple[KeyT, ...]:
        return tuple([key for key in self.param_keys if self.has_grad(*key)])

    def has_param(self, model_id: int, name: str) -> bool:
        return (model_id, name) in self._params

    def has_grad(self, model_id: int, name: str) -> bool:
        return (model_id, name) in self._grads

    def get_param(self, model_id: int, name: str) -> FloatsXd:
        key = (model_id, name)
        if self.proxy is not None:
            self._params[key] = self.proxy.get_param(model_id, name)
        return self._params[key]

    def get_grad(self, model_id: int, name: str) -> FloatsXd:
        key = (model_id, name)
        return self._grads[key]

    def set_param(self, model_id: int, name: str, value: FloatsXd) -> None:
        if self.proxy is not None:
            self.proxy.set_param(model_id, name, value)
        self._params[(model_id, name)] = value

    def set_grad(self, model_id: int, name: str, value: FloatsXd) -> None:
        if self.proxy is not None:
            self.proxy.set_grad(model_id, name, value)
        else:
            self._grads[(model_id, name)] = value

    def inc_grad(self, model_id: int, name: str, value: FloatsXd) -> None:
        key = (model_id, name)
        if self.proxy is not None:
            self.proxy.inc_grad(model_id, name, value)
        elif not self.has_grad(model_id, name):  # pragma: no cover
            if hasattr(value, "copy"):
                # Adjustment for Jax
                self._grads[key] = value.copy()
            elif not value.flags["C_CONTIGUOUS"]:
                xp = get_array_module(value)
                self._grads[(model_id, name)] = xp.ascontiguousarray(value)
            else:
                self._grads[(model_id, name)] = value
        else:
            self._grads[(model_id, name)] += value
