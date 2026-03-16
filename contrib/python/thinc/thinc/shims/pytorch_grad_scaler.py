from typing import Dict, Iterable, List, Union, cast

from ..compat import has_torch_amp, torch
from ..util import is_torch_array


class PyTorchGradScaler:
    """
    Gradient scaler for the PyTorch shim.

    Gradients with small magnitudes are not representable in half-precision and
    will underflow to zero. A gradient scaler counters this issue by scaling
    up the loss before backpropagation, increasing the gradients by the same
    magnitude. A large enough scale will avoid that the gradients underflow.
    The gradients are unscaled in single precision after backpropagation, to
    provide the unscaled gradients to the optimizer.
    """

    def __init__(
        self,
        enabled: bool = False,
        init_scale: float = 2.0**16,
        backoff_factor: float = 0.5,
        growth_factor: float = 2.0,
        growth_interval: int = 2000,
    ):
        """
        Construct a gradient scaler for the PyTorch shim.

        enabled (bool):
            Sets whether the gradient scalar is enabled. If it is disabled, the
            methods of the grad scaler are no-ops.

        init_scale (float):
            The initial scale used to increase the gradient magnitude.

        backoff_factor (float):
            The scale will be multiplied by this factor if any of the gradients
            overflows.

        growth_factor (float):
            The scale will be multiplied by this factor when none of the gradients
            overflowed for "growth_interval" steps.

        growth_interval (int):
            When no overflows were found for this number of steps, the scale will
            be multiplied by "growth_factor".
        """
        self._enabled = enabled
        self._growth_factor = growth_factor
        self._backoff_factor = backoff_factor
        self._growth_interval = growth_interval

        self._growth_tracker = torch.full((1,), 0, dtype=torch.int)
        self._scale = torch.full((1,), init_scale)
        self._found_inf = False

    def to_(self, device):
        self._growth_tracker = self._growth_tracker.to(device)
        self._scale = self._scale.to(device)

    def scale(
        self, tensors: Union["torch.Tensor", Iterable["torch.Tensor"]], inplace=False
    ) -> Union["torch.Tensor", List["torch.Tensor"]]:
        """Scale up the values in the given tensors."""
        if not self._enabled:
            return cast("torch.Tensor", tensors)

        incorrect_type = ValueError(
            "Input to gradient scaling must be a Tensor or Iterable[Tensor]"
        )

        # Cache per-device scales to avoid unnecessary d2d copies of the current scale.
        scale_per_device: Dict["torch.device", "torch.Tensor"] = dict()

        if is_torch_array(tensors):
            tensor = cast("torch.Tensor", tensors)
            return self._scale_tensor(tensor, scale_per_device, inplace)
        elif isinstance(tensors, Iterable):
            scaled_tensors = []

            for tensor in tensors:
                if not is_torch_array(tensor):
                    raise incorrect_type

                scaled_tensors.append(
                    self._scale_tensor(tensor, scale_per_device, inplace)
                )

            return scaled_tensors

        raise incorrect_type

    def _scale_tensor(
        self,
        tensor: "torch.Tensor",
        scale_per_device: Dict["torch.device", "torch.Tensor"],
        inplace: bool,
    ):
        if not has_torch_amp:
            raise ValueError(
                "Gradient scaling is not supported, requires capable GPU and torch>=1.9.0"
            )

        if not tensor.is_cuda:
            msg = (
                "Gradient scaling is only supported for CUDA tensors. "
                "If you are using PyTorch models, you can avoid this "
                "error by disabling mixed-precision support."
            )
            raise ValueError(msg)

        device = tensor.device

        if device not in scale_per_device:
            scale_per_device[device] = self._scale.to(device=device)

        scale = scale_per_device[device]
        if inplace:
            return tensor.mul_(scale)
        else:
            return tensor * scale

    def _tensors_per_device(self, tensors):
        tensors_per_device = dict()
        for tensor in tensors:
            device_tensors = tensors_per_device.setdefault(tensor.device, [])
            device_tensors.append(tensor)

        return tensors_per_device

    @property
    def found_inf(self):
        return self._found_inf

    def unscale(self, tensors):
        """Unscale the given tensors. Returns True if any of the gradients were infinite."""
        if not self._enabled:
            return False

        # Invert scale (in higher precision).
        inv_scale = self._scale.double().reciprocal().float()

        # Apply unscaling to tensors, per device.
        tensors_per_device = self._tensors_per_device(tensors)
        for device, device_tensors in tensors_per_device.items():
            found_inf_device = torch.full((1,), 0.0, device=device)
            inv_scale_device = inv_scale.to(device=device)

            torch._amp_foreach_non_finite_check_and_unscale_(
                device_tensors, found_inf_device, inv_scale_device
            )

            if bool(found_inf_device != 0):
                self._found_inf = True

        return self._found_inf

    def update(self):
        """
        Update the scale factor and clear information about infinities.

        This method should be called after each optimization step.
        """
        if not self._enabled:
            return

        found_inf_device = torch.full(
            (1,), 1.0 if self._found_inf else 0.0, device=self._scale.device
        )
        torch._amp_update_scale_(
            self._scale,
            self._growth_tracker,
            found_inf_device,
            self._growth_factor,
            self._backoff_factor,
            self._growth_interval,
        )

        # Clear infinity found status
        self._found_inf = False
