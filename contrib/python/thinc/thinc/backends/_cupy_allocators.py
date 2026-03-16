from typing import cast

from ..compat import cupy, tensorflow, torch
from ..types import ArrayXd
from ..util import get_torch_default_device, tensorflow2xp


def cupy_tensorflow_allocator(size_in_bytes: int):
    """Function that can be passed into cupy.cuda.set_allocator, to have cupy
    allocate memory via TensorFlow. This is important when using the two libraries
    together, as otherwise OOM errors can occur when there's available memory
    sitting in the other library's pool.
    """
    size_in_bytes = max(1024, size_in_bytes)
    tensor = tensorflow.zeros((size_in_bytes // 4,), dtype=tensorflow.dtypes.float32)  # type: ignore
    # We convert to cupy via dlpack, so that we can get a memory pointer.
    cupy_array = cast(ArrayXd, tensorflow2xp(tensor))
    address = int(cupy_array.data)
    # cupy has a neat class to help us here. Otherwise it will try to free.
    memory = cupy.cuda.memory.UnownedMemory(address, size_in_bytes, cupy_array)
    # Now return a new memory pointer.
    return cupy.cuda.memory.MemoryPointer(memory, 0)


def cupy_pytorch_allocator(size_in_bytes: int):
    device = get_torch_default_device()
    """Function that can be passed into cupy.cuda.set_allocator, to have cupy
    allocate memory via PyTorch. This is important when using the two libraries
    together, as otherwise OOM errors can occur when there's available memory
    sitting in the other library's pool.
    """
    # Cupy was having trouble with very small allocations?
    size_in_bytes = max(1024, size_in_bytes)
    # We use pytorch's underlying FloatStorage type to avoid overhead from
    # creating a whole Tensor.
    # This turns out to be way faster than making FloatStorage? Maybe
    # a Python vs C++ thing I guess?
    torch_tensor = torch.zeros(
        (size_in_bytes // 4,), requires_grad=False, device=device
    )
    # cupy has a neat class to help us here. Otherwise it will try to free.
    # I think this is a private API? It's not in the types.
    address = torch_tensor.data_ptr()  # type: ignore
    memory = cupy.cuda.memory.UnownedMemory(address, size_in_bytes, torch_tensor)
    # Now return a new memory pointer.
    return cupy.cuda.memory.MemoryPointer(memory, 0)
