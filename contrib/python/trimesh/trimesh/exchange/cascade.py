import os
import tempfile

from ..exceptions import ExceptionWrapper
from ..typed import BinaryIO, Dict, Number, Optional

# used as an intermediate format
from .gltf import load_glb


def load_step(
    file_obj: BinaryIO,
    file_type,
    tol_linear: Optional[Number] = None,
    tol_angular: Optional[Number] = None,
    tol_relative: Optional[bool] = False,
    merge_primitives: bool = True,
    **kwargs,
) -> Dict:
    """
    Use `cascadio` a packaged version of OpenCASCADE
    to load a STEP file using GLB as an intermediate.

    Parameters
    -----------
    file_obj
      STEP file to load.
    **kwargs
      Passed to `cascadio.step_to_glb`

    Returns
    ----------
    kwargs
      Keyword arguments for a Scene.
    """
    # TODO : update upstream `cascadio` to accept bytes objects
    # so that we don't need to write a temporary file to disc!
    with tempfile.TemporaryDirectory() as F:
        # temporarily copy the STEP
        stepfile = os.path.join(F, "data.step")
        with open(stepfile, "wb") as f:
            f.write(file_obj.read())

        # where to save the converted GLB
        glbfile = os.path.join(F, "converted.glb")

        # the arguments for cascadio are not optional so
        # filter out any `None` value arguments here
        cascadio_kwargs = {
            "merge_primitives": bool(merge_primitives),
            "tol_linear": tol_linear,
            "tol_angular": tol_angular,
            "tol_relative": tol_relative,
        }
        # run the conversion
        cascadio.step_to_glb(
            stepfile,
            glbfile,
            **{k: v for k, v in cascadio_kwargs.items() if v is not None},
        )

        with open(glbfile, "rb") as f:
            # return the parsed intermediate file
            return load_glb(file_obj=f, merge_primitives=merge_primitives, **kwargs)


try:
    # wheels for most platforms: `pip install cascadio`
    import cascadio

    _cascade_loaders = {"stp": load_step, "step": load_step}
except BaseException as E:
    wrapper = ExceptionWrapper(E)
    _cascade_loaders = {"stp": wrapper, "step": wrapper}
