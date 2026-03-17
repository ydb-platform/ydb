"""
viewer
-------------

View meshes and scenes via pyglet or inline HTML.
"""

from .. import exceptions
from .notebook import (
    in_notebook,
    scene_to_html,
    scene_to_mo_notebook,
    scene_to_notebook,
)

try:
    # try importing windowed which will fail
    # if we can't create an openGL context
    from .windowed import SceneViewer, render_scene
except BaseException as E:
    # if windowed failed to import only raise
    # the exception if someone tries to use them
    SceneViewer = exceptions.ExceptionWrapper(E)
    render_scene = exceptions.ExceptionWrapper(E)


# this is only standard library imports

# explicitly list imports in __all__
# as otherwise flake8 gets mad
__all__ = [
    "SceneViewer",
    "SceneWidget",
    "in_notebook",
    "render_scene",
    "scene_to_html",
    "scene_to_mo_notebook",
    "scene_to_notebook",
]
