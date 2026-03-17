"""
notebook.py
-------------

Render trimesh.Scene objects in HTML
and jupyter and marimo notebooks using three.js
"""

import base64
import os
from typing import Literal

# for our template
from .. import resources, util


def scene_to_html(scene, escape_quotes: bool = False) -> str:
    """
    Return HTML that will render the scene using
    GLTF/GLB encoded to base64 loaded by three.js

    Parameters
    --------------
    scene : trimesh.Scene
      Source geometry
    escape_quotes
      If true, replaces quotes '"' with '&quot;' so that the
      HTML is valid inside a `srcdoc` property.

    Returns
    --------------
    html : str
      HTML containing embedded geometry
    """
    # fetch HTML template from ZIP archive
    # it is bundling all of three.js so compression is nice
    base = (
        util.decompress(resources.get_bytes("templates/viewer.zip"), file_type="zip")[
            "viewer.html.template"
        ]
        .read()
        .decode("utf-8")
    )
    # make sure scene has camera populated before export
    _ = scene.camera
    # get export as bytes
    data = scene.export(file_type="glb")
    # encode as base64 string
    encoded = base64.b64encode(data).decode("utf-8")
    # replace keyword with our scene data
    html = base.replace("$B64GLTF", encoded)

    if escape_quotes:
        return html.replace('"', "&quot;")

    return html


def scene_to_notebook(scene, height=500, **kwargs):
    """
    Convert a scene to HTML containing embedded geometry
    and a three.js viewer that will display nicely in
    an IPython/Jupyter notebook.

    Parameters
    -------------
    scene : trimesh.Scene
      Source geometry

    Returns
    -------------
    html : IPython.display.HTML
      Object containing rendered scene
    """
    # keep as soft dependency
    from IPython import display

    # convert scene to a full HTML page
    as_html = scene_to_html(scene=scene, escape_quotes=True)

    # escape the quotes in the HTML
    srcdoc = as_html
    # embed this puppy as the srcdoc attr of an IFframe
    # I tried this a dozen ways and this is the only one that works
    # display.IFrame/display.Javascript really, really don't work
    # div is to avoid IPython's pointless hardcoded warning
    embedded = display.HTML(
        " ".join(
            [
                '<div><iframe srcdoc="{srcdoc}"',
                'width="100%" height="{height}px"',
                'style="border:none;"></iframe></div>',
            ]
        ).format(srcdoc=srcdoc, height=height)
    )
    return embedded


def scene_to_mo_notebook(scene, height=500, **kwargs):
    """
    Convert a scene to HTML containing embedded geometry
    and a three.js viewer that will display nicely in
    an Marimo notebook.

    Parameters
    -------------
    scene : trimesh.Scene
      Source geometry

    Returns
    -------------
    html : mo.Html
      Object containing rendered scene
    """
    # keep as soft dependency
    import marimo as mo

    # convert scene to a full HTML page
    srcdoc = scene_to_html(scene=scene, escape_quotes=True)

    # Embed as srcdoc attr of IFrame, using mo.iframe
    # turns out displaying an empty image. Likely
    # similar to  display.IFrame
    embedded = mo.Html(
        " ".join(
            [
                '<div><iframe srcdoc="{srcdoc}"',
                'width="100%" height="{height}px"',
                'style="border:none;"></iframe></div>',
            ]
        ).format(srcdoc=srcdoc, height=height)
    )

    return embedded


def in_notebook() -> Literal["jupyter", "marimo", False]:
    """
    Check to see if we are in a Jypyter or Marimo notebook.

    Returns
    -----------
    in_notebook
      Returns the type of notebook we're in or False if it
      is running as terminal application.
    """
    try:
        # function returns IPython context, but only in IPython
        ipy = get_ipython()  # NOQA
        # we only want to render rich output in notebooks
        # in terminals we definitely do not want to output HTML
        name = str(ipy.__class__).lower()
        terminal = "terminal" in name

        # spyder uses ZMQshell, and can appear to be a notebook
        spyder = "_" in os.environ and "spyder" in os.environ["_"]

        # assume we are in a notebook if we are not in
        # a terminal and we haven't been run by spyder
        if (not terminal) and (not spyder):
            return "jupyter"
    except BaseException:
        pass

    try:
        import marimo as mo

        if mo.running_in_notebook():
            return "marimo"

    except BaseException:
        pass

    return False
