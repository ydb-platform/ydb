'''
This module consists of classes specific to HTML5-SVG Elements. In general this module does not include
- Elements that are not specific to SVG (eg. <a>)
- Elements that are deprecated
'''
from dominate.tags import html_tag
from dominate.dom_tag import dom_tag
import numbers


__license__ = '''
This file is part of Dominate.

Dominate is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as
published by the Free Software Foundation, either version 3 of
the License, or (at your option) any later version.

Dominate is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General
Public License along with Dominate.  If not, see
<http://www.gnu.org/licenses/>.
'''

# Tag attributes
_ATTR_GLOBAL = set([
  'accesskey', 'class', 'class', 'contenteditable', 'contextmenu', 'dir',
  'draggable', 'id', 'item', 'hidden', 'lang', 'itemprop', 'spellcheck',
  'style', 'subject', 'tabindex', 'title'
])

# https://developer.mozilla.org/en-US/docs/Web/SVG/Attribute/Events#Attributes
_ATTR_EVENTS = set([
  'onbegin', 'onend', 'onrepeat',
  'onabort', 'onerror', 'onresize', 'onscroll', 'onunload',
  'oncopy', 'oncut', 'onpaste',
  'oncancel', 'oncanplay', 'oncanplaythrough', 'onchange', 'onclick', 'onclose', 'oncuechange', 'ondblclick',
  'ondrag', 'ondragend', 'ondragenter', 'ondragexit', 'ondragleave', 'ondragover', 'ondragstart', 'ondrop',
  'ondurationchange', 'onemptied', 'onended', 'onerror', 'onfocus', 'oninput', 'oninvalid', 'onkeydown', 'onkeypress',
  'onkeyup', 'onload', 'onloadeddata', 'onloadedmetadata','onloadstart', 'onmousedown', 'onmouseenter',
  'onmouseleave', 'onmousemove', 'onmouseout', 'onmouseover', 'onmouseup', 'onmousewheel', 'onpause', 'onplay',
  'onplaying', 'onprogress', 'onratechange', 'onreset', 'onresize', 'onscroll', 'onseeked', 'onseeking', 'onselect',
  'onshow', 'onstalled', 'onsubmit', 'onsuspend', 'ontimeupdate', 'ontoggle', 'onvolumechange', 'onwaiting'
])

DASHED_ATTRIBUTES = set([
  'accent', 'alignment', 'arabic', 'baseline', 'cap', 'clip', 'color', 'dominant', 'enable', 'fill', 'flood', 
  'font', 'glyph', 'horiz', 'image', 'letter', 'lighting', 'marker', 'overline', 'paint', 'panose', 'pointer', 
  'rendering', 'shape', 'stop', 'strikethrough', 'stroke', 'text', 'underline', 'unicode', 'units', 'v', 'vector', 
  'vert', 'word', 'writing', 'x'
])


# https://developer.mozilla.org/en-US/docs/Web/SVG/Element/svg
class svg_tag(html_tag):
  @staticmethod
  def clean_attribute(attribute):
    attribute = html_tag.clean_attribute(attribute)
    words = attribute.split('_')
    if words[0] in DASHED_ATTRIBUTES:
      return attribute.replace('_', '-')
    return attribute


class svg(svg_tag):
  pass


class animate(svg_tag):
  '''
  The  animate SVG element is used to animate an attribute or property of an element over time.
  It's normally inserted inside the element or referenced by the href attribute of the target element.
  '''
  pass


class animateMotion(svg_tag):
  '''
  The <animateMotion> element causes a referenced element to move along a motion path.
  '''
  pass


class animateTransform(svg_tag):
  '''
  The animateTransform element animates a transformation attribute on its target element, thereby allowing
  animations to control translation, scaling, rotation, and/or skewing.
  '''
  is_single = True


class circle(svg_tag):
  '''
  The <circle> SVG element is an SVG basic shape, used to draw circles based on a center point and a radius.
  '''
  pass


class clipPath(svg_tag):
  '''
  The <clipPath> SVG element defines a clipping path, to be used used by the clip-path property.
  '''
  pass


class defs(svg_tag):
  '''
  The <defs> element is used to store graphical objects that will be used at a later time. Objects created inside a
  <defs> element are not rendered directly. To display them you have to reference them
  (with a <use> element for example).
  '''
  pass


class desc(svg_tag):
  '''
  The <desc> element provides an accessible, long-text description of any SVG container element or graphics element.
  '''
  pass


class ellipse(svg_tag):
  '''
  An ellipse element for svg containers
  '''
  pass

# (Note, filters are at the bottom of this file)

class g(svg_tag):
  '''
  The <g> SVG element is a container used to group other SVG elements.
  '''
  pass


class image(svg_tag):
  '''
  The <image> SVG element includes images inside SVG documents. It can display raster image files or other SVG files.
  '''
  pass


class line(svg_tag):
  '''
  The <line> element is an SVG basic shape used to create a line connecting two points.
  '''
  pass


class linearGradient(svg_tag):
  '''
  The <linearGradient> element lets authors define linear gradients that can be applied to fill or
  stroke of graphical elements.
  '''
  pass


class marker(svg_tag):
  '''
  The <marker> element defines the graphic that is to be used for drawing arrowheads or polymarkers on a given <path>, <line>, <polyline> or <polygon> element.
  '''
  pass


class mask(svg_tag):
  '''
  The <mask> element defines an alpha mask for compositing the current object into the background.
  A mask is used/referenced using the mask property.
  '''
  pass


class mpath(svg_tag):
  '''
  The <mpath> sub-element for the <animateMotion> element provides the ability to reference an
  external <path> element as the definition of a motion path.
  '''
  pass


class pattern(svg_tag):
  '''
  The <pattern> element defines a graphics object which can be redrawn at repeated x and y-coordinate
  intervals ("tiled") to cover an area.
  '''
  pass


class polygon(svg_tag):
  '''
  A polygon element for svg containers
  '''
  pass


class polyline(svg_tag):
  '''
  A polyline element for svg containers
  '''
  pass


class radialGradient(svg_tag):
  '''
  The <radialGradient> element lets authors define radial gradients that can be applied to fill
  or stroke of graphical elements.
  '''
  pass


class path(svg_tag):
  '''
  A path element for svg containers
  '''
  pass


class rect(svg_tag):
  '''
  A rectangle element for svg containers
  '''
  pass


class stop(svg_tag):
  '''
  The SVG <stop> element defines a color and its position to use on a gradient.
  This element is always a child of a <linearGradient> or <radialGradient> element.
  '''
  pass


class switch(svg_tag):
  '''
  The <switch> SVG element evaluates any requiredFeatures, requiredExtensions and systemLanguage attributes
  on its direct child elements in order, and then renders the first child where these attributes evaluate to true.
  Other direct children will be bypassed and therefore not rendered. If a child element is a container element,
  like <g>, then its subtree is also processed/rendered or bypassed/not rendered.
  '''
  pass


class symbol(svg_tag):
  '''
  The use of symbol elements for graphics that are used multiple times in the same document adds structure and
   semantics. Documents that are rich in structure may be rendered graphically, as speech, or as Braille,
   and thus promote accessibility.
  '''
  pass


class text(svg_tag):
  '''
  The SVG <text> element draws a graphics element consisting of text. It's possible to apply a gradient,
   pattern, clipping path, mask, or filter to <text>, like any other SVG graphics element.
  '''
  pass


class textPath(svg_tag):
  '''
  To render text along the shape of a <path>, enclose the text in a <textPath> element that has an href
  attribute with a reference to the <path> element.
  '''
  pass


class title(svg_tag):
  '''
  The <title> element provides an accessible, short-text description of any SVG container
  element or graphics element.
  '''
  pass


class tspan(svg_tag):
  '''
  The SVG <tspan> element define a subtext within a <text> element or another <tspan> element.
  It allows to adjust the style and/or position of that subtext as needed.
  '''
  pass


class use(svg_tag):
  '''
  The <use> element takes nodes from within the SVG document, and duplicates them somewhere else.
  '''
  pass


class view(svg_tag):
  '''
  A view is a defined way to view the image, like a zoom level or a detail view.
  '''
  pass


# FILTERS
class filter(svg_tag):
  pass

class feBlend(svg_tag):
  pass

class feColorMatrix(svg_tag):
  pass

class feComponentTransfer(svg_tag):
  pass

class feComposite(svg_tag):
  pass

class feConvolveMatrix(svg_tag):
  pass

class feDiffuseLighting(svg_tag):
  pass

class feDisplacementMap(svg_tag):
  pass

class feFlood(svg_tag):
  pass

class feGaussianBlur(svg_tag):
  pass

class feImage(svg_tag):
  pass

class feMerge(svg_tag):
  pass

class feMorphology(svg_tag):
  pass

class feOffset(svg_tag):
  pass

class feSpecularLighting(svg_tag):
  pass

class feTile(svg_tag):
  pass

class feTurbulence(svg_tag):
  pass

class feDistantLight(svg_tag):
  pass

class fePointLight(svg_tag):
  pass

class feSpotLight(svg_tag):
  pass
