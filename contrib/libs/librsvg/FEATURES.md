# SVG and CSS features that librsvg supports

Table of contents:

[[_TOC_]]

Librsvg tries to be a mostly complete renderer for SVG1.1 and SVG2.

In terms of processing external references, librsvg is a bit more
strict than SVG's "static mode" and a bit more lenient than "secure
static mode".  See "Security and locations of referenced files" in the
reference documentation for details.

Animation, interactivity, and scripting are not supported.

The SVG1.2 specification never made it past draft status in the W3C's
process, and librsvg does not support it.  Note that SVG2 removed some
of the features proposed for SVG1.2.

Generally, librsvg tries to keep up with features in the SVG2
Candidate Recommendation spec.  It ignores features in the SVG2
drafts that are not finalized yet.

Alternative versions of SVG (SVG Tiny, SVG Basic, [SVG
Native](https://gitlab.gnome.org/GNOME/librsvg/-/issues/689)) are not
explicitly supported.  Their features which are a subset of SVG1.1 or
SVG2 are supported if they are equivalent to the ones listed below.

SVG2 offloads many of its features to the family of CSS3
specifications.  Librsvg does not try to support them exhaustively
(there are too many features in CSS!).  Instead, we try to prioritize
new features based on the needs which people express in librsvg's bug
tracker.  Do you want a feature?  [File an
issue](https://gitlab.gnome.org/GNOME/librsvg/issues)!

# Attributes supported by all elements

| Attribute          | Notes                                                                               |
| ---                | ---                                                                                 |
| class              |                                                                                     |
| id                 |                                                                                     |
| requiredExtensions | Used in children of the `switch` element.                                           |
| requiredFeatures   | Used in children of the `switch` element.                                           |
| systemLanguage     | Used in children of the `switch` element.                                           |
| style              |                                                                                     |
| transform          | The `transform` attribute has a different syntax than the CSS `transform` property. |
| xml:lang           |                                                                                     |
| xml:space          |                                                                                     |

# Elements and their specific attributes

| Element             | Attributes          | Notes                                                              |
| ---                 | ---                 | ---                                                                |
| a                   |                     |                                                                    |
|                     | xlink:href          | Needs xlink namespace                                              |
|                     | href                | SVG2                                                               |
| circle              |                     |                                                                    |
|                     | cx                  |                                                                    |
|                     | cy                  |                                                                    |
|                     | r                   |                                                                    |
| clipPath            |                     |                                                                    |
|                     | clipPathUnits       |                                                                    |
| defs                |                     |                                                                    |
| ellipse             |                     |                                                                    |
|                     | cx                  |                                                                    |
|                     | cy                  |                                                                    |
|                     | rx                  |                                                                    |
|                     | ry                  |                                                                    |
| feBlend             |                     | See "Filter effects"                                               |
|                     | in                  |                                                                    |
|                     | in2                 |                                                                    |
|                     | mode                |                                                                    |
| feColorMatrix       |                     | See "Filter effects"                                               |
|                     | in                  |                                                                    |
|                     | type                |                                                                    |
|                     | values              |                                                                    |
| feComponentTransfer |                     | See "Filter effects"                                               |
|                     | in                  |                                                                    |
| feComposite         |                     | See "Filter effects"                                               |
|                     | in                  |                                                                    |
|                     | in2                 |                                                                    |
|                     | operator            |                                                                    |
|                     | k1                  |                                                                    |
|                     | k2                  |                                                                    |
|                     | k3                  |                                                                    |
|                     | k4                  |                                                                    |
| feConvolveMatrix    |                     | See "Filter effects"                                               |
|                     | in                  |                                                                    |
|                     | order               |                                                                    |
|                     | divisor             |                                                                    |
|                     | bias                |                                                                    |
|                     | targetX             |                                                                    |
|                     | targetY             |                                                                    |
|                     | edgeMode            |                                                                    |
|                     | kernelMatrix        |                                                                    |
|                     | kernelUnitLength    |                                                                    |
|                     | preserveAlpha       |                                                                    |
| feDiffuseLighting   |                     | See "Filter effects"                                               |
|                     | in                  |                                                                    |
|                     | surfaceScale        |                                                                    |
|                     | kernelUnitLength    |                                                                    |
|                     | diffuseConstant     |                                                                    |
| feDisplacementMap   |                     | See "Filter effects"                                               |
|                     | in                  |                                                                    |
|                     | in2                 |                                                                    |
|                     | scale               |                                                                    |
|                     | xChannelSelector    |                                                                    |
|                     | yChannelSelector    |                                                                    |
| feDistantLight      |                     |                                                                    |
|                     | azimuth             |                                                                    |
|                     | elevation           |                                                                    |
| feFuncA             |                     | See "Filter effect feComponentTransfer"                            |
| feFuncB             |                     | See "Filter effect feComponentTransfer"                            |
| feFuncG             |                     | See "Filter effect feComponentTransfer"                            |
| feFuncR             |                     | See "Filter effect feComponentTransfer"                            |
| feFlood             |                     | See "Filter effects"                                               |
|                     |                     | Parameters come from the flood-color and flood-opacity properties. |
| feGaussianBlur      |                     | See "Filter effects"                                               |
|                     | in                  |                                                                    |
|                     | stdDeviation        |                                                                    |
| feImage             |                     | See "Filter effects"                                               |
|                     | xlink:href          | Needs xlink namespace                                              |
|                     | href                | SVG2                                                               |
|                     | path                | Non-standard; used by old Adobe Illustrator versions.              |
|                     | preserveAspectRatio |                                                                    |
| feMerge             |                     | See "Filter effects"                                               |
| feMergeNode         |                     |                                                                    |
|                     | in                  |                                                                    |
| feMorphology        |                     | See "Filter effects"                                               |
|                     | in                  |                                                                    |
|                     | operator            |                                                                    |
|                     | radius              |                                                                    |
| feOffset            |                     | See "Filter effects"                                               |
|                     | in                  |                                                                    |
|                     | dx                  |                                                                    |
|                     | dy                  |                                                                    |
| fePointLight        |                     |                                                                    |
|                     | x                   |                                                                    |
|                     | y                   |                                                                    |
|                     | z                   |                                                                    |
| feSpecularLighting  |                     | See "Filter effects"                                               |
|                     | in                  |                                                                    |
|                     | surfaceScale        |                                                                    |
|                     | kernelUnitLength    |                                                                    |
|                     | specularConstant    |                                                                    |
|                     | specularExponent    |                                                                    |
| feSpotLight         |                     |                                                                    |
|                     | x                   |                                                                    |
|                     | y                   |                                                                    |
|                     | z                   |                                                                    |
|                     | pointsAtX           |                                                                    |
|                     | pointsAtY           |                                                                    |
|                     | pointsAtZ           |                                                                    |
|                     | specularExponent    |                                                                    |
|                     | limitingConeAngle   |                                                                    |
| feTile              |                     | See "Filter effects"                                               |
|                     | in                  |                                                                    |
| feTurbulence        |                     | See "Filter effects"                                               |
|                     | baseFrequency       |                                                                    |
|                     | numOctaves          |                                                                    |
|                     | seed                |                                                                    |
|                     | stitchTiles         |                                                                    |
|                     | type                |                                                                    |
| filter              |                     |                                                                    |
|                     | filterUnits         |                                                                    |
|                     | primitiveUnits      |                                                                    |
|                     | x                   |                                                                    |
|                     | y                   |                                                                    |
|                     | width               |                                                                    |
|                     | height              |                                                                    |
| g                   |                     |                                                                    |
| image               |                     |                                                                    |
|                     | xlink:href          | Needs xlink namespace                                              |
|                     | href                | SVG2                                                               |
|                     | path                | Non-standard; used by old Adobe Illustrator versions.              |
|                     | x                   |                                                                    |
|                     | y                   |                                                                    |
|                     | width               |                                                                    |
|                     | height              |                                                                    |
|                     | preserveAspectRatio |                                                                    |
| line                |                     |                                                                    |
|                     | x1                  |                                                                    |
|                     | y1                  |                                                                    |
|                     | x2                  |                                                                    |
|                     | y2                  |                                                                    |
| linearGradient      |                     |                                                                    |
|                     | gradientUnits       |                                                                    |
|                     | gradientTransform   |                                                                    |
|                     | spreadMethod        |                                                                    |
|                     | x1                  |                                                                    |
|                     | y1                  |                                                                    |
|                     | x2                  |                                                                    |
|                     | y2                  |                                                                    |
| marker              |                     |                                                                    |
|                     | markerUnits         |                                                                    |
|                     | refX                |                                                                    |
|                     | refY                |                                                                    |
|                     | markerWidth         |                                                                    |
|                     | markerHeight        |                                                                    |
|                     | orient              |                                                                    |
|                     | preserveAspectRatio |                                                                    |
|                     | viewBox             |                                                                    |
| mask                |                     |                                                                    |
|                     | x                   |                                                                    |
|                     | y                   |                                                                    |
|                     | width               |                                                                    |
|                     | height              |                                                                    |
|                     | maskUnits           |                                                                    |
|                     | maskContentUnits    |                                                                    |
| path                |                     |                                                                    |
|                     | d                   |                                                                    |
| pattern             |                     |                                                                    |
|                     | xlink:href          | Needs xlink namespace                                              |
|                     | href                | SVG2                                                               |
|                     | patternUnits        |                                                                    |
|                     | patternContentUnits |                                                                    |
|                     | patternTransform    |                                                                    |
|                     | preserveAspectRatio |                                                                    |
|                     | viewBox             |                                                                    |
|                     | x                   |                                                                    |
|                     | y                   |                                                                    |
|                     | width               |                                                                    |
|                     | height              |                                                                    |
| polygon             |                     |                                                                    |
|                     | points              |                                                                    |
| polyline            |                     |                                                                    |
|                     | points              |                                                                    |
| radialGradient      |                     |                                                                    |
|                     | gradientUnits       |                                                                    |
|                     | gradientTransform   |                                                                    |
|                     | spreadMethod        |                                                                    |
|                     | cx                  |                                                                    |
|                     | cy                  |                                                                    |
|                     | r                   |                                                                    |
|                     | fx                  |                                                                    |
|                     | fx                  |                                                                    |
|                     | fr                  |                                                                    |
| rect                |                     |                                                                    |
|                     | x                   |                                                                    |
|                     | y                   |                                                                    |
|                     | width               |                                                                    |
|                     | height              |                                                                    |
|                     | rx                  |                                                                    |
|                     | ry                  |                                                                    |
| stop                |                     |                                                                    |
|                     | offset              |                                                                    |
| style               |                     |                                                                    |
|                     | type                |                                                                    |
| svg                 |                     |                                                                    |
|                     | x                   |                                                                    |
|                     | y                   |                                                                    |
|                     | width               |                                                                    |
|                     | height              |                                                                    |
|                     | viewBox             |                                                                    |
|                     | preserveAspectRatio |                                                                    |
| switch              |                     |                                                                    |
| symbol              |                     |                                                                    |
|                     | preserveAspectRatio |                                                                    |
|                     | viewBox             |                                                                    |
| text                |                     |                                                                    |
|                     | x                   |                                                                    |
|                     | y                   |                                                                    |
|                     | dx                  |                                                                    |
|                     | dy                  |                                                                    |
| tref                |                     |                                                                    |
|                     | xlink:href          | Needs xlink namespace                                              |
| tspan               |                     |                                                                    |
|                     | x                   |                                                                    |
|                     | y                   |                                                                    |
|                     | dx                  |                                                                    |
|                     | dy                  |                                                                    |
| use                 |                     |                                                                    |
|                     | xlink:href          | Needs xlink namespace                                              |
|                     | href                | SVG2                                                               |
|                     | x                   |                                                                    |
|                     | y                   |                                                                    |
|                     | width               |                                                                    |
|                     | height              |                                                                    |

# CSS properties

The following are shorthand properties.  They are not available as
presentation attributes, only as style properties, so for example you have to use
`<path style="marker: url(#foo);"/>`, since there is no `marker` attribute.

| Property                   | Notes                                                              |
|----------------------------|--------------------------------------------------------------------|
| font                       |                                                                    |
| glyph-orientation-vertical | Supports only CSS Writing Modes 3 values: auto, 0, 90, 0deg, 90deg |
| marker                     |                                                                    |

The following are longhand properties.  Most of them are available as
presentation attributes, e.g. you can use `<rect fill="blue"/>` as
well as `<rect style="fill: blue;"/>`.  The Notes column indicates
which properties are not available as presentation attributes.

| Property                    | Notes                                                  |
|-----------------------------|--------------------------------------------------------|
| baseline-shift              |                                                        |
| clip-path                   |                                                        |
| clip-rule                   |                                                        |
| color                       |                                                        |
| color-interpolation-filters |                                                        |
| direction                   |                                                        |
| display                     |                                                        |
| enable-background           |                                                        |
| fill                        |                                                        |
| fill-opacity                |                                                        |
| fill-rule                   |                                                        |
| filter                      |                                                        |
| flood-color                 |                                                        |
| flood-opacity               |                                                        |
| font-family                 |                                                        |
| font-size                   |                                                        |
| font-stretch                |                                                        |
| font-style                  |                                                        |
| font-variant                |                                                        |
| font-weight                 |                                                        |
| isolation                   | Not available as a presentation attribute.             |
| letter-spacing              |                                                        |
| lighting-color              |                                                        |
| line-height                 | Not available as a presentation attribute.             |
| marker-end                  |                                                        |
| marker-mid                  |                                                        |
| marker-start                |                                                        |
| mask                        |                                                        |
| mask-type                   |                                                        |
| mix-blend-mode              | Not available as a presentation attribute.             |
| opacity                     |                                                        |
| overflow                    |                                                        |
| paint-order                 |                                                        |
| shape-rendering             |                                                        |
| stop-color                  |                                                        |
| stop-opacity                |                                                        |
| stroke                      |                                                        |
| stroke-dasharray            |                                                        |
| stroke-dashoffset           |                                                        |
| stroke-linecap              |                                                        |
| stroke-linejoin             |                                                        |
| stroke-miterlimit           |                                                        |
| stroke-opacity              |                                                        |
| stroke-width                |                                                        |
| text-anchor                 |                                                        |
| text-decoration             |                                                        |
| text-orientation            | Not available as a presentation attribute.             |
| text-rendering              |                                                        |
| transform                   | SVG2; different syntax from the `transform` attribute. |
| unicode-bidi                |                                                        |
| visibility                  |                                                        |
| writing-mode                |                                                        |

## Filter effects

The following elements are filter effects:

* feBlend
* feColorMatrix
* feComponentTransfer
* feComposite
* feConvolveMatrix
* feDiffuseLighting
* feDisplacementMap
* feFlood
* feGaussianBlur
* feImage
* feMerge
* feMorphology
* feOffset
* feSpecularLighting
* feTile
* feTurbulence

All of those elements for filter effects support these attributes:

| Attribute | Notes |
| ---       | ---   |
| x         |       |
| y         |       |
| width     |       |
| height    |       |
| result    |       |

Some filter effect elements take one input in the `in` attribute, and
some others take two inputs in the `in`, `in2` attributes.  See the
table of elements above for details.

## Filter effect feComponentTransfer

The `feComponentTransfer` element can contain children `feFuncA`,
`feFuncR`, `feFuncG`, `feFuncB`, and those all support these
attributes:

| Attribute   | Notes |
| ---         | ---   |
| type        |       |
| tableValues |       |
| slope       |       |
| intercept   |       |
| amplitude   |       |
| exponent    |       |
| offset      |       |

# CSS features

## Pseudo-classes

| Pseudo-class        | Notes                                                                                |
| ---                 | ---                                                                                  |
| :link               |                                                                                      |
| :visited            | Because librsvg does not maintain browser history, this is parsed, but never matches |
| :lang()             |                                                                                      |
| :not()              | [^1]                                                                                 |
| :first-child        | [^1]                                                                                 |
| :last-child         | [^1]                                                                                 |
| :only-child         | [^1]                                                                                 |
| :root               | [^1]                                                                                 |
| :empty              | [^1]                                                                                 |
| :nth-child()        | [^1]                                                                                 |
| :nth-last-child()   | [^1]                                                                                 |
| :nth-of-type()      | [^1]                                                                                 |
| :nth-last-of-type() | [^1]                                                                                 |
| :first-of-type      | [^1]                                                                                 |
| :last-of-type       | [^1]                                                                                 |
| :only-of-type       | [^1]                                                                                 |

[^1]: These structural pseudo-classes are implemented in rust-selectors, which librsvg uses.

FIXME: which selectors, combinators, at-rules.

# XML features

FIXME: `<xi:include href= parse= encoding=>`

FIXME: `<xi:fallback>`

FIXME: `xml:lang` attribute

FIXME: `xml:space` attribute

# Explicitly Unsupported features

* `flowRoot` element and its children - Inkscape, SVG 1.2 only., #13

* `glyph-orientation-horizontal` property - SVG1.1 only, removed in SVG2

* The pseudo-classes `:is()` and `:where()` are part of Selectors Level 4, which is still a working draft.

# Footnotes
