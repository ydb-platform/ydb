meld3

Development Status

  No further development of the meld3 package is planned.  The meld3 package
  should be considered unmaintained as of April 2020.  Since 2007, meld3
  received only minimal updates to keep compatible with newer Python versions.
  It was only maintained because it was a dependency of the Supervisor package.
  Since Supervisor 4.1.0 (released in October 2019), the meld3 package is
  no longer a dependency of Supervisor.

Overview

  meld3 is an HTML/XML templating system for Python which keeps
  template markup and dynamic rendering logic separate from one
  another.  See http://www.entrian.com/PyMeld for a treatise on the
  benefits of this pattern.

  meld3 can deal with HTML or XML/XHTML input and can output
  well-formed HTML or XML/XHTML.

  meld3 is a variation of Paul Winkler's Meld2, which is itself a
  variation of Richie Hindle's PyMeld.

  meld3 uses Frederik Lundh's ElementTree library.

Requirements

  On Python 3, meld3 requires Python 3.4 or later.

  On Python 2, meld3 requires Python 2.7 or later.

Installation

  Run 'python setup.py install'.

Differences from PyMeld

  - Templates created for use under PyMeld will not work under meld3
    due to differences in meld tag identification (meld3's id
    attributes are in a nondefault XML namespace, PyMeld's are not).
    Rationale: it should be possible to look at a template and have a
    good shot at figuring out which pieces of it might get replaced
    with dynamic content.  XML ids are required for other things like
    CSS styles, so you can't assume if you see an XML id in a template
    that it was put in there to be a meld identifier.  In the worst
    case scenario, if XML ids were used instead of namespaced id's,
    and an unused id was present in the source document, the designer
    would leave it in there even if he wasn't using it because he
    would think it was a meld id and the programmer would leave it in
    there even if he wasn't using it because he would think it was
    being used by the designer's stylesheets.  Menawhile, nobody's
    actually using it and it's just cluttering up the template.  Also,
    having a separate namespace helps the programmer not stomp on the
    designer by changing identifiers (or needing to grep stylesheets),
    and lets them avoid fighting over what to call elements.

  - The "id" attribute used to mark up is in the a separate namespace
    (aka. xmlns="http://www.plope.com/software/meld3").  So instead of
    marking up a tag like this: '<div id="thediv"></div>', meld3
    requires that you qualify the "id" attribute with a "meld"
    namespace element, like this: '<div meld:id="thediv"></div>'.  As
    per the XML namespace specification, the "meld" name is completely
    optional, and must only represent the
    "http://www.plope.com/software/meld3" namespace identifier, so
    '<div xmlns:foo="http://www.plope.com/software/meld3"
    foo:id="thediv"/>' is just as valid as as '<div
    meld:id="thediv"/>'

  - Output documents by default do not include any meld3 namespace id
    attributes.  If you wish to preserve meld3 ids (for instance, in
    order to do pipelining of meld3 templates), you can preserve meld
    ids by passing a "pipeline" option to a "write" function
    (e.g. write_xml, write_xhtml).

  - Output can be performed in "XML mode", "XHTML mode" and "HTML
    mode".  HTML output follows recommendations for HTML 4.01, while
    XML and XHTML output outputs valid XML If you create an empty
    textarea element and output it in XML and XHTML mode the output
    will be rendered <'textarea/>'.  In HTML mode, it will be rendered
    as '<textarea></textarea>'.  In HTML mode, various other tags like
    'img' aren't "balanced" with an ending tag, and so forth.  You can
    decide how you wish to render your templates by passing an 'html'
    flag to the meld 'writer'.

  - meld3 elements are instances of ElementTree elements and support
    the "ElementTree _ElementInterface
    API":http://effbot.org/zone/pythondoc-elementtree-ElementTree.htm#elementtree.ElementTree._ElementInterface-class)
    instead of the PyMeld node API.  The ElementTree _ElementInterface
    API has been extended by meld3 to perform various functions
    specific to meld3.

  - meld3 elements do not support the __mod__ method with a sequence
    argument; they do support the __mod__ method with a dictionary
    argument, however.

  - meld3 elements support various ZPT-alike methods like "repeat",
    "content", "attributes", and "replace" that are meant to work like
    their ZPT counterparts.

Examples

  A valid example meld3 template is as
  follows::

    <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
    <html xmlns="http://www.w3.org/1999/xhtml"
          xmlns:meld="http://www.plope.com/software/meld3"
          xmlns:bar="http://foo/bar">
      <head>
        <meta content="text/html; charset=ISO-8859-1" http-equiv="content-type" />
        <title meld:id="title">This is the title</title>
      </head>
      <body>
        <div/> <!-- empty tag -->
        <div meld:id="content_well">
          <form meld:id="form1" action="." method="POST">
          <table border="0" meld:id="table1">
            <tbody meld:id="tbody">
              <tr>
                <th>Name</th>
                <th>Description</th>
              </tr>
              <tr meld:id="tr" class="foo">
                <td meld:id="td1">Name</td>
                <td meld:id="td2">Description</td>
              </tr>
            </tbody>
          </table>
          <input type="submit" name="next" value=" Next "/>
          </form>
        </div>
      </body>
    </html>

  Note that the script contains no logic, only "meld:id" identifiers.
  All "meld:id" identifiers in a single document must be unique for a
  meld template to be parseable.

  A script which parses the above template and does some
  transformations is below.  Consider the variable "xml" below bound
  to a string representing the XHTML above::

    from meld3 import parse_xmlstring
    from meld3 import parse_htmlstring
    from StringIO import StringIO
    import sys

    root = parse_xmlstring(xml)
    root.findmeld('title').content('My document')
    root.findmeld('form1').attributes(action='./handler')
    data = (
        {'name':'Boys',
         'description':'Ugly'},
        {'name':'Girls',
         'description':'Pretty'},
        )
    iterator = root.findmeld('tr').repeat(data)
    for element, item in iterator:
        element.findmeld('td1').content(item['name'])
        element.findmeld('td2').content(item['description'])

  You used the "parse_xmlstring" function to transform the XML into a
  tree of nodes above.  This was possible because the input was
  well-formed XML.  If it had not been, you would have needed to use
  the "parse_htmlstring" function instead.

  To output the result of the transformations to stdout as XML, we use
  the 'write' method of any element.  Below, we use the root element
  (consider it bound to the value of "root" in the above script)::

    import sys
    root.write_xml(sys.stdout)
    ...
    <?xml version="1.0"?>
    <html:html xmlns:html="http://www.w3.org/1999/xhtml">
      <html:head>
        <html:meta content="text/html; charset=ISO-8859-1" http-equiv="content-type" />
        <html:title>My document</html:title>
      </html:head>
      <html:body>
        <html:div /> <!--  empty tag  -->
        <html:div>
          <html:form action="./handler" method="POST">
          <html:table border="0">
            <html:tbody>
              <html:tr>
                <html:th>Name</html:th>
                <html:th>Description</html:th>
              </html:tr>
              <html:tr class="foo">
                <html:td>Boys</html:td>
                <html:td>Ugly</html:td>
              </html:tr>
            <html:tr class="foo">
                <html:td>Girls</html:td>
                <html:td>Pretty</html:td>
              </html:tr>
            </html:tbody>
          </html:table>
          <html:input name="next" type="submit" value=" Next " />
          </html:form>
        </html:div>
      </html:body>
    </html:html>

  We can also serialize our element tree as well-formed XHTML, which
  is largely like rendering to XML except it by default doesn't emit
  the XML declaration and it removes all "html" namespace declarations
  from the output.  It also emits a XHTML 'loose' doctype declaration
  near the top of the document::

    import sys
    root.write_xhtml(sys.stdout)
    ...
    <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
    <html>
      <head>
        <meta content="text/html; charset=ISO-8859-1" http-equiv="content-type" />
        <title>My document</title>
      </head>
      <body>
        <div /> <!--  empty tag  -->
        <div>
          <form action="./handler" method="POST">
          <table border="0">
            <tbody>
              <tr>
                <th>Name</th>
                <th>Description</th>
              </tr>
              <tr class="foo">
                <td>Boys</td>
                <td>Ugly</td>
              </tr>
            <tr class="foo">
                <td>Girls</td>
                <td>Pretty</td>
              </tr>
            </tbody>
          </table>
          <input name="next" type="submit" value=" Next " />
          </form>
        </div>
      </body>
    </html>

  We can also output text in HTML mode, This serializes the node and
  its children to HTML (this feature was inspired by and based on code
  Ian Bicking).  By default, the serialization will include a 'loose'
  HTML DTD doctype (this can be overridden with the doctype=
  argument).  "Empty" shortcut elements such as '<div/>' will be
  converted to a balanced pair of tags e.g. '<div></div>'.  But some
  HTML tags (defined as per the HTML 4 spec as area, base, basefont,
  br, col, frame, hr, img, input, isindex, link, meta, param) will not
  be followed with a balanced ending tag; only the beginning tag will
  be output.  Additionally, "boolean" tag attributes will not be
  followed with any value.  The "boolean" tags are selected, checked,
  compact, declare, defer, disabled, ismap, multiple, nohref,
  noresize, noshade, and nowrap.  So the XML input '<input
  type="checkbox" checked="checked"/>' will be turned into '<input
  type="checkbox" checked>'.  Additionally, 'script' and 'style' tags
  will not have their contents escaped (e.g. so "&" will not be turned
  into '&amp;' when it's iside the textual content of a script or
  style tag.)::

    import sys
    root.write_html(sys.stdout)
    ...
    <!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
    <html>
      <head>
        <meta content="text/html; charset=ISO-8859-1" http-equiv="content-type">
        <title>My document</title>
      </head>
      <body>
        <div></div> <!--  empty tag  -->
        <div>
          <form action="./handler" method="POST">
          <table border="0">
            <tbody>
              <tr>
                <th>Name</th>
                <th>Description</th>
              </tr>
              <tr class="foo">
                <td>Boys</td>
                <td>Ugly</td>
              </tr>
            <tr class="foo">
                <td>Girls</td>
                <td>Pretty</td>
              </tr>
            </tbody>
          </table>
          <input name="next" type="submit" value=" Next ">
          </form>
        </div>
      </body>
    </html>

Element API

  meld3 elements support all of the "ElementTree _ElementInterface
  API":http://effbot.org/zone/pythondoc-elementtree-ElementTree.htm#elementtree.ElementTree._ElementInterface-class
  .  Other meld-specific methods of elements are as follows::

    "clone(parent=None)": clones a node and all of its children via a
    recursive copy.  If parent is passed in, append the clone to the
    parent node.

    "findmeld(name, default=None)": searches the this element and its
    children for elements that have a 'meld:id' attribute that matches
    "name"; if no element can be found, return the default.

    "meldid()": Returns the "meld id" of the element or None if the element
    has no meld id.

    "repeat(iterable, childname=None)": repeats an element with values
    from an iterable.  If 'childname' is not None, repeat the element on
    which repeat was called, otherwise find the child element with a
    'meld:id' matching 'childname' and repeat that.  The element is
    repeated within its parent element.  This method returns an
    iterable; the value of each iteration is a two-sequence in the form
    (newelement, data).  'newelement' is a clone of the template element
    (including clones of its children) which has already been seated in
    its parent element in the template. 'data' is a value from the
    passed in iterable.  Changing 'newelement' (typically based on
    values from 'data') mutates the element "in place".

    "replace(text, structure=False)": (ala ZPT's 'replace' comnand)
    Replace this element in our parent with a 'Replace' node
    representing the text 'text'.  Return the index of the index
    position in our parent that was replaced.  If 'structure' is true,
    at rendering time, the outputted text will not be escaped in the
    serialization.  If we have no parent, do nothing, and return None.
    This method leaves a special kind of node in the element tree (a
    'Replace' node) to represent the replacement.  NOTE: This command
    has the potential to cause a non-well-formed XML/HTML
    serialization at render time if "structure" is True.

    "content(text, structure=False)": (ala ZPT's 'content' command)
    Delete every child element in this element and append a Replace
    node that contains 'text'.  Always return None.  If 'structure' is
    true, at rendering time, the outputted text will not be escaped in
    the serialization.  If we have no parent, do nothing, and return
    None.  NOTE: This command has the potential to cause a
    non-well-formed XML/HTML serialization at render time if
    "structure" is True.

    "attributes(**kw)": (ala ZPT's 'attributes' command) For each key
    value pair in the kw list, add an attribute to this node where the
    attribute's name is 'key' and the attributes value is 'value'.
    Keys and values must be string or unicode types, else a ValueError
    is raised.  Returns None.

    "__mod__(other)": Fill in the text values of meld nodes in this
    element and children recursively; only support dictionarylike
    "other" operand (sequence operand doesn't seem to make sense here).

    "fillmelds(**kw)":Fill in the text values of meld nodes in this
    element and children recursively.  Return the names of keys in the
    **kw dictionary that could not be found anywhere in the tree.  Never
    raise an exception.

    "write_xml(file, encoding=None, doctype=None, fragment=False,
    declaration=True, pipeline=False)":
    Write XML to 'file' (which can be a filename or filelike object)
    encoding    -- encoding string (if None, 'utf-8' encoding is assumed)
                   Must be a recognizable Python encoding type.
    doctype     -- 3-tuple indicating name, pubid, system of doctype.
                   The default is to prevent a doctype from being emitted.
    fragment    -- True if a 'fragment' should be emitted for this node (no
                   declaration, no doctype).  This causes both the
                   'declaration' and 'doctype' parameters to become ignored
                   if provided.
    declaration -- emit an xml declaration header (including an encoding
                   if it's not None).  The default is to emit the
                   doctype.
    pipeline    -- preserve 'meld' namespace identifiers in output
                   for use in pipelining

    "write_xhtml(self, file, encoding=None, doctype=doctype.xhtml,
    fragment=False, declaration=False, pipeline=False)":
    Write XHTML to 'file' (which can be a filename or filelike object)

    encoding    -- encoding string (if None, 'utf-8' encoding is assumed)
                   Must be a recognizable Python encoding type.
    doctype     -- 3-tuple indicating name, pubid, system of doctype.
                   The default is the value of doctype.xhtml (XHTML
                   'loose').
    fragment    -- True if a 'fragment' should be emitted for this node (no
                   declaration, no doctype).  This causes both the
                   'declaration' and 'doctype' parameters to be ignored.
    declaration -- emit an xml declaration header (including an encoding
                   string if 'encoding' is not None)
    pipeline    -- preserve 'meld' namespace identifiers in output
                   for use in pipelining

    Note that despite the fact that you can tell meld which doctype to
    serve, meld does no semantic or syntactical validation of
    attributes or elements when serving content in XHTML mode; you as
    a programmer are still responsible for ensuring that your
    rendering does not include font tags, for instance.

    Rationale for defaults: By default, 'write_xhtml' doesn't emit an
    XML declaration because versions of IE before 7 apparently go into
    "quirks" layout mode when they see an XML declaration, instead of
    sniffing the DOCTYPE like they do when the xml declaration is not
    present to determine the layout mode.  (see
    http://hsivonen.iki.fi/doctype/ and
    http://blogs.msdn.com/ie/archive/2005/09/15/467901.aspx).
    'write_xhtml' emits a 'loose' XHTML doctype by default instead of
    a 'strict' XHTML doctype because 'tidy' emits a 'loose' doctye by
    default when you convert an HTML document into XHTML via
    '-asxhtml', and I couldn't think of a good reason to contradict
    that precedent.

    A note about emitting the proper Content-Type header when serving
    pages rendered with write_xhtml: you can sometimes use the
    content-type 'application/xhtml+xml' (see
    "http://www.w3.org/TR/xhtml-media-types/#application-xhtml-xml").
    The official specification calls for this.  But not all browsers
    support this content type (notably, no version of IE supports it,
    nor apparently does Safari).  So these pages *may* be served using
    the 'text/html' content type and most browsers will attempt to do
    doctype sniffing to figure out if the document is actually XHTML.
    It appears that you can use the Accepts header in the request to
    figure out if the user agent accepts 'application/xhtml+xml' if
    you're a stickler for correctness.  In practice, this seems like
    the right thing to do.  See
    "http://keystonewebsites.com/articles/mime_type.php" for more
    information on serving up the correct content type header.

    "write_html(self, file, encoding=None, doctype=doctype.html,fragment=False)":
    Write HTML to 'file' (which can be a filename or filelike object)
    encoding    -- encoding string (if None, 'utf-8' encoding is assumed).
                   Unlike XML output, this is not used in a declaration,
                   but it is used to do actual character encoding during
                   output.  Must be a recognizable Python encoding type.
    doctype     -- 3-tuple indicating name, pubid, system of doctype.
                   The default is the value of doctype.html (HTML 4.0
                   'loose')
    fragment    -- True if a "fragment" should be omitted (no doctype).
                   This overrides any provided "doctype" parameter if
                   provided.
    Namespace'd elements and attributes have their namespaces removed
    during output when writing HTML, so pipelining cannot be performed.
    HTML is not valid XML, so an XML declaration header is never emitted.

    In general: For all output methods, comments are preserved in
    output.  They are also present in the ElementTree node tree (as
    Comment elements), so beware. Processing instructions (e.g. '<?xml
    version="1.0">') are completely thrown away at parse time and do
    not exist anywhere in the element tree or in the output (use the
    declaration= parameter to emit a declaration processing
    instruction).

Parsing API

  XML source text is turned into element trees using the
  "parse_xmlstring" function (demonstrated in examples above).  A
  function that accepts a filename or a filelike object instead of a
  string, but which performs the same function is named "parse_xml",
  e.g.::

    from meld3 import parse_xml
    from meld3 import parse_xmlstring

  HTML source text is turned into element trees using the
  "parse_htmlstring" function.  A function that accepts a filename or
  a filelike object instead of a string, but which performs the same
  function is named "parse_html", e.g.::

    from meld3 import parse_html
    from meld3 import parse_htmlstring

  Using duplicate meld identifiers on separate elements in the source
  document causes a ValueError to be raised at parse time.

  When using parse_xml and parse_xmlstring, documents which contain
  entity references (e.g. '&nbsp;') must have the entities defined in
  the source document or must have a DOCTYPE declaration that allows
  those entities to be resolved by the expat parser.

  When using parse_xml and parse_xmlstring, input documents must
  include the meld3 namespace declaration (conventionally on the root
  element).  For example, '<html
  xmlns:meld="http://www.plope.com/software/meld3">...</html>'

  parse_html and parse_htmlstring take an optional "encoding" argument
  which specifies the document source encoding.

To Do

  This implementation depends on classes internal to ElementTree and
  hasn't been tested with cElementTree or lxml, and almost certainly
  won't work with either due to this.

  See TODO.txt for more to-do items.

