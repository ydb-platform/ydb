'''
HTML tag classes.
'''
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
from .dom_tag  import dom_tag, attr, get_current
from .dom1core import dom1core

try:
  basestring = basestring
except NameError: # py3
  basestring = str
  unicode = str

underscored_classes = set(['del', 'input', 'map', 'object'])

# Tag attributes
_ATTR_GLOBAL = set([
  'accesskey', 'class', 'class', 'contenteditable', 'contextmenu', 'dir',
  'draggable', 'id', 'item', 'hidden', 'lang', 'itemprop', 'spellcheck',
  'style', 'subject', 'tabindex', 'title'
])
_ATTR_EVENTS = set([
  'onabort', 'onblur', 'oncanplay', 'oncanplaythrough', 'onchange', 'onclick',
  'oncontextmenu', 'ondblclick', 'ondrag', 'ondragend', 'ondragenter',
  'ondragleave', 'ondragover', 'ondragstart', 'ondrop', 'ondurationchange',
  'onemptied', 'onended', 'onerror', 'onfocus', 'onformchange', 'onforminput',
  'oninput', 'oninvalid', 'onkeydown', 'onkeypress', 'onkeyup', 'onload',
  'onloadeddata', 'onloadedmetadata', 'onloadstart', 'onmousedown',
  'onmousemove', 'onmouseout', 'onmouseover', 'onmouseup', 'onmousewheel',
  'onpause', 'onplay', 'onplaying', 'onprogress', 'onratechange',
  'onreadystatechange', 'onscroll', 'onseeked', 'onseeking', 'onselect',
  'onshow', 'onstalled', 'onsubmit', 'onsuspend', 'ontimeupdate',
  'onvolumechange', 'onwaiting'
])


ERR_ATTRIBUTE = 'attributes'
ERR_CONTEXT = 'context'
ERR_CONTENT = 'content'


class html_tag(dom_tag, dom1core):
  def __init__(self, *args, **kwargs):
    '''
    Creates a new html tag instance.
    '''
    super(html_tag, self).__init__(*args, **kwargs)


  # def validate(self):
  #   '''
  #   Validate the tag. This will check the attributes, context, and contents and
  #   emit tuples in the form of: element, message.
  #   '''
  #   errors = []

  #   errors.extend(self.validate_attributes())
  #   errors.extend(self.validate_context())
  #   errors.extend(self.validate_content())

  #   return errors

  # def validate_attributes(self):
  #   '''
  #   Validate the tag attributes.
  #   '''
  #   return []

  # def validate_context(self):
  #   '''
  #   Validate the tag context.
  #   '''
  #   return []

  # def validate_content(self):
  #   '''
  #   Validate the content of the tag.
  #   '''
  #   return []

  # def _check_attributes(self, *attrs):
  #   valid = set([])
  #   for attr in attrs:
  #     if hasattr(attr, '__iter__'):
  #       valid |= set(attr)
  #     else:
  #       valid.add(attr)
  #   return set(list(self.attributes.iterkeys())) - valid



################################################################################
############################### Html Tag Classes ###############################
################################################################################

# Root element

class html(html_tag):
  '''
  The html element represents the root of an HTML document.
  '''
  pass
  # def validate_attributes(self):
  #   errors = []
  #   for invalid in self._check_attributes(_ATTR_GLOBAL, 'manifest'):
  #     errors.append( (self, ERR_ATTRIBUTE, 'Invalid attribute: "%s"' % invalid) )
  #   return errors

  # def validate_context(self):
  #   if self.parent is not None and not isinstance(self.parent, iframe):
  #     return [(self, ERR_CONTEXT, 'Must be root element or child of an <iframe>')]
  #   return []

  # def validate_content(self):
  #   if len(self) != 2 or not isinstance(self[0], head) or not isinstance(self[1], body):
  #     return [(self, ERR_CONTENT, 'Children must be <head> and then <body>.')]
  #   return []


# Document metadata
class head(html_tag):
  '''
  The head element represents a collection of metadata for the document.
  '''
  pass


class title(html_tag):
  '''
  The title element represents the document's title or name. Authors should use
  titles that identify their documents even when they are used out of context,
  for example in a user's history or bookmarks, or in search results. The
  document's title is often different from its first heading, since the first
  heading does not have to stand alone when taken out of context.
  '''
  def _get_text(self):
    return u''.join(self.get(basestring))
  def _set_text(self, text):
    self.clear()
    self.add(text)
  text = property(_get_text, _set_text)


class base(html_tag):
  '''
  The base element allows authors to specify the document base URL for the
  purposes of resolving relative URLs, and the name of the default browsing
  context for the purposes of following hyperlinks. The element does not
  represent any content beyond this information.
  '''
  is_single = True


class link(html_tag):
  '''
  The link element allows authors to link their document to other resources.
  '''
  is_single = True


class meta(html_tag):
  '''
  The meta element represents various kinds of metadata that cannot be
  expressed using the title, base, link, style, and script elements.
  '''
  is_single = True


class style(html_tag):
  '''
  The style element allows authors to embed style information in their
  documents. The style element is one of several inputs to the styling
  processing model. The element does not represent content for the user.
  '''
  is_pretty = False


# Scripting
class script(html_tag):
  '''
  The script element allows authors to include dynamic script and data blocks
  in their documents. The element does not represent content for the user.
  '''
  is_pretty = False


class noscript(html_tag):
  '''
  The noscript element represents nothing if scripting is enabled, and
  represents its children if scripting is disabled. It is used to present
  different markup to user agents that support scripting and those that don't
  support scripting, by affecting how the document is parsed.
  '''
  pass


# Sections
class body(html_tag):
  '''
  The body element represents the main content of the document.
  '''
  pass

class main(html_tag):
  '''
  The main content area of a document includes content that is unique to that
  document and excludes content that is repeated across a set of documents such
  as site navigation links, copyright information, site logos and banners and
  search forms (unless the document or application's main function is that of a
  search form).
  '''

class section(html_tag):
  '''
  The section element represents a generic section of a document or
  application. A section, in this context, is a thematic grouping of content,
  typically with a heading.
  '''
  pass


class nav(html_tag):
  '''
  The nav element represents a section of a page that links to other pages or
  to parts within the page: a section with navigation links.
  '''
  pass


class article(html_tag):
  '''
  The article element represents a self-contained composition in a document,
  page, application, or site and that is, in principle, independently
  distributable or reusable, e.g. in syndication. This could be a forum post, a
  magazine or newspaper article, a blog entry, a user-submitted comment, an
  interactive widget or gadget, or any other independent item of content.
  '''
  pass


class aside(html_tag):
  '''
  The aside element represents a section of a page that consists of content
  that is tangentially related to the content around the aside element, and
  which could be considered separate from that content. Such sections are
  often represented as sidebars in printed typography.
  '''
  pass


class h1(html_tag):
  '''
  Represents the highest ranking heading.
  '''
  pass


class h2(html_tag):
  '''
  Represents the second-highest ranking heading.
  '''
  pass


class h3(html_tag):
  '''
  Represents the third-highest ranking heading.
  '''
  pass


class h4(html_tag):
  '''
  Represents the fourth-highest ranking heading.
  '''
  pass


class h5(html_tag):
  '''
  Represents the fifth-highest ranking heading.
  '''
  pass


class h6(html_tag):
  '''
  Represents the sixth-highest ranking heading.
  '''
  pass


class hgroup(html_tag):
  '''
  The hgroup element represents the heading of a section. The element is used
  to group a set of h1-h6 elements when the heading has multiple levels, such
  as subheadings, alternative titles, or taglines.
  '''
  pass


class header(html_tag):
  '''
  The header element represents a group of introductory or navigational aids.
  '''
  pass


class footer(html_tag):
  '''
  The footer element represents a footer for its nearest ancestor sectioning
  content or sectioning root element. A footer typically contains information
  about its section such as who wrote it, links to related documents,
  copyright data, and the like.
  '''
  pass


class address(html_tag):
  '''
  The address element represents the contact information for its nearest
  article or body element ancestor. If that is the body element, then the
  contact information applies to the document as a whole.
  '''
  pass


# Grouping content
class p(html_tag):
  '''
  The p element represents a paragraph.
  '''
  pass


class hr(html_tag):
  '''
  The hr element represents a paragraph-level thematic break, e.g. a scene
  change in a story, or a transition to another topic within a section of a
  reference book.
  '''
  is_single = True


class pre(html_tag):
  '''
  The pre element represents a block of preformatted text, in which structure
  is represented by typographic conventions rather than by elements.
  '''
  is_pretty = False


class blockquote(html_tag):
  '''
  The blockquote element represents a section that is quoted from another
  source.
  '''
  pass


class ol(html_tag):
  '''
  The ol element represents a list of items, where the items have been
  intentionally ordered, such that changing the order would change the
  meaning of the document.
  '''
  pass


class ul(html_tag):
  '''
  The ul element represents a list of items, where the order of the items is
  not important - that is, where changing the order would not materially change
  the meaning of the document.
  '''
  pass


class li(html_tag):
  '''
  The li element represents a list item. If its parent element is an ol, ul, or
  menu element, then the element is an item of the parent element's list, as
  defined for those elements. Otherwise, the list item has no defined
  list-related relationship to any other li element.
  '''
  pass


class dl(html_tag):
  '''
  The dl element represents an association list consisting of zero or more
  name-value groups (a description list). Each group must consist of one or
  more names (dt elements) followed by one or more values (dd elements).
  Within a single dl element, there should not be more than one dt element for
  each name.
  '''
  pass


class dt(html_tag):
  '''
  The dt element represents the term, or name, part of a term-description group
  in a description list (dl element).
  '''
  pass


class dd(html_tag):
  '''
  The dd element represents the description, definition, or value, part of a
  term-description group in a description list (dl element).
  '''
  pass


class figure(html_tag):
  '''
  The figure element represents some flow content, optionally with a caption,
  that is self-contained and is typically referenced as a single unit from the
  main flow of the document.
  '''
  pass


class figcaption(html_tag):
  '''
  The figcaption element represents a caption or legend for the rest of the
  contents of the figcaption element's parent figure element, if any.
  '''
  pass


class div(html_tag):
  '''
  The div element has no special meaning at all. It represents its children. It
  can be used with the class, lang, and title attributes to mark up semantics
  common to a group of consecutive elements.
  '''
  pass


# Text semantics
class a(html_tag):
  '''
  If the a element has an href attribute, then it represents a hyperlink (a
  hypertext anchor).

  If the a element has no href attribute, then the element represents a
  placeholder for where a link might otherwise have been placed, if it had been
  relevant.
  '''
  pass


class em(html_tag):
  '''
  The em element represents stress emphasis of its contents.
  '''
  pass


class strong(html_tag):
  '''
  The strong element represents strong importance for its contents.
  '''
  pass


class small(html_tag):
  '''
  The small element represents side comments such as small print.
  '''
  pass


class s(html_tag):
  '''
  The s element represents contents that are no longer accurate or no longer
  relevant.
  '''
  pass


class cite(html_tag):
  '''
  The cite element represents the title of a work (e.g. a book, a paper, an
  essay, a poem, a score, a song, a script, a film, a TV show, a game, a
  sculpture, a painting, a theatre production, a play, an opera, a musical, an
  exhibition, a legal case report, etc). This can be a work that is being
  quoted or referenced in detail (i.e. a citation), or it can just be a work
  that is mentioned in passing.
  '''
  pass


class q(html_tag):
  '''
  The q element represents some phrasing content quoted from another source.
  '''
  pass


class dfn(html_tag):
  '''
  The dfn element represents the defining instance of a term. The paragraph,
  description list group, or section that is the nearest ancestor of the dfn
  element must also contain the definition(s) for the term given by the dfn
  element.
  '''
  pass


class abbr(html_tag):
  '''
  The abbr element represents an abbreviation or acronym, optionally with its
  expansion. The title attribute may be used to provide an expansion of the
  abbreviation. The attribute, if specified, must contain an expansion of the
  abbreviation, and nothing else.
  '''
  pass


class time_(html_tag):
  '''
  The time element represents either a time on a 24 hour clock, or a precise
  date in the proleptic Gregorian calendar, optionally with a time and a
  time-zone offset.
  '''
  pass
_time = time_


class code(html_tag):
  '''
  The code element represents a fragment of computer code. This could be an XML
  element name, a filename, a computer program, or any other string that a
  computer would recognize.
  '''
  pass


class var(html_tag):
  '''
  The var element represents a variable. This could be an actual variable in a
  mathematical expression or programming context, an identifier representing a
  constant, a function parameter, or just be a term used as a placeholder in
  prose.
  '''
  pass


class samp(html_tag):
  '''
  The samp element represents (sample) output from a program or computing
  system.
  '''
  pass


class kbd(html_tag):
  '''
  The kbd element represents user input (typically keyboard input, although it
  may also be used to represent other input, such as voice commands).
  '''
  pass


class sub(html_tag):
  '''
  The sub element represents a subscript.
  '''
  pass


class sup(html_tag):
  '''
  The sup element represents a superscript.
  '''
  pass


class i(html_tag):
  is_inline = True
  '''
  The i element represents a span of text in an alternate voice or mood, or
  otherwise offset from the normal prose in a manner indicating a different
  quality of text, such as a taxonomic designation, a technical term, an
  idiomatic phrase from another language, a thought, or a ship name in Western
  texts.
  '''
  pass


class b(html_tag):
  '''
  The b element represents a span of text to which attention is being drawn for
  utilitarian purposes without conveying any extra importance and with no
  implication of an alternate voice or mood, such as key words in a document
  abstract, product names in a review, actionable words in interactive
  text-driven software, or an article lede.
  '''
  pass


class u(html_tag):
  '''
  The u element represents a span of text with an unarticulated, though
  explicitly rendered, non-textual annotation, such as labeling the text as
  being a proper name in Chinese text (a Chinese proper name mark), or
  labeling the text as being misspelt.
  '''
  pass


class mark(html_tag):
  '''
  The mark element represents a run of text in one document marked or
  highlighted for reference purposes, due to its relevance in another context.
  When used in a quotation or other block of text referred to from the prose,
  it indicates a highlight that was not originally present but which has been
  added to bring the reader's attention to a part of the text that might not
  have been considered important by the original author when the block was
  originally written, but which is now under previously unexpected scrutiny.
  When used in the main prose of a document, it indicates a part of the
  document that has been highlighted due to its likely relevance to the user's
  current activity.
  '''
  pass


class ruby(html_tag):
  '''
  The ruby element allows one or more spans of phrasing content to be marked
  with ruby annotations. Ruby annotations are short runs of text presented
  alongside base text, primarily used in East Asian typography as a guide for
  pronunciation or to include other annotations. In Japanese, this form of
  typography is also known as furigana.
  '''
  pass


class rt(html_tag):
  '''
  The rt element marks the ruby text component of a ruby annotation.
  '''
  pass


class rp(html_tag):
  '''
  The rp element can be used to provide parentheses around a ruby text
  component of a ruby annotation, to be shown by user agents that don't support
  ruby annotations.
  '''
  pass


class bdi(html_tag):
  '''
  The bdi element represents a span of text that is to be isolated from its
  surroundings for the purposes of bidirectional text formatting.
  '''
  pass


class bdo(html_tag):
  '''
  The bdo element represents explicit text directionality formatting control
  for its children. It allows authors to override the Unicode bidirectional
  algorithm by explicitly specifying a direction override.
  '''
  pass


class span(html_tag):
  '''
  The span element doesn't mean anything on its own, but can be useful when
  used together with the global attributes, e.g. class, lang, or dir. It
  represents its children.
  '''
  pass


class br(html_tag):
  '''
  The br element represents a line break.
  '''
  is_single = True
  is_inline = True


class wbr(html_tag):
  '''
  The wbr element represents a line break opportunity.
  '''
  is_single = True
  is_inline = True


# Edits
class ins(html_tag):
  '''
  The ins element represents an addition to the document.
  '''
  pass


class del_(html_tag):
  '''
  The del element represents a removal from the document.
  '''
  pass
_del = del_

# Embedded content
class img(html_tag):
  '''
  An img element represents an image.
  '''
  is_single = True


class iframe(html_tag):
  '''
  The iframe element represents a nested browsing context.
  '''
  pass


class embed(html_tag):
  '''
  The embed element represents an integration point for an external (typically
  non-HTML) application or interactive content.
  '''
  is_single = True


class object_(html_tag):
  '''
  The object element can represent an external resource, which, depending on
  the type of the resource, will either be treated as an image, as a nested
  browsing context, or as an external resource to be processed by a plugin.
  '''
  pass
_object = object_


class param(html_tag):
  '''
  The param element defines parameters for plugins invoked by object elements.
  It does not represent anything on its own.
  '''
  is_single = True


class video(html_tag):
  '''
  A video element is used for playing videos or movies, and audio files with
  captions.
  '''
  pass


class audio(html_tag):
  '''
  An audio element represents a sound or audio stream.
  '''
  pass


class source(html_tag):
  '''
  The source element allows authors to specify multiple alternative media
  resources for media elements. It does not represent anything on its own.
  '''
  is_single = True


class track(html_tag):
  '''
  The track element allows authors to specify explicit external timed text
  tracks for media elements. It does not represent anything on its own.
  '''
  is_single = True


class canvas(html_tag):
  '''
  The canvas element provides scripts with a resolution-dependent bitmap
  canvas, which can be used for rendering graphs, game graphics, or other
  visual images on the fly.
  '''
  pass


class map_(html_tag):
  '''
  The map element, in conjunction with any area element descendants, defines an
  image map. The element represents its children.
  '''
  pass
_map = map_

class area(html_tag):
  '''
  The area element represents either a hyperlink with some text and a
  corresponding area on an image map, or a dead area on an image map.
  '''
  is_single = True


# Tabular data
class table(html_tag):
  '''
  The table element represents data with more than one dimension, in the form
  of a table.
  '''
  pass


class caption(html_tag):
  '''
  The caption element represents the title of the table that is its parent, if
  it has a parent and that is a table element.
  '''
  pass


class colgroup(html_tag):
  '''
  The colgroup element represents a group of one or more columns in the table
  that is its parent, if it has a parent and that is a table element.
  '''
  pass


class col(html_tag):
  '''
  If a col element has a parent and that is a colgroup element that itself has
  a parent that is a table element, then the col element represents one or more
  columns in the column group represented by that colgroup.
  '''
  is_single = True


class tbody(html_tag):
  '''
  The tbody element represents a block of rows that consist of a body of data
  for the parent table element, if the tbody element has a parent and it is a
  table.
  '''
  pass


class thead(html_tag):
  '''
  The thead element represents the block of rows that consist of the column
  labels (headers) for the parent table element, if the thead element has a
  parent and it is a table.
  '''
  pass


class tfoot(html_tag):
  '''
  The tfoot element represents the block of rows that consist of the column
  summaries (footers) for the parent table element, if the tfoot element has a
  parent and it is a table.
  '''
  pass


class tr(html_tag):
  '''
  The tr element represents a row of cells in a table.
  '''
  pass


class td(html_tag):
  '''
  The td element represents a data cell in a table.
  '''
  pass


class th(html_tag):
  '''
  The th element represents a header cell in a table.
  '''
  pass


# Forms
class form(html_tag):
  '''
  The form element represents a collection of form-associated elements, some of
  which can represent editable values that can be submitted to a server for
  processing.
  '''
  pass


class fieldset(html_tag):
  '''
  The fieldset element represents a set of form controls optionally grouped
  under a common name.
  '''
  pass


class legend(html_tag):
  '''
  The legend element represents a caption for the rest of the contents of the
  legend element's parent fieldset element, if any.
  '''
  pass


class label(html_tag):
  '''
  The label represents a caption in a user interface. The caption can be
  associated with a specific form control, known as the label element's labeled
  control, either using for attribute, or by putting the form control inside
  the label element itself.
  '''
  pass


class input_(html_tag):
  '''
  The input element represents a typed data field, usually with a form control
  to allow the user to edit the data.
  '''
  is_single = True
_input = input_


class button(html_tag):
  '''
  The button element represents a button. If the element is not disabled, then
  the user agent should allow the user to activate the button.
  '''
  pass


class select(html_tag):
  '''
  The select element represents a control for selecting amongst a set of
  options.
  '''
  pass


class datalist(html_tag):
  '''
  The datalist element represents a set of option elements that represent
  predefined options for other controls. The contents of the element represents
  fallback content for legacy user agents, intermixed with option elements that
  represent the predefined options. In the rendering, the datalist element
  represents nothing and it, along with its children, should be hidden.
  '''
  pass


class optgroup(html_tag):
  '''
  The optgroup element represents a group of option elements with a common
  label.
  '''
  pass


class option(html_tag):
  '''
  The option element represents an option in a select element or as part of a
  list of suggestions in a datalist element.
  '''
  pass


class textarea(html_tag):
  '''
  The textarea element represents a multiline plain text edit control for the
  element's raw value. The contents of the control represent the control's
  default value.
  '''
  pass


class keygen(html_tag):
  '''
  The keygen element represents a key pair generator control. When the
  control's form is submitted, the private key is stored in the local keystore,
  and the public key is packaged and sent to the server.
  '''
  is_single = True


class output(html_tag):
  '''
  The output element represents the result of a calculation.
  '''
  pass


class progress(html_tag):
  '''
  The progress element represents the completion progress of a task. The
  progress is either indeterminate, indicating that progress is being made but
  that it is not clear how much more work remains to be done before the task is
  complete (e.g. because the task is waiting for a remote host to respond), or
  the progress is a number in the range zero to a maximum, giving the fraction
  of work that has so far been completed.
  '''
  pass


class meter(html_tag):
  '''
  The meter element represents a scalar measurement within a known range, or a
  fractional value; for example disk usage, the relevance of a query result, or
  the fraction of a voting population to have selected a particular candidate.
  '''
  pass


# Interactive elements
class details(html_tag):
  '''
  The details element represents a disclosure widget from which the user can
  obtain additional information or controls.
  '''
  pass


class summary(html_tag):
  '''
  The summary element represents a summary, caption, or legend for the rest of
  the contents of the summary element's parent details element, if any.
  '''
  pass


class command(html_tag):
  '''
  The command element represents a command that the user can invoke.
  '''
  is_single = True


class menu(html_tag):
  '''
  The menu element represents a list of commands.
  '''
  pass


class font(html_tag):
  '''
  The font element represents the font in a html .
  '''
  pass


# Additional markup
class comment(html_tag):
  '''
  Normal, one-line comment:
    >>> print comment("Hello, comments!")
    <!--Hello, comments!-->

  For IE's "if" statement comments:
    >>> print comment(p("Upgrade your browser."), condition='lt IE6')
    <!--[if lt IE6]><p>Upgrade your browser.</p><![endif]-->

  Downlevel conditional comments:
    >>> print comment(p("You are using a ", em("downlevel"), " browser."),
            condition='false', downlevel='revealed')
    <![if false]><p>You are using a <em>downlevel</em> browser.</p><![endif]>

  For more on conditional comments see:
    http://msdn.microsoft.com/en-us/library/ms537512(VS.85).aspx
  '''

  ATTRIBUTE_CONDITION = 'condition'

  # Valid values are 'hidden', 'downlevel' or 'revealed'
  ATTRIBUTE_DOWNLEVEL = 'downlevel'

  def _render(self, sb, indent_level=1, indent_str='  ', pretty=True, xhtml=False):
    has_condition = comment.ATTRIBUTE_CONDITION in self.attributes
    is_revealed   = comment.ATTRIBUTE_DOWNLEVEL in self.attributes and \
        self.attributes[comment.ATTRIBUTE_DOWNLEVEL] == 'revealed'

    sb.append('<!')
    if not is_revealed:
      sb.append('--')
    if has_condition:
      sb.append('[if %s]>' % self.attributes[comment.ATTRIBUTE_CONDITION])

    pretty = self._render_children(sb, indent_level - 1, indent_str, pretty, xhtml)

    # if len(self.children) > 1:
    if any(isinstance(child, dom_tag) for child in self):
      sb.append('\n')
      sb.append(indent_str * (indent_level - 1))

    if has_condition:
      sb.append('<![endif]')
    if not is_revealed:
      sb.append('--')
    sb.append('>')

    return sb
