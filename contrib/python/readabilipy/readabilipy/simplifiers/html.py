"""Common HTML cleaning functions."""
from bs4 import Comment, Doctype, NavigableString
from .text import normalise_text


def elements_to_delete():
    """Elements that will be deleted together with their contents."""
    html5_form_elements = ['button', 'datalist', 'fieldset', 'form', 'input',
                           'label', 'legend', 'meter', 'optgroup', 'option',
                           'output', 'progress', 'select', 'textarea']
    html5_image_elements = ['area', 'img', 'map', 'picture', 'source']
    html5_media_elements = ['audio', 'track', 'video']
    html5_embedded_elements = ['embed', 'iframe', 'math', 'object', 'param', 'svg']
    html5_interactive_elements = ['details', 'dialog', 'summary']
    html5_scripting_elements = ['canvas', 'noscript', 'script', 'template']
    html5_data_elements = ['data', 'link']
    html5_formatting_elements = ['style']
    html5_navigation_elements = ['nav']

    elements = html5_form_elements + html5_image_elements \
        + html5_media_elements + html5_embedded_elements \
        + html5_interactive_elements + html5_scripting_elements \
        + html5_data_elements + html5_formatting_elements \
        + html5_navigation_elements

    return elements


def elements_to_replace_with_contents():
    """Elements that we will discard while keeping their contents."""
    elements = ['a', 'abbr', 'address', 'b', 'bdi', 'bdo', 'center', 'cite',
                'code', 'del', 'dfn', 'em', 'i', 'ins', 'kbs', 'mark',
                'rb', 'ruby', 'rp', 'rt', 'rtc', 's', 'samp', 'small', 'span',
                'strong', 'time', 'u', 'var', 'wbr']
    return elements


def special_elements():
    """Elements that we will discard while keeping their contents that need
    additional processing."""
    elements = ['q', 'sub', 'sup']
    return elements


def block_level_whitelist():
    """Elements that we will always accept."""
    elements = ['article', 'aside', 'blockquote', 'caption', 'colgroup', 'col',
                'div', 'dl', 'dt', 'dd', 'figure', 'figcaption', 'footer',
                'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'header', 'li', 'main',
                'ol', 'p', 'pre', 'section', 'table', 'tbody', 'thead',
                'tfoot', 'tr', 'td', 'th', 'ul']
    return elements


def structural_elements():
    """Structural elements we do no further processing on (though we do remove attributes and alter their contents)"""
    return ['html', 'head', 'body']


def metadata_elements():
    """Metadata elements we do no further processing on (though we do remove attributes and alter their contents)"""
    return ['meta', 'link', 'base', 'title']


def linebreak_elements():
    return ['br', 'hr']


def known_elements():
    """All elements that we know by name."""
    return structural_elements() + metadata_elements() + linebreak_elements() + elements_to_delete() \
        + elements_to_replace_with_contents() + special_elements() \
        + block_level_whitelist()


def remove_metadata(soup):
    """Remove comments, CData and doctype. These are not rendered by browsers.
    The lxml-based parsers automatically convert CData to comments unless it is
    inside <script> tags. CData will therefore be removed either as a comment
    or as part of a <script> but if any other behaviour is desired, the HTML
    will need to be pre-processed before giving it to the BeautifulSoup parser.

    We were a bit worried about potentially removing content here but satisfied
    ourselves it won't be displayed by most browsers in most cases
    (see https://github.com/alan-turing-institute/ReadabiliPy/issues/32)"""
    for comment in soup.findAll(string=lambda text: any(isinstance(text, x) for x in [Comment, Doctype])):
        comment.extract()


def strip_attributes(soup):
    """Strip class and style attributes."""
    for element in soup.find_all():
        element.attrs.pop("class", None)
        element.attrs.pop("style", None)


def remove_blacklist(soup):
    """Remove all blacklisted elements."""
    for element_name in elements_to_delete():
        for element in soup.find_all(element_name):
            element.decompose()


def unwrap_elements(soup):
    """Flatten all elements where we are only interested in their contents."""
    # We do not need to unwrap from the "bottom up" as all we are doing is replacing elements with their contents so
    # we will still find child elements after their parent has been unwrapped.
    for element_name in elements_to_replace_with_contents():
        for element in soup.find_all(element_name):
            element.unwrap()


def process_special_elements(soup):
    """Flatten special elements while processing their contents."""
    for element_name in special_elements():
        for element in soup.find_all(element_name):
            # Insert appropriate strings before and/or after the contents
            if element.name == 'q':
                element.insert_before(NavigableString('"'))
                element.insert_after(NavigableString('"'))
            if element.name == 'sub':
                element.insert_before(NavigableString('_'))
            if element.name == 'sup':
                element.insert_before(NavigableString('^'))
            # Replace the element by its contents
            element.unwrap()


def process_unknown_elements(soup):
    """Replace any unknown elements with their contents."""
    for element in soup.find_all():
        if element.name not in known_elements():
            element.unwrap()


def consolidate_text(soup):
    """Join any consecutive NavigableStrings together."""
    # Iterate over all strings in the tree
    for element in soup.find_all(string=True):
        # If the previous element is the same type then extract the current string and append to previous
        if type(element.previous_sibling) is type(element):
            text = "".join([str(element.previous_sibling), str(element)])
            element.previous_sibling.replace_with(text)
            element.extract()


def remove_empty_strings_and_elements(soup):
    """Remove any strings which contain only whitespace. Without this,
    consecutive linebreaks may not be identified correctly."""
    for element in list(soup.descendants):
        if not normalise_text(str(element)):
            element.extract()


def unnest_paragraphs(soup):
    """Split out block-level elements illegally contained inside paragraphs."""
    illegal_elements = ["address", "article", "aside", "blockquote", "canvas", "dd", "div", "dl", "dt", "fieldset",
                        "figcaption", "figure", "footer", "form", "h1>-<h6", "header", "hr", "li", "main", "nav",
                        "noscript", "ol", "p", "pre", "section", "table", "tfoot", "ul", "video"]
    for nested_type in illegal_elements:
        # Search for nested elements that need to be split out
        nested_elements = [e for e in soup.find_all('p') if e.find(nested_type)]
        while nested_elements:
            # Separate this element into the nested element, plus before and after
            elem_nested = nested_elements[0].find(nested_type)
            p_before = soup.new_tag("p")
            for sibling in list(elem_nested.previous_siblings):
                p_before.append(sibling)
            p_after = soup.new_tag("p")
            for sibling in list(elem_nested.next_siblings):
                p_after.append(sibling)
            # Replace element by before/nested/after.
            # NB. this is done in reverse order as we are adding after the current position
            nested_elements[0].insert_after(p_after)
            nested_elements[0].insert_after(elem_nested)
            nested_elements[0].insert_after(p_before)
            nested_elements[0].decompose()
            # Rerun search for nested elements now that we have rewritten the tree
            nested_elements = [e for e in soup.find_all('p') if e.find(nested_type)]


def insert_paragraph_breaks(soup):
    """Identify <br> and <hr> and split their parent element into multiple elements where appropriate."""
    # Indicator which is used as a placeholder to mark paragraph breaks
    BREAK_INDICATOR = "|BREAK_HERE|"

    # Find consecutive <br> elements and replace with a break marker
    for element in soup.find_all('br'):
        # When the next element is not another <br> count how long the chain is
        if (element.next_sibling is None) or (element.next_sibling.name != 'br'):
            br_element_chain = [element]
            while (br_element_chain[-1].previous_sibling is not None) and (br_element_chain[-1].previous_sibling.name == 'br'):
                br_element_chain.append(br_element_chain[-1].previous_sibling)

            # If there's only one <br> then we replace it with a space
            if len(br_element_chain) == 1:
                br_element_chain[0].replace_with(' ')
            # If there are multiple <br>s then replace them with BREAK_INDICATOR
            else:
                br_element_chain[0].replace_with(BREAK_INDICATOR)
                for inner_element in br_element_chain[1:]:
                    inner_element.decompose()

    # Find consecutive <hr> elements and replace with a break marker
    # Use a list rather than the generator, since we are altering the tree as we traverse it
    for element in list(soup.find_all('hr')):
        element.replace_with(BREAK_INDICATOR)

    # Consolidate the text again now that we have added strings to the tree
    consolidate_text(soup)

    # Iterate through the tree, splitting string elements which contain BREAK_INDICATOR
    # Use a list rather than the generator, since we are altering the tree as we traverse it
    for element in list(soup.find_all(string=True)):
        if BREAK_INDICATOR in element:
            # Split the text into two or more fragments (there maybe be multiple BREAK_INDICATORs in the string)
            text_fragments = [s.strip() for s in str(element).split(BREAK_INDICATOR)]

            # Get the parent element
            parent_element = element.parent

            # If the parent is a paragraph then we want to close and reopen by creating a new tag
            if parent_element.name == "p":
                # Iterate in reverse order as we are repeatedly adding new elements directly after the original one
                for text_fragment in text_fragments[:0:-1]:
                    new_p_element = soup.new_tag("p")
                    new_p_element.string = text_fragment
                    parent_element.insert_after(new_p_element)
                # Replace this element by a navigable string containing the first text fragment
                element.replace_with(NavigableString(text_fragments[0]))
            # Otherwise we want to simply include all the text fragments as independent NavigableStrings (that will be wrapped later)
            else:
                # Iterate in reverse order as we are repeatedly adding new elements directly after the original one
                for text_fragment in text_fragments[:0:-1]:
                    element.insert_after(soup.new_string(text_fragment))
                element.string.replace_with(text_fragments[0])


def normalise_strings(soup):
    """Remove extraneous whitespace and fix unicode issues in all strings."""
    # Iterate over all strings in the tree (including bare strings outside tags)
    for element in soup.find_all(string=True):
        # Treat Beautiful Soup text elements as strings when normalising since normalisation returns a copy of the string
        text = str(element)
        normalised_text = normalise_text(text)
        # Replace the element with a new string element of the same type, but containing the normalised text
        element.replace_with(type(element)(normalised_text))


def wrap_bare_text(soup):
    """Wrap any remaining bare text in <p> tags.

    We do this to ensure that there is a strong, unique correspondance between presentational paragraphs and DOM structure
     - all presentational paragraphs should be the only content associated with their immediate parent
     - all presentational paragraphs at the same conceptual level should be equally nested
     - the string as displayed in the browser should be equivalent to the innerHTML of the parent (so that indexing is equivalent between presentation and source)

    The following examples should not be allowed:

     1. Two presentational elements at the same DOM level have non-equivalent index levels
       <div index="1.1">
         text
         <p index="1.1.1">more text</p>
       </div>

     2. Index 1.1 might contain both strings
       <div index="1.1">
         <p index="1.1.1">more text</p>
         text
       </div>

     3. Two presentational paragraphs are included in the same index
       <div index="1.1">
         text
         <p index="1.1.1">more text</p>
         yet more text
       </div>
    """
    # Iterate over all strings in the tree
    for element in soup.find_all(string=True):
        # If this is the only child of a whitelisted block then do nothing
        # if we add <p> tags here then:
        # - this might not be allowed for all whitelisted elements
        # - we are adding additional structure that was not present in the original document
        if element.parent.name in block_level_whitelist() and len(element.parent.contents) == 1:
            pass
        # ... otherwise wrap them in <p> tags
        else:
            p_element = soup.new_tag("p")
            p_element.string = element
            element.replace_with(p_element)


def recursively_prune_elements(soup):
    """Recursively prune out any elements which have no children or only zero-length children."""
    def single_replace():
        n_removed = 0
        # Remove elements with no children
        for element in soup.find_all(lambda elem: len(list(elem.children)) == 0):
            element.decompose()
            n_removed += 1
        # Remove elements with only zero-length children
        for element in soup.find_all(lambda elem: sum(len(c) for c in elem.children) == 0):
            element.decompose()
            n_removed += 1
        return n_removed
    # Repeatedly apply single_replace() until no elements are being removed
    while single_replace():
        pass
