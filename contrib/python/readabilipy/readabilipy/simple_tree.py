"""Turn input HTML into a cleaned parsed tree."""
from bs4 import BeautifulSoup
from .simplifiers.html import consolidate_text, insert_paragraph_breaks, normalise_strings, process_special_elements, process_unknown_elements, recursively_prune_elements, remove_blacklist, remove_empty_strings_and_elements, remove_metadata, strip_attributes, structural_elements, unnest_paragraphs, unwrap_elements, wrap_bare_text


def simple_tree_from_html_string(html):
    """Turn input HTML into a cleaned parsed tree."""
    # Insert space into non-spaced comments so that html5lib can interpret them correctly
    html = html.replace("<!---->", "<!-- -->")

    # Convert the HTML into a Soup parse tree
    soup = BeautifulSoup(html, "html5lib")

    # Remove comments, CDATA (which is converted to comments) and DOCTYPE
    remove_metadata(soup)

    # Strip tag attributes apart from 'class' and 'style'
    strip_attributes(soup)

    # Remove blacklisted elements
    remove_blacklist(soup)

    # Unwrap elements where we want to keep the text but drop the containing tag
    unwrap_elements(soup)

    # Process elements with special innerText handling
    process_special_elements(soup)

    # Process unknown elements
    process_unknown_elements(soup)

    # Consolidate text, joining any consecutive NavigableStrings together.
    # Must come before any whitespace operations (eg. remove_empty_strings_and_elements or normalise_strings)
    consolidate_text(soup)

    # Remove empty string elements
    remove_empty_strings_and_elements(soup)

    # Split out block-level elements illegally contained inside paragraphs
    unnest_paragraphs(soup)

    # Replace <br> and <hr> elements with paragraph breaks
    # Must come after remove_empty_strings_and_elements so that consecutive <br>s can be identified
    # Re-consolidates strings at the end, so must come before normalise_strings
    insert_paragraph_breaks(soup)

    # Wrap any remaining bare text in a suitable block level element
    # Must come after consolidate_text and identify_and_replace_break_elements
    # otherwise there may be multiple strings inside a <p> tag which would create nested <p>s
    wrap_bare_text(soup)

    # Normalise all strings, removing whitespace and fixing unicode issues
    # Must come after consolidate_text and insert_paragraph_breaks which join
    # strings with semantic whitespace
    normalise_strings(soup)

    # Recursively replace any elements which have no children or only zero-length children
    recursively_prune_elements(soup)

    # Finally ensure that the whole tree is wrapped in a div
    # Strip out enclosing elements that cannot live inside a div
    while soup.contents and (soup.contents[0].name in structural_elements()):
        soup.contents[0].unwrap()
    # If the outermost tag is a single div then return it
    if len(soup.contents) == 1 and soup.contents[0].name == "div":
        return soup

    # ... otherwise wrap in a div and return that
    root = soup.new_tag("div")
    root.append(soup)
    return root
