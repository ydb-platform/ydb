from collections import defaultdict
import lxml.html
from ..simplifiers import normalise_whitespace


def extract_element(html, xpaths, process_dict_fn=None):
    """Return the relevant elements (titles, dates or bylines) from article HTML, specified by xpaths.
        xpaths should be a list of tuples, each with the xpath and a reliability scores.
        Processing of the dictionary can be handled with the arg function.
        The returned dictionary should have the processed elements as keys and dicts with scores and the xpaths used as values
    """
    # Attempt to parse the html, aborting here if it is not parseable
    try:
        lxml_html = lxml.html.fromstring(html)
    except lxml.etree.ParserError:
        return None

    # Get all elements specified and combine scores
    extracted_strings = defaultdict(dict)
    for extraction_xpath, score in xpaths:
        found_elements = lxml_html.xpath(extraction_xpath)
        found_elements = found_elements if isinstance(found_elements, list) else [found_elements]
        for found_element in found_elements:
            element = normalise_whitespace(found_element)
            if element:
                try:
                    extracted_strings[element]['score'] += score
                    extracted_strings[element]['xpaths'].append(extraction_xpath)
                    extracted_strings[element]['xpaths'].sort()
                except KeyError:
                    extracted_strings[element]['score'] = score
                    extracted_strings[element]['xpaths'] = [extraction_xpath]

    # Edit the dictionary
    if process_dict_fn:
        extracted_strings = process_dict_fn(extracted_strings)

    return extracted_strings
