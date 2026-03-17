from . import documents


def extract_raw_text_from_element(element):
    if isinstance(element, documents.Text):
        return element.value
    elif isinstance(element, documents.Tab):
        return "\t"
    else:
        text = "".join(map(extract_raw_text_from_element, getattr(element, "children", [])))
        if isinstance(element, documents.Paragraph):
            return text + "\n\n"
        else:
            return text
