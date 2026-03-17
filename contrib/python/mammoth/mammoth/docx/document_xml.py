from .. import documents


def read_document_xml_element(
        element,
        body_reader,
        notes=None,
        comments=None):

    if notes is None:
        notes = []
    if comments is None:
        comments = []

    body_element = element.find_child("w:body")

    if body_element is None:
        raise ValueError("Could not find the body element: are you sure this is a docx file?")

    return body_reader.read_all(body_element.children) \
        .map(lambda children: documents.document(
            children,
            notes=documents.notes(notes),
            comments=comments
        ))
