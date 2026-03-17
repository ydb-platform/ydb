import functools

from .. import lists
from .. import documents
from .. import results


def _read_notes(note_type, element, body_reader):
    def read_notes_xml_element(element):
        note_elements = lists.filter(
            _is_note_element,
            element.find_children("w:" + note_type),
        )
        return results.combine(lists.map(_read_note_element, note_elements))


    def _is_note_element(element):
        return element.attributes.get("w:type") not in ["continuationSeparator", "separator"]


    def _read_note_element(element):
        return body_reader.read_all(element.children).map(lambda body: 
            documents.note(
                note_type=note_type,
                note_id=element.attributes["w:id"],
                body=body
            ))
    
    return read_notes_xml_element(element)

read_footnotes_xml_element = functools.partial(_read_notes, "footnote")
read_endnotes_xml_element = functools.partial(_read_notes, "endnote")
