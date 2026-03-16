from .. import lists
from .. import documents
from .. import results


def read_comments_xml_element(element, body_reader):
    def read_comments_xml_element(element):
        comment_elements = element.find_children("w:comment")
        return results.combine(lists.map(_read_comment_element, comment_elements))


    def _read_comment_element(element):
        def read_optional_attribute(name):
            return element.attributes.get(name, "").strip() or None

        return body_reader.read_all(element.children).map(lambda body:
            documents.comment(
                comment_id=element.attributes["w:id"],
                body=body,
                author_name=read_optional_attribute("w:author"),
                author_initials=read_optional_attribute("w:initials"),
            ))

    return read_comments_xml_element(element)
