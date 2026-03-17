from lxml import etree


def xmlstring(root):
    if isinstance(root, str):
        return root
    if hasattr(root, "xmlelement"):
        root = root.xmlelement()
    try:
        return etree.tostring(root, pretty_print=True).decode("utf-8")
    except:
        return root


def printxml(root) -> None:
    print(xmlstring(root))
