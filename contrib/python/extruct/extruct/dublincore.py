# mypy: disallow_untyped_defs=False
import re

from w3lib.html import strip_html5_whitespace

from extruct.utils import parse_html

_DC_ELEMENTS = (
    {  # Defined according DCMES(DCM Version 1.1): http://dublincore.org/documents/dces/
        "contributor": "http://purl.org/dc/elements/1.1/contributor",
        "coverage": "http://purl.org/dc/elements/1.1/coverage",
        "creator": "http://purl.org/dc/elements/1.1/creator",
        "date": "http://purl.org/dc/elements/1.1/date",
        "description": "http://purl.org/dc/elements/1.1/description",
        "format": "http://purl.org/dc/elements/1.1/format",
        "identifier": "http://purl.org/dc/elements/1.1/identifier",
        "language": "http://purl.org/dc/elements/1.1/language",
        "publisher": "http://purl.org/dc/elements/1.1/publisher",
        "relation": "http://purl.org/dc/elements/1.1/relation",
        "rights": "http://purl.org/dc/elements/1.1/rights",
        "source": "http://purl.org/dc/elements/1.1/source",
        "subject": "http://purl.org/dc/elements/1.1/subject",
        "title": "http://purl.org/dc/elements/1.1/title",
        "type": "http://purl.org/dc/elements/1.1/type",
    }
)

_DC_TERMS = {  # Defined according: http://dublincore.org/documents/2008/01/14/dcmi-terms/
    "abstract": "http://purl.org/dc/terms/abstract",
    "description": "http://purl.org/dc/terms/description",
    "accessrights": "http://purl.org/dc/terms/accessRights",
    "rights": "http://purl.org/dc/terms/rights",
    "rightsstatement": "http://purl.org/dc/terms/RightsStatement",
    "accrualmethod": "http://purl.org/dc/terms/accrualMethod",
    "collection": "http://purl.org/dc/terms/Collection",
    "methodOfaccrual": "http://purl.org/dc/terms/MethodOfAccrual",
    "accrualperiodicity": "http://purl.org/dc/terms/accrualPeriodicity",
    "frequency": "http://purl.org/dc/terms/Frequency",
    "accrualpolicy": "http://purl.org/dc/terms/accrualPolicy",
    "policy": "http://purl.org/dc/terms/Policy",
    "alternative": "http://purl.org/dc/terms/alternative",
    "title": "http://purl.org/dc/terms/title",
    "audience": "http://purl.org/dc/terms/audience",
    "agentclass": "http://purl.org/dc/terms/AgentClass",
    "available": "http://purl.org/dc/terms/available",
    "date": "http://purl.org/dc/terms/date",
    "bibliographiccitation": "http://purl.org/dc/terms/bibliographicCitation",
    "identifier": "http://purl.org/dc/terms/identifier",
    "bibliographicresource": "http://purl.org/dc/terms/BibliographicResource",
    "conformsto": "http://purl.org/dc/terms/conformsTo",
    "relation": "http://purl.org/dc/terms/relation",
    "standard": "http://purl.org/dc/terms/Standard",
    "contributor": "http://purl.org/dc/terms/contributor",
    "agent": "http://purl.org/dc/terms/Agent",
    "coverage": "http://purl.org/dc/terms/coverage",
    "locationperiodorjurisdiction": "http://purl.org/dc/terms/LocationPeriodOrJurisdiction",
    "created": "http://purl.org/dc/terms/created",
    "creator": "http://purl.org/dc/terms/creator",
    "dateaccepted": "http://purl.org/dc/terms/dateAccepted",
    "datecopyrighted": "http://purl.org/dc/terms/dateCopyrighted",
    "datesubmitted": "http://purl.org/dc/terms/dateSubmitted",
    "educationlevel": "http://purl.org/dc/terms/educationLevel",
    "extent": "http://purl.org/dc/terms/extent",
    "format": "http://purl.org/dc/terms/format",
    "sizeorduration": "http://purl.org/dc/terms/SizeOrDuration",
    "mediatypeorextent": "http://purl.org/dc/terms/MediaTypeOrExtent",
    "hasformat": "http://purl.org/dc/terms/hasFormat",
    "haspart": "http://purl.org/dc/terms/hasPart",
    "hasversion": "http://purl.org/dc/terms/hasVersion",
    "instructionalmethod": "http://purl.org/dc/terms/instructionalMethod",
    "methodofinstruction": "http://purl.org/dc/terms/MethodOfInstruction",
    "isformatof": "http://purl.org/dc/terms/isFormatOf",
    "ispartof": "http://purl.org/dc/terms/isPartOf",
    "isreferencedby": "http://purl.org/dc/terms/isReferencedBy",
    "isreplacedby": "http://purl.org/dc/terms/isReplacedBy",
    "isrequiredby": "http://purl.org/dc/terms/isRequiredBy",
    "issued": "http://purl.org/dc/terms/issued",
    "isversionof": "http://purl.org/dc/terms/isVersionOf",
    "language": "http://purl.org/dc/terms/language",
    "linguisticsystem": "http://purl.org/dc/terms/LinguisticSystem",
    "license": "http://purl.org/dc/terms/license",
    "licensedocument": "http://purl.org/dc/terms/LicenseDocument",
    "mediator": "http://purl.org/dc/terms/mediator",
    "medium": "http://purl.org/dc/terms/medium",
    "physicalresource": "http://purl.org/dc/terms/PhysicalResource",
    "physicalmedium": "http://purl.org/dc/terms/PhysicalMedium",
    "modified": "http://purl.org/dc/terms/modified",
    "provenance": "http://purl.org/dc/terms/provenance",
    "provenancestatement": "http://purl.org/dc/terms/ProvenanceStatement",
    "publisher": "http://purl.org/dc/terms/publisher",
    "references": "http://purl.org/dc/terms/references",
    "replaces": "http://purl.org/dc/terms/replaces",
    "requires": "http://purl.org/dc/terms/requires",
    "rightsholder": "http://purl.org/dc/terms/rightsHolder",
    "source": "http://purl.org/dc/terms/source",
    "spatial": "http://purl.org/dc/terms/spatial",
    "location": "http://purl.org/dc/terms/Location",
    "subject": "http://purl.org/dc/terms/subject",
    "tableofcontents": "http://purl.org/dc/terms/tableOfContents",
    "temporal": "http://purl.org/dc/terms/temporal",
    "periodoftime": "http://purl.org/dc/terms/PeriodOfTime",
    "type": "http://purl.org/dc/terms/type",
    "valid": "http://purl.org/dc/terms/valid",
}

_URL_NAMESPACES = ["http://purl.org/dc/terms/", "http://purl.org/dc/elements/1.1/"]


def get_lower_attrib(name):
    # get attribute to compare against _DC_TERMS or _DC_ELEMENTS
    return re.sub(r".*\.", "", name).lower()


class DublinCoreExtractor:
    """DublinCore extractor following extruct API."""

    def extract(self, htmlstring, base_url=None, encoding="UTF-8"):
        tree = parse_html(htmlstring, encoding=encoding)
        return list(self.extract_items(tree, base_url=base_url))

    def extract_items(self, document, base_url=None):
        elements = []
        terms = []

        def attrib_to_dict(attribs):
            # convert _attrib type to dict
            return dict(attribs.items())

        def populate_results(node, main_attrib):
            # fill list with DC Elements or DC Terms
            node_attrib = node.attrib
            if main_attrib not in node_attrib:
                return

            name = node.attrib[main_attrib]
            lower_name = get_lower_attrib(name)
            if lower_name in _DC_ELEMENTS:
                node.attrib.update({"URI": _DC_ELEMENTS[lower_name]})
                elements.append(attrib_to_dict(node.attrib))

            elif lower_name in _DC_TERMS:
                node.attrib.update({"URI": _DC_TERMS[lower_name]})
                terms.append(attrib_to_dict(node.attrib))

        namespaces_nodes = document.xpath('//link[contains(@rel,"schema")]')
        namespaces = {}
        for i in namespaces_nodes:
            url = strip_html5_whitespace(i.attrib["href"])
            if url in _URL_NAMESPACES:
                namespaces.update({re.sub(r"schema\.", "", i.attrib["rel"]): url})

        list_meta_node = document.xpath("//meta")
        for meta_node in list_meta_node:
            populate_results(meta_node, "name")

        list_link_node = document.xpath("//link")
        for link_node in list_link_node:
            populate_results(link_node, "rel")

        yield {"namespaces": namespaces, "elements": elements, "terms": terms}
