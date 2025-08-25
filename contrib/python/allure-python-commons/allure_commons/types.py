from enum import Enum

ALLURE_UNIQUE_LABELS = ['severity', 'thread', 'host']


class Severity(str, Enum):
    BLOCKER = 'blocker'
    CRITICAL = 'critical'
    NORMAL = 'normal'
    MINOR = 'minor'
    TRIVIAL = 'trivial'


class LinkType:
    LINK = 'link'
    ISSUE = 'issue'
    TEST_CASE = 'tms'


class LabelType(str):
    EPIC = 'epic'
    FEATURE = 'feature'
    STORY = 'story'
    PARENT_SUITE = 'parentSuite'
    SUITE = 'suite'
    SUB_SUITE = 'subSuite'
    SEVERITY = 'severity'
    THREAD = 'thread'
    HOST = 'host'
    TAG = 'tag'
    ID = 'as_id'
    FRAMEWORK = 'framework'
    LANGUAGE = 'language'
    MANUAL = 'ALLURE_MANUAL'


class AttachmentType(Enum):

    def __init__(self, mime_type, extension):
        self.mime_type = mime_type
        self.extension = extension

    TEXT = ("text/plain", "txt")
    CSV = ("text/csv", "csv")
    TSV = ("text/tab-separated-values", "tsv")
    URI_LIST = ("text/uri-list", "uri")

    HTML = ("text/html", "html")
    XML = ("application/xml", "xml")
    JSON = ("application/json", "json")
    YAML = ("application/yaml", "yaml")
    PCAP = ("application/vnd.tcpdump.pcap", "pcap")

    PNG = ("image/png", "png")
    JPG = ("image/jpg", "jpg")
    SVG = ("image/svg+xml", "svg")
    GIF = ("image/gif", "gif")
    BMP = ("image/bmp", "bmp")
    TIFF = ("image/tiff", "tiff")

    MP4 = ("video/mp4", "mp4")
    OGG = ("video/ogg", "ogg")
    WEBM = ("video/webm", "webm")

    PDF = ("application/pdf", "pdf")


class ParameterMode(Enum):
    HIDDEN = 'hidden'
    MASKED = 'masked'
    DEFAULT = None
