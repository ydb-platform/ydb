# encoding: utf-8

"""Constant values related to the Open Packaging Convention.

In particular, this includes content (MIME) types and relationship types.
"""


class CONTENT_TYPE(object):
    """Content type URIs (like MIME-types) that specify a part's format."""

    ASF = "video/x-ms-asf"
    AVI = "video/avi"
    BMP = "image/bmp"
    DML_CHART = "application/vnd.openxmlformats-officedocument.drawingml.chart+xml"
    DML_CHARTSHAPES = (
        "application/vnd.openxmlformats-officedocument.drawingml.chartshapes+xml"
    )
    DML_DIAGRAM_COLORS = (
        "application/vnd.openxmlformats-officedocument.drawingml.diagramColors+xml"
    )
    DML_DIAGRAM_DATA = (
        "application/vnd.openxmlformats-officedocument.drawingml.diagramData+xml"
    )
    DML_DIAGRAM_DRAWING = "application/vnd.ms-office.drawingml.diagramDrawing+xml"
    DML_DIAGRAM_LAYOUT = (
        "application/vnd.openxmlformats-officedocument.drawingml.diagramLayout+xml"
    )
    DML_DIAGRAM_STYLE = (
        "application/vnd.openxmlformats-officedocument.drawingml.diagramStyle+xml"
    )
    GIF = "image/gif"
    INK = "application/inkml+xml"
    JPEG = "image/jpeg"
    MOV = "video/quicktime"
    MP4 = "video/mp4"
    MPG = "video/mpeg"
    MS_PHOTO = "image/vnd.ms-photo"
    MS_VIDEO = "video/msvideo"
    OFC_CHART_COLORS = "application/vnd.ms-office.chartcolorstyle+xml"
    OFC_CHART_EX = "application/vnd.ms-office.chartex+xml"
    OFC_CHART_STYLE = "application/vnd.ms-office.chartstyle+xml"
    OFC_CUSTOM_PROPERTIES = (
        "application/vnd.openxmlformats-officedocument.custom-properties+xml"
    )
    OFC_CUSTOM_XML_PROPERTIES = (
        "application/vnd.openxmlformats-officedocument.customXmlProperties+xml"
    )
    OFC_DRAWING = "application/vnd.openxmlformats-officedocument.drawing+xml"
    OFC_EXTENDED_PROPERTIES = (
        "application/vnd.openxmlformats-officedocument.extended-properties+xml"
    )
    OFC_OLE_OBJECT = "application/vnd.openxmlformats-officedocument.oleObject"
    OFC_PACKAGE = "application/vnd.openxmlformats-officedocument.package"
    OFC_THEME = "application/vnd.openxmlformats-officedocument.theme+xml"
    OFC_THEME_OVERRIDE = (
        "application/vnd.openxmlformats-officedocument.themeOverride+xml"
    )
    OFC_VML_DRAWING = "application/vnd.openxmlformats-officedocument.vmlDrawing"
    OPC_CORE_PROPERTIES = "application/vnd.openxmlformats-package.core-properties+xml"
    OPC_DIGITAL_SIGNATURE_CERTIFICATE = (
        "application/vnd.openxmlformats-package.digital-signature-certificate"
    )
    OPC_DIGITAL_SIGNATURE_ORIGIN = (
        "application/vnd.openxmlformats-package.digital-signature-origin"
    )
    OPC_DIGITAL_SIGNATURE_XMLSIGNATURE = (
        "application/vnd.openxmlformats-package.digital-signature-xmlsignature+xml"
    )
    OPC_RELATIONSHIPS = "application/vnd.openxmlformats-package.relationships+xml"
    PML_COMMENTS = (
        "application/vnd.openxmlformats-officedocument.presentationml.comments+xml"
    )
    PML_COMMENT_AUTHORS = (
        "application/vnd.openxmlformats-officedocument.presentationml.commen"
        "tAuthors+xml"
    )
    PML_HANDOUT_MASTER = (
        "application/vnd.openxmlformats-officedocument.presentationml.handou"
        "tMaster+xml"
    )
    PML_NOTES_MASTER = (
        "application/vnd.openxmlformats-officedocument.presentationml.notesM"
        "aster+xml"
    )
    PML_NOTES_SLIDE = (
        "application/vnd.openxmlformats-officedocument.presentationml.notesSlide+xml"
    )
    PML_PRESENTATION = (
        "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    )
    PML_PRESENTATION_MAIN = (
        "application/vnd.openxmlformats-officedocument.presentationml.presentation.ma"
        "in+xml"
    )
    PML_PRES_MACRO_MAIN = (
        "application/vnd.ms-powerpoint.presentation.macroEnabled.main+xml"
    )
    PML_PRES_PROPS = (
        "application/vnd.openxmlformats-officedocument.presentationml.presProps+xml"
    )
    PML_PRINTER_SETTINGS = (
        "application/vnd.openxmlformats-officedocument.presentationml.printerSettings"
    )
    PML_SLIDE = "application/vnd.openxmlformats-officedocument.presentationml.slide+xml"
    PML_SLIDESHOW_MAIN = (
        "application/vnd.openxmlformats-officedocument.presentationml.slideshow.main+"
        "xml"
    )
    PML_SLIDE_LAYOUT = (
        "application/vnd.openxmlformats-officedocument.presentationml.slideLayout+xml"
    )
    PML_SLIDE_MASTER = (
        "application/vnd.openxmlformats-officedocument.presentationml.slideMaster+xml"
    )
    PML_SLIDE_UPDATE_INFO = (
        "application/vnd.openxmlformats-officedocument.presentationml.slideUpdateInfo"
        "+xml"
    )
    PML_TABLE_STYLES = (
        "application/vnd.openxmlformats-officedocument.presentationml.tableStyles+xml"
    )
    PML_TAGS = "application/vnd.openxmlformats-officedocument.presentationml.tags+xml"
    PML_TEMPLATE_MAIN = (
        "application/vnd.openxmlformats-officedocument.presentationml.template.main+x"
        "ml"
    )
    PML_VIEW_PROPS = (
        "application/vnd.openxmlformats-officedocument.presentationml.viewProps+xml"
    )
    PNG = "image/png"
    SML_CALC_CHAIN = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.calcChain+xml"
    )
    SML_CHARTSHEET = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.chartsheet+xml"
    )
    SML_COMMENTS = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.comments+xml"
    )
    SML_CONNECTIONS = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.connections+xml"
    )
    SML_CUSTOM_PROPERTY = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.customProperty"
    )
    SML_DIALOGSHEET = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.dialogsheet+xml"
    )
    SML_EXTERNAL_LINK = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.externalLink+xml"
    )
    SML_PIVOT_CACHE_DEFINITION = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.pivotCacheDefini"
        "tion+xml"
    )
    SML_PIVOT_CACHE_RECORDS = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.pivotCacheRecord"
        "s+xml"
    )
    SML_PIVOT_TABLE = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.pivotTable+xml"
    )
    SML_PRINTER_SETTINGS = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.printerSettings"
    )
    SML_QUERY_TABLE = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.queryTable+xml"
    )
    SML_REVISION_HEADERS = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.revisionHeaders+"
        "xml"
    )
    SML_REVISION_LOG = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.revisionLog+xml"
    )
    SML_SHARED_STRINGS = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"
    )
    SML_SHEET = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    SML_SHEET_MAIN = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"
    )
    SML_SHEET_METADATA = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheetMetadata+xml"
    )
    SML_STYLES = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"
    )
    SML_TABLE = "application/vnd.openxmlformats-officedocument.spreadsheetml.table+xml"
    SML_TABLE_SINGLE_CELLS = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.tableSingleCells"
        "+xml"
    )
    SML_TEMPLATE_MAIN = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.template.main+xml"
    )
    SML_USER_NAMES = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.userNames+xml"
    )
    SML_VOLATILE_DEPENDENCIES = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.volatileDependen"
        "cies+xml"
    )
    SML_WORKSHEET = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"
    )
    SWF = "application/x-shockwave-flash"
    TIFF = "image/tiff"
    VIDEO = "video/unknown"
    WML_COMMENTS = (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.comments+xml"
    )
    WML_DOCUMENT = (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    )
    WML_DOCUMENT_GLOSSARY = (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document.glos"
        "sary+xml"
    )
    WML_DOCUMENT_MAIN = (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document.main"
        "+xml"
    )
    WML_ENDNOTES = (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.endnotes+xml"
    )
    WML_FONT_TABLE = (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.fontTable+xml"
    )
    WML_FOOTER = (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.footer+xml"
    )
    WML_FOOTNOTES = (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.foot"
        "notes+xml"
    )
    WML_HEADER = (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.header+xml"
    )
    WML_NUMBERING = (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.numbering+xml"
    )
    WML_PRINTER_SETTINGS = (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.printerSettin"
        "gs"
    )
    WML_SETTINGS = (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.settings+xml"
    )
    WML_STYLES = (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml"
    )
    WML_WEB_SETTINGS = (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.webSettings+x"
        "ml"
    )
    WMV = "video/x-ms-wmv"
    XML = "application/xml"
    X_EMF = "image/x-emf"
    X_FONTDATA = "application/x-fontdata"
    X_FONT_TTF = "application/x-font-ttf"
    X_MS_VIDEO = "video/x-msvideo"
    X_WMF = "image/x-wmf"


class NAMESPACE(object):
    """Constant values for OPC XML namespaces"""

    DML_WORDPROCESSING_DRAWING = (
        "http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing"
    )
    OFC_RELATIONSHIPS = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
    )
    OPC_RELATIONSHIPS = "http://schemas.openxmlformats.org/package/2006/relationships"
    OPC_CONTENT_TYPES = "http://schemas.openxmlformats.org/package/2006/content-types"
    WML_MAIN = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"


class RELATIONSHIP_TARGET_MODE(object):
    """Open XML relationship target modes"""

    EXTERNAL = "External"
    INTERNAL = "Internal"


class RELATIONSHIP_TYPE(object):
    AUDIO = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/audio"
    A_F_CHUNK = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/aFChunk"
    )
    CALC_CHAIN = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/calcChain"
    )
    CERTIFICATE = (
        "http://schemas.openxmlformats.org/package/2006/relationships/digital-signatu"
        "re/certificate"
    )
    CHART = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/chart"
    CHARTSHEET = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/chartsheet"
    )
    CHART_COLOR_STYLE = (
        "http://schemas.microsoft.com/office/2011/relationships/chartColorStyle"
    )
    CHART_USER_SHAPES = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/chartUse"
        "rShapes"
    )
    COMMENTS = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/comments"
    )
    COMMENT_AUTHORS = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/commentA"
        "uthors"
    )
    CONNECTIONS = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/connecti"
        "ons"
    )
    CONTROL = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/control"
    )
    CORE_PROPERTIES = (
        "http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-p"
        "roperties"
    )
    CUSTOM_PROPERTIES = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/custom-p"
        "roperties"
    )
    CUSTOM_PROPERTY = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
        "/customProperty"
    )
    CUSTOM_XML = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/customXml"
    )
    CUSTOM_XML_PROPS = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/customXm"
        "lProps"
    )
    DIAGRAM_COLORS = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/diagramC"
        "olors"
    )
    DIAGRAM_DATA = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/diagramD"
        "ata"
    )
    DIAGRAM_LAYOUT = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/diagramL"
        "ayout"
    )
    DIAGRAM_QUICK_STYLE = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/diagramQ"
        "uickStyle"
    )
    DIALOGSHEET = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/dialogsh"
        "eet"
    )
    DRAWING = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/drawing"
    )
    ENDNOTES = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/endnotes"
    )
    EXTENDED_PROPERTIES = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended"
        "-properties"
    )
    EXTERNAL_LINK = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/external"
        "Link"
    )
    FONT = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/font"
    FONT_TABLE = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/fontTable"
    )
    FOOTER = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/footer"
    )
    FOOTNOTES = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/footnotes"
    )
    GLOSSARY_DOCUMENT = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/glossary"
        "Document"
    )
    HANDOUT_MASTER = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/handoutM"
        "aster"
    )
    HEADER = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/header"
    )
    HYPERLINK = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/hyperlin"
        "k"
    )
    IMAGE = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/image"
    MEDIA = "http://schemas.microsoft.com/office/2007/relationships/media"
    NOTES_MASTER = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/notesMas"
        "ter"
    )
    NOTES_SLIDE = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/notesSli"
        "de"
    )
    NUMBERING = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/numberin"
        "g"
    )
    OFFICE_DOCUMENT = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDo"
        "cument"
    )
    OLE_OBJECT = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/oleObjec"
        "t"
    )
    ORIGIN = (
        "http://schemas.openxmlformats.org/package/2006/relationships/digital-signatu"
        "re/origin"
    )
    PACKAGE = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/package"
    )
    PIVOT_CACHE_DEFINITION = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/pivotCac"
        "heDefinition"
    )
    PIVOT_CACHE_RECORDS = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/spreadsh"
        "eetml/pivotCacheRecords"
    )
    PIVOT_TABLE = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/pivotTab"
        "le"
    )
    PRES_PROPS = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/presProp"
        "s"
    )
    PRINTER_SETTINGS = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/printerS"
        "ettings"
    )
    QUERY_TABLE = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/queryTab"
        "le"
    )
    REVISION_HEADERS = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/revision"
        "Headers"
    )
    REVISION_LOG = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/revision"
        "Log"
    )
    SETTINGS = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/settings"
    )
    SHARED_STRINGS = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedSt"
        "rings"
    )
    SHEET_METADATA = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/sheetMet"
        "adata"
    )
    SIGNATURE = (
        "http://schemas.openxmlformats.org/package/2006/relationships/digital-signatu"
        "re/signature"
    )
    SLIDE = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/slide"
    SLIDE_LAYOUT = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/slideLay"
        "out"
    )
    SLIDE_MASTER = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/slideMas"
        "ter"
    )
    SLIDE_UPDATE_INFO = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/slideUpd"
        "ateInfo"
    )
    STYLES = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles"
    )
    TABLE = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/table"
    TABLE_SINGLE_CELLS = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/tableSin"
        "gleCells"
    )
    TABLE_STYLES = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/tableSty"
        "les"
    )
    TAGS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/tags"
    THEME = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/theme"
    THEME_OVERRIDE = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/themeOve"
        "rride"
    )
    THUMBNAIL = (
        "http://schemas.openxmlformats.org/package/2006/relationships/metadata/thumbn"
        "ail"
    )
    USERNAMES = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/username"
        "s"
    )
    VIDEO = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/video"
    VIEW_PROPS = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/viewProp"
        "s"
    )
    VML_DRAWING = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/vmlDrawi"
        "ng"
    )
    VOLATILE_DEPENDENCIES = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/volatile"
        "Dependencies"
    )
    WEB_SETTINGS = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/webSetti"
        "ngs"
    )
    WORKSHEET_SOURCE = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/workshee"
        "tSource"
    )
    XML_MAPS = (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/xmlMaps"
    )
