"""Enumerations used by shapes and related objects."""

from __future__ import annotations

import enum

from pptx.enum.base import BaseEnum, BaseXmlEnum


class MSO_AUTO_SHAPE_TYPE(BaseXmlEnum):
    """Specifies a type of AutoShape, e.g. DOWN_ARROW.

    Alias: ``MSO_SHAPE``

    Example::

        from pptx.enum.shapes import MSO_SHAPE
        from pptx.util import Inches

        left = top = width = height = Inches(1.0)
        slide.shapes.add_shape(
            MSO_SHAPE.ROUNDED_RECTANGLE, left, top, width, height
        )

    MS API Name: `MsoAutoShapeType`

    https://learn.microsoft.com/en-us/office/vba/api/Office.MsoAutoShapeType
    """

    ACTION_BUTTON_BACK_OR_PREVIOUS = (
        129,
        "actionButtonBackPrevious",
        "Back or Previous button. Supports mouse-click and mouse-over actions",
    )
    """Back or Previous button. Supports mouse-click and mouse-over actions"""

    ACTION_BUTTON_BEGINNING = (
        131,
        "actionButtonBeginning",
        "Beginning button. Supports mouse-click and mouse-over actions",
    )
    """Beginning button. Supports mouse-click and mouse-over actions"""

    ACTION_BUTTON_CUSTOM = (
        125,
        "actionButtonBlank",
        "Button with no default picture or text. Supports mouse-click and mouse-over actions",
    )
    """Button with no default picture or text. Supports mouse-click and mouse-over actions"""

    ACTION_BUTTON_DOCUMENT = (
        134,
        "actionButtonDocument",
        "Document button. Supports mouse-click and mouse-over actions",
    )
    """Document button. Supports mouse-click and mouse-over actions"""

    ACTION_BUTTON_END = (
        132,
        "actionButtonEnd",
        "End button. Supports mouse-click and mouse-over actions",
    )
    """End button. Supports mouse-click and mouse-over actions"""

    ACTION_BUTTON_FORWARD_OR_NEXT = (
        130,
        "actionButtonForwardNext",
        "Forward or Next button. Supports mouse-click and mouse-over actions",
    )
    """Forward or Next button. Supports mouse-click and mouse-over actions"""

    ACTION_BUTTON_HELP = (
        127,
        "actionButtonHelp",
        "Help button. Supports mouse-click and mouse-over actions",
    )
    """Help button. Supports mouse-click and mouse-over actions"""

    ACTION_BUTTON_HOME = (
        126,
        "actionButtonHome",
        "Home button. Supports mouse-click and mouse-over actions",
    )
    """Home button. Supports mouse-click and mouse-over actions"""

    ACTION_BUTTON_INFORMATION = (
        128,
        "actionButtonInformation",
        "Information button. Supports mouse-click and mouse-over actions",
    )
    """Information button. Supports mouse-click and mouse-over actions"""

    ACTION_BUTTON_MOVIE = (
        136,
        "actionButtonMovie",
        "Movie button. Supports mouse-click and mouse-over actions",
    )
    """Movie button. Supports mouse-click and mouse-over actions"""

    ACTION_BUTTON_RETURN = (
        133,
        "actionButtonReturn",
        "Return button. Supports mouse-click and mouse-over actions",
    )
    """Return button. Supports mouse-click and mouse-over actions"""

    ACTION_BUTTON_SOUND = (
        135,
        "actionButtonSound",
        "Sound button. Supports mouse-click and mouse-over actions",
    )
    """Sound button. Supports mouse-click and mouse-over actions"""

    ARC = (25, "arc", "Arc")
    """Arc"""

    BALLOON = (137, "wedgeRoundRectCallout", "Rounded Rectangular Callout")
    """Rounded Rectangular Callout"""

    BENT_ARROW = (41, "bentArrow", "Block arrow that follows a curved 90-degree angle")
    """Block arrow that follows a curved 90-degree angle"""

    BENT_UP_ARROW = (
        44,
        "bentUpArrow",
        "Block arrow that follows a sharp 90-degree angle. Points up by default",
    )
    """Block arrow that follows a sharp 90-degree angle. Points up by default"""

    BEVEL = (15, "bevel", "Bevel")
    """Bevel"""

    BLOCK_ARC = (20, "blockArc", "Block arc")
    """Block arc"""

    CAN = (13, "can", "Can")
    """Can"""

    CHART_PLUS = (182, "chartPlus", "Chart Plus")
    """Chart Plus"""

    CHART_STAR = (181, "chartStar", "Chart Star")
    """Chart Star"""

    CHART_X = (180, "chartX", "Chart X")
    """Chart X"""

    CHEVRON = (52, "chevron", "Chevron")
    """Chevron"""

    CHORD = (161, "chord", "Geometric chord shape")
    """Geometric chord shape"""

    CIRCULAR_ARROW = (60, "circularArrow", "Block arrow that follows a curved 180-degree angle")
    """Block arrow that follows a curved 180-degree angle"""

    CLOUD = (179, "cloud", "Cloud")
    """Cloud"""

    CLOUD_CALLOUT = (108, "cloudCallout", "Cloud callout")
    """Cloud callout"""

    CORNER = (162, "corner", "Corner")
    """Corner"""

    CORNER_TABS = (169, "cornerTabs", "Corner Tabs")
    """Corner Tabs"""

    CROSS = (11, "plus", "Cross")
    """Cross"""

    CUBE = (14, "cube", "Cube")
    """Cube"""

    CURVED_DOWN_ARROW = (48, "curvedDownArrow", "Block arrow that curves down")
    """Block arrow that curves down"""

    CURVED_DOWN_RIBBON = (100, "ellipseRibbon", "Ribbon banner that curves down")
    """Ribbon banner that curves down"""

    CURVED_LEFT_ARROW = (46, "curvedLeftArrow", "Block arrow that curves left")
    """Block arrow that curves left"""

    CURVED_RIGHT_ARROW = (45, "curvedRightArrow", "Block arrow that curves right")
    """Block arrow that curves right"""

    CURVED_UP_ARROW = (47, "curvedUpArrow", "Block arrow that curves up")
    """Block arrow that curves up"""

    CURVED_UP_RIBBON = (99, "ellipseRibbon2", "Ribbon banner that curves up")
    """Ribbon banner that curves up"""

    DECAGON = (144, "decagon", "Decagon")
    """Decagon"""

    DIAGONAL_STRIPE = (141, "diagStripe", "Diagonal Stripe")
    """Diagonal Stripe"""

    DIAMOND = (4, "diamond", "Diamond")
    """Diamond"""

    DODECAGON = (146, "dodecagon", "Dodecagon")
    """Dodecagon"""

    DONUT = (18, "donut", "Donut")
    """Donut"""

    DOUBLE_BRACE = (27, "bracePair", "Double brace")
    """Double brace"""

    DOUBLE_BRACKET = (26, "bracketPair", "Double bracket")
    """Double bracket"""

    DOUBLE_WAVE = (104, "doubleWave", "Double wave")
    """Double wave"""

    DOWN_ARROW = (36, "downArrow", "Block arrow that points down")
    """Block arrow that points down"""

    DOWN_ARROW_CALLOUT = (56, "downArrowCallout", "Callout with arrow that points down")
    """Callout with arrow that points down"""

    DOWN_RIBBON = (98, "ribbon", "Ribbon banner with center area below ribbon ends")
    """Ribbon banner with center area below ribbon ends"""

    EXPLOSION1 = (89, "irregularSeal1", "Explosion")
    """Explosion"""

    EXPLOSION2 = (90, "irregularSeal2", "Explosion")
    """Explosion"""

    FLOWCHART_ALTERNATE_PROCESS = (
        62,
        "flowChartAlternateProcess",
        "Alternate process flowchart symbol",
    )
    """Alternate process flowchart symbol"""

    FLOWCHART_CARD = (75, "flowChartPunchedCard", "Card flowchart symbol")
    """Card flowchart symbol"""

    FLOWCHART_COLLATE = (79, "flowChartCollate", "Collate flowchart symbol")
    """Collate flowchart symbol"""

    FLOWCHART_CONNECTOR = (73, "flowChartConnector", "Connector flowchart symbol")
    """Connector flowchart symbol"""

    FLOWCHART_DATA = (64, "flowChartInputOutput", "Data flowchart symbol")
    """Data flowchart symbol"""

    FLOWCHART_DECISION = (63, "flowChartDecision", "Decision flowchart symbol")
    """Decision flowchart symbol"""

    FLOWCHART_DELAY = (84, "flowChartDelay", "Delay flowchart symbol")
    """Delay flowchart symbol"""

    FLOWCHART_DIRECT_ACCESS_STORAGE = (
        87,
        "flowChartMagneticDrum",
        "Direct access storage flowchart symbol",
    )
    """Direct access storage flowchart symbol"""

    FLOWCHART_DISPLAY = (88, "flowChartDisplay", "Display flowchart symbol")
    """Display flowchart symbol"""

    FLOWCHART_DOCUMENT = (67, "flowChartDocument", "Document flowchart symbol")
    """Document flowchart symbol"""

    FLOWCHART_EXTRACT = (81, "flowChartExtract", "Extract flowchart symbol")
    """Extract flowchart symbol"""

    FLOWCHART_INTERNAL_STORAGE = (
        66,
        "flowChartInternalStorage",
        "Internal storage flowchart symbol",
    )
    """Internal storage flowchart symbol"""

    FLOWCHART_MAGNETIC_DISK = (86, "flowChartMagneticDisk", "Magnetic disk flowchart symbol")
    """Magnetic disk flowchart symbol"""

    FLOWCHART_MANUAL_INPUT = (71, "flowChartManualInput", "Manual input flowchart symbol")
    """Manual input flowchart symbol"""

    FLOWCHART_MANUAL_OPERATION = (
        72,
        "flowChartManualOperation",
        "Manual operation flowchart symbol",
    )
    """Manual operation flowchart symbol"""

    FLOWCHART_MERGE = (82, "flowChartMerge", "Merge flowchart symbol")
    """Merge flowchart symbol"""

    FLOWCHART_MULTIDOCUMENT = (68, "flowChartMultidocument", "Multi-document flowchart symbol")
    """Multi-document flowchart symbol"""

    FLOWCHART_OFFLINE_STORAGE = (139, "flowChartOfflineStorage", "Offline Storage")
    """Offline Storage"""

    FLOWCHART_OFFPAGE_CONNECTOR = (
        74,
        "flowChartOffpageConnector",
        "Off-page connector flowchart symbol",
    )
    """Off-page connector flowchart symbol"""

    FLOWCHART_OR = (78, "flowChartOr", '"Or" flowchart symbol')
    """\"Or\" flowchart symbol"""

    FLOWCHART_PREDEFINED_PROCESS = (
        65,
        "flowChartPredefinedProcess",
        "Predefined process flowchart symbol",
    )
    """Predefined process flowchart symbol"""

    FLOWCHART_PREPARATION = (70, "flowChartPreparation", "Preparation flowchart symbol")
    """Preparation flowchart symbol"""

    FLOWCHART_PROCESS = (61, "flowChartProcess", "Process flowchart symbol")
    """Process flowchart symbol"""

    FLOWCHART_PUNCHED_TAPE = (76, "flowChartPunchedTape", "Punched tape flowchart symbol")
    """Punched tape flowchart symbol"""

    FLOWCHART_SEQUENTIAL_ACCESS_STORAGE = (
        85,
        "flowChartMagneticTape",
        "Sequential access storage flowchart symbol",
    )
    """Sequential access storage flowchart symbol"""

    FLOWCHART_SORT = (80, "flowChartSort", "Sort flowchart symbol")
    """Sort flowchart symbol"""

    FLOWCHART_STORED_DATA = (83, "flowChartOnlineStorage", "Stored data flowchart symbol")
    """Stored data flowchart symbol"""

    FLOWCHART_SUMMING_JUNCTION = (
        77,
        "flowChartSummingJunction",
        "Summing junction flowchart symbol",
    )
    """Summing junction flowchart symbol"""

    FLOWCHART_TERMINATOR = (69, "flowChartTerminator", "Terminator flowchart symbol")
    """Terminator flowchart symbol"""

    FOLDED_CORNER = (16, "foldedCorner", "Folded corner")
    """Folded corner"""

    FRAME = (158, "frame", "Frame")
    """Frame"""

    FUNNEL = (174, "funnel", "Funnel")
    """Funnel"""

    GEAR_6 = (172, "gear6", "Gear 6")
    """Gear 6"""

    GEAR_9 = (173, "gear9", "Gear 9")
    """Gear 9"""

    HALF_FRAME = (159, "halfFrame", "Half Frame")
    """Half Frame"""

    HEART = (21, "heart", "Heart")
    """Heart"""

    HEPTAGON = (145, "heptagon", "Heptagon")
    """Heptagon"""

    HEXAGON = (10, "hexagon", "Hexagon")
    """Hexagon"""

    HORIZONTAL_SCROLL = (102, "horizontalScroll", "Horizontal scroll")
    """Horizontal scroll"""

    ISOSCELES_TRIANGLE = (7, "triangle", "Isosceles triangle")
    """Isosceles triangle"""

    LEFT_ARROW = (34, "leftArrow", "Block arrow that points left")
    """Block arrow that points left"""

    LEFT_ARROW_CALLOUT = (54, "leftArrowCallout", "Callout with arrow that points left")
    """Callout with arrow that points left"""

    LEFT_BRACE = (31, "leftBrace", "Left brace")
    """Left brace"""

    LEFT_BRACKET = (29, "leftBracket", "Left bracket")
    """Left bracket"""

    LEFT_CIRCULAR_ARROW = (176, "leftCircularArrow", "Left Circular Arrow")
    """Left Circular Arrow"""

    LEFT_RIGHT_ARROW = (
        37,
        "leftRightArrow",
        "Block arrow with arrowheads that point both left and right",
    )
    """Block arrow with arrowheads that point both left and right"""

    LEFT_RIGHT_ARROW_CALLOUT = (
        57,
        "leftRightArrowCallout",
        "Callout with arrowheads that point both left and right",
    )
    """Callout with arrowheads that point both left and right"""

    LEFT_RIGHT_CIRCULAR_ARROW = (177, "leftRightCircularArrow", "Left Right Circular Arrow")
    """Left Right Circular Arrow"""

    LEFT_RIGHT_RIBBON = (140, "leftRightRibbon", "Left Right Ribbon")
    """Left Right Ribbon"""

    LEFT_RIGHT_UP_ARROW = (
        40,
        "leftRightUpArrow",
        "Block arrow with arrowheads that point left, right, and up",
    )
    """Block arrow with arrowheads that point left, right, and up"""

    LEFT_UP_ARROW = (43, "leftUpArrow", "Block arrow with arrowheads that point left and up")
    """Block arrow with arrowheads that point left and up"""

    LIGHTNING_BOLT = (22, "lightningBolt", "Lightning bolt")
    """Lightning bolt"""

    LINE_CALLOUT_1 = (109, "borderCallout1", "Callout with border and horizontal callout line")
    """Callout with border and horizontal callout line"""

    LINE_CALLOUT_1_ACCENT_BAR = (113, "accentCallout1", "Callout with vertical accent bar")
    """Callout with vertical accent bar"""

    LINE_CALLOUT_1_BORDER_AND_ACCENT_BAR = (
        121,
        "accentBorderCallout1",
        "Callout with border and vertical accent bar",
    )
    """Callout with border and vertical accent bar"""

    LINE_CALLOUT_1_NO_BORDER = (117, "callout1", "Callout with horizontal line")
    """Callout with horizontal line"""

    LINE_CALLOUT_2 = (110, "borderCallout2", "Callout with diagonal straight line")
    """Callout with diagonal straight line"""

    LINE_CALLOUT_2_ACCENT_BAR = (
        114,
        "accentCallout2",
        "Callout with diagonal callout line and accent bar",
    )
    """Callout with diagonal callout line and accent bar"""

    LINE_CALLOUT_2_BORDER_AND_ACCENT_BAR = (
        122,
        "accentBorderCallout2",
        "Callout with border, diagonal straight line, and accent bar",
    )
    """Callout with border, diagonal straight line, and accent bar"""

    LINE_CALLOUT_2_NO_BORDER = (118, "callout2", "Callout with no border and diagonal callout line")
    """Callout with no border and diagonal callout line"""

    LINE_CALLOUT_3 = (111, "borderCallout3", "Callout with angled line")
    """Callout with angled line"""

    LINE_CALLOUT_3_ACCENT_BAR = (
        115,
        "accentCallout3",
        "Callout with angled callout line and accent bar",
    )
    """Callout with angled callout line and accent bar"""

    LINE_CALLOUT_3_BORDER_AND_ACCENT_BAR = (
        123,
        "accentBorderCallout3",
        "Callout with border, angled callout line, and accent bar",
    )
    """Callout with border, angled callout line, and accent bar"""

    LINE_CALLOUT_3_NO_BORDER = (119, "callout3", "Callout with no border and angled callout line")
    """Callout with no border and angled callout line"""

    LINE_CALLOUT_4 = (
        112,
        "borderCallout3",
        "Callout with callout line segments forming a U-shape.",
    )
    """Callout with callout line segments forming a U-shape."""

    LINE_CALLOUT_4_ACCENT_BAR = (
        116,
        "accentCallout3",
        "Callout with accent bar and callout line segments forming a U-shape.",
    )
    """Callout with accent bar and callout line segments forming a U-shape."""

    LINE_CALLOUT_4_BORDER_AND_ACCENT_BAR = (
        124,
        "accentBorderCallout3",
        "Callout with border, accent bar, and callout line segments forming a U-shape.",
    )
    """Callout with border, accent bar, and callout line segments forming a U-shape."""

    LINE_CALLOUT_4_NO_BORDER = (
        120,
        "callout3",
        "Callout with no border and callout line segments forming a U-shape.",
    )
    """Callout with no border and callout line segments forming a U-shape."""

    LINE_INVERSE = (183, "lineInv", "Straight Connector")
    """Straight Connector"""

    MATH_DIVIDE = (166, "mathDivide", "Division")
    """Division"""

    MATH_EQUAL = (167, "mathEqual", "Equal")
    """Equal"""

    MATH_MINUS = (164, "mathMinus", "Minus")
    """Minus"""

    MATH_MULTIPLY = (165, "mathMultiply", "Multiply")
    """Multiply"""

    MATH_NOT_EQUAL = (168, "mathNotEqual", "Not Equal")
    """Not Equal"""

    MATH_PLUS = (163, "mathPlus", "Plus")
    """Plus"""

    MOON = (24, "moon", "Moon")
    """Moon"""

    NON_ISOSCELES_TRAPEZOID = (143, "nonIsoscelesTrapezoid", "Non-isosceles Trapezoid")
    """Non-isosceles Trapezoid"""

    NOTCHED_RIGHT_ARROW = (50, "notchedRightArrow", "Notched block arrow that points right")
    """Notched block arrow that points right"""

    NO_SYMBOL = (19, "noSmoking", "'No' Symbol")
    """'No' Symbol"""

    OCTAGON = (6, "octagon", "Octagon")
    """Octagon"""

    OVAL = (9, "ellipse", "Oval")
    """Oval"""

    OVAL_CALLOUT = (107, "wedgeEllipseCallout", "Oval-shaped callout")
    """Oval-shaped callout"""

    PARALLELOGRAM = (2, "parallelogram", "Parallelogram")
    """Parallelogram"""

    PENTAGON = (51, "homePlate", "Pentagon")
    """Pentagon"""

    PIE = (142, "pie", "Pie")
    """Pie"""

    PIE_WEDGE = (175, "pieWedge", "Pie")
    """Pie"""

    PLAQUE = (28, "plaque", "Plaque")
    """Plaque"""

    PLAQUE_TABS = (171, "plaqueTabs", "Plaque Tabs")
    """Plaque Tabs"""

    QUAD_ARROW = (39, "quadArrow", "Block arrows that point up, down, left, and right")
    """Block arrows that point up, down, left, and right"""

    QUAD_ARROW_CALLOUT = (
        59,
        "quadArrowCallout",
        "Callout with arrows that point up, down, left, and right",
    )
    """Callout with arrows that point up, down, left, and right"""

    RECTANGLE = (1, "rect", "Rectangle")
    """Rectangle"""

    RECTANGULAR_CALLOUT = (105, "wedgeRectCallout", "Rectangular callout")
    """Rectangular callout"""

    REGULAR_PENTAGON = (12, "pentagon", "Pentagon")
    """Pentagon"""

    RIGHT_ARROW = (33, "rightArrow", "Block arrow that points right")
    """Block arrow that points right"""

    RIGHT_ARROW_CALLOUT = (53, "rightArrowCallout", "Callout with arrow that points right")
    """Callout with arrow that points right"""

    RIGHT_BRACE = (32, "rightBrace", "Right brace")
    """Right brace"""

    RIGHT_BRACKET = (30, "rightBracket", "Right bracket")
    """Right bracket"""

    RIGHT_TRIANGLE = (8, "rtTriangle", "Right triangle")
    """Right triangle"""

    ROUNDED_RECTANGLE = (5, "roundRect", "Rounded rectangle")
    """Rounded rectangle"""

    ROUNDED_RECTANGULAR_CALLOUT = (106, "wedgeRoundRectCallout", "Rounded rectangle-shaped callout")
    """Rounded rectangle-shaped callout"""

    ROUND_1_RECTANGLE = (151, "round1Rect", "Round Single Corner Rectangle")
    """Round Single Corner Rectangle"""

    ROUND_2_DIAG_RECTANGLE = (153, "round2DiagRect", "Round Diagonal Corner Rectangle")
    """Round Diagonal Corner Rectangle"""

    ROUND_2_SAME_RECTANGLE = (152, "round2SameRect", "Round Same Side Corner Rectangle")
    """Round Same Side Corner Rectangle"""

    SMILEY_FACE = (17, "smileyFace", "Smiley face")
    """Smiley face"""

    SNIP_1_RECTANGLE = (155, "snip1Rect", "Snip Single Corner Rectangle")
    """Snip Single Corner Rectangle"""

    SNIP_2_DIAG_RECTANGLE = (157, "snip2DiagRect", "Snip Diagonal Corner Rectangle")
    """Snip Diagonal Corner Rectangle"""

    SNIP_2_SAME_RECTANGLE = (156, "snip2SameRect", "Snip Same Side Corner Rectangle")
    """Snip Same Side Corner Rectangle"""

    SNIP_ROUND_RECTANGLE = (154, "snipRoundRect", "Snip and Round Single Corner Rectangle")
    """Snip and Round Single Corner Rectangle"""

    SQUARE_TABS = (170, "squareTabs", "Square Tabs")
    """Square Tabs"""

    STAR_10_POINT = (149, "star10", "10-Point Star")
    """10-Point Star"""

    STAR_12_POINT = (150, "star12", "12-Point Star")
    """12-Point Star"""

    STAR_16_POINT = (94, "star16", "16-point star")
    """16-point star"""

    STAR_24_POINT = (95, "star24", "24-point star")
    """24-point star"""

    STAR_32_POINT = (96, "star32", "32-point star")
    """32-point star"""

    STAR_4_POINT = (91, "star4", "4-point star")
    """4-point star"""

    STAR_5_POINT = (92, "star5", "5-point star")
    """5-point star"""

    STAR_6_POINT = (147, "star6", "6-Point Star")
    """6-Point Star"""

    STAR_7_POINT = (148, "star7", "7-Point Star")
    """7-Point Star"""

    STAR_8_POINT = (93, "star8", "8-point star")
    """8-point star"""

    STRIPED_RIGHT_ARROW = (
        49,
        "stripedRightArrow",
        "Block arrow that points right with stripes at the tail",
    )
    """Block arrow that points right with stripes at the tail"""

    SUN = (23, "sun", "Sun")
    """Sun"""

    SWOOSH_ARROW = (178, "swooshArrow", "Swoosh Arrow")
    """Swoosh Arrow"""

    TEAR = (160, "teardrop", "Teardrop")
    """Teardrop"""

    TRAPEZOID = (3, "trapezoid", "Trapezoid")
    """Trapezoid"""

    UP_ARROW = (35, "upArrow", "Block arrow that points up")
    """Block arrow that points up"""

    UP_ARROW_CALLOUT = (55, "upArrowCallout", "Callout with arrow that points up")
    """Callout with arrow that points up"""

    UP_DOWN_ARROW = (38, "upDownArrow", "Block arrow that points up and down")
    """Block arrow that points up and down"""

    UP_DOWN_ARROW_CALLOUT = (58, "upDownArrowCallout", "Callout with arrows that point up and down")
    """Callout with arrows that point up and down"""

    UP_RIBBON = (97, "ribbon2", "Ribbon banner with center area above ribbon ends")
    """Ribbon banner with center area above ribbon ends"""

    U_TURN_ARROW = (42, "uturnArrow", "Block arrow forming a U shape")
    """Block arrow forming a U shape"""

    VERTICAL_SCROLL = (101, "verticalScroll", "Vertical scroll")
    """Vertical scroll"""

    WAVE = (103, "wave", "Wave")
    """Wave"""


MSO_SHAPE = MSO_AUTO_SHAPE_TYPE


class MSO_CONNECTOR_TYPE(BaseXmlEnum):
    """
    Specifies a type of connector.

    Alias: ``MSO_CONNECTOR``

    Example::

        from pptx.enum.shapes import MSO_CONNECTOR
        from pptx.util import Cm

        shapes = prs.slides[0].shapes
        connector = shapes.add_connector(
            MSO_CONNECTOR.STRAIGHT, Cm(2), Cm(2), Cm(10), Cm(10)
        )
        assert connector.left.cm == 2

    MS API Name: `MsoConnectorType`

    http://msdn.microsoft.com/en-us/library/office/ff860918.aspx
    """

    CURVE = (3, "curvedConnector3", "Curved connector.")
    """Curved connector."""

    ELBOW = (2, "bentConnector3", "Elbow connector.")
    """Elbow connector."""

    STRAIGHT = (1, "line", "Straight line connector.")
    """Straight line connector."""

    MIXED = (-2, "", "Return value only; indicates a combination of other states.")
    """Return value only; indicates a combination of other states."""


MSO_CONNECTOR = MSO_CONNECTOR_TYPE


class MSO_SHAPE_TYPE(BaseEnum):
    """Specifies the type of a shape, more specifically than the five base types.

    Alias: ``MSO``

    Example::

        from pptx.enum.shapes import MSO_SHAPE_TYPE

        assert shape.type == MSO_SHAPE_TYPE.PICTURE

    MS API Name: `MsoShapeType`

    http://msdn.microsoft.com/en-us/library/office/ff860759(v=office.15).aspx
    """

    AUTO_SHAPE = (1, "AutoShape")
    """AutoShape"""

    CALLOUT = (2, "Callout shape")
    """Callout shape"""

    CANVAS = (20, "Drawing canvas")
    """Drawing canvas"""

    CHART = (3, "Chart, e.g. pie chart, bar chart")
    """Chart, e.g. pie chart, bar chart"""

    COMMENT = (4, "Comment")
    """Comment"""

    DIAGRAM = (21, "Diagram")
    """Diagram"""

    EMBEDDED_OLE_OBJECT = (7, "Embedded OLE object")
    """Embedded OLE object"""

    FORM_CONTROL = (8, "Form control")
    """Form control"""

    FREEFORM = (5, "Freeform")
    """Freeform"""

    GROUP = (6, "Group shape")
    """Group shape"""

    IGX_GRAPHIC = (24, "SmartArt graphic")
    """SmartArt graphic"""

    INK = (22, "Ink")
    """Ink"""

    INK_COMMENT = (23, "Ink Comment")
    """Ink Comment"""

    LINE = (9, "Line")
    """Line"""

    LINKED_OLE_OBJECT = (10, "Linked OLE object")
    """Linked OLE object"""

    LINKED_PICTURE = (11, "Linked picture")
    """Linked picture"""

    MEDIA = (16, "Media")
    """Media"""

    OLE_CONTROL_OBJECT = (12, "OLE control object")
    """OLE control object"""

    PICTURE = (13, "Picture")
    """Picture"""

    PLACEHOLDER = (14, "Placeholder")
    """Placeholder"""

    SCRIPT_ANCHOR = (18, "Script anchor")
    """Script anchor"""

    TABLE = (19, "Table")
    """Table"""

    TEXT_BOX = (17, "Text box")
    """Text box"""

    TEXT_EFFECT = (15, "Text effect")
    """Text effect"""

    WEB_VIDEO = (26, "Web video")
    """Web video"""

    MIXED = (-2, "Multiple shape types (read-only).")
    """Multiple shape types (read-only)."""


MSO = MSO_SHAPE_TYPE


class PP_MEDIA_TYPE(BaseEnum):
    """Indicates the OLE media type.

    Example::

        from pptx.enum.shapes import PP_MEDIA_TYPE

        movie = slide.shapes[0]
        assert movie.media_type == PP_MEDIA_TYPE.MOVIE

    MS API Name: `PpMediaType`

    https://msdn.microsoft.com/en-us/library/office/ff746008.aspx
    """

    MOVIE = (3, "Video media such as MP4.")
    """Video media such as MP4."""

    OTHER = (1, "Other media types")
    """Other media types"""

    SOUND = (1, "Audio media such as MP3.")
    """Audio media such as MP3."""

    MIXED = (
        -2,
        "Return value only; indicates multiple media types, typically for a collection of shapes."
        " May not be applicable in python-pptx.",
    )
    """Return value only; indicates multiple media types.

    Typically for a collection of shapes. May not be applicable in python-pptx.
    """


class PP_PLACEHOLDER_TYPE(BaseXmlEnum):
    """Specifies one of the 18 distinct types of placeholder.

    Alias: ``PP_PLACEHOLDER``

    Example::

        from pptx.enum.shapes import PP_PLACEHOLDER

        placeholder = slide.placeholders[0]
        assert placeholder.type == PP_PLACEHOLDER.TITLE

    MS API name: `PpPlaceholderType`

    http://msdn.microsoft.com/en-us/library/office/ff860759(v=office.15 ").aspx"
    """

    BITMAP = (9, "clipArt", "Clip art placeholder")
    """Clip art placeholder"""

    BODY = (2, "body", "Body")
    """Body"""

    CENTER_TITLE = (3, "ctrTitle", "Center Title")
    """Center Title"""

    CHART = (8, "chart", "Chart")
    """Chart"""

    DATE = (16, "dt", "Date")
    """Date"""

    FOOTER = (15, "ftr", "Footer")
    """Footer"""

    HEADER = (14, "hdr", "Header")
    """Header"""

    MEDIA_CLIP = (10, "media", "Media Clip")
    """Media Clip"""

    OBJECT = (7, "obj", "Object")
    """Object"""

    ORG_CHART = (11, "dgm", "SmartArt placeholder. Organization chart is a legacy name.")
    """SmartArt placeholder. Organization chart is a legacy name."""

    PICTURE = (18, "pic", "Picture")
    """Picture"""

    SLIDE_IMAGE = (101, "sldImg", "Slide Image")
    """Slide Image"""

    SLIDE_NUMBER = (13, "sldNum", "Slide Number")
    """Slide Number"""

    SUBTITLE = (4, "subTitle", "Subtitle")
    """Subtitle"""

    TABLE = (12, "tbl", "Table")
    """Table"""

    TITLE = (1, "title", "Title")
    """Title"""

    VERTICAL_BODY = (6, "", "Vertical Body (read-only).")
    """Vertical Body (read-only)."""

    VERTICAL_OBJECT = (17, "", "Vertical Object (read-only).")
    """Vertical Object (read-only)."""

    VERTICAL_TITLE = (5, "", "Vertical Title (read-only).")
    """Vertical Title (read-only)."""

    MIXED = (-2, "", "Return value only; multiple placeholders of differing types.")
    """Return value only; multiple placeholders of differing types."""


PP_PLACEHOLDER = PP_PLACEHOLDER_TYPE


class PROG_ID(enum.Enum):
    """One-off Enum-like object for progId values.

    Indicates the type of an OLE object in terms of the program used to open it.

    A member of this enumeration can be used in a `SlideShapes.add_ole_object()` call to
    specify a Microsoft Office file-type (Excel, PowerPoint, or Word), which will
    then not require several of the arguments required to embed other object types.

    Example::

        from pptx.enum.shapes import PROG_ID
        from pptx.util import Inches

        embedded_xlsx_shape = slide.shapes.add_ole_object(
            "workbook.xlsx", PROG_ID.XLSX, left=Inches(1), top=Inches(1)
        )
        assert embedded_xlsx_shape.ole_format.prog_id == "Excel.Sheet.12"
    """

    _progId: str
    _icon_filename: str
    _width: int
    _height: int

    def __new__(cls, value: str, progId: str, icon_filename: str, width: int, height: int):
        self = object.__new__(cls)
        self._value_ = value
        self._progId = progId
        self._icon_filename = icon_filename
        self._width = width
        self._height = height
        return self

    @property
    def height(self):
        return self._height

    @property
    def icon_filename(self):
        return self._icon_filename

    @property
    def progId(self):
        return self._progId

    @property
    def width(self):
        return self._width

    DOCX = ("DOCX", "Word.Document.12", "docx-icon.emf", 965200, 609600)
    """`progId` for an embedded Word 2007+ (.docx) document."""

    PPTX = ("PPTX", "PowerPoint.Show.12", "pptx-icon.emf", 965200, 609600)
    """`progId` for an embedded PowerPoint 2007+ (.pptx) document."""

    XLSX = ("XLSX", "Excel.Sheet.12", "xlsx-icon.emf", 965200, 609600)
    """`progId` for an embedded Excel 2007+ (.xlsx) document."""
