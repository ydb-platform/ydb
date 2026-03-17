# encoding: utf-8

"""Enumerations used by shapes and related objects."""

from pptx.enum.base import (
    alias,
    Enumeration,
    EnumMember,
    ReturnValueOnlyEnumMember,
    XmlEnumeration,
    XmlMappedEnumMember,
)
from pptx.util import lazyproperty


@alias("MSO_SHAPE")
class MSO_AUTO_SHAPE_TYPE(XmlEnumeration):
    """
    Specifies a type of AutoShape, e.g. DOWN_ARROW

    Alias: ``MSO_SHAPE``

    Example::

        from pptx.enum.shapes import MSO_SHAPE
        from pptx.util import Inches

        left = top = width = height = Inches(1.0)
        slide.shapes.add_shape(
            MSO_SHAPE.ROUNDED_RECTANGLE, left, top, width, height
        )
    """

    __ms_name__ = "MsoAutoShapeType"

    __url__ = (
        "http://msdn.microsoft.com/en-us/library/office/ff862770(v=office.15" ").aspx"
    )

    __members__ = (
        XmlMappedEnumMember(
            "ACTION_BUTTON_BACK_OR_PREVIOUS",
            129,
            "actionButtonBackPrevious",
            "Back or Previous button. Supports " "mouse-click and mouse-over actions",
        ),
        XmlMappedEnumMember(
            "ACTION_BUTTON_BEGINNING",
            131,
            "actionButtonBeginning",
            "Beginning button. Supports mouse-click and mouse-over actions",
        ),
        XmlMappedEnumMember(
            "ACTION_BUTTON_CUSTOM",
            125,
            "actionButtonBlank",
            "Button with no default picture or text. Supports mouse-click an"
            "d mouse-over actions",
        ),
        XmlMappedEnumMember(
            "ACTION_BUTTON_DOCUMENT",
            134,
            "actionButtonDocument",
            "Document button. Supports mouse-click and mouse-over actions",
        ),
        XmlMappedEnumMember(
            "ACTION_BUTTON_END",
            132,
            "actionButtonEnd",
            "End button. Supports mouse-click and mouse-over actions",
        ),
        XmlMappedEnumMember(
            "ACTION_BUTTON_FORWARD_OR_NEXT",
            130,
            "actionButtonForwardNext",
            "Forward or Next button. Supports mouse-click and mouse-over act" "ions",
        ),
        XmlMappedEnumMember(
            "ACTION_BUTTON_HELP",
            127,
            "actionButtonHelp",
            "Help button. Supports mouse-click and mouse-over actions",
        ),
        XmlMappedEnumMember(
            "ACTION_BUTTON_HOME",
            126,
            "actionButtonHome",
            "Home button. Supports mouse-click and mouse-over actions",
        ),
        XmlMappedEnumMember(
            "ACTION_BUTTON_INFORMATION",
            128,
            "actionButtonInformation",
            "Information button. Supports mouse-click and mouse-over actions",
        ),
        XmlMappedEnumMember(
            "ACTION_BUTTON_MOVIE",
            136,
            "actionButtonMovie",
            "Movie button. Supports mouse-click and mouse-over actions",
        ),
        XmlMappedEnumMember(
            "ACTION_BUTTON_RETURN",
            133,
            "actionButtonReturn",
            "Return button. Supports mouse-click and mouse-over actions",
        ),
        XmlMappedEnumMember(
            "ACTION_BUTTON_SOUND",
            135,
            "actionButtonSound",
            "Sound button. Supports mouse-click and mouse-over actions",
        ),
        XmlMappedEnumMember("ARC", 25, "arc", "Arc"),
        XmlMappedEnumMember(
            "BALLOON", 137, "wedgeRoundRectCallout", "Rounded Rectangular Callout"
        ),
        XmlMappedEnumMember(
            "BENT_ARROW",
            41,
            "bentArrow",
            "Block arrow that follows a curved 90-degree angle",
        ),
        XmlMappedEnumMember(
            "BENT_UP_ARROW",
            44,
            "bentUpArrow",
            "Block arrow that follows a sharp 90-degree angle. Points up by " "default",
        ),
        XmlMappedEnumMember("BEVEL", 15, "bevel", "Bevel"),
        XmlMappedEnumMember("BLOCK_ARC", 20, "blockArc", "Block arc"),
        XmlMappedEnumMember("CAN", 13, "can", "Can"),
        XmlMappedEnumMember("CHART_PLUS", 182, "chartPlus", "Chart Plus"),
        XmlMappedEnumMember("CHART_STAR", 181, "chartStar", "Chart Star"),
        XmlMappedEnumMember("CHART_X", 180, "chartX", "Chart X"),
        XmlMappedEnumMember("CHEVRON", 52, "chevron", "Chevron"),
        XmlMappedEnumMember("CHORD", 161, "chord", "Geometric chord shape"),
        XmlMappedEnumMember(
            "CIRCULAR_ARROW",
            60,
            "circularArrow",
            "Block arrow that follows a curved 180-degree angle",
        ),
        XmlMappedEnumMember("CLOUD", 179, "cloud", "Cloud"),
        XmlMappedEnumMember("CLOUD_CALLOUT", 108, "cloudCallout", "Cloud callout"),
        XmlMappedEnumMember("CORNER", 162, "corner", "Corner"),
        XmlMappedEnumMember("CORNER_TABS", 169, "cornerTabs", "Corner Tabs"),
        XmlMappedEnumMember("CROSS", 11, "plus", "Cross"),
        XmlMappedEnumMember("CUBE", 14, "cube", "Cube"),
        XmlMappedEnumMember(
            "CURVED_DOWN_ARROW", 48, "curvedDownArrow", "Block arrow that curves down"
        ),
        XmlMappedEnumMember(
            "CURVED_DOWN_RIBBON", 100, "ellipseRibbon", "Ribbon banner that curves down"
        ),
        XmlMappedEnumMember(
            "CURVED_LEFT_ARROW", 46, "curvedLeftArrow", "Block arrow that curves left"
        ),
        XmlMappedEnumMember(
            "CURVED_RIGHT_ARROW",
            45,
            "curvedRightArrow",
            "Block arrow that curves right",
        ),
        XmlMappedEnumMember(
            "CURVED_UP_ARROW", 47, "curvedUpArrow", "Block arrow that curves up"
        ),
        XmlMappedEnumMember(
            "CURVED_UP_RIBBON", 99, "ellipseRibbon2", "Ribbon banner that curves up"
        ),
        XmlMappedEnumMember("DECAGON", 144, "decagon", "Decagon"),
        XmlMappedEnumMember("DIAGONAL_STRIPE", 141, "diagStripe", "Diagonal Stripe"),
        XmlMappedEnumMember("DIAMOND", 4, "diamond", "Diamond"),
        XmlMappedEnumMember("DODECAGON", 146, "dodecagon", "Dodecagon"),
        XmlMappedEnumMember("DONUT", 18, "donut", "Donut"),
        XmlMappedEnumMember("DOUBLE_BRACE", 27, "bracePair", "Double brace"),
        XmlMappedEnumMember("DOUBLE_BRACKET", 26, "bracketPair", "Double bracket"),
        XmlMappedEnumMember("DOUBLE_WAVE", 104, "doubleWave", "Double wave"),
        XmlMappedEnumMember(
            "DOWN_ARROW", 36, "downArrow", "Block arrow that points down"
        ),
        XmlMappedEnumMember(
            "DOWN_ARROW_CALLOUT",
            56,
            "downArrowCallout",
            "Callout with arrow that points down",
        ),
        XmlMappedEnumMember(
            "DOWN_RIBBON",
            98,
            "ribbon",
            "Ribbon banner with center area below ribbon ends",
        ),
        XmlMappedEnumMember("EXPLOSION1", 89, "irregularSeal1", "Explosion"),
        XmlMappedEnumMember("EXPLOSION2", 90, "irregularSeal2", "Explosion"),
        XmlMappedEnumMember(
            "FLOWCHART_ALTERNATE_PROCESS",
            62,
            "flowChartAlternateProcess",
            "Alternate process flowchart symbol",
        ),
        XmlMappedEnumMember(
            "FLOWCHART_CARD", 75, "flowChartPunchedCard", "Card flowchart symbol"
        ),
        XmlMappedEnumMember(
            "FLOWCHART_COLLATE", 79, "flowChartCollate", "Collate flowchart symbol"
        ),
        XmlMappedEnumMember(
            "FLOWCHART_CONNECTOR",
            73,
            "flowChartConnector",
            "Connector flowchart symbol",
        ),
        XmlMappedEnumMember(
            "FLOWCHART_DATA", 64, "flowChartInputOutput", "Data flowchart symbol"
        ),
        XmlMappedEnumMember(
            "FLOWCHART_DECISION", 63, "flowChartDecision", "Decision flowchart symbol"
        ),
        XmlMappedEnumMember(
            "FLOWCHART_DELAY", 84, "flowChartDelay", "Delay flowchart symbol"
        ),
        XmlMappedEnumMember(
            "FLOWCHART_DIRECT_ACCESS_STORAGE",
            87,
            "flowChartMagneticDrum",
            "Direct access storage flowchart symbol",
        ),
        XmlMappedEnumMember(
            "FLOWCHART_DISPLAY", 88, "flowChartDisplay", "Display flowchart symbol"
        ),
        XmlMappedEnumMember(
            "FLOWCHART_DOCUMENT", 67, "flowChartDocument", "Document flowchart symbol"
        ),
        XmlMappedEnumMember(
            "FLOWCHART_EXTRACT", 81, "flowChartExtract", "Extract flowchart symbol"
        ),
        XmlMappedEnumMember(
            "FLOWCHART_INTERNAL_STORAGE",
            66,
            "flowChartInternalStorage",
            "Internal storage flowchart symbol",
        ),
        XmlMappedEnumMember(
            "FLOWCHART_MAGNETIC_DISK",
            86,
            "flowChartMagneticDisk",
            "Magnetic disk flowchart symbol",
        ),
        XmlMappedEnumMember(
            "FLOWCHART_MANUAL_INPUT",
            71,
            "flowChartManualInput",
            "Manual input flowchart symbol",
        ),
        XmlMappedEnumMember(
            "FLOWCHART_MANUAL_OPERATION",
            72,
            "flowChartManualOperation",
            "Manual operation flowchart symbol",
        ),
        XmlMappedEnumMember(
            "FLOWCHART_MERGE", 82, "flowChartMerge", "Merge flowchart symbol"
        ),
        XmlMappedEnumMember(
            "FLOWCHART_MULTIDOCUMENT",
            68,
            "flowChartMultidocument",
            "Multi-document flowchart symbol",
        ),
        XmlMappedEnumMember(
            "FLOWCHART_OFFLINE_STORAGE",
            139,
            "flowChartOfflineStorage",
            "Offline Storage",
        ),
        XmlMappedEnumMember(
            "FLOWCHART_OFFPAGE_CONNECTOR",
            74,
            "flowChartOffpageConnector",
            "Off-page connector flowchart symbol",
        ),
        XmlMappedEnumMember("FLOWCHART_OR", 78, "flowChartOr", '"Or" flowchart symbol'),
        XmlMappedEnumMember(
            "FLOWCHART_PREDEFINED_PROCESS",
            65,
            "flowChartPredefinedProcess",
            "Predefined process flowchart symbol",
        ),
        XmlMappedEnumMember(
            "FLOWCHART_PREPARATION",
            70,
            "flowChartPreparation",
            "Preparation flowchart symbol",
        ),
        XmlMappedEnumMember(
            "FLOWCHART_PROCESS", 61, "flowChartProcess", "Process flowchart symbol"
        ),
        XmlMappedEnumMember(
            "FLOWCHART_PUNCHED_TAPE",
            76,
            "flowChartPunchedTape",
            "Punched tape flowchart symbol",
        ),
        XmlMappedEnumMember(
            "FLOWCHART_SEQUENTIAL_ACCESS_STORAGE",
            85,
            "flowChartMagneticTape",
            "Sequential access storage flowchart symbol",
        ),
        XmlMappedEnumMember(
            "FLOWCHART_SORT", 80, "flowChartSort", "Sort flowchart symbol"
        ),
        XmlMappedEnumMember(
            "FLOWCHART_STORED_DATA",
            83,
            "flowChartOnlineStorage",
            "Stored data flowchart symbol",
        ),
        XmlMappedEnumMember(
            "FLOWCHART_SUMMING_JUNCTION",
            77,
            "flowChartSummingJunction",
            "Summing junction flowchart symbol",
        ),
        XmlMappedEnumMember(
            "FLOWCHART_TERMINATOR",
            69,
            "flowChartTerminator",
            "Terminator flowchart symbol",
        ),
        XmlMappedEnumMember("FOLDED_CORNER", 16, "foldedCorner", "Folded corner"),
        XmlMappedEnumMember("FRAME", 158, "frame", "Frame"),
        XmlMappedEnumMember("FUNNEL", 174, "funnel", "Funnel"),
        XmlMappedEnumMember("GEAR_6", 172, "gear6", "Gear 6"),
        XmlMappedEnumMember("GEAR_9", 173, "gear9", "Gear 9"),
        XmlMappedEnumMember("HALF_FRAME", 159, "halfFrame", "Half Frame"),
        XmlMappedEnumMember("HEART", 21, "heart", "Heart"),
        XmlMappedEnumMember("HEPTAGON", 145, "heptagon", "Heptagon"),
        XmlMappedEnumMember("HEXAGON", 10, "hexagon", "Hexagon"),
        XmlMappedEnumMember(
            "HORIZONTAL_SCROLL", 102, "horizontalScroll", "Horizontal scroll"
        ),
        XmlMappedEnumMember("ISOSCELES_TRIANGLE", 7, "triangle", "Isosceles triangle"),
        XmlMappedEnumMember(
            "LEFT_ARROW", 34, "leftArrow", "Block arrow that points left"
        ),
        XmlMappedEnumMember(
            "LEFT_ARROW_CALLOUT",
            54,
            "leftArrowCallout",
            "Callout with arrow that points left",
        ),
        XmlMappedEnumMember("LEFT_BRACE", 31, "leftBrace", "Left brace"),
        XmlMappedEnumMember("LEFT_BRACKET", 29, "leftBracket", "Left bracket"),
        XmlMappedEnumMember(
            "LEFT_CIRCULAR_ARROW", 176, "leftCircularArrow", "Left Circular Arrow"
        ),
        XmlMappedEnumMember(
            "LEFT_RIGHT_ARROW",
            37,
            "leftRightArrow",
            "Block arrow with arrowheads that point both left and right",
        ),
        XmlMappedEnumMember(
            "LEFT_RIGHT_ARROW_CALLOUT",
            57,
            "leftRightArrowCallout",
            "Callout with arrowheads that point both left and right",
        ),
        XmlMappedEnumMember(
            "LEFT_RIGHT_CIRCULAR_ARROW",
            177,
            "leftRightCircularArrow",
            "Left Right Circular Arrow",
        ),
        XmlMappedEnumMember(
            "LEFT_RIGHT_RIBBON", 140, "leftRightRibbon", "Left Right Ribbon"
        ),
        XmlMappedEnumMember(
            "LEFT_RIGHT_UP_ARROW",
            40,
            "leftRightUpArrow",
            "Block arrow with arrowheads that point left, right, and up",
        ),
        XmlMappedEnumMember(
            "LEFT_UP_ARROW",
            43,
            "leftUpArrow",
            "Block arrow with arrowheads that point left and up",
        ),
        XmlMappedEnumMember("LIGHTNING_BOLT", 22, "lightningBolt", "Lightning bolt"),
        XmlMappedEnumMember(
            "LINE_CALLOUT_1",
            109,
            "borderCallout1",
            "Callout with border and horizontal callout line",
        ),
        XmlMappedEnumMember(
            "LINE_CALLOUT_1_ACCENT_BAR",
            113,
            "accentCallout1",
            "Callout with vertical accent bar",
        ),
        XmlMappedEnumMember(
            "LINE_CALLOUT_1_BORDER_AND_ACCENT_BAR",
            121,
            "accentBorderCallout1",
            "Callout with border and vertical accent bar",
        ),
        XmlMappedEnumMember(
            "LINE_CALLOUT_1_NO_BORDER", 117, "callout1", "Callout with horizontal line"
        ),
        XmlMappedEnumMember(
            "LINE_CALLOUT_2",
            110,
            "borderCallout2",
            "Callout with diagonal straight line",
        ),
        XmlMappedEnumMember(
            "LINE_CALLOUT_2_ACCENT_BAR",
            114,
            "accentCallout2",
            "Callout with diagonal callout line and accent bar",
        ),
        XmlMappedEnumMember(
            "LINE_CALLOUT_2_BORDER_AND_ACCENT_BAR",
            122,
            "accentBorderCallout2",
            "Callout with border, diagonal straight line, and accent bar",
        ),
        XmlMappedEnumMember(
            "LINE_CALLOUT_2_NO_BORDER",
            118,
            "callout2",
            "Callout with no border and diagonal callout line",
        ),
        XmlMappedEnumMember(
            "LINE_CALLOUT_3", 111, "borderCallout3", "Callout with angled line"
        ),
        XmlMappedEnumMember(
            "LINE_CALLOUT_3_ACCENT_BAR",
            115,
            "accentCallout3",
            "Callout with angled callout line and accent bar",
        ),
        XmlMappedEnumMember(
            "LINE_CALLOUT_3_BORDER_AND_ACCENT_BAR",
            123,
            "accentBorderCallout3",
            "Callout with border, angled callout line, and accent bar",
        ),
        XmlMappedEnumMember(
            "LINE_CALLOUT_3_NO_BORDER",
            119,
            "callout3",
            "Callout with no border and angled callout line",
        ),
        XmlMappedEnumMember(
            "LINE_CALLOUT_4",
            112,
            "borderCallout3",
            "Callout with callout line segments forming a U-shape.",
        ),
        XmlMappedEnumMember(
            "LINE_CALLOUT_4_ACCENT_BAR",
            116,
            "accentCallout3",
            "Callout with accent bar and callout line segments forming a U-s" "hape.",
        ),
        XmlMappedEnumMember(
            "LINE_CALLOUT_4_BORDER_AND_ACCENT_BAR",
            124,
            "accentBorderCallout3",
            "Callout with border, accent bar, and callout line segments form"
            "ing a U-shape.",
        ),
        XmlMappedEnumMember(
            "LINE_CALLOUT_4_NO_BORDER",
            120,
            "callout3",
            "Callout with no border and callout line segments forming a U-sh" "ape.",
        ),
        XmlMappedEnumMember("LINE_INVERSE", 183, "lineInv", "Straight Connector"),
        XmlMappedEnumMember("MATH_DIVIDE", 166, "mathDivide", "Division"),
        XmlMappedEnumMember("MATH_EQUAL", 167, "mathEqual", "Equal"),
        XmlMappedEnumMember("MATH_MINUS", 164, "mathMinus", "Minus"),
        XmlMappedEnumMember("MATH_MULTIPLY", 165, "mathMultiply", "Multiply"),
        XmlMappedEnumMember("MATH_NOT_EQUAL", 168, "mathNotEqual", "Not Equal"),
        XmlMappedEnumMember("MATH_PLUS", 163, "mathPlus", "Plus"),
        XmlMappedEnumMember("MOON", 24, "moon", "Moon"),
        XmlMappedEnumMember(
            "NON_ISOSCELES_TRAPEZOID",
            143,
            "nonIsoscelesTrapezoid",
            "Non-isosceles Trapezoid",
        ),
        XmlMappedEnumMember(
            "NOTCHED_RIGHT_ARROW",
            50,
            "notchedRightArrow",
            "Notched block arrow that points right",
        ),
        XmlMappedEnumMember("NO_SYMBOL", 19, "noSmoking", '"No" Symbol'),
        XmlMappedEnumMember("OCTAGON", 6, "octagon", "Octagon"),
        XmlMappedEnumMember("OVAL", 9, "ellipse", "Oval"),
        XmlMappedEnumMember(
            "OVAL_CALLOUT", 107, "wedgeEllipseCallout", "Oval-shaped callout"
        ),
        XmlMappedEnumMember("PARALLELOGRAM", 2, "parallelogram", "Parallelogram"),
        XmlMappedEnumMember("PENTAGON", 51, "homePlate", "Pentagon"),
        XmlMappedEnumMember("PIE", 142, "pie", "Pie"),
        XmlMappedEnumMember("PIE_WEDGE", 175, "pieWedge", "Pie"),
        XmlMappedEnumMember("PLAQUE", 28, "plaque", "Plaque"),
        XmlMappedEnumMember("PLAQUE_TABS", 171, "plaqueTabs", "Plaque Tabs"),
        XmlMappedEnumMember(
            "QUAD_ARROW",
            39,
            "quadArrow",
            "Block arrows that point up, down, left, and right",
        ),
        XmlMappedEnumMember(
            "QUAD_ARROW_CALLOUT",
            59,
            "quadArrowCallout",
            "Callout with arrows that point up, down, left, and right",
        ),
        XmlMappedEnumMember("RECTANGLE", 1, "rect", "Rectangle"),
        XmlMappedEnumMember(
            "RECTANGULAR_CALLOUT", 105, "wedgeRectCallout", "Rectangular callout"
        ),
        XmlMappedEnumMember("REGULAR_PENTAGON", 12, "pentagon", "Pentagon"),
        XmlMappedEnumMember(
            "RIGHT_ARROW", 33, "rightArrow", "Block arrow that points right"
        ),
        XmlMappedEnumMember(
            "RIGHT_ARROW_CALLOUT",
            53,
            "rightArrowCallout",
            "Callout with arrow that points right",
        ),
        XmlMappedEnumMember("RIGHT_BRACE", 32, "rightBrace", "Right brace"),
        XmlMappedEnumMember("RIGHT_BRACKET", 30, "rightBracket", "Right bracket"),
        XmlMappedEnumMember("RIGHT_TRIANGLE", 8, "rtTriangle", "Right triangle"),
        XmlMappedEnumMember("ROUNDED_RECTANGLE", 5, "roundRect", "Rounded rectangle"),
        XmlMappedEnumMember(
            "ROUNDED_RECTANGULAR_CALLOUT",
            106,
            "wedgeRoundRectCallout",
            "Rounded rectangle-shaped callout",
        ),
        XmlMappedEnumMember(
            "ROUND_1_RECTANGLE", 151, "round1Rect", "Round Single Corner Rectangle"
        ),
        XmlMappedEnumMember(
            "ROUND_2_DIAG_RECTANGLE",
            153,
            "round2DiagRect",
            "Round Diagonal Corner Rectangle",
        ),
        XmlMappedEnumMember(
            "ROUND_2_SAME_RECTANGLE",
            152,
            "round2SameRect",
            "Round Same Side Corner Rectangle",
        ),
        XmlMappedEnumMember("SMILEY_FACE", 17, "smileyFace", "Smiley face"),
        XmlMappedEnumMember(
            "SNIP_1_RECTANGLE", 155, "snip1Rect", "Snip Single Corner Rectangle"
        ),
        XmlMappedEnumMember(
            "SNIP_2_DIAG_RECTANGLE",
            157,
            "snip2DiagRect",
            "Snip Diagonal Corner Rectangle",
        ),
        XmlMappedEnumMember(
            "SNIP_2_SAME_RECTANGLE",
            156,
            "snip2SameRect",
            "Snip Same Side Corner Rectangle",
        ),
        XmlMappedEnumMember(
            "SNIP_ROUND_RECTANGLE",
            154,
            "snipRoundRect",
            "Snip and Round Single Corner Rectangle",
        ),
        XmlMappedEnumMember("SQUARE_TABS", 170, "squareTabs", "Square Tabs"),
        XmlMappedEnumMember("STAR_10_POINT", 149, "star10", "10-Point Star"),
        XmlMappedEnumMember("STAR_12_POINT", 150, "star12", "12-Point Star"),
        XmlMappedEnumMember("STAR_16_POINT", 94, "star16", "16-point star"),
        XmlMappedEnumMember("STAR_24_POINT", 95, "star24", "24-point star"),
        XmlMappedEnumMember("STAR_32_POINT", 96, "star32", "32-point star"),
        XmlMappedEnumMember("STAR_4_POINT", 91, "star4", "4-point star"),
        XmlMappedEnumMember("STAR_5_POINT", 92, "star5", "5-point star"),
        XmlMappedEnumMember("STAR_6_POINT", 147, "star6", "6-Point Star"),
        XmlMappedEnumMember("STAR_7_POINT", 148, "star7", "7-Point Star"),
        XmlMappedEnumMember("STAR_8_POINT", 93, "star8", "8-point star"),
        XmlMappedEnumMember(
            "STRIPED_RIGHT_ARROW",
            49,
            "stripedRightArrow",
            "Block arrow that points right with stripes at the tail",
        ),
        XmlMappedEnumMember("SUN", 23, "sun", "Sun"),
        XmlMappedEnumMember("SWOOSH_ARROW", 178, "swooshArrow", "Swoosh Arrow"),
        XmlMappedEnumMember("TEAR", 160, "teardrop", "Teardrop"),
        XmlMappedEnumMember("TRAPEZOID", 3, "trapezoid", "Trapezoid"),
        XmlMappedEnumMember("UP_ARROW", 35, "upArrow", "Block arrow that points up"),
        XmlMappedEnumMember(
            "UP_ARROW_CALLOUT",
            55,
            "upArrowCallout",
            "Callout with arrow that points up",
        ),
        XmlMappedEnumMember(
            "UP_DOWN_ARROW", 38, "upDownArrow", "Block arrow that points up and down"
        ),
        XmlMappedEnumMember(
            "UP_DOWN_ARROW_CALLOUT",
            58,
            "upDownArrowCallout",
            "Callout with arrows that point up and down",
        ),
        XmlMappedEnumMember(
            "UP_RIBBON",
            97,
            "ribbon2",
            "Ribbon banner with center area above ribbon ends",
        ),
        XmlMappedEnumMember(
            "U_TURN_ARROW", 42, "uturnArrow", "Block arrow forming a U shape"
        ),
        XmlMappedEnumMember(
            "VERTICAL_SCROLL", 101, "verticalScroll", "Vertical scroll"
        ),
        XmlMappedEnumMember("WAVE", 103, "wave", "Wave"),
    )


@alias("MSO_CONNECTOR")
class MSO_CONNECTOR_TYPE(XmlEnumeration):
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
    """

    __ms_name__ = "MsoConnectorType"

    __url__ = "http://msdn.microsoft.com/en-us/library/office/ff860918.aspx"

    __members__ = (
        XmlMappedEnumMember("CURVE", 3, "curvedConnector3", "Curved connector."),
        XmlMappedEnumMember("ELBOW", 2, "bentConnector3", "Elbow connector."),
        XmlMappedEnumMember("STRAIGHT", 1, "line", "Straight line connector."),
        ReturnValueOnlyEnumMember(
            "MIXED",
            -2,
            "Return value only; indicates a combination of othe" "r states.",
        ),
    )


@alias("MSO")
class MSO_SHAPE_TYPE(Enumeration):
    """
    Specifies the type of a shape

    Alias: ``MSO``

    Example::

        from pptx.enum.shapes import MSO_SHAPE_TYPE

        assert shape.type == MSO_SHAPE_TYPE.PICTURE
    """

    __ms_name__ = "MsoShapeType"

    __url__ = (
        "http://msdn.microsoft.com/en-us/library/office/ff860759(v=office.15" ").aspx"
    )

    __members__ = (
        EnumMember("AUTO_SHAPE", 1, "AutoShape"),
        EnumMember("CALLOUT", 2, "Callout shape"),
        EnumMember("CANVAS", 20, "Drawing canvas"),
        EnumMember("CHART", 3, "Chart, e.g. pie chart, bar chart"),
        EnumMember("COMMENT", 4, "Comment"),
        EnumMember("DIAGRAM", 21, "Diagram"),
        EnumMember("EMBEDDED_OLE_OBJECT", 7, "Embedded OLE object"),
        EnumMember("FORM_CONTROL", 8, "Form control"),
        EnumMember("FREEFORM", 5, "Freeform"),
        EnumMember("GROUP", 6, "Group shape"),
        EnumMember("IGX_GRAPHIC", 24, "SmartArt graphic"),
        EnumMember("INK", 22, "Ink"),
        EnumMember("INK_COMMENT", 23, "Ink Comment"),
        EnumMember("LINE", 9, "Line"),
        EnumMember("LINKED_OLE_OBJECT", 10, "Linked OLE object"),
        EnumMember("LINKED_PICTURE", 11, "Linked picture"),
        EnumMember("MEDIA", 16, "Media"),
        EnumMember("OLE_CONTROL_OBJECT", 12, "OLE control object"),
        EnumMember("PICTURE", 13, "Picture"),
        EnumMember("PLACEHOLDER", 14, "Placeholder"),
        EnumMember("SCRIPT_ANCHOR", 18, "Script anchor"),
        EnumMember("TABLE", 19, "Table"),
        EnumMember("TEXT_BOX", 17, "Text box"),
        EnumMember("TEXT_EFFECT", 15, "Text effect"),
        EnumMember("WEB_VIDEO", 26, "Web video"),
        ReturnValueOnlyEnumMember("MIXED", -2, "Mixed shape types"),
    )


class PP_MEDIA_TYPE(Enumeration):
    """
    Indicates the OLE media type.

    Example::

        from pptx.enum.shapes import PP_MEDIA_TYPE

        movie = slide.shapes[0]
        assert movie.media_type == PP_MEDIA_TYPE.MOVIE
    """

    __ms_name__ = "PpMediaType"

    __url__ = "https://msdn.microsoft.com/en-us/library/office/ff746008.aspx"

    __members__ = (
        EnumMember("MOVIE", 3, "Video media such as MP4."),
        EnumMember("OTHER", 1, "Other media types"),
        EnumMember("SOUND", 1, "Audio media such as MP3."),
        ReturnValueOnlyEnumMember(
            "MIXED",
            -2,
            "Return value only; indicates multiple media types,"
            " typically for a collection of shapes. May not be applicable in"
            " python-pptx.",
        ),
    )


@alias("PP_PLACEHOLDER")
class PP_PLACEHOLDER_TYPE(XmlEnumeration):
    """
    Specifies one of the 18 distinct types of placeholder.

    Alias: ``PP_PLACEHOLDER``

    Example::

        from pptx.enum.shapes import PP_PLACEHOLDER

        placeholder = slide.placeholders[0]
        assert placeholder.type == PP_PLACEHOLDER.TITLE
    """

    __ms_name__ = "PpPlaceholderType"

    __url__ = (
        "http://msdn.microsoft.com/en-us/library/office/ff860759(v=office.15" ").aspx"
    )

    __members__ = (
        XmlMappedEnumMember("BITMAP", 9, "clipArt", "Clip art placeholder"),
        XmlMappedEnumMember("BODY", 2, "body", "Body"),
        XmlMappedEnumMember("CENTER_TITLE", 3, "ctrTitle", "Center Title"),
        XmlMappedEnumMember("CHART", 8, "chart", "Chart"),
        XmlMappedEnumMember("DATE", 16, "dt", "Date"),
        XmlMappedEnumMember("FOOTER", 15, "ftr", "Footer"),
        XmlMappedEnumMember("HEADER", 14, "hdr", "Header"),
        XmlMappedEnumMember("MEDIA_CLIP", 10, "media", "Media Clip"),
        XmlMappedEnumMember("OBJECT", 7, "obj", "Object"),
        XmlMappedEnumMember(
            "ORG_CHART",
            11,
            "dgm",
            "SmartArt placeholder. Organization char" "t is a legacy name.",
        ),
        XmlMappedEnumMember("PICTURE", 18, "pic", "Picture"),
        XmlMappedEnumMember("SLIDE_IMAGE", 101, "sldImg", "Slide Image"),
        XmlMappedEnumMember("SLIDE_NUMBER", 13, "sldNum", "Slide Number"),
        XmlMappedEnumMember("SUBTITLE", 4, "subTitle", "Subtitle"),
        XmlMappedEnumMember("TABLE", 12, "tbl", "Table"),
        XmlMappedEnumMember("TITLE", 1, "title", "Title"),
        ReturnValueOnlyEnumMember("VERTICAL_BODY", 6, "Vertical Body"),
        ReturnValueOnlyEnumMember("VERTICAL_OBJECT", 17, "Vertical Object"),
        ReturnValueOnlyEnumMember("VERTICAL_TITLE", 5, "Vertical Title"),
        ReturnValueOnlyEnumMember(
            "MIXED",
            -2,
            "Return value only; multiple placeholders of differ" "ing types.",
        ),
    )


class _ProgIdEnum(object):
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

    class Member(object):
        """A particular progID with its attributes."""

        def __init__(self, name, progId, icon_filename, width, height):
            self._name = name
            self._progId = progId
            self._icon_filename = icon_filename
            self._width = width
            self._height = height

        def __repr__(self):
            return "PROG_ID.%s" % self._name

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

    def __contains__(self, item):
        return item in (self.DOCX, self.PPTX, self.XLSX,)

    def __repr__(self):
        return "%s.PROG_ID" % __name__

    @lazyproperty
    def DOCX(self):
        return self.Member("DOCX", "Word.Document.12", "docx-icon.emf", 965200, 609600)

    @lazyproperty
    def PPTX(self):
        return self.Member(
            "PPTX", "PowerPoint.Show.12", "pptx-icon.emf", 965200, 609600
        )

    @lazyproperty
    def XLSX(self):
        return self.Member("XLSX", "Excel.Sheet.12", "xlsx-icon.emf", 965200, 609600)


PROG_ID = _ProgIdEnum()
