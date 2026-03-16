# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

import pypdfium2.raw as pdfium_c
from pypdfium2.version import PDFIUM_INFO


class _fallback_dict (dict):
    
    def get(self, key, default_prefix="Unhandled constant"):
        return dict.get(self, key, f"{default_prefix} {key}")


#: Convert a rotation value in degrees to a PDFium constant.
RotationToConst = {
    0:   0,
    90:  1,
    180: 2,
    270: 3,
}

#: Convert a PDFium rotation constant to a value in degrees. Inversion of :data:`.RotationToConst`.
RotationToDegrees = {v: k for k, v in RotationToConst.items()}


#: Get the number of channels for a PDFium bitmap format. (:attr:`FPDFBitmap_Unknown` is deliberately not handled.)
BitmapTypeToNChannels = {
    pdfium_c.FPDFBitmap_Gray: 1,
    pdfium_c.FPDFBitmap_BGR:  3,
    pdfium_c.FPDFBitmap_BGRx: 4,
    pdfium_c.FPDFBitmap_BGRA: 4,
}

#: Convert a PDFium bitmap format to string, assuming BGR byte order. (:attr:`FPDFBitmap_Unknown` is deliberately not handled.)
BitmapTypeToStr = {
    pdfium_c.FPDFBitmap_Gray: "L",
    pdfium_c.FPDFBitmap_BGR:  "BGR",
    pdfium_c.FPDFBitmap_BGRx: "BGRX",
    pdfium_c.FPDFBitmap_BGRA: "BGRA",
}

#: Convert a PDFium bitmap format to string, assuming RGB byte order. (:attr:`FPDFBitmap_Unknown` is deliberately not handled.)
BitmapTypeToStrReverse = {
    pdfium_c.FPDFBitmap_Gray: "L",
    pdfium_c.FPDFBitmap_BGR:  "RGB",
    pdfium_c.FPDFBitmap_BGRx: "RGBX",
    pdfium_c.FPDFBitmap_BGRA: "RGBA",
}

if PDFIUM_INFO.build >= 7098:
    # New pixel format FPDFBitmap_BGRA_Premul
    # Skia-only at the time of writing. Added for completeness and to satisfy the test suite.
    BitmapTypeToNChannels[pdfium_c.FPDFBitmap_BGRA_Premul] = 4
    BitmapTypeToStr[pdfium_c.FPDFBitmap_BGRA_Premul] = "BGRa"
    BitmapTypeToStrReverse[pdfium_c.FPDFBitmap_BGRA_Premul] = "RGBa"

# TODO consider a bi-directional dict in the future?
#: Convert a string to PDFium bitmap format, assuming BGR byte order. Inversion of :data:`BitmapTypeToStr`.
BitmapStrToConst = {v: k for k, v in BitmapTypeToStr.items()}

#: Convert a string to PDFium bitmap format, assuming RGB byte order. Inversion of :data:`BitmapTypeToStrReverse`.
BitmapStrReverseToConst = {v: k for k, v in BitmapTypeToStrReverse.items()}

#: Convert a PDFium form type (:attr:`FORMTYPE_*`) to string.
FormTypeToStr = _fallback_dict({
    pdfium_c.FORMTYPE_NONE:           "None",
    pdfium_c.FORMTYPE_ACRO_FORM:      "AcroForm",
    pdfium_c.FORMTYPE_XFA_FULL:       "XFA",
    pdfium_c.FORMTYPE_XFA_FOREGROUND: "XFAF",
})

#: Convert a PDFium color space constant (:attr:`FPDF_COLORSPACE_*`) to string.
ColorspaceToStr = _fallback_dict({
    pdfium_c.FPDF_COLORSPACE_UNKNOWN:    "?",
    pdfium_c.FPDF_COLORSPACE_DEVICEGRAY: "DeviceGray",
    pdfium_c.FPDF_COLORSPACE_DEVICERGB:  "DeviceRGB",
    pdfium_c.FPDF_COLORSPACE_DEVICECMYK: "DeviceCMYK",
    pdfium_c.FPDF_COLORSPACE_CALGRAY:    "CalGray",
    pdfium_c.FPDF_COLORSPACE_CALRGB:     "CalRGB",
    pdfium_c.FPDF_COLORSPACE_LAB:        "Lab",
    pdfium_c.FPDF_COLORSPACE_ICCBASED:   "ICCBased",
    pdfium_c.FPDF_COLORSPACE_SEPARATION: "Separation",
    pdfium_c.FPDF_COLORSPACE_DEVICEN:    "DeviceN",
    pdfium_c.FPDF_COLORSPACE_INDEXED:    "Indexed",  # i.e. palettized
    pdfium_c.FPDF_COLORSPACE_PATTERN:    "Pattern",
})

#: Convert a PDFium view mode constant (:attr:`PDFDEST_VIEW_*`) to string.
ViewmodeToStr = _fallback_dict({
    pdfium_c.PDFDEST_VIEW_UNKNOWN_MODE: "?",
    pdfium_c.PDFDEST_VIEW_XYZ:   "XYZ",
    pdfium_c.PDFDEST_VIEW_FIT:   "Fit",
    pdfium_c.PDFDEST_VIEW_FITH:  "FitH",
    pdfium_c.PDFDEST_VIEW_FITV:  "FitV",
    pdfium_c.PDFDEST_VIEW_FITR:  "FitR",
    pdfium_c.PDFDEST_VIEW_FITB:  "FitB",
    pdfium_c.PDFDEST_VIEW_FITBH: "FitBH",
    pdfium_c.PDFDEST_VIEW_FITBV: "FitBV",
})

#: Convert a PDFium object type constant (:attr:`FPDF_PAGEOBJ_*`) to string.
ObjectTypeToStr = _fallback_dict({
    pdfium_c.FPDF_PAGEOBJ_UNKNOWN: "?",
    pdfium_c.FPDF_PAGEOBJ_TEXT:    "text",
    pdfium_c.FPDF_PAGEOBJ_PATH:    "path",
    pdfium_c.FPDF_PAGEOBJ_IMAGE:   "image",
    pdfium_c.FPDF_PAGEOBJ_SHADING: "shading",
    pdfium_c.FPDF_PAGEOBJ_FORM:    "form",
})

#: Convert an object type string to a PDFium constant. Inversion of :data:`.ObjectTypeToStr`.
ObjectTypeToConst = {v: k for k, v in ObjectTypeToStr.items()}

#: Convert a PDFium page mode constant (:attr:`PAGEMODE_*`) to string.
PageModeToStr = _fallback_dict({
    pdfium_c.PAGEMODE_UNKNOWN:        "?",
    pdfium_c.PAGEMODE_USENONE:        "None",
    pdfium_c.PAGEMODE_USEOUTLINES:    "Outline",
    pdfium_c.PAGEMODE_USETHUMBS:      "Thumbnails",
    pdfium_c.PAGEMODE_FULLSCREEN:     "Full-screen",
    pdfium_c.PAGEMODE_USEOC:          "Layers",
    pdfium_c.PAGEMODE_USEATTACHMENTS: "Attachments",
})

#: Convert a PDFium error constant (:attr:`FPDF_ERR_*`) to string.
ErrorToStr = _fallback_dict({
    pdfium_c.FPDF_ERR_SUCCESS:  "Success",
    pdfium_c.FPDF_ERR_UNKNOWN:  "Unknown error",
    pdfium_c.FPDF_ERR_FILE:     "File access error",
    pdfium_c.FPDF_ERR_FORMAT:   "Data format error",
    pdfium_c.FPDF_ERR_PASSWORD: "Incorrect password error",
    pdfium_c.FPDF_ERR_SECURITY: "Unsupported security scheme error",
    pdfium_c.FPDF_ERR_PAGE:     "Page not found or content error",
})


if "XFA" in PDFIUM_INFO.flags:  # pragma: no cover
    #: [XFA builds only] Convert a PDFium XFA error constant (:attr:`FPDF_ERR_XFA*`) to string.
    XFAErrorToStr = _fallback_dict({
        pdfium_c.FPDF_ERR_XFALOAD:   "Load error",
        pdfium_c.FPDF_ERR_XFALAYOUT: "Layout error",
    })

#: Convert a PDFium unsupported constant (:attr:`FPDF_UNSP_*`) to string.
UnsupportedInfoToStr = _fallback_dict({
    pdfium_c.FPDF_UNSP_DOC_XFAFORM:               "XFA form",
    pdfium_c.FPDF_UNSP_DOC_PORTABLECOLLECTION:    "Portable collection",
    # https://crbug.com/pdfium/1945
    pdfium_c.FPDF_UNSP_DOC_ATTACHMENT:            "Attachment (incomplete support)",
    pdfium_c.FPDF_UNSP_DOC_SECURITY:              "Security",
    pdfium_c.FPDF_UNSP_DOC_SHAREDREVIEW:          "Shared review",
    pdfium_c.FPDF_UNSP_DOC_SHAREDFORM_ACROBAT:    "Shared form (acrobat)",
    pdfium_c.FPDF_UNSP_DOC_SHAREDFORM_FILESYSTEM: "Shared form (filesystem)",
    pdfium_c.FPDF_UNSP_DOC_SHAREDFORM_EMAIL:      "Shared form (email)",
    pdfium_c.FPDF_UNSP_ANNOT_3DANNOT:             "3D annotation",
    pdfium_c.FPDF_UNSP_ANNOT_MOVIE:               "Movie annotation",
    pdfium_c.FPDF_UNSP_ANNOT_SOUND:               "Sound annotation",
    pdfium_c.FPDF_UNSP_ANNOT_SCREEN_MEDIA:        "Screen media annotation",
    pdfium_c.FPDF_UNSP_ANNOT_SCREEN_RICHMEDIA:    "Screen rich media annotation",
    pdfium_c.FPDF_UNSP_ANNOT_ATTACHMENT:          "Attachment annotation",
    pdfium_c.FPDF_UNSP_ANNOT_SIG:                 "Signature annotation",
})
