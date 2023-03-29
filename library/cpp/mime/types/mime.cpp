#include "mime.h"

#include <util/system/defaults.h>
#include <util/generic/hash.h>
#include <util/generic/strbuf.h>
#include <util/generic/singleton.h>
#include <util/generic/yexception.h>

#include <cctype>

/*
 * MIME types
 */

class TMimeTypes {
    // Constructor
public:
    TMimeTypes();

    // Methods
public:
    const char* StrByExt(const char* ext) const;

    MimeTypes MimeByStr(const char* str) const;
    MimeTypes MimeByStr(const TStringBuf& str) const;
    const char* StrByMime(MimeTypes mime) const;

    // Constants
public:
    static const size_t MAX_EXT_LEN = 11; // max length of supported extensions

    // Helper methods
private:
    void SetContentTypes();
    void SetExt();

    // Types
private:
    struct TRecord {
        MimeTypes Mime;
        const char* ContentType;
        const char* Ext;
    };

    typedef THashMap<const char*, int> TRecordHash;

    // Fields
private:
    static const TRecord Records[];
    TRecordHash ContentTypes;
    TRecordHash Ext;
};

const TMimeTypes::TRecord TMimeTypes::Records[] = {
    {MIME_UNKNOWN, nullptr, nullptr},
    {MIME_TEXT, "text/plain\0", "asc\0txt\0"},
    {MIME_HTML, "text/html\0", "html\0htm\0shtml\0"},
    {MIME_PDF, "application/pdf\0", "pdf\0"},
    {MIME_RTF, "text/rtf\0application/rtf\0", "rtf\0"},
    {MIME_DOC, "application/msword\0", "doc\0"},
    {MIME_MPEG, "audio/mpeg\0", "mp3\0mpa\0m2a\0mp2\0mpg\0mpga\0"},
    {MIME_XML, "text/xml\0application/xml\0application/rss+xml\0application/rdf+xml\0application/atom+xml\0", "xml\0rss\0"},
    {MIME_WML, "text/vnd.wap.wml\0", "wml\0"},
    {MIME_SWF, "application/x-shockwave-flash\0", "swf\0"},
    {MIME_XLS, "application/vnd.ms-excel\0", "xls\0"},
    {MIME_PPT, "application/vnd.ms-powerpoint\0", "ppt\0"},
    {MIME_IMAGE_JPG, "image/jpeg\0image/jpg\0", "jpeg\0jpg\0"},
    {MIME_IMAGE_PJPG, "image/pjpeg\0", "pjpeg\0"},
    {MIME_IMAGE_PNG, "image/png\0", "png\0"},
    {MIME_IMAGE_GIF, "image/gif\0", "gif\0"},
    {MIME_DOCX, "application/vnd.openxmlformats-officedocument.wordprocessingml.document\0", "docx\0"},
    {MIME_ODT, "application/vnd.oasis.opendocument.text\0", "odt\0"},
    {MIME_ODP, "application/vnd.oasis.opendocument.presentation\0", "odp\0"},
    {MIME_ODS, "application/vnd.oasis.opendocument.spreadsheet\0", "ods\0"},
    {MIME_UNKNOWN, nullptr, nullptr},
    {MIME_IMAGE_BMP, "image/bmp\0image/x-ms-bmp\0image/x-windows-bmp\0", "bmp\0"},
    {MIME_WAV, "audio/x-wav\0", "wav\0"},
    {MIME_ARCHIVE, "application/x-archive\0application/x-tar\0application/x-ustar\0application/x-gtar\0application/x-bzip2\0application/x-rar\0", "tar\0rar\0bzip2\0"},
    {MIME_EXE, "application/exe\0application/octet-stream\0application/x-dosexec\0application/x-msdownload\0", "exe\0"},
    {MIME_ODG, "application/vnd.oasis.opendocument.graphics\0", "odg\0"},
    {MIME_GZIP, "application/x-gzip\0", "gz\0gzip\0"},
    {MIME_XLSX, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet\0", "xlsx\0"},
    {MIME_PPTX, "application/vnd.openxmlformats-officedocument.presentationml.presentation\0", "pptx\0"},
    {MIME_JAVASCRIPT, "application/javascript\0text/javascript\0", "js\0"},
    {MIME_EPUB, "application/epub+zip\0", "epub\0"},
    {MIME_TEX, "application/x-tex\0application/x-latex\0text/x-tex\0", "tex\0"},
    {MIME_JSON, "application/json\0application/x-amz-json-1.0\0application/x-amz-json-1.1\0", "json\0"},
    {MIME_APK, "application/vnd.android.package-archive\0", "apk\0"},
    {MIME_CSS, "text/css\0", "css\0"},
    {MIME_IMAGE_WEBP, "image/webp\0", "webp\0"},
    {MIME_DJVU, "image/vnd.djvu\0image/x-djvu\0", "djvu\0djv\0"},
    {MIME_CHM, "application/x-chm\0application/vnd.ms-htmlhelp\0", "chm\0"},
    {MIME_FB2ZIP, "application/zip\0", "fb2zip\0"},
    {MIME_IMAGE_TIFF, "image/tiff\0image/tiff-fx\0", "tif\0tiff\0"},
    {MIME_IMAGE_PNM, "image/x-portable-anymap\0", "pnm\0pgm\0ppm\0pbm\0"},
    {MIME_IMAGE_SVG, "image/svg+xml\0", "svg\0"},
    {MIME_IMAGE_ICON, "image/x-icon\0image/vnd.microsoft.icon\0", "ico\0"},
    {MIME_WOFF, "font/woff\0", "woff\0"},
    {MIME_WOFF2, "font/woff2\0", "woff2\0"},
    {MIME_TTF, "font/ttf\0", "ttf\0"},
    {MIME_WEBMANIFEST, "application/manifest+json\0", "webmanifest\0"},
    {MIME_CBOR, "application/cbor\0application/x-amz-cbor-1.1\0", "cbor\0"},
    {MIME_CSV, "text/csv\0", "csv\0"},
    {MIME_VIDEO_MP4, "video/mp4\0", "mp4\0"},
    {MIME_VIDEO_AVI, "video/x-msvideo\0", "avi\0"},
    {MIME_MAX, nullptr, nullptr},

    // Additional records
    {MIME_HTML, "application/xhtml+xml\0", "xhtml\0"},
};

TMimeTypes::TMimeTypes()
    : ContentTypes()
    , Ext()
{
    SetContentTypes();
    SetExt();
}

void TMimeTypes::SetContentTypes() {
    for (int i = 0; i < (int)Y_ARRAY_SIZE(Records); ++i) {
        const TRecord& record(Records[i]);
        assert(i == record.Mime || i > MIME_MAX || record.Mime == MIME_UNKNOWN);
        if (!record.ContentType)
            continue;
        for (const char* type = record.ContentType; *type; type += strlen(type) + 1) {
            assert(ContentTypes.find(type) == ContentTypes.end());
            ContentTypes[type] = i;
        }
    }
}

void TMimeTypes::SetExt() {
    for (int i = 0; i < (int)Y_ARRAY_SIZE(Records); ++i) {
        const TRecord& record(Records[i]);
        if (!record.Ext)
            continue;
        for (const char* ext = record.Ext; *ext; ext += strlen(ext) + 1) {
            assert(strlen(ext) <= MAX_EXT_LEN);
            assert(Ext.find(ext) == Ext.end());
            Ext[ext] = i;
        }
    }
}

const char* TMimeTypes::StrByExt(const char* ext) const {
    TRecordHash::const_iterator it = Ext.find(ext);
    if (it == Ext.end())
        return nullptr;
    return Records[it->second].ContentType;
}

MimeTypes TMimeTypes::MimeByStr(const char* str) const {
    TRecordHash::const_iterator it = ContentTypes.find(str);
    if (it == ContentTypes.end())
        return MIME_UNKNOWN;
    return Records[it->second].Mime;
}

MimeTypes TMimeTypes::MimeByStr(const TStringBuf& str) const {
    TRecordHash::const_iterator it = ContentTypes.find(str);
    if (it == ContentTypes.end())
        return MIME_UNKNOWN;
    return Records[it->second].Mime;
}

const char* TMimeTypes::StrByMime(MimeTypes mime) const {
    return Records[mime].ContentType;
}

const char* mimetypeByExt(const char* fname, const char* check_ext) {
    const char* ext_p;
    if (fname == nullptr || *fname == 0 ||
        (ext_p = strrchr(fname, '.')) == nullptr || strlen(ext_p) - 1 > TMimeTypes::MAX_EXT_LEN) {
        return nullptr;
    }

    char ext[TMimeTypes::MAX_EXT_LEN + 1];
    size_t i;
    ext_p++;
    for (i = 0; i < TMimeTypes::MAX_EXT_LEN && ext_p[i]; i++)
        ext[i] = (char)tolower(ext_p[i]);
    ext[i] = 0;

    if (check_ext != nullptr) {
        if (strcmp(ext, check_ext) == 0)
            return check_ext;
        else
            return nullptr;
    }

    return Singleton<TMimeTypes>()->StrByExt(ext);
}

MimeTypes mimeByStr(const char* mimeStr) {
    return Singleton<TMimeTypes>()->MimeByStr(mimeStr);
}

MimeTypes mimeByStr(const TStringBuf& mimeStr) {
    return Singleton<TMimeTypes>()->MimeByStr(mimeStr);
}

const char* strByMime(MimeTypes mime) {
    if (mime < 0 || mime > MIME_MAX)
        return nullptr; // index may contain documents with invalid MIME (ex. 255)
    return Singleton<TMimeTypes>()->StrByMime(mime);
}

const char* MimeNames[MIME_MAX] = {
    "unknown", // MIME_UNKNOWN         //  0
    "text",    // MIME_TEXT            //  1
    "html",    // MIME_HTML            //  2
    "pdf",     // MIME_PDF             //  3
    "rtf",     // MIME_RTF             //  4
    "doc",     // MIME_DOC             //  5
    "mpeg",    // MIME_MPEG            //  6
    "xml",     // MIME_XML             //  7
    "wap",     // MIME_WML             //  8
    "swf",     // MIME_SWF             //  9
    "xls",     // MIME_XLS             // 10
    "ppt",     // MIME_PPT             // 11
    "jpg",     // MIME_IMAGE_JPG       // 12
    "pjpg",    // MIME_IMAGE_PJPG      // 13
    "png",     // MIME_IMAGE_PNG       // 14
    "gif",     // MIME_IMAGE_GIF       // 15
    "docx",    // MIME_DOCX            // 16
    "odt",     // MIME_ODT             // 17
    "odp",     // MIME_ODP             // 18
    "ods",     // MIME_ODS             // 19
    "xmlhtml", // MIME_XHTMLXML        // 20
    "bmp",     // MIME_IMAGE_BMP       // 21
    "wav",     // MIME_WAV             // 22
    "archive", // MIME_ARCHIVE         // 23
    "exe",     // MIME_EXE             // 24
    "odg",     // MIME_ODG             // 25
    "gzip",    // MIME_GZIP            // 26
    "xlsx",    // MIME_XLSX            // 27
    "pptx",    // MIME_PPTX            // 28
    "js",      // MIME_JAVASCRIPT      // 29
    "epub",    // MIME_EPUB            // 30
    "tex",     // MIME_TEX             // 31
    "json",    // MIME_JSON            // 32
    "apk",     // MIME_APK             // 33
    "css",     // MIME_CSS             // 34
    "webp",    // MIME_IMAGE_WEBP      // 35
    "djvu",    // MIME_DJVU            // 36
    "chm",     // MIME_CHM             // 37
    "fb2zip",  // MIME_FB2ZIP          // 38
    "tiff",    // MIME_IMAGE_TIFF      // 39
    "pnm",     // MIME_IMAGE_PNM       // 40
    "svg",     // MIME_IMAGE_SVG       // 41
    "ico",     // MIME_IMAGE_ICON      // 42
    "woff",    // MIME_WOFF            // 43
    "woff2",   // MIME_WOFF2           // 44
    "ttf",     // MIME_TTF             // 45
    "webmanifest", // MIME_WEBMANIFEST // 46
    "cbor",    // MIME_CBOR            // 47
    "csv",     // MIME_CSV             // 48
    "mp4",     // MIME_VIDEO_MP4       // 49
    "avi",     // MIME_VIDEO_AVI       // 50
};
