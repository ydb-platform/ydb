/******************************************************************************
 * Project:  PROJ
 * Purpose:  Grid management
 * Author:   Even Rouault, <even.rouault at spatialys.com>
 *
 ******************************************************************************
 * Copyright (c) 2000, Frank Warmerdam <warmerdam@pobox.com>
 * Copyright (c) 2019, Even Rouault, <even.rouault at spatialys.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *****************************************************************************/

#ifndef FROM_PROJ_CPP
#define FROM_PROJ_CPP
#endif
#define LRU11_DO_NOT_DEFINE_OUT_OF_CLASS_METHODS

#include "grids.hpp"
#include "filemanager.hpp"
#include "proj/internal/internal.hpp"
#include "proj/internal/lru_cache.hpp"
#include "proj_internal.h"

#ifdef TIFF_ENABLED
#include "tiffio.h"
#endif

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>

NS_PROJ_START

using namespace internal;

/************************************************************************/
/*                             swap_words()                             */
/*                                                                      */
/*      Convert the byte order of the given word(s) in place.           */
/************************************************************************/

static const int byte_order_test = 1;
#define IS_LSB                                                                 \
    (1 == (reinterpret_cast<const unsigned char *>(&byte_order_test))[0])

static void swap_words(void *dataIn, size_t word_size, size_t word_count)

{
    unsigned char *data = static_cast<unsigned char *>(dataIn);
    for (size_t word = 0; word < word_count; word++) {
        for (size_t i = 0; i < word_size / 2; i++) {
            unsigned char t;

            t = data[i];
            data[i] = data[word_size - i - 1];
            data[word_size - i - 1] = t;
        }

        data += word_size;
    }
}

// ---------------------------------------------------------------------------

void ExtentAndRes::computeInvRes() {
    invResX = 1.0 / resX;
    invResY = 1.0 / resY;
}

// ---------------------------------------------------------------------------

bool ExtentAndRes::fullWorldLongitude() const {
    return isGeographic && east - west + resX >= 2 * M_PI - 1e-10;
}

// ---------------------------------------------------------------------------

bool ExtentAndRes::contains(const ExtentAndRes &other) const {
    return other.west >= west && other.east <= east && other.south >= south &&
           other.north <= north;
}

// ---------------------------------------------------------------------------

bool ExtentAndRes::intersects(const ExtentAndRes &other) const {
    return other.west < east && west <= other.west && other.south < north &&
           south <= other.north;
}

// ---------------------------------------------------------------------------

Grid::Grid(const std::string &nameIn, int widthIn, int heightIn,
           const ExtentAndRes &extentIn)
    : m_name(nameIn), m_width(widthIn), m_height(heightIn), m_extent(extentIn) {
}

// ---------------------------------------------------------------------------

Grid::~Grid() = default;

// ---------------------------------------------------------------------------

VerticalShiftGrid::VerticalShiftGrid(const std::string &nameIn, int widthIn,
                                     int heightIn, const ExtentAndRes &extentIn)
    : Grid(nameIn, widthIn, heightIn, extentIn) {}

// ---------------------------------------------------------------------------

VerticalShiftGrid::~VerticalShiftGrid() = default;

// ---------------------------------------------------------------------------

static ExtentAndRes globalExtent() {
    ExtentAndRes extent;
    extent.isGeographic = true;
    extent.west = -M_PI;
    extent.south = -M_PI / 2;
    extent.east = M_PI;
    extent.north = M_PI / 2;
    extent.resX = M_PI;
    extent.resY = M_PI / 2;
    extent.computeInvRes();
    return extent;
}

// ---------------------------------------------------------------------------

static const std::string emptyString;

class NullVerticalShiftGrid : public VerticalShiftGrid {

  public:
    NullVerticalShiftGrid() : VerticalShiftGrid("null", 3, 3, globalExtent()) {}

    bool isNullGrid() const override { return true; }
    bool valueAt(int, int, float &out) const override;
    bool isNodata(float, double) const override { return false; }
    void reassign_context(PJ_CONTEXT *) override {}
    bool hasChanged() const override { return false; }
    const std::string &metadataItem(const std::string &, int) const override {
        return emptyString;
    }
};

// ---------------------------------------------------------------------------

bool NullVerticalShiftGrid::valueAt(int, int, float &out) const {
    out = 0.0f;
    return true;
}

// ---------------------------------------------------------------------------

class FloatLineCache {

  private:
    typedef uint64_t Key;
    lru11::Cache<Key, std::vector<float>, lru11::NullLock> cache_;

  public:
    explicit FloatLineCache(size_t maxSize) : cache_(maxSize) {}
    void insert(uint32_t subgridIdx, uint32_t lineNumber,
                const std::vector<float> &data);
    const std::vector<float> *get(uint32_t subgridIdx, uint32_t lineNumber);
};

// ---------------------------------------------------------------------------

void FloatLineCache::insert(uint32_t subgridIdx, uint32_t lineNumber,
                            const std::vector<float> &data) {
    cache_.insert((static_cast<uint64_t>(subgridIdx) << 32) | lineNumber, data);
}

// ---------------------------------------------------------------------------

const std::vector<float> *FloatLineCache::get(uint32_t subgridIdx,
                                              uint32_t lineNumber) {
    return cache_.getPtr((static_cast<uint64_t>(subgridIdx) << 32) |
                         lineNumber);
}

// ---------------------------------------------------------------------------

class GTXVerticalShiftGrid : public VerticalShiftGrid {
    PJ_CONTEXT *m_ctx;
    std::unique_ptr<File> m_fp;
    std::unique_ptr<FloatLineCache> m_cache;
    mutable std::vector<float> m_buffer{};

    GTXVerticalShiftGrid(const GTXVerticalShiftGrid &) = delete;
    GTXVerticalShiftGrid &operator=(const GTXVerticalShiftGrid &) = delete;

  public:
    explicit GTXVerticalShiftGrid(PJ_CONTEXT *ctx, std::unique_ptr<File> &&fp,
                                  const std::string &nameIn, int widthIn,
                                  int heightIn, const ExtentAndRes &extentIn,
                                  std::unique_ptr<FloatLineCache> &&cache)
        : VerticalShiftGrid(nameIn, widthIn, heightIn, extentIn), m_ctx(ctx),
          m_fp(std::move(fp)), m_cache(std::move(cache)) {}

    ~GTXVerticalShiftGrid() override;

    bool valueAt(int x, int y, float &out) const override;
    bool isNodata(float val, double multiplier) const override;

    const std::string &metadataItem(const std::string &, int) const override {
        return emptyString;
    }

    static GTXVerticalShiftGrid *open(PJ_CONTEXT *ctx, std::unique_ptr<File> fp,
                                      const std::string &name);

    void reassign_context(PJ_CONTEXT *ctx) override {
        m_ctx = ctx;
        m_fp->reassign_context(ctx);
    }

    bool hasChanged() const override { return m_fp->hasChanged(); }
};

// ---------------------------------------------------------------------------

GTXVerticalShiftGrid::~GTXVerticalShiftGrid() = default;

// ---------------------------------------------------------------------------

GTXVerticalShiftGrid *GTXVerticalShiftGrid::open(PJ_CONTEXT *ctx,
                                                 std::unique_ptr<File> fp,
                                                 const std::string &name) {
    unsigned char header[40];

    /* -------------------------------------------------------------------- */
    /*      Read the header.                                                */
    /* -------------------------------------------------------------------- */
    if (fp->read(header, sizeof(header)) != sizeof(header)) {
        pj_log(ctx, PJ_LOG_ERROR, _("Cannot read grid header"));
        proj_context_errno_set(ctx,
                               PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
        return nullptr;
    }

    /* -------------------------------------------------------------------- */
    /*      Regularize fields of interest and extract.                      */
    /* -------------------------------------------------------------------- */
    if (IS_LSB) {
        swap_words(header + 0, 8, 4);
        swap_words(header + 32, 4, 2);
    }

    double xorigin, yorigin, xstep, ystep;
    int rows, columns;

    memcpy(&yorigin, header + 0, 8);
    memcpy(&xorigin, header + 8, 8);
    memcpy(&ystep, header + 16, 8);
    memcpy(&xstep, header + 24, 8);

    memcpy(&rows, header + 32, 4);
    memcpy(&columns, header + 36, 4);

    if (columns <= 0 || rows <= 0 || xorigin < -360 || xorigin > 360 ||
        yorigin < -90 || yorigin > 90) {
        pj_log(ctx, PJ_LOG_ERROR,
               _("gtx file header has invalid extents, corrupt?"));
        proj_context_errno_set(ctx,
                               PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
        return nullptr;
    }

    /* some GTX files come in 0-360 and we shift them back into the
       expected -180 to 180 range if possible.  This does not solve
       problems with grids spanning the dateline. */
    if (xorigin >= 180.0)
        xorigin -= 360.0;

    if (xorigin >= 0.0 && xorigin + xstep * columns > 180.0) {
        pj_log(ctx, PJ_LOG_DEBUG,
               "This GTX spans the dateline!  This will cause problems.");
    }

    ExtentAndRes extent;
    extent.isGeographic = true;
    extent.west = xorigin * DEG_TO_RAD;
    extent.south = yorigin * DEG_TO_RAD;
    extent.resX = xstep * DEG_TO_RAD;
    extent.resY = ystep * DEG_TO_RAD;
    extent.east = (xorigin + xstep * (columns - 1)) * DEG_TO_RAD;
    extent.north = (yorigin + ystep * (rows - 1)) * DEG_TO_RAD;
    extent.computeInvRes();

    // Cache up to 1 megapixel per GTX file
    const int maxLinesInCache = 1024 * 1024 / columns;
    auto cache = std::make_unique<FloatLineCache>(maxLinesInCache);
    return new GTXVerticalShiftGrid(ctx, std::move(fp), name, columns, rows,
                                    extent, std::move(cache));
}

// ---------------------------------------------------------------------------

bool GTXVerticalShiftGrid::valueAt(int x, int y, float &out) const {
    assert(x >= 0 && y >= 0 && x < m_width && y < m_height);

    const std::vector<float> *pBuffer = m_cache->get(0, y);
    if (pBuffer == nullptr) {
        try {
            m_buffer.resize(m_width);
        } catch (const std::exception &e) {
            pj_log(m_ctx, PJ_LOG_ERROR, _("Exception %s"), e.what());
            return false;
        }

        const size_t nLineSizeInBytes = sizeof(float) * m_width;
        m_fp->seek(40 + nLineSizeInBytes * static_cast<unsigned long long>(y));
        if (m_fp->read(&m_buffer[0], nLineSizeInBytes) != nLineSizeInBytes) {
            proj_context_errno_set(
                m_ctx, PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
            return false;
        }

        if (IS_LSB) {
            swap_words(&m_buffer[0], sizeof(float), m_width);
        }

        out = m_buffer[x];
        try {
            m_cache->insert(0, y, m_buffer);
        } catch (const std::exception &e) {
            // Should normally not happen
            pj_log(m_ctx, PJ_LOG_ERROR, _("Exception %s"), e.what());
        }
    } else {
        out = (*pBuffer)[x];
    }

    return true;
}

// ---------------------------------------------------------------------------

bool GTXVerticalShiftGrid::isNodata(float val, double multiplier) const {
    /* nodata?  */
    /* GTX official nodata value if  -88.88880f, but some grids also */
    /* use other  big values for nodata (e.g naptrans2008.gtx has */
    /* nodata values like -2147479936), so test them too */
    return val * multiplier > 1000 || val * multiplier < -1000 ||
           val == -88.88880f;
}

// ---------------------------------------------------------------------------

VerticalShiftGridSet::VerticalShiftGridSet() = default;

// ---------------------------------------------------------------------------

VerticalShiftGridSet::~VerticalShiftGridSet() = default;

// ---------------------------------------------------------------------------

static bool IsTIFF(size_t header_size, const unsigned char *header) {
    // Test combinations of signature for ClassicTIFF/BigTIFF little/big endian
    return header_size >= 4 && (((header[0] == 'I' && header[1] == 'I') ||
                                 (header[0] == 'M' && header[1] == 'M')) &&
                                ((header[2] == 0x2A && header[3] == 0) ||
                                 (header[3] == 0x2A && header[2] == 0) ||
                                 (header[2] == 0x2B && header[3] == 0) ||
                                 (header[3] == 0x2B && header[2] == 0)));
}

#ifdef TIFF_ENABLED

// ---------------------------------------------------------------------------

enum class TIFFDataType { Int16, UInt16, Int32, UInt32, Float32, Float64 };

// ---------------------------------------------------------------------------

constexpr uint16_t TIFFTAG_GEOPIXELSCALE = 33550;
constexpr uint16_t TIFFTAG_GEOTIEPOINTS = 33922;
constexpr uint16_t TIFFTAG_GEOTRANSMATRIX = 34264;
constexpr uint16_t TIFFTAG_GEOKEYDIRECTORY = 34735;
constexpr uint16_t TIFFTAG_GEODOUBLEPARAMS = 34736;
constexpr uint16_t TIFFTAG_GEOASCIIPARAMS = 34737;
#ifndef TIFFTAG_GDAL_METADATA
// Starting with libtiff > 4.1.0, those symbolic names are #define in tiff.h
constexpr uint16_t TIFFTAG_GDAL_METADATA = 42112;
constexpr uint16_t TIFFTAG_GDAL_NODATA = 42113;
#endif

// ---------------------------------------------------------------------------

class BlockCache {
  public:
    void insert(uint32_t ifdIdx, uint32_t blockNumber,
                const std::vector<unsigned char> &data);
    const std::vector<unsigned char> *get(uint32_t ifdIdx,
                                          uint32_t blockNumber);

  private:
    typedef uint64_t Key;

    static constexpr int NUM_BLOCKS_AT_CROSSING_TILES = 4;
    static constexpr int MAX_SAMPLE_COUNT = 3;
    lru11::Cache<Key, std::vector<unsigned char>, lru11::NullLock> cache_{
        NUM_BLOCKS_AT_CROSSING_TILES * MAX_SAMPLE_COUNT};
};

// ---------------------------------------------------------------------------

void BlockCache::insert(uint32_t ifdIdx, uint32_t blockNumber,
                        const std::vector<unsigned char> &data) {
    cache_.insert((static_cast<uint64_t>(ifdIdx) << 32) | blockNumber, data);
}

// ---------------------------------------------------------------------------

const std::vector<unsigned char> *BlockCache::get(uint32_t ifdIdx,
                                                  uint32_t blockNumber) {
    return cache_.getPtr((static_cast<uint64_t>(ifdIdx) << 32) | blockNumber);
}

// ---------------------------------------------------------------------------

class GTiffGrid : public Grid {
    PJ_CONTEXT *m_ctx;   // owned by the belonging GTiffDataset
    TIFF *m_hTIFF;       // owned by the belonging GTiffDataset
    BlockCache &m_cache; // owned by the belonging GTiffDataset
    File *m_fp;          // owned by the belonging GTiffDataset
    uint32_t m_ifdIdx;
    TIFFDataType m_dt;
    uint16_t m_samplesPerPixel;
    uint16_t m_planarConfig; // set to -1 if m_samplesPerPixel == 1
    bool m_bottomUp;
    toff_t m_dirOffset;
    bool m_tiled;
    uint32_t m_blockWidth = 0;
    uint32_t m_blockHeight = 0;
    mutable std::vector<unsigned char> m_buffer{};
    mutable uint32_t m_bufferBlockId = std::numeric_limits<uint32_t>::max();
    unsigned m_blocksPerRow = 0;
    unsigned m_blocksPerCol = 0;
    unsigned m_blocks = 0;
    std::vector<double> m_adfOffset{};
    std::vector<double> m_adfScale{};
    std::map<std::pair<int, std::string>, std::string> m_metadata{};
    bool m_hasNodata = false;
    bool m_blockIs256Pixel = false;
    bool m_isSingleBlock = false;
    float m_noData = 0.0f;
    uint32_t m_subfileType = 0;

    GTiffGrid(const GTiffGrid &) = delete;
    GTiffGrid &operator=(const GTiffGrid &) = delete;

    template <class T>
    float readValue(const std::vector<unsigned char> &buffer,
                    uint32_t offsetInBlock, uint16_t sample) const;

  public:
    GTiffGrid(PJ_CONTEXT *ctx, TIFF *hTIFF, BlockCache &cache, File *fp,
              uint32_t ifdIdx, const std::string &nameIn, int widthIn,
              int heightIn, const ExtentAndRes &extentIn, TIFFDataType dtIn,
              uint16_t samplesPerPixelIn, uint16_t planarConfig,
              bool bottomUpIn);

    ~GTiffGrid() override;

    uint16_t samplesPerPixel() const { return m_samplesPerPixel; }

    bool valueAt(uint16_t sample, int x, int y, float &out) const;

    bool valuesAt(int x_start, int y_start, int x_count, int y_count,
                  int sample_count, const int *sample_idx, float *out,
                  bool &nodataFound) const;

    bool isNodata(float val) const;

    const std::string &metadataItem(const std::string &key,
                                    int sample = -1) const override;

    uint32_t subfileType() const { return m_subfileType; }

    void reassign_context(PJ_CONTEXT *ctx) { m_ctx = ctx; }

    bool hasChanged() const override { return m_fp->hasChanged(); }
};

// ---------------------------------------------------------------------------

GTiffGrid::GTiffGrid(PJ_CONTEXT *ctx, TIFF *hTIFF, BlockCache &cache, File *fp,
                     uint32_t ifdIdx, const std::string &nameIn, int widthIn,
                     int heightIn, const ExtentAndRes &extentIn,
                     TIFFDataType dtIn, uint16_t samplesPerPixelIn,
                     uint16_t planarConfig, bool bottomUpIn)
    : Grid(nameIn, widthIn, heightIn, extentIn), m_ctx(ctx), m_hTIFF(hTIFF),
      m_cache(cache), m_fp(fp), m_ifdIdx(ifdIdx), m_dt(dtIn),
      m_samplesPerPixel(samplesPerPixelIn),
      m_planarConfig(samplesPerPixelIn == 1 ? static_cast<uint16_t>(-1)
                                            : planarConfig),
      m_bottomUp(bottomUpIn), m_dirOffset(TIFFCurrentDirOffset(hTIFF)),
      m_tiled(TIFFIsTiled(hTIFF) != 0) {

    if (m_tiled) {
        TIFFGetField(m_hTIFF, TIFFTAG_TILEWIDTH, &m_blockWidth);
        TIFFGetField(m_hTIFF, TIFFTAG_TILELENGTH, &m_blockHeight);
    } else {
        m_blockWidth = widthIn;
        TIFFGetField(m_hTIFF, TIFFTAG_ROWSPERSTRIP, &m_blockHeight);
        if (m_blockHeight > static_cast<unsigned>(m_height))
            m_blockHeight = m_height;
    }

    m_blockIs256Pixel = (m_blockWidth == 256) && (m_blockHeight == 256);
    m_isSingleBlock = (m_blockWidth == static_cast<uint32_t>(m_width)) &&
                      (m_blockHeight == static_cast<uint32_t>(m_height));

    TIFFGetField(m_hTIFF, TIFFTAG_SUBFILETYPE, &m_subfileType);

    m_blocksPerRow = (m_width + m_blockWidth - 1) / m_blockWidth;
    m_blocksPerCol = (m_height + m_blockHeight - 1) / m_blockHeight;
    m_blocks = m_blocksPerRow * m_blocksPerCol;

    const char *text = nullptr;
    // Poor-man XML parsing of TIFFTAG_GDAL_METADATA tag. Hopefully good
    // enough for our purposes.
    if (TIFFGetField(m_hTIFF, TIFFTAG_GDAL_METADATA, &text)) {
        const char *ptr = text;
        while (true) {
            ptr = strstr(ptr, "<Item ");
            if (ptr == nullptr)
                break;
            const char *endTag = strchr(ptr, '>');
            if (endTag == nullptr)
                break;
            const char *endValue = strchr(endTag, '<');
            if (endValue == nullptr)
                break;

            std::string tag;
            tag.append(ptr, endTag - ptr);

            std::string value;
            value.append(endTag + 1, endValue - (endTag + 1));

            std::string gridName;
            auto namePos = tag.find("name=\"");
            if (namePos == std::string::npos)
                break;
            {
                namePos += strlen("name=\"");
                const auto endQuote = tag.find('"', namePos);
                if (endQuote == std::string::npos)
                    break;
                gridName = tag.substr(namePos, endQuote - namePos);
            }

            const auto samplePos = tag.find("sample=\"");
            int sample = -1;
            if (samplePos != std::string::npos) {
                sample = atoi(tag.c_str() + samplePos + strlen("sample=\""));
            }

            m_metadata[std::pair<int, std::string>(sample, gridName)] = value;

            auto rolePos = tag.find("role=\"");
            if (rolePos != std::string::npos) {
                rolePos += strlen("role=\"");
                const auto endQuote = tag.find('"', rolePos);
                if (endQuote == std::string::npos)
                    break;
                const auto role = tag.substr(rolePos, endQuote - rolePos);
                if (role == "offset") {
                    if (sample >= 0 &&
                        static_cast<unsigned>(sample) <= m_samplesPerPixel) {
                        try {
                            if (m_adfOffset.empty()) {
                                m_adfOffset.resize(m_samplesPerPixel);
                                m_adfScale.resize(m_samplesPerPixel, 1);
                            }
                            m_adfOffset[sample] = c_locale_stod(value);
                        } catch (const std::exception &) {
                        }
                    }
                } else if (role == "scale") {
                    if (sample >= 0 &&
                        static_cast<unsigned>(sample) <= m_samplesPerPixel) {
                        try {
                            if (m_adfOffset.empty()) {
                                m_adfOffset.resize(m_samplesPerPixel);
                                m_adfScale.resize(m_samplesPerPixel, 1);
                            }
                            m_adfScale[sample] = c_locale_stod(value);
                        } catch (const std::exception &) {
                        }
                    }
                }
            }

            ptr = endValue + 1;
        }
    }

    if (TIFFGetField(m_hTIFF, TIFFTAG_GDAL_NODATA, &text)) {
        try {
            m_noData = static_cast<float>(c_locale_stod(text));
            m_hasNodata = true;
        } catch (const std::exception &) {
        }
    }

    auto oIter = m_metadata.find(std::pair<int, std::string>(-1, "grid_name"));
    if (oIter != m_metadata.end()) {
        m_name += ", " + oIter->second;
    }
}

// ---------------------------------------------------------------------------

GTiffGrid::~GTiffGrid() = default;

// ---------------------------------------------------------------------------

template <class T>
float GTiffGrid::readValue(const std::vector<unsigned char> &buffer,
                           uint32_t offsetInBlock, uint16_t sample) const {
    const auto ptr = reinterpret_cast<const T *>(buffer.data());
    assert(offsetInBlock < buffer.size() / sizeof(T));
    const auto val = ptr[offsetInBlock];
    if ((!m_hasNodata || static_cast<float>(val) != m_noData) &&
        sample < m_adfScale.size()) {
        double scale = m_adfScale[sample];
        double offset = m_adfOffset[sample];
        return static_cast<float>(val * scale + offset);
    } else {
        return static_cast<float>(val);
    }
}

// ---------------------------------------------------------------------------

bool GTiffGrid::valueAt(uint16_t sample, int x, int yFromBottom,
                        float &out) const {
    assert(x >= 0 && yFromBottom >= 0 && x < m_width && yFromBottom < m_height);
    assert(sample < m_samplesPerPixel);

    // All non-TIFF grids have the first rows in the file being the one
    // corresponding to the southern-most row. In GeoTIFF, the convention is
    // *generally* different (when m_bottomUp == false), TIFF being an
    // image-oriented image. If m_bottomUp == true, then we had GeoTIFF hints
    // that the first row of the image is the southern-most.
    const int yTIFF = m_bottomUp ? yFromBottom : m_height - 1 - yFromBottom;

    int blockXOff;
    int blockYOff;
    uint32_t blockId;

    if (m_blockIs256Pixel) {
        const int blockX = x / 256;
        blockXOff = x % 256;
        const int blockY = yTIFF / 256;
        blockYOff = yTIFF % 256;
        blockId = blockY * m_blocksPerRow + blockX;
    } else if (m_isSingleBlock) {
        blockXOff = x;
        blockYOff = yTIFF;
        blockId = 0;
    } else {
        const int blockX = x / m_blockWidth;
        blockXOff = x % m_blockWidth;
        const int blockY = yTIFF / m_blockHeight;
        blockYOff = yTIFF % m_blockHeight;
        blockId = blockY * m_blocksPerRow + blockX;
    }

    if (m_planarConfig == PLANARCONFIG_SEPARATE) {
        blockId += sample * m_blocks;
    }

    const std::vector<unsigned char> *pBuffer =
        blockId == m_bufferBlockId ? &m_buffer : m_cache.get(m_ifdIdx, blockId);
    if (pBuffer == nullptr) {
        if (TIFFCurrentDirOffset(m_hTIFF) != m_dirOffset &&
            !TIFFSetSubDirectory(m_hTIFF, m_dirOffset)) {
            return false;
        }
        if (m_buffer.empty()) {
            const auto blockSize = static_cast<size_t>(
                m_tiled ? TIFFTileSize64(m_hTIFF) : TIFFStripSize64(m_hTIFF));
            try {
                m_buffer.resize(blockSize);
            } catch (const std::exception &e) {
                pj_log(m_ctx, PJ_LOG_ERROR, _("Exception %s"), e.what());
                return false;
            }
        }

        if (m_tiled) {
            if (TIFFReadEncodedTile(m_hTIFF, blockId, m_buffer.data(),
                                    m_buffer.size()) == -1) {
                return false;
            }
        } else {
            if (TIFFReadEncodedStrip(m_hTIFF, blockId, m_buffer.data(),
                                     m_buffer.size()) == -1) {
                return false;
            }
        }

        pBuffer = &m_buffer;
        try {
            m_cache.insert(m_ifdIdx, blockId, m_buffer);
            m_bufferBlockId = blockId;
        } catch (const std::exception &e) {
            // Should normally not happen
            pj_log(m_ctx, PJ_LOG_ERROR, _("Exception %s"), e.what());
        }
    }

    uint32_t offsetInBlock;
    if (m_blockIs256Pixel)
        offsetInBlock = blockXOff + blockYOff * 256U;
    else
        offsetInBlock = blockXOff + blockYOff * m_blockWidth;
    if (m_planarConfig == PLANARCONFIG_CONTIG)
        offsetInBlock = offsetInBlock * m_samplesPerPixel + sample;

    switch (m_dt) {
    case TIFFDataType::Int16:
        out = readValue<short>(*pBuffer, offsetInBlock, sample);
        break;

    case TIFFDataType::UInt16:
        out = readValue<unsigned short>(*pBuffer, offsetInBlock, sample);
        break;

    case TIFFDataType::Int32:
        out = readValue<int>(*pBuffer, offsetInBlock, sample);
        break;

    case TIFFDataType::UInt32:
        out = readValue<unsigned int>(*pBuffer, offsetInBlock, sample);
        break;

    case TIFFDataType::Float32:
        out = readValue<float>(*pBuffer, offsetInBlock, sample);
        break;

    case TIFFDataType::Float64:
        out = readValue<double>(*pBuffer, offsetInBlock, sample);
        break;
    }

    return true;
}

// ---------------------------------------------------------------------------

bool GTiffGrid::valuesAt(int x_start, int y_start, int x_count, int y_count,
                         int sample_count, const int *sample_idx, float *out,
                         bool &nodataFound) const {
    const auto getTIFFRow = [this](int y) {
        return m_bottomUp ? y : m_height - 1 - y;
    };
    nodataFound = false;
    if (m_blockIs256Pixel && m_planarConfig == PLANARCONFIG_CONTIG &&
        m_dt == TIFFDataType::Float32 &&
        (x_start / 256) == (x_start + x_count - 1) / 256 &&
        getTIFFRow(y_start) / 256 == getTIFFRow(y_start + y_count - 1) / 256 &&
        !m_hasNodata && m_adfScale.empty() &&
        (sample_count == 1 ||
         (sample_count == 2 && sample_idx[1] == sample_idx[0] + 1) ||
         (sample_count == 3 && sample_idx[1] == sample_idx[0] + 1 &&
          sample_idx[2] == sample_idx[0] + 2))) {
        const int yTIFF = m_bottomUp ? y_start : m_height - (y_start + y_count);
        int blockXOff;
        int blockYOff;
        uint32_t blockId;
        const int blockX = x_start / 256;
        blockXOff = x_start % 256;
        const int blockY = yTIFF / 256;
        blockYOff = yTIFF % 256;
        blockId = blockY * m_blocksPerRow + blockX;

        const std::vector<unsigned char> *pBuffer =
            blockId == m_bufferBlockId ? &m_buffer
                                       : m_cache.get(m_ifdIdx, blockId);
        if (pBuffer == nullptr) {
            if (TIFFCurrentDirOffset(m_hTIFF) != m_dirOffset &&
                !TIFFSetSubDirectory(m_hTIFF, m_dirOffset)) {
                return false;
            }
            if (m_buffer.empty()) {
                const auto blockSize =
                    static_cast<size_t>(m_tiled ? TIFFTileSize64(m_hTIFF)
                                                : TIFFStripSize64(m_hTIFF));
                try {
                    m_buffer.resize(blockSize);
                } catch (const std::exception &e) {
                    pj_log(m_ctx, PJ_LOG_ERROR, _("Exception %s"), e.what());
                    return false;
                }
            }

            if (m_tiled) {
                if (TIFFReadEncodedTile(m_hTIFF, blockId, m_buffer.data(),
                                        m_buffer.size()) == -1) {
                    return false;
                }
            } else {
                if (TIFFReadEncodedStrip(m_hTIFF, blockId, m_buffer.data(),
                                         m_buffer.size()) == -1) {
                    return false;
                }
            }

            pBuffer = &m_buffer;
            try {
                m_cache.insert(m_ifdIdx, blockId, m_buffer);
                m_bufferBlockId = blockId;
            } catch (const std::exception &e) {
                // Should normally not happen
                pj_log(m_ctx, PJ_LOG_ERROR, _("Exception %s"), e.what());
            }
        }

        uint32_t offsetInBlockStart = blockXOff + blockYOff * 256U;

        if (sample_count == m_samplesPerPixel) {
            const int sample_count_mul_x_count = sample_count * x_count;
            for (int y = 0; y < y_count; ++y) {
                uint32_t offsetInBlock =
                    (offsetInBlockStart +
                     256 * (m_bottomUp ? y : y_count - 1 - y)) *
                        m_samplesPerPixel +
                    sample_idx[0];
                memcpy(out,
                       reinterpret_cast<const float *>(pBuffer->data()) +
                           offsetInBlock,
                       sample_count_mul_x_count * sizeof(float));
                out += sample_count_mul_x_count;
            }
        } else {
            switch (sample_count) {
            case 1:
                for (int y = 0; y < y_count; ++y) {
                    uint32_t offsetInBlock =
                        (offsetInBlockStart +
                         256 * (m_bottomUp ? y : y_count - 1 - y)) *
                            m_samplesPerPixel +
                        sample_idx[0];
                    const float *in_ptr =
                        reinterpret_cast<const float *>(pBuffer->data()) +
                        offsetInBlock;
                    for (int x = 0; x < x_count; ++x) {
                        memcpy(out, in_ptr, sample_count * sizeof(float));
                        in_ptr += m_samplesPerPixel;
                        out += sample_count;
                    }
                }
                break;
            case 2:
                for (int y = 0; y < y_count; ++y) {
                    uint32_t offsetInBlock =
                        (offsetInBlockStart +
                         256 * (m_bottomUp ? y : y_count - 1 - y)) *
                            m_samplesPerPixel +
                        sample_idx[0];
                    const float *in_ptr =
                        reinterpret_cast<const float *>(pBuffer->data()) +
                        offsetInBlock;
                    for (int x = 0; x < x_count; ++x) {
                        memcpy(out, in_ptr, sample_count * sizeof(float));
                        in_ptr += m_samplesPerPixel;
                        out += sample_count;
                    }
                }
                break;
            case 3:
                for (int y = 0; y < y_count; ++y) {
                    uint32_t offsetInBlock =
                        (offsetInBlockStart +
                         256 * (m_bottomUp ? y : y_count - 1 - y)) *
                            m_samplesPerPixel +
                        sample_idx[0];
                    const float *in_ptr =
                        reinterpret_cast<const float *>(pBuffer->data()) +
                        offsetInBlock;
                    for (int x = 0; x < x_count; ++x) {
                        memcpy(out, in_ptr, sample_count * sizeof(float));
                        in_ptr += m_samplesPerPixel;
                        out += sample_count;
                    }
                }
                break;
            }
        }
        return true;
    }

    for (int y = y_start; y < y_start + y_count; ++y) {
        for (int x = x_start; x < x_start + x_count; ++x) {
            for (int isample = 0; isample < sample_count; ++isample) {
                if (!valueAt(static_cast<uint16_t>(sample_idx[isample]), x, y,
                             *out))
                    return false;
                if (isNodata(*out)) {
                    nodataFound = true;
                }
                ++out;
            }
        }
    }
    return true;
}

// ---------------------------------------------------------------------------

bool GTiffGrid::isNodata(float val) const {
    return (m_hasNodata && val == m_noData) || std::isnan(val);
}

// ---------------------------------------------------------------------------

const std::string &GTiffGrid::metadataItem(const std::string &key,
                                           int sample) const {
    auto iter = m_metadata.find(std::pair<int, std::string>(sample, key));
    if (iter == m_metadata.end()) {
        return emptyString;
    }
    return iter->second;
}

// ---------------------------------------------------------------------------

class GTiffDataset {
    PJ_CONTEXT *m_ctx;
    std::unique_ptr<File> m_fp;
    TIFF *m_hTIFF = nullptr;
    bool m_hasNextGrid = false;
    uint32_t m_ifdIdx = 0;
    toff_t m_nextDirOffset = 0;
    std::string m_filename{};
    BlockCache m_cache{};

    GTiffDataset(const GTiffDataset &) = delete;
    GTiffDataset &operator=(const GTiffDataset &) = delete;

    // libtiff I/O routines
    static tsize_t tiffReadProc(thandle_t fd, tdata_t buf, tsize_t size) {
        GTiffDataset *self = static_cast<GTiffDataset *>(fd);
        return self->m_fp->read(buf, size);
    }

    static tsize_t tiffWriteProc(thandle_t, tdata_t, tsize_t) {
        assert(false);
        return 0;
    }

    static toff_t tiffSeekProc(thandle_t fd, toff_t off, int whence) {
        GTiffDataset *self = static_cast<GTiffDataset *>(fd);
        if (self->m_fp->seek(off, whence))
            return static_cast<toff_t>(self->m_fp->tell());
        else
            return static_cast<toff_t>(-1);
    }

    static int tiffCloseProc(thandle_t) {
        // done in destructor
        return 0;
    }

    static toff_t tiffSizeProc(thandle_t fd) {
        GTiffDataset *self = static_cast<GTiffDataset *>(fd);
        const auto old_off = self->m_fp->tell();
        self->m_fp->seek(0, SEEK_END);
        const auto file_size = static_cast<toff_t>(self->m_fp->tell());
        self->m_fp->seek(old_off);
        return file_size;
    }

    static int tiffMapProc(thandle_t, tdata_t *, toff_t *) { return (0); }

    static void tiffUnmapProc(thandle_t, tdata_t, toff_t) {}

  public:
    GTiffDataset(PJ_CONTEXT *ctx, std::unique_ptr<File> &&fp)
        : m_ctx(ctx), m_fp(std::move(fp)) {}
    virtual ~GTiffDataset();

    bool openTIFF(const std::string &filename);

    std::unique_ptr<GTiffGrid> nextGrid();

    void reassign_context(PJ_CONTEXT *ctx) {
        m_ctx = ctx;
        m_fp->reassign_context(ctx);
    }
};

// ---------------------------------------------------------------------------

GTiffDataset::~GTiffDataset() {
    if (m_hTIFF)
        TIFFClose(m_hTIFF);
}

// ---------------------------------------------------------------------------
class OneTimeTIFFTagInit {

    static TIFFExtendProc ParentExtender;

    // Function called by libtiff when initializing a TIFF directory
    static void GTiffTagExtender(TIFF *tif) {
        static const TIFFFieldInfo xtiffFieldInfo[] = {
            // GeoTIFF tags
            {TIFFTAG_GEOPIXELSCALE, -1, -1, TIFF_DOUBLE, FIELD_CUSTOM, TRUE,
             TRUE, const_cast<char *>("GeoPixelScale")},
            {TIFFTAG_GEOTIEPOINTS, -1, -1, TIFF_DOUBLE, FIELD_CUSTOM, TRUE,
             TRUE, const_cast<char *>("GeoTiePoints")},
            {TIFFTAG_GEOTRANSMATRIX, -1, -1, TIFF_DOUBLE, FIELD_CUSTOM, TRUE,
             TRUE, const_cast<char *>("GeoTransformationMatrix")},

            {TIFFTAG_GEOKEYDIRECTORY, -1, -1, TIFF_SHORT, FIELD_CUSTOM, TRUE,
             TRUE, const_cast<char *>("GeoKeyDirectory")},
            {TIFFTAG_GEODOUBLEPARAMS, -1, -1, TIFF_DOUBLE, FIELD_CUSTOM, TRUE,
             TRUE, const_cast<char *>("GeoDoubleParams")},
            {TIFFTAG_GEOASCIIPARAMS, -1, -1, TIFF_ASCII, FIELD_CUSTOM, TRUE,
             FALSE, const_cast<char *>("GeoASCIIParams")},

            // GDAL tags
            {TIFFTAG_GDAL_METADATA, -1, -1, TIFF_ASCII, FIELD_CUSTOM, TRUE,
             FALSE, const_cast<char *>("GDALMetadata")},
            {TIFFTAG_GDAL_NODATA, -1, -1, TIFF_ASCII, FIELD_CUSTOM, TRUE, FALSE,
             const_cast<char *>("GDALNoDataValue")},

        };

        if (ParentExtender)
            (*ParentExtender)(tif);

        TIFFMergeFieldInfo(tif, xtiffFieldInfo,
                           sizeof(xtiffFieldInfo) / sizeof(xtiffFieldInfo[0]));
    }

  public:
    OneTimeTIFFTagInit() {
        assert(ParentExtender == nullptr);
        // Install our TIFF tag extender
        ParentExtender = TIFFSetTagExtender(GTiffTagExtender);
    }
};

TIFFExtendProc OneTimeTIFFTagInit::ParentExtender = nullptr;

// ---------------------------------------------------------------------------

bool GTiffDataset::openTIFF(const std::string &filename) {
    static OneTimeTIFFTagInit oneTimeTIFFTagInit;
    m_hTIFF =
        TIFFClientOpen(filename.c_str(), "r", static_cast<thandle_t>(this),
                       GTiffDataset::tiffReadProc, GTiffDataset::tiffWriteProc,
                       GTiffDataset::tiffSeekProc, GTiffDataset::tiffCloseProc,
                       GTiffDataset::tiffSizeProc, GTiffDataset::tiffMapProc,
                       GTiffDataset::tiffUnmapProc);

    m_filename = filename;
    m_hasNextGrid = true;
    return m_hTIFF != nullptr;
}
// ---------------------------------------------------------------------------

std::unique_ptr<GTiffGrid> GTiffDataset::nextGrid() {
    if (!m_hasNextGrid)
        return nullptr;
    if (m_nextDirOffset) {
        TIFFSetSubDirectory(m_hTIFF, m_nextDirOffset);
    }

    uint32_t width = 0;
    uint32_t height = 0;
    TIFFGetField(m_hTIFF, TIFFTAG_IMAGEWIDTH, &width);
    TIFFGetField(m_hTIFF, TIFFTAG_IMAGELENGTH, &height);
    if (width == 0 || height == 0 || width > INT_MAX || height > INT_MAX) {
        pj_log(m_ctx, PJ_LOG_ERROR, _("Invalid image size"));
        return nullptr;
    }

    uint16_t samplesPerPixel = 0;
    if (!TIFFGetField(m_hTIFF, TIFFTAG_SAMPLESPERPIXEL, &samplesPerPixel)) {
        pj_log(m_ctx, PJ_LOG_ERROR, _("Missing SamplesPerPixel tag"));
        return nullptr;
    }
    if (samplesPerPixel == 0) {
        pj_log(m_ctx, PJ_LOG_ERROR, _("Invalid SamplesPerPixel value"));
        return nullptr;
    }

    uint16_t bitsPerSample = 0;
    if (!TIFFGetField(m_hTIFF, TIFFTAG_BITSPERSAMPLE, &bitsPerSample)) {
        pj_log(m_ctx, PJ_LOG_ERROR, _("Missing BitsPerSample tag"));
        return nullptr;
    }

    uint16_t planarConfig = 0;
    if (!TIFFGetField(m_hTIFF, TIFFTAG_PLANARCONFIG, &planarConfig)) {
        pj_log(m_ctx, PJ_LOG_ERROR, _("Missing PlanarConfig tag"));
        return nullptr;
    }

    uint16_t sampleFormat = 0;
    if (!TIFFGetField(m_hTIFF, TIFFTAG_SAMPLEFORMAT, &sampleFormat)) {
        pj_log(m_ctx, PJ_LOG_ERROR, _("Missing SampleFormat tag"));
        return nullptr;
    }

    TIFFDataType dt;
    if (sampleFormat == SAMPLEFORMAT_INT && bitsPerSample == 16)
        dt = TIFFDataType::Int16;
    else if (sampleFormat == SAMPLEFORMAT_UINT && bitsPerSample == 16)
        dt = TIFFDataType::UInt16;
    else if (sampleFormat == SAMPLEFORMAT_INT && bitsPerSample == 32)
        dt = TIFFDataType::Int32;
    else if (sampleFormat == SAMPLEFORMAT_UINT && bitsPerSample == 32)
        dt = TIFFDataType::UInt32;
    else if (sampleFormat == SAMPLEFORMAT_IEEEFP && bitsPerSample == 32)
        dt = TIFFDataType::Float32;
    else if (sampleFormat == SAMPLEFORMAT_IEEEFP && bitsPerSample == 64)
        dt = TIFFDataType::Float64;
    else {
        pj_log(m_ctx, PJ_LOG_ERROR,
               _("Unsupported combination of SampleFormat "
                 "and BitsPerSample values"));
        return nullptr;
    }

    uint16_t photometric = PHOTOMETRIC_MINISBLACK;
    if (!TIFFGetField(m_hTIFF, TIFFTAG_PHOTOMETRIC, &photometric))
        photometric = PHOTOMETRIC_MINISBLACK;
    if (photometric != PHOTOMETRIC_MINISBLACK) {
        pj_log(m_ctx, PJ_LOG_ERROR, _("Unsupported Photometric value"));
        return nullptr;
    }

    uint16_t compression = COMPRESSION_NONE;
    if (!TIFFGetField(m_hTIFF, TIFFTAG_COMPRESSION, &compression))
        compression = COMPRESSION_NONE;

    if (compression != COMPRESSION_NONE &&
        !TIFFIsCODECConfigured(compression)) {
        pj_log(m_ctx, PJ_LOG_ERROR,
               _("Cannot open TIFF file due to missing codec."));
        return nullptr;
    }
    // We really don't want to try dealing with old-JPEG images
    if (compression == COMPRESSION_OJPEG) {
        pj_log(m_ctx, PJ_LOG_ERROR, _("Unsupported compression method."));
        return nullptr;
    }

    const auto blockSize = TIFFIsTiled(m_hTIFF) ? TIFFTileSize64(m_hTIFF)
                                                : TIFFStripSize64(m_hTIFF);
    if (blockSize == 0 || blockSize > 64 * 1024 * 2014) {
        pj_log(m_ctx, PJ_LOG_ERROR, _("Unsupported block size."));
        return nullptr;
    }

    unsigned short count = 0;
    unsigned short *geokeys = nullptr;
    bool pixelIsArea = false;

    ExtentAndRes extent;
    extent.isGeographic = true;

    if (!TIFFGetField(m_hTIFF, TIFFTAG_GEOKEYDIRECTORY, &count, &geokeys)) {
        pj_log(m_ctx, PJ_LOG_TRACE, "No GeoKeys tag");
    } else {
        if (count < 4 || (count % 4) != 0) {
            pj_log(m_ctx, PJ_LOG_ERROR,
                   _("Wrong number of values in GeoKeys tag"));
            return nullptr;
        }

        if (geokeys[0] != 1) {
            pj_log(m_ctx, PJ_LOG_ERROR, _("Unsupported GeoTIFF major version"));
            return nullptr;
        }
        // We only know that we support GeoTIFF 1.0 and 1.1 at that time
        if (geokeys[1] != 1 || geokeys[2] > 1) {
            pj_log(m_ctx, PJ_LOG_TRACE, "GeoTIFF %d.%d possibly not handled",
                   geokeys[1], geokeys[2]);
        }

        for (unsigned int i = 4; i + 3 < count; i += 4) {
            constexpr unsigned short GTModelTypeGeoKey = 1024;
            constexpr unsigned short ModelTypeProjected = 1;
            constexpr unsigned short ModelTypeGeographic = 2;

            constexpr unsigned short GTRasterTypeGeoKey = 1025;
            constexpr unsigned short RasterPixelIsArea = 1;
            // constexpr unsigned short RasterPixelIsPoint = 2;

            if (geokeys[i] == GTModelTypeGeoKey) {
                if (geokeys[i + 3] == ModelTypeProjected) {
                    extent.isGeographic = false;
                } else if (geokeys[i + 3] != ModelTypeGeographic) {
                    pj_log(m_ctx, PJ_LOG_ERROR,
                           _("Only GTModelTypeGeoKey = "
                             "ModelTypeGeographic or ModelTypeProjected are "
                             "supported"));
                    return nullptr;
                }
            } else if (geokeys[i] == GTRasterTypeGeoKey) {
                if (geokeys[i + 3] == RasterPixelIsArea) {
                    pixelIsArea = true;
                }
            }
        }
    }

    double hRes = 0;
    double vRes = 0;
    double west = 0;
    double north = 0;

    double *matrix = nullptr;
    if (TIFFGetField(m_hTIFF, TIFFTAG_GEOTRANSMATRIX, &count, &matrix) &&
        count == 16) {
        // If using GDAL to produce a bottom-up georeferencing, it will produce
        // a GeoTransformationMatrix, since negative values in GeoPixelScale
        // have historically been implementation bugs.
        if (matrix[1] != 0 || matrix[4] != 0) {
            pj_log(m_ctx, PJ_LOG_ERROR,
                   _("Rotational terms not supported in "
                     "GeoTransformationMatrix tag"));
            return nullptr;
        }

        west = matrix[3];
        hRes = matrix[0];
        north = matrix[7];
        vRes = -matrix[5]; // negation to simulate GeoPixelScale convention
    } else {
        double *geopixelscale = nullptr;
        if (TIFFGetField(m_hTIFF, TIFFTAG_GEOPIXELSCALE, &count,
                         &geopixelscale) != 1) {
            pj_log(m_ctx, PJ_LOG_ERROR, _("No GeoPixelScale tag"));
            return nullptr;
        }
        if (count != 3) {
            pj_log(m_ctx, PJ_LOG_ERROR,
                   _("Wrong number of values in GeoPixelScale tag"));
            return nullptr;
        }
        hRes = geopixelscale[0];
        vRes = geopixelscale[1];

        double *geotiepoints = nullptr;
        if (TIFFGetField(m_hTIFF, TIFFTAG_GEOTIEPOINTS, &count,
                         &geotiepoints) != 1) {
            pj_log(m_ctx, PJ_LOG_ERROR, _("No GeoTiePoints tag"));
            return nullptr;
        }
        if (count != 6) {
            pj_log(m_ctx, PJ_LOG_ERROR,
                   _("Wrong number of values in GeoTiePoints tag"));
            return nullptr;
        }

        west = geotiepoints[3] - geotiepoints[0] * hRes;
        north = geotiepoints[4] + geotiepoints[1] * vRes;
    }

    if (pixelIsArea) {
        west += 0.5 * hRes;
        north -= 0.5 * vRes;
    }

    const double mulFactor = extent.isGeographic ? DEG_TO_RAD : 1;
    extent.west = west * mulFactor;
    extent.north = north * mulFactor;
    extent.resX = hRes * mulFactor;
    extent.resY = fabs(vRes) * mulFactor;
    extent.east = (west + hRes * (width - 1)) * mulFactor;
    extent.south = (north - vRes * (height - 1)) * mulFactor;
    extent.computeInvRes();

    if (vRes < 0) {
        std::swap(extent.north, extent.south);
    }

    if (!((!extent.isGeographic ||
           (fabs(extent.west) <= 4 * M_PI && fabs(extent.east) <= 4 * M_PI &&
            fabs(extent.north) <= M_PI + 1e-5 &&
            fabs(extent.south) <= M_PI + 1e-5)) &&
          extent.west < extent.east && extent.south < extent.north &&
          extent.resX > 1e-10 && extent.resY > 1e-10)) {
        pj_log(m_ctx, PJ_LOG_ERROR, _("Inconsistent georeferencing for %s"),
               m_filename.c_str());
        return nullptr;
    }

    auto ret = std::unique_ptr<GTiffGrid>(new GTiffGrid(
        m_ctx, m_hTIFF, m_cache, m_fp.get(), m_ifdIdx, m_filename, width,
        height, extent, dt, samplesPerPixel, planarConfig, vRes < 0));
    m_ifdIdx++;
    m_hasNextGrid = TIFFReadDirectory(m_hTIFF) != 0;
    m_nextDirOffset = TIFFCurrentDirOffset(m_hTIFF);

    // If the TIFF file contains multiple grids, append the index of the grid
    // in the grid name to help debugging.
    if (m_ifdIdx >= 2 || m_hasNextGrid) {
        ret->m_name += " (index ";
        ret->m_name += std::to_string(m_ifdIdx); // 1-based
        ret->m_name += ')';
    }

    return ret;
}

// ---------------------------------------------------------------------------

class GTiffVGridShiftSet : public VerticalShiftGridSet {

    std::unique_ptr<GTiffDataset> m_GTiffDataset;

    GTiffVGridShiftSet(PJ_CONTEXT *ctx, std::unique_ptr<File> &&fp)
        : m_GTiffDataset(new GTiffDataset(ctx, std::move(fp))) {}

  public:
    ~GTiffVGridShiftSet() override;

    static std::unique_ptr<GTiffVGridShiftSet>
    open(PJ_CONTEXT *ctx, std::unique_ptr<File> fp,
         const std::string &filename);

    void reassign_context(PJ_CONTEXT *ctx) override {
        VerticalShiftGridSet::reassign_context(ctx);
        if (m_GTiffDataset) {
            m_GTiffDataset->reassign_context(ctx);
        }
    }

    bool reopen(PJ_CONTEXT *ctx) override {
        pj_log(ctx, PJ_LOG_DEBUG, "Grid %s has changed. Re-loading it",
               m_name.c_str());
        m_grids.clear();
        m_GTiffDataset.reset();
        auto fp = FileManager::open_resource_file(ctx, m_name.c_str());
        if (!fp) {
            return false;
        }
        auto newGS = open(ctx, std::move(fp), m_name);
        if (newGS) {
            m_grids = std::move(newGS->m_grids);
            m_GTiffDataset = std::move(newGS->m_GTiffDataset);
        }
        return !m_grids.empty();
    }
};

// ---------------------------------------------------------------------------

template <class GridType, class GenericGridType>
static void
insertIntoHierarchy(PJ_CONTEXT *ctx, std::unique_ptr<GridType> &&grid,
                    const std::string &gridName, const std::string &parentName,
                    std::vector<std::unique_ptr<GenericGridType>> &topGrids,
                    std::map<std::string, GridType *> &mapGrids) {
    const auto &extent = grid->extentAndRes();

    // If we have one or both of grid_name and parent_grid_name, try to use
    // the names to recreate the hierarchy
    if (!gridName.empty()) {
        if (mapGrids.find(gridName) != mapGrids.end()) {
            pj_log(ctx, PJ_LOG_DEBUG, "Several grids called %s found!",
                   gridName.c_str());
        }
        mapGrids[gridName] = grid.get();
    }

    if (!parentName.empty()) {
        auto iter = mapGrids.find(parentName);
        if (iter == mapGrids.end()) {
            pj_log(ctx, PJ_LOG_DEBUG,
                   "Grid %s refers to non-existing parent %s. "
                   "Using bounding-box method.",
                   gridName.c_str(), parentName.c_str());
        } else {
            if (iter->second->extentAndRes().contains(extent)) {
                iter->second->m_children.emplace_back(std::move(grid));
                return;
            } else {
                pj_log(ctx, PJ_LOG_DEBUG,
                       "Grid %s refers to parent %s, but its extent is "
                       "not included in it. Using bounding-box method.",
                       gridName.c_str(), parentName.c_str());
            }
        }
    } else if (!gridName.empty()) {
        topGrids.emplace_back(std::move(grid));
        return;
    }

    const std::string &type = grid->metadataItem("TYPE");

    // Fallback to analyzing spatial extents
    for (const auto &candidateParent : topGrids) {
        if (!type.empty() && candidateParent->metadataItem("TYPE") != type) {
            continue;
        }

        const auto &candidateParentExtent = candidateParent->extentAndRes();
        if (candidateParentExtent.contains(extent)) {
            static_cast<GridType *>(candidateParent.get())
                ->insertGrid(ctx, std::move(grid));
            return;
        } else if (candidateParentExtent.intersects(extent)) {
            pj_log(ctx, PJ_LOG_DEBUG, "Partially intersecting grids found!");
        }
    }

    topGrids.emplace_back(std::move(grid));
}

// ---------------------------------------------------------------------------

class GTiffVGrid : public VerticalShiftGrid {
    friend void insertIntoHierarchy<GTiffVGrid, VerticalShiftGrid>(
        PJ_CONTEXT *ctx, std::unique_ptr<GTiffVGrid> &&grid,
        const std::string &gridName, const std::string &parentName,
        std::vector<std::unique_ptr<VerticalShiftGrid>> &topGrids,
        std::map<std::string, GTiffVGrid *> &mapGrids);

    std::unique_ptr<GTiffGrid> m_grid;
    uint16_t m_idxSample;

  public:
    GTiffVGrid(std::unique_ptr<GTiffGrid> &&grid, uint16_t idxSample);

    ~GTiffVGrid() override;

    bool valueAt(int x, int y, float &out) const override {
        return m_grid->valueAt(m_idxSample, x, y, out);
    }

    bool isNodata(float val, double /* multiplier */) const override {
        return m_grid->isNodata(val);
    }

    const std::string &metadataItem(const std::string &key,
                                    int sample = -1) const override {
        return m_grid->metadataItem(key, sample);
    }

    void insertGrid(PJ_CONTEXT *ctx, std::unique_ptr<GTiffVGrid> &&subgrid);

    void reassign_context(PJ_CONTEXT *ctx) override {
        m_grid->reassign_context(ctx);
    }

    bool hasChanged() const override { return m_grid->hasChanged(); }
};

// ---------------------------------------------------------------------------

GTiffVGridShiftSet::~GTiffVGridShiftSet() = default;

// ---------------------------------------------------------------------------

GTiffVGrid::GTiffVGrid(std::unique_ptr<GTiffGrid> &&grid, uint16_t idxSample)
    : VerticalShiftGrid(grid->name(), grid->width(), grid->height(),
                        grid->extentAndRes()),
      m_grid(std::move(grid)), m_idxSample(idxSample) {}

// ---------------------------------------------------------------------------

GTiffVGrid::~GTiffVGrid() = default;

// ---------------------------------------------------------------------------

void GTiffVGrid::insertGrid(PJ_CONTEXT *ctx,
                            std::unique_ptr<GTiffVGrid> &&subgrid) {
    bool gridInserted = false;
    const auto &extent = subgrid->extentAndRes();
    for (const auto &candidateParent : m_children) {
        const auto &candidateParentExtent = candidateParent->extentAndRes();
        if (candidateParentExtent.contains(extent)) {
            static_cast<GTiffVGrid *>(candidateParent.get())
                ->insertGrid(ctx, std::move(subgrid));
            gridInserted = true;
            break;
        } else if (candidateParentExtent.intersects(extent)) {
            pj_log(ctx, PJ_LOG_DEBUG, "Partially intersecting grids found!");
        }
    }
    if (!gridInserted) {
        m_children.emplace_back(std::move(subgrid));
    }
}

// ---------------------------------------------------------------------------

std::unique_ptr<GTiffVGridShiftSet>
GTiffVGridShiftSet::open(PJ_CONTEXT *ctx, std::unique_ptr<File> fp,
                         const std::string &filename) {
    auto set = std::unique_ptr<GTiffVGridShiftSet>(
        new GTiffVGridShiftSet(ctx, std::move(fp)));
    set->m_name = filename;
    set->m_format = "gtiff";
    if (!set->m_GTiffDataset->openTIFF(filename)) {
        return nullptr;
    }
    uint16_t idxSample = 0;

    std::map<std::string, GTiffVGrid *> mapGrids;
    for (int ifd = 0;; ++ifd) {
        auto grid = set->m_GTiffDataset->nextGrid();
        if (!grid) {
            if (ifd == 0) {
                return nullptr;
            }
            break;
        }

        const auto subfileType = grid->subfileType();
        if (subfileType != 0 && subfileType != FILETYPE_PAGE) {
            if (ifd == 0) {
                pj_log(ctx, PJ_LOG_ERROR, _("Invalid subfileType"));
                return nullptr;
            } else {
                pj_log(ctx, PJ_LOG_DEBUG,
                       "Ignoring IFD %d as it has a unsupported subfileType",
                       ifd);
                continue;
            }
        }

        // Identify the index of the vertical correction
        bool foundDescriptionForAtLeastOneSample = false;
        bool foundDescriptionForShift = false;
        for (int i = 0; i < static_cast<int>(grid->samplesPerPixel()); ++i) {
            const auto &desc = grid->metadataItem("DESCRIPTION", i);
            if (!desc.empty()) {
                foundDescriptionForAtLeastOneSample = true;
            }
            if (desc == "geoid_undulation" || desc == "vertical_offset" ||
                desc == "hydroid_height" ||
                desc == "ellipsoidal_height_offset") {
                idxSample = static_cast<uint16_t>(i);
                foundDescriptionForShift = true;
            }
        }

        if (foundDescriptionForAtLeastOneSample) {
            if (!foundDescriptionForShift) {
                if (ifd > 0) {
                    // Assuming that extra IFD without our channel of interest
                    // can be ignored
                    // One could imagine to put the accuracy values in separate
                    // IFD for example
                    pj_log(ctx, PJ_LOG_DEBUG,
                           "Ignoring IFD %d as it has no "
                           "geoid_undulation/vertical_offset/hydroid_height/"
                           "ellipsoidal_height_offset channel",
                           ifd);
                    continue;
                } else {
                    pj_log(ctx, PJ_LOG_DEBUG,
                           "IFD 0 has channel descriptions, but no "
                           "geoid_undulation/vertical_offset/hydroid_height/"
                           "ellipsoidal_height_offset channel");
                    return nullptr;
                }
            }
        }

        if (idxSample >= grid->samplesPerPixel()) {
            pj_log(ctx, PJ_LOG_ERROR, _("Invalid sample index"));
            return nullptr;
        }

        const std::string &gridName = grid->metadataItem("grid_name");
        const std::string &parentName = grid->metadataItem("parent_grid_name");

        auto vgrid = std::make_unique<GTiffVGrid>(std::move(grid), idxSample);

        insertIntoHierarchy(ctx, std::move(vgrid), gridName, parentName,
                            set->m_grids, mapGrids);
    }
    return set;
}
#endif // TIFF_ENABLED

// ---------------------------------------------------------------------------

std::unique_ptr<VerticalShiftGridSet>
VerticalShiftGridSet::open(PJ_CONTEXT *ctx, const std::string &filename) {
    if (filename == "null") {
        auto set =
            std::unique_ptr<VerticalShiftGridSet>(new VerticalShiftGridSet());
        set->m_name = filename;
        set->m_format = "null";
        set->m_grids.push_back(std::unique_ptr<NullVerticalShiftGrid>(
            new NullVerticalShiftGrid()));
        return set;
    }

    auto fp = FileManager::open_resource_file(ctx, filename.c_str());
    if (!fp) {
        return nullptr;
    }
    const auto &actualName(fp->name());
    if (ends_with(actualName, "gtx") || ends_with(actualName, "GTX")) {
        auto grid = GTXVerticalShiftGrid::open(ctx, std::move(fp), actualName);
        if (!grid) {
            return nullptr;
        }
        auto set =
            std::unique_ptr<VerticalShiftGridSet>(new VerticalShiftGridSet());
        set->m_name = actualName;
        set->m_format = "gtx";
        set->m_grids.push_back(std::unique_ptr<VerticalShiftGrid>(grid));
        return set;
    }

    /* -------------------------------------------------------------------- */
    /*      Load a header, to determine the file type.                      */
    /* -------------------------------------------------------------------- */
    unsigned char header[4];
    size_t header_size = fp->read(header, sizeof(header));
    if (header_size != sizeof(header)) {
        return nullptr;
    }
    fp->seek(0);

    if (IsTIFF(header_size, header)) {
#ifdef TIFF_ENABLED
        auto set = std::unique_ptr<VerticalShiftGridSet>(
            GTiffVGridShiftSet::open(ctx, std::move(fp), actualName));
        if (!set)
            proj_context_errno_set(
                ctx, PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
        return set;
#else
        pj_log(ctx, PJ_LOG_ERROR,
               _("TIFF grid, but TIFF support disabled in this build"));
        return nullptr;
#endif
    }

    pj_log(ctx, PJ_LOG_ERROR,
           "Unrecognized vertical grid format for filename '%s'",
           filename.c_str());
    return nullptr;
}

// ---------------------------------------------------------------------------

bool VerticalShiftGridSet::reopen(PJ_CONTEXT *ctx) {
    pj_log(ctx, PJ_LOG_DEBUG, "Grid %s has changed. Re-loading it",
           m_name.c_str());
    auto newGS = open(ctx, m_name);
    m_grids.clear();
    if (newGS) {
        m_grids = std::move(newGS->m_grids);
    }
    return !m_grids.empty();
}

// ---------------------------------------------------------------------------

static bool isPointInExtent(double x, double y, const ExtentAndRes &extent,
                            double eps = 0) {
    if (!(y + eps >= extent.south && y - eps <= extent.north))
        return false;
    if (extent.fullWorldLongitude())
        return true;
    if (extent.isGeographic) {
        if (x + eps < extent.west)
            x += 2 * M_PI;
        else if (x - eps > extent.east)
            x -= 2 * M_PI;
    }
    if (!(x + eps >= extent.west && x - eps <= extent.east))
        return false;
    return true;
}

// ---------------------------------------------------------------------------

const VerticalShiftGrid *VerticalShiftGrid::gridAt(double longitude,
                                                   double lat) const {
    for (const auto &child : m_children) {
        const auto &extentChild = child->extentAndRes();
        if (isPointInExtent(longitude, lat, extentChild)) {
            return child->gridAt(longitude, lat);
        }
    }
    return this;
}
// ---------------------------------------------------------------------------

const VerticalShiftGrid *VerticalShiftGridSet::gridAt(double longitude,
                                                      double lat) const {
    for (const auto &grid : m_grids) {
        if (grid->isNullGrid()) {
            return grid.get();
        }
        const auto &extent = grid->extentAndRes();
        if (isPointInExtent(longitude, lat, extent)) {
            return grid->gridAt(longitude, lat);
        }
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

void VerticalShiftGridSet::reassign_context(PJ_CONTEXT *ctx) {
    for (const auto &grid : m_grids) {
        grid->reassign_context(ctx);
    }
}

// ---------------------------------------------------------------------------

HorizontalShiftGrid::HorizontalShiftGrid(const std::string &nameIn, int widthIn,
                                         int heightIn,
                                         const ExtentAndRes &extentIn)
    : Grid(nameIn, widthIn, heightIn, extentIn) {}

// ---------------------------------------------------------------------------

HorizontalShiftGrid::~HorizontalShiftGrid() = default;

// ---------------------------------------------------------------------------

HorizontalShiftGridSet::HorizontalShiftGridSet() = default;

// ---------------------------------------------------------------------------

HorizontalShiftGridSet::~HorizontalShiftGridSet() = default;

// ---------------------------------------------------------------------------

class NullHorizontalShiftGrid : public HorizontalShiftGrid {

  public:
    NullHorizontalShiftGrid()
        : HorizontalShiftGrid("null", 3, 3, globalExtent()) {}

    bool isNullGrid() const override { return true; }

    bool valueAt(int, int, bool, float &longShift,
                 float &latShift) const override;

    void reassign_context(PJ_CONTEXT *) override {}

    bool hasChanged() const override { return false; }

    const std::string &metadataItem(const std::string &, int) const override {
        return emptyString;
    }
};

// ---------------------------------------------------------------------------

bool NullHorizontalShiftGrid::valueAt(int, int, bool, float &longShift,
                                      float &latShift) const {
    longShift = 0.0f;
    latShift = 0.0f;
    return true;
}

// ---------------------------------------------------------------------------

static double to_double(const void *data) {
    double d;
    memcpy(&d, data, sizeof(d));
    return d;
}

// ---------------------------------------------------------------------------

class NTv1Grid : public HorizontalShiftGrid {
    PJ_CONTEXT *m_ctx;
    std::unique_ptr<File> m_fp;

    NTv1Grid(const NTv1Grid &) = delete;
    NTv1Grid &operator=(const NTv1Grid &) = delete;

  public:
    explicit NTv1Grid(PJ_CONTEXT *ctx, std::unique_ptr<File> &&fp,
                      const std::string &nameIn, int widthIn, int heightIn,
                      const ExtentAndRes &extentIn)
        : HorizontalShiftGrid(nameIn, widthIn, heightIn, extentIn), m_ctx(ctx),
          m_fp(std::move(fp)) {}

    ~NTv1Grid() override;

    bool valueAt(int, int, bool, float &longShift,
                 float &latShift) const override;

    static NTv1Grid *open(PJ_CONTEXT *ctx, std::unique_ptr<File> fp,
                          const std::string &filename);

    void reassign_context(PJ_CONTEXT *ctx) override {
        m_ctx = ctx;
        m_fp->reassign_context(ctx);
    }

    bool hasChanged() const override { return m_fp->hasChanged(); }

    const std::string &metadataItem(const std::string &, int) const override {
        return emptyString;
    }
};

// ---------------------------------------------------------------------------

NTv1Grid::~NTv1Grid() = default;

// ---------------------------------------------------------------------------

NTv1Grid *NTv1Grid::open(PJ_CONTEXT *ctx, std::unique_ptr<File> fp,
                         const std::string &filename) {
    unsigned char header[192];

    /* -------------------------------------------------------------------- */
    /*      Read the header.                                                */
    /* -------------------------------------------------------------------- */
    if (fp->read(header, sizeof(header)) != sizeof(header)) {
        proj_context_errno_set(ctx,
                               PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
        return nullptr;
    }

    /* -------------------------------------------------------------------- */
    /*      Regularize fields of interest.                                  */
    /* -------------------------------------------------------------------- */
    if (IS_LSB) {
        swap_words(header + 8, sizeof(int), 1);
        swap_words(header + 24, sizeof(double), 1);
        swap_words(header + 40, sizeof(double), 1);
        swap_words(header + 56, sizeof(double), 1);
        swap_words(header + 72, sizeof(double), 1);
        swap_words(header + 88, sizeof(double), 1);
        swap_words(header + 104, sizeof(double), 1);
    }

    int recordCount;
    memcpy(&recordCount, header + 8, sizeof(recordCount));
    if (recordCount != 12) {
        pj_log(ctx, PJ_LOG_ERROR,
               _("NTv1 grid shift file has wrong record count, corrupt?"));
        proj_context_errno_set(ctx,
                               PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
        return nullptr;
    }

    ExtentAndRes extent;
    extent.isGeographic = true;
    extent.west = -to_double(header + 72) * DEG_TO_RAD;
    extent.south = to_double(header + 24) * DEG_TO_RAD;
    extent.east = -to_double(header + 56) * DEG_TO_RAD;
    extent.north = to_double(header + 40) * DEG_TO_RAD;
    extent.resX = to_double(header + 104) * DEG_TO_RAD;
    extent.resY = to_double(header + 88) * DEG_TO_RAD;
    extent.computeInvRes();

    if (!(fabs(extent.west) <= 4 * M_PI && fabs(extent.east) <= 4 * M_PI &&
          fabs(extent.north) <= M_PI + 1e-5 &&
          fabs(extent.south) <= M_PI + 1e-5 && extent.west < extent.east &&
          extent.south < extent.north && extent.resX > 1e-10 &&
          extent.resY > 1e-10)) {
        pj_log(ctx, PJ_LOG_ERROR, _("Inconsistent georeferencing for %s"),
               filename.c_str());
        proj_context_errno_set(ctx,
                               PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
        return nullptr;
    }
    const int columns = static_cast<int>(
        fabs((extent.east - extent.west) * extent.invResX + 0.5) + 1);
    const int rows = static_cast<int>(
        fabs((extent.north - extent.south) * extent.invResY + 0.5) + 1);

    return new NTv1Grid(ctx, std::move(fp), filename, columns, rows, extent);
}

// ---------------------------------------------------------------------------

bool NTv1Grid::valueAt(int x, int y, bool compensateNTConvention,
                       float &longShift, float &latShift) const {
    assert(x >= 0 && y >= 0 && x < m_width && y < m_height);

    double two_doubles[2];
    // NTv1 is organized from east to west !
    m_fp->seek(192 + 2 * sizeof(double) * (y * m_width + m_width - 1 - x));
    if (m_fp->read(&two_doubles[0], sizeof(two_doubles)) !=
        sizeof(two_doubles)) {
        proj_context_errno_set(m_ctx,
                               PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
        return false;
    }
    if (IS_LSB) {
        swap_words(&two_doubles[0], sizeof(double), 2);
    }
    /* convert seconds to radians */
    latShift = static_cast<float>(two_doubles[0] * ((M_PI / 180.0) / 3600.0));
    // west longitude positive convention !
    longShift = (compensateNTConvention ? -1 : 1) *
                static_cast<float>(two_doubles[1] * ((M_PI / 180.0) / 3600.0));

    return true;
}

// ---------------------------------------------------------------------------

class CTable2Grid : public HorizontalShiftGrid {
    PJ_CONTEXT *m_ctx;
    std::unique_ptr<File> m_fp;

    CTable2Grid(const CTable2Grid &) = delete;
    CTable2Grid &operator=(const CTable2Grid &) = delete;

  public:
    CTable2Grid(PJ_CONTEXT *ctx, std::unique_ptr<File> fp,
                const std::string &nameIn, int widthIn, int heightIn,
                const ExtentAndRes &extentIn)
        : HorizontalShiftGrid(nameIn, widthIn, heightIn, extentIn), m_ctx(ctx),
          m_fp(std::move(fp)) {}

    ~CTable2Grid() override;

    bool valueAt(int, int, bool, float &longShift,
                 float &latShift) const override;

    static CTable2Grid *open(PJ_CONTEXT *ctx, std::unique_ptr<File> fp,
                             const std::string &filename);

    void reassign_context(PJ_CONTEXT *ctx) override {
        m_ctx = ctx;
        m_fp->reassign_context(ctx);
    }

    bool hasChanged() const override { return m_fp->hasChanged(); }

    const std::string &metadataItem(const std::string &, int) const override {
        return emptyString;
    }
};

// ---------------------------------------------------------------------------

CTable2Grid::~CTable2Grid() = default;

// ---------------------------------------------------------------------------

CTable2Grid *CTable2Grid::open(PJ_CONTEXT *ctx, std::unique_ptr<File> fp,
                               const std::string &filename) {
    unsigned char header[160];

    /* -------------------------------------------------------------------- */
    /*      Read the header.                                                */
    /* -------------------------------------------------------------------- */
    if (fp->read(header, sizeof(header)) != sizeof(header)) {
        proj_context_errno_set(ctx,
                               PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
        return nullptr;
    }

    /* -------------------------------------------------------------------- */
    /*      Regularize fields of interest.                                  */
    /* -------------------------------------------------------------------- */
    if (!IS_LSB) {
        swap_words(header + 96, sizeof(double), 4);
        swap_words(header + 128, sizeof(int), 2);
    }

    ExtentAndRes extent;
    extent.isGeographic = true;
    static_assert(sizeof(extent.west) == 8, "wrong sizeof");
    static_assert(sizeof(extent.south) == 8, "wrong sizeof");
    static_assert(sizeof(extent.resX) == 8, "wrong sizeof");
    static_assert(sizeof(extent.resY) == 8, "wrong sizeof");
    memcpy(&extent.west, header + 96, 8);
    memcpy(&extent.south, header + 104, 8);
    memcpy(&extent.resX, header + 112, 8);
    memcpy(&extent.resY, header + 120, 8);
    if (!(fabs(extent.west) <= 4 * M_PI && fabs(extent.south) <= M_PI + 1e-5 &&
          extent.resX > 1e-10 && extent.resY > 1e-10)) {
        pj_log(ctx, PJ_LOG_ERROR, _("Inconsistent georeferencing for %s"),
               filename.c_str());
        proj_context_errno_set(ctx,
                               PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
        return nullptr;
    }
    int width;
    int height;
    memcpy(&width, header + 128, 4);
    memcpy(&height, header + 132, 4);
    if (width <= 0 || height <= 0) {
        proj_context_errno_set(ctx,
                               PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
        return nullptr;
    }
    extent.east = extent.west + (width - 1) * extent.resX;
    extent.north = extent.south + (height - 1) * extent.resX;
    extent.computeInvRes();

    return new CTable2Grid(ctx, std::move(fp), filename, width, height, extent);
}

// ---------------------------------------------------------------------------

bool CTable2Grid::valueAt(int x, int y, bool compensateNTConvention,
                          float &longShift, float &latShift) const {
    assert(x >= 0 && y >= 0 && x < m_width && y < m_height);

    float two_floats[2];
    m_fp->seek(160 + 2 * sizeof(float) * (y * m_width + x));
    if (m_fp->read(&two_floats[0], sizeof(two_floats)) != sizeof(two_floats)) {
        proj_context_errno_set(m_ctx,
                               PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
        return false;
    }
    if (!IS_LSB) {
        swap_words(&two_floats[0], sizeof(float), 2);
    }

    latShift = two_floats[1];
    // west longitude positive convention !
    longShift = (compensateNTConvention ? -1 : 1) * two_floats[0];

    return true;
}

// ---------------------------------------------------------------------------

class NTv2GridSet : public HorizontalShiftGridSet {
    std::unique_ptr<File> m_fp;
    std::unique_ptr<FloatLineCache> m_cache{};

    NTv2GridSet(const NTv2GridSet &) = delete;
    NTv2GridSet &operator=(const NTv2GridSet &) = delete;

    explicit NTv2GridSet(std::unique_ptr<File> &&fp) : m_fp(std::move(fp)) {}

  public:
    ~NTv2GridSet() override;

    static std::unique_ptr<NTv2GridSet> open(PJ_CONTEXT *ctx,
                                             std::unique_ptr<File> fp,
                                             const std::string &filename);

    void reassign_context(PJ_CONTEXT *ctx) override {
        HorizontalShiftGridSet::reassign_context(ctx);
        m_fp->reassign_context(ctx);
    }
};

// ---------------------------------------------------------------------------

class NTv2Grid : public HorizontalShiftGrid {
    friend class NTv2GridSet;

    PJ_CONTEXT *m_ctx;                 // owned by the parent NTv2GridSet
    File *m_fp;                        // owned by the parent NTv2GridSet
    FloatLineCache *m_cache = nullptr; // owned by the parent NTv2GridSet
    uint32_t m_gridIdx;
    unsigned long long m_offset;
    bool m_mustSwap;
    mutable std::vector<float> m_buffer{};

    NTv2Grid(const NTv2Grid &) = delete;
    NTv2Grid &operator=(const NTv2Grid &) = delete;

  public:
    NTv2Grid(const std::string &nameIn, PJ_CONTEXT *ctx, File *fp,
             uint32_t gridIdx, unsigned long long offsetIn, bool mustSwapIn,
             int widthIn, int heightIn, const ExtentAndRes &extentIn)
        : HorizontalShiftGrid(nameIn, widthIn, heightIn, extentIn), m_ctx(ctx),
          m_fp(fp), m_gridIdx(gridIdx), m_offset(offsetIn),
          m_mustSwap(mustSwapIn) {}

    bool valueAt(int, int, bool, float &longShift,
                 float &latShift) const override;

    const std::string &metadataItem(const std::string &, int) const override {
        return emptyString;
    }

    void setCache(FloatLineCache *cache) { m_cache = cache; }

    void reassign_context(PJ_CONTEXT *ctx) override {
        m_ctx = ctx;
        m_fp->reassign_context(ctx);
    }

    bool hasChanged() const override { return m_fp->hasChanged(); }
};

// ---------------------------------------------------------------------------

bool NTv2Grid::valueAt(int x, int y, bool compensateNTConvention,
                       float &longShift, float &latShift) const {
    assert(x >= 0 && y >= 0 && x < m_width && y < m_height);

    const std::vector<float> *pBuffer = m_cache->get(m_gridIdx, y);
    if (pBuffer == nullptr) {
        try {
            m_buffer.resize(4 * m_width);
        } catch (const std::exception &e) {
            pj_log(m_ctx, PJ_LOG_ERROR, _("Exception %s"), e.what());
            return false;
        }

        const size_t nLineSizeInBytes = 4 * sizeof(float) * m_width;
        // there are 4 components: lat shift, long shift, lat error, long error
        m_fp->seek(m_offset +
                   nLineSizeInBytes * static_cast<unsigned long long>(y));
        if (m_fp->read(&m_buffer[0], nLineSizeInBytes) != nLineSizeInBytes) {
            proj_context_errno_set(
                m_ctx, PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
            return false;
        }
        // Remove lat and long error
        for (int i = 1; i < m_width; ++i) {
            m_buffer[2 * i] = m_buffer[4 * i];
            m_buffer[2 * i + 1] = m_buffer[4 * i + 1];
        }
        m_buffer.resize(2 * m_width);
        if (m_mustSwap) {
            swap_words(&m_buffer[0], sizeof(float), 2 * m_width);
        }
        // NTv2 is organized from east to west !
        for (int i = 0; i < m_width / 2; ++i) {
            std::swap(m_buffer[2 * i], m_buffer[2 * (m_width - 1 - i)]);
            std::swap(m_buffer[2 * i + 1], m_buffer[2 * (m_width - 1 - i) + 1]);
        }

        try {
            m_cache->insert(m_gridIdx, y, m_buffer);
        } catch (const std::exception &e) {
            // Should normally not happen
            pj_log(m_ctx, PJ_LOG_ERROR, _("Exception %s"), e.what());
        }
    }
    const std::vector<float> &buffer = pBuffer ? *pBuffer : m_buffer;

    /* convert seconds to radians */
    latShift = static_cast<float>(buffer[2 * x] * ((M_PI / 180.0) / 3600.0));
    // west longitude positive convention !
    longShift =
        (compensateNTConvention ? -1 : 1) *
        static_cast<float>(buffer[2 * x + 1] * ((M_PI / 180.0) / 3600.0));
    return true;
}

// ---------------------------------------------------------------------------

NTv2GridSet::~NTv2GridSet() = default;

// ---------------------------------------------------------------------------

std::unique_ptr<NTv2GridSet> NTv2GridSet::open(PJ_CONTEXT *ctx,
                                               std::unique_ptr<File> fp,
                                               const std::string &filename) {
    File *fpRaw = fp.get();
    auto set = std::unique_ptr<NTv2GridSet>(new NTv2GridSet(std::move(fp)));
    set->m_name = filename;
    set->m_format = "ntv2";

    char header[11 * 16];

    /* -------------------------------------------------------------------- */
    /*      Read the header.                                                */
    /* -------------------------------------------------------------------- */
    if (fpRaw->read(header, sizeof(header)) != sizeof(header)) {
        proj_context_errno_set(ctx,
                               PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
        return nullptr;
    }

    constexpr int OFFSET_GS_TYPE = 56;
    if (memcmp(header + OFFSET_GS_TYPE, "SECONDS", 7) != 0) {
        pj_log(ctx, PJ_LOG_ERROR, _("Only GS_TYPE=SECONDS is supported"));
        proj_context_errno_set(ctx,
                               PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
        return nullptr;
    }

    const bool must_swap = (header[8] == 11) ? !IS_LSB : IS_LSB;
    constexpr int OFFSET_NUM_SUBFILES = 8 + 32;
    if (must_swap) {
        // swap_words( header+8, 4, 1 );
        // swap_words( header+8+16, 4, 1 );
        swap_words(header + OFFSET_NUM_SUBFILES, 4, 1);
        // swap_words( header+8+7*16, 8, 1 );
        // swap_words( header+8+8*16, 8, 1 );
        // swap_words( header+8+9*16, 8, 1 );
        // swap_words( header+8+10*16, 8, 1 );
    }

    /* -------------------------------------------------------------------- */
    /*      Get the subfile count out ... all we really use for now.        */
    /* -------------------------------------------------------------------- */
    unsigned int num_subfiles;
    memcpy(&num_subfiles, header + OFFSET_NUM_SUBFILES, 4);

    std::map<std::string, NTv2Grid *> mapGrids;

    /* ==================================================================== */
    /*      Step through the subfiles, creating a grid for each.            */
    /* ==================================================================== */
    int largestLine = 1;
    for (unsigned subfile = 0; subfile < num_subfiles; subfile++) {
        // Read header
        if (fpRaw->read(header, sizeof(header)) != sizeof(header)) {
            proj_context_errno_set(
                ctx, PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
            return nullptr;
        }

        if (strncmp(header, "SUB_NAME", 8) != 0) {
            proj_context_errno_set(
                ctx, PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
            return nullptr;
        }

        // Byte swap interesting fields if needed.
        constexpr int OFFSET_GS_COUNT = 8 + 16 * 10;
        constexpr int OFFSET_SOUTH_LAT = 8 + 16 * 4;
        if (must_swap) {
            // 6 double values: south, north, east, west, resY,
            // resX
            for (int i = 0; i < 6; i++) {
                swap_words(header + OFFSET_SOUTH_LAT + 16 * i, sizeof(double),
                           1);
            }
            swap_words(header + OFFSET_GS_COUNT, sizeof(int), 1);
        }

        std::string gridName;
        gridName.append(header + 8, 8);

        ExtentAndRes extent;
        extent.isGeographic = true;
        extent.south = to_double(header + OFFSET_SOUTH_LAT) * DEG_TO_RAD /
                       3600.0; /* S_LAT */
        extent.north = to_double(header + OFFSET_SOUTH_LAT + 16) * DEG_TO_RAD /
                       3600.0; /* N_LAT */
        extent.east = -to_double(header + OFFSET_SOUTH_LAT + 16 * 2) *
                      DEG_TO_RAD / 3600.0; /* E_LONG */
        extent.west = -to_double(header + OFFSET_SOUTH_LAT + 16 * 3) *
                      DEG_TO_RAD / 3600.0; /* W_LONG */
        extent.resY =
            to_double(header + OFFSET_SOUTH_LAT + 16 * 4) * DEG_TO_RAD / 3600.0;
        extent.resX =
            to_double(header + OFFSET_SOUTH_LAT + 16 * 5) * DEG_TO_RAD / 3600.0;
        extent.computeInvRes();

        if (!(fabs(extent.west) <= 4 * M_PI && fabs(extent.east) <= 4 * M_PI &&
              fabs(extent.north) <= M_PI + 1e-5 &&
              fabs(extent.south) <= M_PI + 1e-5 && extent.west < extent.east &&
              extent.south < extent.north && extent.resX > 1e-10 &&
              extent.resY > 1e-10)) {
            pj_log(ctx, PJ_LOG_ERROR, _("Inconsistent georeferencing for %s"),
                   filename.c_str());
            proj_context_errno_set(
                ctx, PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
            return nullptr;
        }
        const int columns = static_cast<int>(
            fabs((extent.east - extent.west) * extent.invResX + 0.5) + 1);
        const int rows = static_cast<int>(
            fabs((extent.north - extent.south) * extent.invResY + 0.5) + 1);
        if (columns > largestLine)
            largestLine = columns;

        pj_log(ctx, PJ_LOG_TRACE,
               "NTv2 %s %dx%d: LL=(%.9g,%.9g) UR=(%.9g,%.9g)", gridName.c_str(),
               columns, rows, extent.west * RAD_TO_DEG,
               extent.south * RAD_TO_DEG, extent.east * RAD_TO_DEG,
               extent.north * RAD_TO_DEG);

        unsigned int gs_count;
        memcpy(&gs_count, header + OFFSET_GS_COUNT, 4);
        if (gs_count / columns != static_cast<unsigned>(rows)) {
            pj_log(ctx, PJ_LOG_ERROR,
                   _("GS_COUNT(%u) does not match expected cells (%dx%d)"),
                   gs_count, columns, rows);
            proj_context_errno_set(
                ctx, PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
            return nullptr;
        }

        const auto offset = fpRaw->tell();
        auto grid = std::unique_ptr<NTv2Grid>(new NTv2Grid(
            std::string(filename).append(", ").append(gridName), ctx, fpRaw,
            subfile, offset, must_swap, columns, rows, extent));
        std::string parentName;
        parentName.assign(header + 24, 8);
        auto iter = mapGrids.find(parentName);
        auto gridPtr = grid.get();
        if (iter == mapGrids.end()) {
            set->m_grids.emplace_back(std::move(grid));
        } else {
            iter->second->m_children.emplace_back(std::move(grid));
        }
        mapGrids[gridName] = gridPtr;

        // Skip grid data. 4 components of size float
        fpRaw->seek(static_cast<unsigned long long>(gs_count) * 4 * 4,
                    SEEK_CUR);
    }

    // Cache up to 1 megapixel per NTv2 file
    const int maxLinesInCache = 1024 * 1024 / largestLine;
    set->m_cache = std::make_unique<FloatLineCache>(maxLinesInCache);
    for (const auto &kv : mapGrids) {
        kv.second->setCache(set->m_cache.get());
    }

    return set;
}

#ifdef TIFF_ENABLED

// ---------------------------------------------------------------------------

class GTiffHGridShiftSet : public HorizontalShiftGridSet {

    std::unique_ptr<GTiffDataset> m_GTiffDataset;

    GTiffHGridShiftSet(PJ_CONTEXT *ctx, std::unique_ptr<File> &&fp)
        : m_GTiffDataset(new GTiffDataset(ctx, std::move(fp))) {}

  public:
    ~GTiffHGridShiftSet() override;

    static std::unique_ptr<GTiffHGridShiftSet>
    open(PJ_CONTEXT *ctx, std::unique_ptr<File> fp,
         const std::string &filename);

    void reassign_context(PJ_CONTEXT *ctx) override {
        HorizontalShiftGridSet::reassign_context(ctx);
        if (m_GTiffDataset) {
            m_GTiffDataset->reassign_context(ctx);
        }
    }

    bool reopen(PJ_CONTEXT *ctx) override {
        pj_log(ctx, PJ_LOG_DEBUG, "Grid %s has changed. Re-loading it",
               m_name.c_str());
        m_grids.clear();
        m_GTiffDataset.reset();
        auto fp = FileManager::open_resource_file(ctx, m_name.c_str());
        if (!fp) {
            return false;
        }
        auto newGS = open(ctx, std::move(fp), m_name);
        if (newGS) {
            m_grids = std::move(newGS->m_grids);
            m_GTiffDataset = std::move(newGS->m_GTiffDataset);
        }
        return !m_grids.empty();
    }
};

// ---------------------------------------------------------------------------

class GTiffHGrid : public HorizontalShiftGrid {
    friend void insertIntoHierarchy<GTiffHGrid, HorizontalShiftGrid>(
        PJ_CONTEXT *ctx, std::unique_ptr<GTiffHGrid> &&grid,
        const std::string &gridName, const std::string &parentName,
        std::vector<std::unique_ptr<HorizontalShiftGrid>> &topGrids,
        std::map<std::string, GTiffHGrid *> &mapGrids);

    std::unique_ptr<GTiffGrid> m_grid;
    uint16_t m_idxLatShift;
    uint16_t m_idxLongShift;
    double m_convFactorToRadian;
    bool m_positiveEast;

  public:
    GTiffHGrid(std::unique_ptr<GTiffGrid> &&grid, uint16_t idxLatShift,
               uint16_t idxLongShift, double convFactorToRadian,
               bool positiveEast);

    ~GTiffHGrid() override;

    bool valueAt(int x, int y, bool, float &longShift,
                 float &latShift) const override;

    const std::string &metadataItem(const std::string &key,
                                    int sample = -1) const override {
        return m_grid->metadataItem(key, sample);
    }

    void insertGrid(PJ_CONTEXT *ctx, std::unique_ptr<GTiffHGrid> &&subgrid);

    void reassign_context(PJ_CONTEXT *ctx) override {
        m_grid->reassign_context(ctx);
    }

    bool hasChanged() const override { return m_grid->hasChanged(); }
};

// ---------------------------------------------------------------------------

GTiffHGridShiftSet::~GTiffHGridShiftSet() = default;

// ---------------------------------------------------------------------------

GTiffHGrid::GTiffHGrid(std::unique_ptr<GTiffGrid> &&grid, uint16_t idxLatShift,
                       uint16_t idxLongShift, double convFactorToRadian,
                       bool positiveEast)
    : HorizontalShiftGrid(grid->name(), grid->width(), grid->height(),
                          grid->extentAndRes()),
      m_grid(std::move(grid)), m_idxLatShift(idxLatShift),
      m_idxLongShift(idxLongShift), m_convFactorToRadian(convFactorToRadian),
      m_positiveEast(positiveEast) {}

// ---------------------------------------------------------------------------

GTiffHGrid::~GTiffHGrid() = default;

// ---------------------------------------------------------------------------

bool GTiffHGrid::valueAt(int x, int y, bool, float &longShift,
                         float &latShift) const {
    if (!m_grid->valueAt(m_idxLatShift, x, y, latShift) ||
        !m_grid->valueAt(m_idxLongShift, x, y, longShift)) {
        return false;
    }
    // From arc-seconds to radians
    latShift = static_cast<float>(latShift * m_convFactorToRadian);
    longShift = static_cast<float>(longShift * m_convFactorToRadian);
    if (!m_positiveEast) {
        longShift = -longShift;
    }
    return true;
}

// ---------------------------------------------------------------------------

void GTiffHGrid::insertGrid(PJ_CONTEXT *ctx,
                            std::unique_ptr<GTiffHGrid> &&subgrid) {
    bool gridInserted = false;
    const auto &extent = subgrid->extentAndRes();
    for (const auto &candidateParent : m_children) {
        const auto &candidateParentExtent = candidateParent->extentAndRes();
        if (candidateParentExtent.contains(extent)) {
            static_cast<GTiffHGrid *>(candidateParent.get())
                ->insertGrid(ctx, std::move(subgrid));
            gridInserted = true;
            break;
        } else if (candidateParentExtent.intersects(extent)) {
            pj_log(ctx, PJ_LOG_DEBUG, "Partially intersecting grids found!");
        }
    }
    if (!gridInserted) {
        m_children.emplace_back(std::move(subgrid));
    }
}

// ---------------------------------------------------------------------------

std::unique_ptr<GTiffHGridShiftSet>
GTiffHGridShiftSet::open(PJ_CONTEXT *ctx, std::unique_ptr<File> fp,
                         const std::string &filename) {
    auto set = std::unique_ptr<GTiffHGridShiftSet>(
        new GTiffHGridShiftSet(ctx, std::move(fp)));
    set->m_name = filename;
    set->m_format = "gtiff";
    if (!set->m_GTiffDataset->openTIFF(filename)) {
        return nullptr;
    }

    // Defaults inspired from NTv2
    uint16_t idxLatShift = 0;
    uint16_t idxLongShift = 1;
    constexpr double ARC_SECOND_TO_RADIAN = (M_PI / 180.0) / 3600.0;
    double convFactorToRadian = ARC_SECOND_TO_RADIAN;
    bool positiveEast = true;

    std::map<std::string, GTiffHGrid *> mapGrids;
    for (int ifd = 0;; ++ifd) {
        auto grid = set->m_GTiffDataset->nextGrid();
        if (!grid) {
            if (ifd == 0) {
                return nullptr;
            }
            break;
        }

        const auto subfileType = grid->subfileType();
        if (subfileType != 0 && subfileType != FILETYPE_PAGE) {
            if (ifd == 0) {
                pj_log(ctx, PJ_LOG_ERROR, _("Invalid subfileType"));
                return nullptr;
            } else {
                pj_log(ctx, PJ_LOG_DEBUG,
                       _("Ignoring IFD %d as it has a unsupported subfileType"),
                       ifd);
                continue;
            }
        }

        if (grid->samplesPerPixel() < 2) {
            if (ifd == 0) {
                pj_log(ctx, PJ_LOG_ERROR,
                       _("At least 2 samples per pixel needed"));
                return nullptr;
            } else {
                pj_log(ctx, PJ_LOG_DEBUG,
                       _("Ignoring IFD %d as it has not at least 2 samples"),
                       ifd);
                continue;
            }
        }

        // Identify the index of the latitude and longitude offset channels
        bool foundDescriptionForAtLeastOneSample = false;
        bool foundDescriptionForLatOffset = false;
        bool foundDescriptionForLongOffset = false;
        for (int i = 0; i < static_cast<int>(grid->samplesPerPixel()); ++i) {
            const auto &desc = grid->metadataItem("DESCRIPTION", i);
            if (!desc.empty()) {
                foundDescriptionForAtLeastOneSample = true;
            }
            if (desc == "latitude_offset") {
                idxLatShift = static_cast<uint16_t>(i);
                foundDescriptionForLatOffset = true;
            } else if (desc == "longitude_offset") {
                idxLongShift = static_cast<uint16_t>(i);
                foundDescriptionForLongOffset = true;
            }
        }

        if (foundDescriptionForAtLeastOneSample) {
            if (!foundDescriptionForLongOffset &&
                !foundDescriptionForLatOffset) {
                if (ifd > 0) {
                    // Assuming that extra IFD without
                    // longitude_offset/latitude_offset can be ignored
                    // One could imagine to put the accuracy values in separate
                    // IFD for example
                    pj_log(ctx, PJ_LOG_DEBUG,
                           "Ignoring IFD %d as it has no "
                           "longitude_offset/latitude_offset channel",
                           ifd);
                    continue;
                } else {
                    pj_log(ctx, PJ_LOG_DEBUG,
                           "IFD 0 has channel descriptions, but no "
                           "longitude_offset/latitude_offset channel");
                    return nullptr;
                }
            }
        }
        if (foundDescriptionForLatOffset && !foundDescriptionForLongOffset) {
            pj_log(
                ctx, PJ_LOG_ERROR,
                _("Found latitude_offset channel, but not longitude_offset"));
            return nullptr;
        } else if (foundDescriptionForLongOffset &&
                   !foundDescriptionForLatOffset) {
            pj_log(
                ctx, PJ_LOG_ERROR,
                _("Found longitude_offset channel, but not latitude_offset"));
            return nullptr;
        }

        if (idxLatShift >= grid->samplesPerPixel() ||
            idxLongShift >= grid->samplesPerPixel()) {
            pj_log(ctx, PJ_LOG_ERROR, _("Invalid sample index"));
            return nullptr;
        }

        if (foundDescriptionForLongOffset) {
            const std::string &positiveValue =
                grid->metadataItem("positive_value", idxLongShift);
            if (!positiveValue.empty()) {
                if (positiveValue == "west") {
                    positiveEast = false;
                } else if (positiveValue == "east") {
                    positiveEast = true;
                } else {
                    pj_log(ctx, PJ_LOG_ERROR,
                           _("Unsupported value %s for 'positive_value'"),
                           positiveValue.c_str());
                    return nullptr;
                }
            }
        }

        // Identify their unit
        {
            const auto &unitLatShift =
                grid->metadataItem("UNITTYPE", idxLatShift);
            const auto &unitLongShift =
                grid->metadataItem("UNITTYPE", idxLongShift);
            if (unitLatShift != unitLongShift) {
                pj_log(ctx, PJ_LOG_ERROR,
                       _("Different unit for longitude and latitude offset"));
                return nullptr;
            }
            if (!unitLatShift.empty()) {
                if (unitLatShift == "arc-second" ||
                    unitLatShift == "arc-seconds per year") {
                    convFactorToRadian = ARC_SECOND_TO_RADIAN;
                } else if (unitLatShift == "radian") {
                    convFactorToRadian = 1.0;
                } else if (unitLatShift == "degree") {
                    convFactorToRadian = M_PI / 180.0;
                } else {
                    pj_log(ctx, PJ_LOG_ERROR, _("Unsupported unit %s"),
                           unitLatShift.c_str());
                    return nullptr;
                }
            }
        }

        const std::string &gridName = grid->metadataItem("grid_name");
        const std::string &parentName = grid->metadataItem("parent_grid_name");

        auto hgrid = std::make_unique<GTiffHGrid>(
            std::move(grid), idxLatShift, idxLongShift, convFactorToRadian,
            positiveEast);

        insertIntoHierarchy(ctx, std::move(hgrid), gridName, parentName,
                            set->m_grids, mapGrids);
    }
    return set;
}
#endif // TIFF_ENABLED

// ---------------------------------------------------------------------------

std::unique_ptr<HorizontalShiftGridSet>
HorizontalShiftGridSet::open(PJ_CONTEXT *ctx, const std::string &filename) {
    if (filename == "null") {
        auto set = std::unique_ptr<HorizontalShiftGridSet>(
            new HorizontalShiftGridSet());
        set->m_name = filename;
        set->m_format = "null";
        set->m_grids.push_back(std::unique_ptr<NullHorizontalShiftGrid>(
            new NullHorizontalShiftGrid()));
        return set;
    }

    auto fp = FileManager::open_resource_file(ctx, filename.c_str());
    if (!fp) {
        return nullptr;
    }
    const auto &actualName(fp->name());

    char header[160];
    /* -------------------------------------------------------------------- */
    /*      Load a header, to determine the file type.                      */
    /* -------------------------------------------------------------------- */
    size_t header_size = fp->read(header, sizeof(header));
    if (header_size != sizeof(header)) {
        /* some files may be smaller that sizeof(header), eg 160, so */
        ctx->last_errno = 0; /* don't treat as a persistent error */
        pj_log(ctx, PJ_LOG_DEBUG,
               "pj_gridinfo_init: short header read of %d bytes",
               (int)header_size);
    }
    fp->seek(0);

    /* -------------------------------------------------------------------- */
    /*      Determine file type.                                            */
    /* -------------------------------------------------------------------- */
    if (header_size >= 144 + 16 && strncmp(header + 0, "HEADER", 6) == 0 &&
        strncmp(header + 96, "W GRID", 6) == 0 &&
        strncmp(header + 144, "TO      NAD83   ", 16) == 0) {
        auto grid = NTv1Grid::open(ctx, std::move(fp), actualName);
        if (!grid) {
            return nullptr;
        }
        auto set = std::unique_ptr<HorizontalShiftGridSet>(
            new HorizontalShiftGridSet());
        set->m_name = actualName;
        set->m_format = "ntv1";
        set->m_grids.push_back(std::unique_ptr<HorizontalShiftGrid>(grid));
        return set;
    } else if (header_size >= 9 && strncmp(header + 0, "CTABLE V2", 9) == 0) {
        auto grid = CTable2Grid::open(ctx, std::move(fp), actualName);
        if (!grid) {
            return nullptr;
        }
        auto set = std::unique_ptr<HorizontalShiftGridSet>(
            new HorizontalShiftGridSet());
        set->m_name = actualName;
        set->m_format = "ctable2";
        set->m_grids.push_back(std::unique_ptr<HorizontalShiftGrid>(grid));
        return set;
    } else if (header_size >= 48 + 7 &&
               strncmp(header + 0, "NUM_OREC", 8) == 0 &&
               strncmp(header + 48, "GS_TYPE", 7) == 0) {
        return NTv2GridSet::open(ctx, std::move(fp), actualName);
    } else if (IsTIFF(header_size,
                      reinterpret_cast<const unsigned char *>(header))) {
#ifdef TIFF_ENABLED
        auto set = std::unique_ptr<HorizontalShiftGridSet>(
            GTiffHGridShiftSet::open(ctx, std::move(fp), actualName));
        if (!set)
            proj_context_errno_set(
                ctx, PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
        return set;
#else
        pj_log(ctx, PJ_LOG_ERROR,
               _("TIFF grid, but TIFF support disabled in this build"));
        return nullptr;
#endif
    }

    pj_log(ctx, PJ_LOG_ERROR,
           "Unrecognized horizontal grid format for filename '%s'",
           filename.c_str());
    return nullptr;
}

// ---------------------------------------------------------------------------

bool HorizontalShiftGridSet::reopen(PJ_CONTEXT *ctx) {
    pj_log(ctx, PJ_LOG_DEBUG, "Grid %s has changed. Re-loading it",
           m_name.c_str());
    auto newGS = open(ctx, m_name);
    m_grids.clear();
    if (newGS) {
        m_grids = std::move(newGS->m_grids);
    }
    return !m_grids.empty();
}

// ---------------------------------------------------------------------------

#define REL_TOLERANCE_HGRIDSHIFT 1e-5

const HorizontalShiftGrid *HorizontalShiftGrid::gridAt(double longitude,
                                                       double lat) const {
    for (const auto &child : m_children) {
        const auto &extentChild = child->extentAndRes();
        const double epsilon =
            (extentChild.resX + extentChild.resY) * REL_TOLERANCE_HGRIDSHIFT;
        if (isPointInExtent(longitude, lat, extentChild, epsilon)) {
            return child->gridAt(longitude, lat);
        }
    }
    return this;
}
// ---------------------------------------------------------------------------

const HorizontalShiftGrid *HorizontalShiftGridSet::gridAt(double longitude,
                                                          double lat) const {
    for (const auto &grid : m_grids) {
        if (grid->isNullGrid()) {
            return grid.get();
        }
        const auto &extent = grid->extentAndRes();
        const double epsilon =
            (extent.resX + extent.resY) * REL_TOLERANCE_HGRIDSHIFT;
        if (isPointInExtent(longitude, lat, extent, epsilon)) {
            return grid->gridAt(longitude, lat);
        }
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

void HorizontalShiftGridSet::reassign_context(PJ_CONTEXT *ctx) {
    for (const auto &grid : m_grids) {
        grid->reassign_context(ctx);
    }
}

#ifdef TIFF_ENABLED
// ---------------------------------------------------------------------------

class GTiffGenericGridShiftSet : public GenericShiftGridSet {

    std::unique_ptr<GTiffDataset> m_GTiffDataset;

    GTiffGenericGridShiftSet(PJ_CONTEXT *ctx, std::unique_ptr<File> &&fp)
        : m_GTiffDataset(new GTiffDataset(ctx, std::move(fp))) {}

  public:
    ~GTiffGenericGridShiftSet() override;

    static std::unique_ptr<GTiffGenericGridShiftSet>
    open(PJ_CONTEXT *ctx, std::unique_ptr<File> fp,
         const std::string &filename);

    void reassign_context(PJ_CONTEXT *ctx) override {
        GenericShiftGridSet::reassign_context(ctx);
        if (m_GTiffDataset) {
            m_GTiffDataset->reassign_context(ctx);
        }
    }

    bool reopen(PJ_CONTEXT *ctx) override {
        pj_log(ctx, PJ_LOG_DEBUG, "Grid %s has changed. Re-loading it",
               m_name.c_str());
        m_grids.clear();
        m_GTiffDataset.reset();
        auto fp = FileManager::open_resource_file(ctx, m_name.c_str());
        if (!fp) {
            return false;
        }
        auto newGS = open(ctx, std::move(fp), m_name);
        if (newGS) {
            m_grids = std::move(newGS->m_grids);
            m_GTiffDataset = std::move(newGS->m_GTiffDataset);
        }
        return !m_grids.empty();
    }
};

// ---------------------------------------------------------------------------

class GTiffGenericGrid final : public GenericShiftGrid {
    friend void insertIntoHierarchy<GTiffGenericGrid, GenericShiftGrid>(
        PJ_CONTEXT *ctx, std::unique_ptr<GTiffGenericGrid> &&grid,
        const std::string &gridName, const std::string &parentName,
        std::vector<std::unique_ptr<GenericShiftGrid>> &topGrids,
        std::map<std::string, GTiffGenericGrid *> &mapGrids);

    std::unique_ptr<GTiffGrid> m_grid;
    const GenericShiftGrid *m_firstGrid = nullptr;
    mutable std::string m_type{};
    mutable bool m_bTypeSet = false;

  public:
    GTiffGenericGrid(std::unique_ptr<GTiffGrid> &&grid);

    ~GTiffGenericGrid() override;

    bool valueAt(int x, int y, int sample, float &out) const override;

    bool valuesAt(int x_start, int y_start, int x_count, int y_count,
                  int sample_count, const int *sample_idx, float *out,
                  bool &nodataFound) const override;

    int samplesPerPixel() const override { return m_grid->samplesPerPixel(); }

    std::string unit(int sample) const override {
        return metadataItem("UNITTYPE", sample);
    }

    std::string description(int sample) const override {
        return metadataItem("DESCRIPTION", sample);
    }

    const std::string &metadataItem(const std::string &key,
                                    int sample = -1) const override {
        const std::string &ret = m_grid->metadataItem(key, sample);
        if (ret.empty() && m_firstGrid) {
            return m_firstGrid->metadataItem(key, sample);
        }
        return ret;
    }

    const std::string &type() const override {
        if (!m_bTypeSet) {
            m_bTypeSet = true;
            m_type = metadataItem("TYPE");
        }
        return m_type;
    }

    void setFirstGrid(const GenericShiftGrid *firstGrid) {
        m_firstGrid = firstGrid;
    }

    void insertGrid(PJ_CONTEXT *ctx,
                    std::unique_ptr<GTiffGenericGrid> &&subgrid);

    void reassign_context(PJ_CONTEXT *ctx) override {
        m_grid->reassign_context(ctx);
    }

    bool hasChanged() const override { return m_grid->hasChanged(); }

  private:
    GTiffGenericGrid(const GTiffGenericGrid &) = delete;
    GTiffGenericGrid &operator=(const GTiffGenericGrid &) = delete;
};

// ---------------------------------------------------------------------------

GTiffGenericGridShiftSet::~GTiffGenericGridShiftSet() = default;

// ---------------------------------------------------------------------------

GTiffGenericGrid::GTiffGenericGrid(std::unique_ptr<GTiffGrid> &&grid)
    : GenericShiftGrid(grid->name(), grid->width(), grid->height(),
                       grid->extentAndRes()),
      m_grid(std::move(grid)) {}

// ---------------------------------------------------------------------------

GTiffGenericGrid::~GTiffGenericGrid() = default;

// ---------------------------------------------------------------------------

bool GTiffGenericGrid::valueAt(int x, int y, int sample, float &out) const {
    if (sample < 0 ||
        static_cast<unsigned>(sample) >= m_grid->samplesPerPixel())
        return false;
    return m_grid->valueAt(static_cast<uint16_t>(sample), x, y, out);
}

// ---------------------------------------------------------------------------

bool GTiffGenericGrid::valuesAt(int x_start, int y_start, int x_count,
                                int y_count, int sample_count,
                                const int *sample_idx, float *out,
                                bool &nodataFound) const {
    return m_grid->valuesAt(x_start, y_start, x_count, y_count, sample_count,
                            sample_idx, out, nodataFound);
}

// ---------------------------------------------------------------------------

void GTiffGenericGrid::insertGrid(PJ_CONTEXT *ctx,
                                  std::unique_ptr<GTiffGenericGrid> &&subgrid) {
    bool gridInserted = false;
    const auto &extent = subgrid->extentAndRes();
    for (const auto &candidateParent : m_children) {
        const auto &candidateParentExtent = candidateParent->extentAndRes();
        if (candidateParentExtent.contains(extent)) {
            static_cast<GTiffGenericGrid *>(candidateParent.get())
                ->insertGrid(ctx, std::move(subgrid));
            gridInserted = true;
            break;
        } else if (candidateParentExtent.intersects(extent)) {
            pj_log(ctx, PJ_LOG_DEBUG, "Partially intersecting grids found!");
        }
    }
    if (!gridInserted) {
        m_children.emplace_back(std::move(subgrid));
    }
}
#endif // TIFF_ENABLED

// ---------------------------------------------------------------------------

class NullGenericShiftGrid : public GenericShiftGrid {

  public:
    NullGenericShiftGrid() : GenericShiftGrid("null", 3, 3, globalExtent()) {}

    bool isNullGrid() const override { return true; }
    bool valueAt(int, int, int, float &out) const override;

    const std::string &type() const override { return emptyString; }

    int samplesPerPixel() const override { return 0; }

    std::string unit(int) const override { return std::string(); }

    std::string description(int) const override { return std::string(); }

    const std::string &metadataItem(const std::string &, int) const override {
        return emptyString;
    }

    void reassign_context(PJ_CONTEXT *) override {}

    bool hasChanged() const override { return false; }
};

// ---------------------------------------------------------------------------

bool NullGenericShiftGrid::valueAt(int, int, int, float &out) const {
    out = 0.0f;
    return true;
}

// ---------------------------------------------------------------------------

#ifdef TIFF_ENABLED

std::unique_ptr<GTiffGenericGridShiftSet>
GTiffGenericGridShiftSet::open(PJ_CONTEXT *ctx, std::unique_ptr<File> fp,
                               const std::string &filename) {
    auto set = std::unique_ptr<GTiffGenericGridShiftSet>(
        new GTiffGenericGridShiftSet(ctx, std::move(fp)));
    set->m_name = filename;
    set->m_format = "gtiff";
    if (!set->m_GTiffDataset->openTIFF(filename)) {
        return nullptr;
    }

    std::map<std::string, GTiffGenericGrid *> mapGrids;
    for (int ifd = 0;; ++ifd) {
        auto grid = set->m_GTiffDataset->nextGrid();
        if (!grid) {
            if (ifd == 0) {
                return nullptr;
            }
            break;
        }

        const auto subfileType = grid->subfileType();
        if (subfileType != 0 && subfileType != FILETYPE_PAGE) {
            if (ifd == 0) {
                pj_log(ctx, PJ_LOG_ERROR, _("Invalid subfileType"));
                return nullptr;
            } else {
                pj_log(ctx, PJ_LOG_DEBUG,
                       _("Ignoring IFD %d as it has a unsupported subfileType"),
                       ifd);
                continue;
            }
        }

        const std::string &gridName = grid->metadataItem("grid_name");
        const std::string &parentName = grid->metadataItem("parent_grid_name");

        auto ggrid = std::make_unique<GTiffGenericGrid>(std::move(grid));
        if (!set->m_grids.empty() && ggrid->metadataItem("TYPE").empty() &&
            !set->m_grids[0]->metadataItem("TYPE").empty()) {
            ggrid->setFirstGrid(set->m_grids[0].get());
        }

        insertIntoHierarchy(ctx, std::move(ggrid), gridName, parentName,
                            set->m_grids, mapGrids);
    }
    return set;
}
#endif // TIFF_ENABLED

// ---------------------------------------------------------------------------

GenericShiftGrid::GenericShiftGrid(const std::string &nameIn, int widthIn,
                                   int heightIn, const ExtentAndRes &extentIn)
    : Grid(nameIn, widthIn, heightIn, extentIn) {}

// ---------------------------------------------------------------------------

GenericShiftGrid::~GenericShiftGrid() = default;

// ---------------------------------------------------------------------------

bool GenericShiftGrid::valuesAt(int x_start, int y_start, int x_count,
                                int y_count, int sample_count,
                                const int *sample_idx, float *out,
                                bool &nodataFound) const {
    nodataFound = false;
    for (int y = y_start; y < y_start + y_count; ++y) {
        for (int x = x_start; x < x_start + x_count; ++x) {
            for (int isample = 0; isample < sample_count; ++isample) {
                if (!valueAt(x, y, sample_idx[isample], *out))
                    return false;
                ++out;
            }
        }
    }
    return true;
}

// ---------------------------------------------------------------------------

GenericShiftGridSet::GenericShiftGridSet() = default;

// ---------------------------------------------------------------------------

GenericShiftGridSet::~GenericShiftGridSet() = default;

// ---------------------------------------------------------------------------

std::unique_ptr<GenericShiftGridSet>
GenericShiftGridSet::open(PJ_CONTEXT *ctx, const std::string &filename) {
    if (filename == "null") {
        auto set =
            std::unique_ptr<GenericShiftGridSet>(new GenericShiftGridSet());
        set->m_name = filename;
        set->m_format = "null";
        set->m_grids.push_back(
            std::unique_ptr<NullGenericShiftGrid>(new NullGenericShiftGrid()));
        return set;
    }

    auto fp = FileManager::open_resource_file(ctx, filename.c_str());
    if (!fp) {
        return nullptr;
    }

    /* -------------------------------------------------------------------- */
    /*      Load a header, to determine the file type.                      */
    /* -------------------------------------------------------------------- */
    unsigned char header[4];
    size_t header_size = fp->read(header, sizeof(header));
    if (header_size != sizeof(header)) {
        return nullptr;
    }
    fp->seek(0);

    if (IsTIFF(header_size, header)) {
#ifdef TIFF_ENABLED
        const std::string actualName(fp->name());
        auto set = std::unique_ptr<GenericShiftGridSet>(
            GTiffGenericGridShiftSet::open(ctx, std::move(fp), actualName));
        if (!set)
            proj_context_errno_set(
                ctx, PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
        return set;
#else
        pj_log(ctx, PJ_LOG_ERROR,
               _("TIFF grid, but TIFF support disabled in this build"));
        return nullptr;
#endif
    }

    pj_log(ctx, PJ_LOG_ERROR,
           "Unrecognized generic grid format for filename '%s'",
           filename.c_str());
    return nullptr;
}

// ---------------------------------------------------------------------------

bool GenericShiftGridSet::reopen(PJ_CONTEXT *ctx) {
    pj_log(ctx, PJ_LOG_DEBUG, "Grid %s has changed. Re-loading it",
           m_name.c_str());
    auto newGS = open(ctx, m_name);
    m_grids.clear();
    if (newGS) {
        m_grids = std::move(newGS->m_grids);
    }
    return !m_grids.empty();
}

// ---------------------------------------------------------------------------

const GenericShiftGrid *GenericShiftGrid::gridAt(double x, double y) const {
    for (const auto &child : m_children) {
        const auto &extentChild = child->extentAndRes();
        if (isPointInExtent(x, y, extentChild)) {
            return child->gridAt(x, y);
        }
    }
    return this;
}

// ---------------------------------------------------------------------------

const GenericShiftGrid *GenericShiftGridSet::gridAt(double x, double y) const {
    for (const auto &grid : m_grids) {
        if (grid->isNullGrid()) {
            return grid.get();
        }
        const auto &extent = grid->extentAndRes();
        if (isPointInExtent(x, y, extent)) {
            return grid->gridAt(x, y);
        }
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

const GenericShiftGrid *GenericShiftGridSet::gridAt(const std::string &type,
                                                    double x, double y) const {
    for (const auto &grid : m_grids) {
        if (grid->isNullGrid()) {
            return grid.get();
        }
        if (grid->type() != type) {
            continue;
        }
        const auto &extent = grid->extentAndRes();
        if (isPointInExtent(x, y, extent)) {
            return grid->gridAt(x, y);
        }
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

void GenericShiftGridSet::reassign_context(PJ_CONTEXT *ctx) {
    for (const auto &grid : m_grids) {
        grid->reassign_context(ctx);
    }
}

// ---------------------------------------------------------------------------

ListOfGenericGrids pj_generic_grid_init(PJ *P, const char *gridkey) {
    std::string key("s");
    key += gridkey;
    const char *gridnames = pj_param(P->ctx, P->params, key.c_str()).s;
    if (gridnames == nullptr)
        return {};

    auto listOfGridNames = internal::split(std::string(gridnames), ',');
    ListOfGenericGrids grids;
    for (const auto &gridnameStr : listOfGridNames) {
        const char *gridname = gridnameStr.c_str();
        bool canFail = false;
        if (gridname[0] == '@') {
            canFail = true;
            gridname++;
        }
        auto gridSet = GenericShiftGridSet::open(P->ctx, gridname);
        if (!gridSet) {
            if (!canFail) {
                if (proj_context_errno(P->ctx) !=
                    PROJ_ERR_OTHER_NETWORK_ERROR) {
                    proj_context_errno_set(
                        P->ctx, PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
                }
                return {};
            }
            proj_context_errno_set(P->ctx,
                                   0); // don't treat as a persistent error
        } else {
            grids.emplace_back(std::move(gridSet));
        }
    }

    return grids;
}

// ---------------------------------------------------------------------------

static const HorizontalShiftGrid *
findGrid(const ListOfHGrids &grids, const PJ_LP &input,
         HorizontalShiftGridSet *&gridSetOut) {
    for (const auto &gridset : grids) {
        auto grid = gridset->gridAt(input.lam, input.phi);
        if (grid) {
            gridSetOut = gridset.get();
            return grid;
        }
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

static ListOfHGrids getListOfGridSets(PJ_CONTEXT *ctx, const char *grids) {
    ListOfHGrids list;
    auto listOfGrids = internal::split(std::string(grids), ',');
    for (const auto &grid : listOfGrids) {
        const char *gridname = grid.c_str();
        bool canFail = false;
        if (gridname[0] == '@') {
            canFail = true;
            gridname++;
        }
        auto gridSet = HorizontalShiftGridSet::open(ctx, gridname);
        if (!gridSet) {
            if (!canFail) {
                if (proj_context_errno(ctx) != PROJ_ERR_OTHER_NETWORK_ERROR) {
                    proj_context_errno_set(
                        ctx, PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
                }
                return {};
            }
            proj_context_errno_set(ctx, 0); // don't treat as a persistent error
        } else {
            list.emplace_back(std::move(gridSet));
        }
    }
    return list;
}

/**********************************************/
ListOfHGrids pj_hgrid_init(PJ *P, const char *gridkey) {
    /**********************************************

      Initizalize and populate list of horizontal
      grids.

        Takes a PJ-object and the plus-parameter
        name that is used in the proj-string to
        specify the grids to load, e.g. "+grids".
        The + should be left out here.

        Returns the number of loaded grids.

    ***********************************************/

    std::string key("s");
    key += gridkey;
    const char *grids = pj_param(P->ctx, P->params, key.c_str()).s;
    if (grids == nullptr)
        return {};

    return getListOfGridSets(P->ctx, grids);
}

// ---------------------------------------------------------------------------

typedef struct {
    int32_t lam, phi;
} ILP;

// Apply bilinear interpolation for horizontal shift grids
static PJ_LP pj_hgrid_interpolate(PJ_LP t, const HorizontalShiftGrid *grid,
                                  bool compensateNTConvention) {
    PJ_LP val, frct;
    ILP indx;
    int in;

    const auto &extent = grid->extentAndRes();
    t.lam /= extent.resX;
    indx.lam = std::isnan(t.lam) ? 0 : (int32_t)lround(floor(t.lam));
    t.phi /= extent.resY;
    indx.phi = std::isnan(t.phi) ? 0 : (int32_t)lround(floor(t.phi));

    frct.lam = t.lam - indx.lam;
    frct.phi = t.phi - indx.phi;
    val.lam = val.phi = HUGE_VAL;
    if (indx.lam < 0) {
        if (indx.lam == -1 && frct.lam > 1 - 10 * REL_TOLERANCE_HGRIDSHIFT) {
            ++indx.lam;
            frct.lam = 0.;
        } else
            return val;
    } else if ((in = indx.lam + 1) >= grid->width()) {
        if (in == grid->width() && frct.lam < 10 * REL_TOLERANCE_HGRIDSHIFT) {
            --indx.lam;
            frct.lam = 1.;
        } else
            return val;
    }
    if (indx.phi < 0) {
        if (indx.phi == -1 && frct.phi > 1 - 10 * REL_TOLERANCE_HGRIDSHIFT) {
            ++indx.phi;
            frct.phi = 0.;
        } else
            return val;
    } else if ((in = indx.phi + 1) >= grid->height()) {
        if (in == grid->height() && frct.phi < 10 * REL_TOLERANCE_HGRIDSHIFT) {
            --indx.phi;
            frct.phi = 1.;
        } else
            return val;
    }

    float f00Long = 0, f00Lat = 0;
    float f10Long = 0, f10Lat = 0;
    float f01Long = 0, f01Lat = 0;
    float f11Long = 0, f11Lat = 0;
    if (!grid->valueAt(indx.lam, indx.phi, compensateNTConvention, f00Long,
                       f00Lat) ||
        !grid->valueAt(indx.lam + 1, indx.phi, compensateNTConvention, f10Long,
                       f10Lat) ||
        !grid->valueAt(indx.lam, indx.phi + 1, compensateNTConvention, f01Long,
                       f01Lat) ||
        !grid->valueAt(indx.lam + 1, indx.phi + 1, compensateNTConvention,
                       f11Long, f11Lat)) {
        return val;
    }

    double m10 = frct.lam;
    double m11 = m10;
    double m01 = 1. - frct.lam;
    double m00 = m01;
    m11 *= frct.phi;
    m01 *= frct.phi;
    frct.phi = 1. - frct.phi;
    m00 *= frct.phi;
    m10 *= frct.phi;
    val.lam = m00 * f00Long + m10 * f10Long + m01 * f01Long + m11 * f11Long;
    val.phi = m00 * f00Lat + m10 * f10Lat + m01 * f01Lat + m11 * f11Lat;
    return val;
}

// ---------------------------------------------------------------------------

#define MAX_ITERATIONS 10
#define TOL 1e-12

static PJ_LP pj_hgrid_apply_internal(PJ_CONTEXT *ctx, PJ_LP in,
                                     PJ_DIRECTION direction,
                                     const HorizontalShiftGrid *grid,
                                     HorizontalShiftGridSet *gridset,
                                     const ListOfHGrids &grids,
                                     bool &shouldRetry) {
    PJ_LP t, tb, del, dif;
    int i = MAX_ITERATIONS;
    const double toltol = TOL * TOL;

    shouldRetry = false;
    if (in.lam == HUGE_VAL)
        return in;

    /* normalize input to ll origin */
    tb = in;
    const auto *extent = &(grid->extentAndRes());
    const double epsilon =
        (extent->resX + extent->resY) * REL_TOLERANCE_HGRIDSHIFT;
    tb.lam -= extent->west;
    if (tb.lam + epsilon < 0)
        tb.lam += 2 * M_PI;
    else if (tb.lam - epsilon > extent->east - extent->west)
        tb.lam -= 2 * M_PI;
    tb.phi -= extent->south;

    t = pj_hgrid_interpolate(tb, grid, true);
    if (grid->hasChanged()) {
        shouldRetry = gridset->reopen(ctx);
        return t;
    }
    if (t.lam == HUGE_VAL)
        return t;

    if (direction == PJ_FWD) {
        in.lam += t.lam;
        in.phi += t.phi;
        return in;
    }

    t.lam = tb.lam - t.lam;
    t.phi = tb.phi - t.phi;

    do {
        del = pj_hgrid_interpolate(t, grid, true);
        if (grid->hasChanged()) {
            shouldRetry = gridset->reopen(ctx);
            return t;
        }

        /* We can possibly go outside of the initial guessed grid, so try */
        /* to fetch a new grid into which iterate... */
        if (del.lam == HUGE_VAL) {
            PJ_LP lp;
            lp.lam = t.lam + extent->west;
            lp.phi = t.phi + extent->south;
            auto newGrid = findGrid(grids, lp, gridset);
            if (newGrid == nullptr || newGrid == grid || newGrid->isNullGrid())
                break;
            pj_log(ctx, PJ_LOG_TRACE, "Switching from grid %s to grid %s",
                   grid->name().c_str(), newGrid->name().c_str());
            grid = newGrid;
            extent = &(grid->extentAndRes());
            t.lam = lp.lam - extent->west;
            t.phi = lp.phi - extent->south;
            tb = in;
            tb.lam -= extent->west;
            if (tb.lam + epsilon < 0)
                tb.lam += 2 * M_PI;
            else if (tb.lam - epsilon > extent->east - extent->west)
                tb.lam -= 2 * M_PI;
            tb.phi -= extent->south;
            dif.lam = std::numeric_limits<double>::max();
            dif.phi = std::numeric_limits<double>::max();
            continue;
        }

        dif.lam = t.lam + del.lam - tb.lam;
        dif.phi = t.phi + del.phi - tb.phi;
        t.lam -= dif.lam;
        t.phi -= dif.phi;

    } while (--i && (dif.lam * dif.lam + dif.phi * dif.phi >
                     toltol)); /* prob. slightly faster than hypot() */

    if (i == 0) {
        pj_log(ctx, PJ_LOG_TRACE,
               "Inverse grid shift iterator failed to converge.\n");
        proj_context_errno_set(ctx, PROJ_ERR_COORD_TRANSFM_OUTSIDE_GRID);
        t.lam = t.phi = HUGE_VAL;
        return t;
    }

    /* and again: pj_log and ctx->errno */
    if (del.lam == HUGE_VAL) {
        pj_log(ctx, PJ_LOG_TRACE,
               "Inverse grid shift iteration failed, presumably at "
               "grid edge.\nUsing first approximation.\n");
    }

    in.lam = adjlon(t.lam + extent->west);
    in.phi = t.phi + extent->south;
    return in;
}

// ---------------------------------------------------------------------------

PJ_LP pj_hgrid_apply(PJ_CONTEXT *ctx, const ListOfHGrids &grids, PJ_LP lp,
                     PJ_DIRECTION direction) {
    PJ_LP out;

    out.lam = HUGE_VAL;
    out.phi = HUGE_VAL;

    while (true) {
        HorizontalShiftGridSet *gridset = nullptr;
        const auto grid = findGrid(grids, lp, gridset);
        if (!grid) {
            proj_context_errno_set(ctx, PROJ_ERR_COORD_TRANSFM_OUTSIDE_GRID);
            return out;
        }
        if (grid->isNullGrid()) {
            return lp;
        }

        bool shouldRetry = false;
        out = pj_hgrid_apply_internal(ctx, lp, direction, grid, gridset, grids,
                                      shouldRetry);
        if (!shouldRetry) {
            break;
        }
    }

    if (out.lam == HUGE_VAL || out.phi == HUGE_VAL)
        proj_context_errno_set(ctx, PROJ_ERR_COORD_TRANSFM_OUTSIDE_GRID);

    return out;
}

/********************************************/
/*           proj_hgrid_value()             */
/*                                          */
/*    Return coordinate offset in grid      */
/********************************************/
PJ_LP pj_hgrid_value(PJ *P, const ListOfHGrids &grids, PJ_LP lp) {
    PJ_LP out = proj_coord_error().lp;

    HorizontalShiftGridSet *gridset = nullptr;
    const auto grid = findGrid(grids, lp, gridset);
    if (!grid) {
        proj_context_errno_set(P->ctx, PROJ_ERR_COORD_TRANSFM_OUTSIDE_GRID);
        return out;
    }

    /* normalize input to ll origin */
    const auto &extent = grid->extentAndRes();
    if (!extent.isGeographic) {
        pj_log(P->ctx, PJ_LOG_ERROR,
               _("Can only handle grids referenced in a geographic CRS"));
        proj_context_errno_set(P->ctx,
                               PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
        return out;
    }

    const double epsilon =
        (extent.resX + extent.resY) * REL_TOLERANCE_HGRIDSHIFT;
    lp.lam -= extent.west;
    if (lp.lam + epsilon < 0)
        lp.lam += 2 * M_PI;
    else if (lp.lam - epsilon > extent.east - extent.west)
        lp.lam -= 2 * M_PI;
    lp.phi -= extent.south;

    out = pj_hgrid_interpolate(lp, grid, false);
    if (grid->hasChanged()) {
        if (gridset->reopen(P->ctx)) {
            return pj_hgrid_value(P, grids, lp);
        }
        out.lam = HUGE_VAL;
        out.phi = HUGE_VAL;
    }

    if (out.lam == HUGE_VAL || out.phi == HUGE_VAL) {
        proj_context_errno_set(P->ctx, PROJ_ERR_COORD_TRANSFM_OUTSIDE_GRID);
    }

    return out;
}

// ---------------------------------------------------------------------------

static double read_vgrid_value(PJ_CONTEXT *ctx, const ListOfVGrids &grids,
                               const PJ_LP &input, const double vmultiplier) {

    /* do not deal with NaN coordinates */
    /* cppcheck-suppress duplicateExpression */
    if (std::isnan(input.phi) || std::isnan(input.lam)) {
        return HUGE_VAL;
    }

    VerticalShiftGridSet *curGridset = nullptr;
    const VerticalShiftGrid *grid = nullptr;
    for (const auto &gridset : grids) {
        grid = gridset->gridAt(input.lam, input.phi);
        if (grid) {
            curGridset = gridset.get();
            break;
        }
    }
    if (!grid) {
        proj_context_errno_set(ctx, PROJ_ERR_COORD_TRANSFM_OUTSIDE_GRID);
        return HUGE_VAL;
    }
    if (grid->isNullGrid()) {
        return 0;
    }

    const auto &extent = grid->extentAndRes();
    if (!extent.isGeographic) {
        pj_log(ctx, PJ_LOG_ERROR,
               _("Can only handle grids referenced in a geographic CRS"));
        proj_context_errno_set(ctx,
                               PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
        return HUGE_VAL;
    }

    /* Interpolation of a location within the grid */
    double grid_x = (input.lam - extent.west) * extent.invResX;
    if (input.lam < extent.west) {
        if (extent.fullWorldLongitude()) {
            // The first fmod goes to ]-lim, lim[ range
            // So we add lim again to be in ]0, 2*lim[ and fmod again
            grid_x = fmod(fmod(grid_x + grid->width(), grid->width()) +
                              grid->width(),
                          grid->width());
        } else {
            grid_x = (input.lam + 2 * M_PI - extent.west) * extent.invResX;
        }
    } else if (input.lam > extent.east) {
        if (extent.fullWorldLongitude()) {
            // The first fmod goes to ]-lim, lim[ range
            // So we add lim again to be in ]0, 2*lim[ and fmod again
            grid_x = fmod(fmod(grid_x + grid->width(), grid->width()) +
                              grid->width(),
                          grid->width());
        } else {
            grid_x = (input.lam - 2 * M_PI - extent.west) * extent.invResX;
        }
    }
    double grid_y = (input.phi - extent.south) * extent.invResY;
    int grid_ix = static_cast<int>(lround(floor(grid_x)));
    if (!(grid_ix >= 0 && grid_ix < grid->width())) {
        // in the unlikely case we end up here...
        pj_log(ctx, PJ_LOG_ERROR, _("grid_ix not in grid"));
        proj_context_errno_set(ctx, PROJ_ERR_COORD_TRANSFM_OUTSIDE_GRID);
        return HUGE_VAL;
    }
    int grid_iy = static_cast<int>(lround(floor(grid_y)));
    assert(grid_iy >= 0 && grid_iy < grid->height());
    grid_x -= grid_ix;
    grid_y -= grid_iy;

    int grid_ix2 = grid_ix + 1;
    if (grid_ix2 >= grid->width()) {
        if (extent.fullWorldLongitude()) {
            grid_ix2 = 0;
        } else {
            grid_ix2 = grid->width() - 1;
        }
    }
    int grid_iy2 = grid_iy + 1;
    if (grid_iy2 >= grid->height())
        grid_iy2 = grid->height() - 1;

    float value_a = 0;
    float value_b = 0;
    float value_c = 0;
    float value_d = 0;
    bool error = (!grid->valueAt(grid_ix, grid_iy, value_a) ||
                  !grid->valueAt(grid_ix2, grid_iy, value_b) ||
                  !grid->valueAt(grid_ix, grid_iy2, value_c) ||
                  !grid->valueAt(grid_ix2, grid_iy2, value_d));
    if (grid->hasChanged()) {
        if (curGridset->reopen(ctx)) {
            return read_vgrid_value(ctx, grids, input, vmultiplier);
        }
        error = true;
    }

    if (error) {
        return HUGE_VAL;
    }

    double value = 0.0;

    const double grid_x_y = grid_x * grid_y;
    const bool a_valid = !grid->isNodata(value_a, vmultiplier);
    const bool b_valid = !grid->isNodata(value_b, vmultiplier);
    const bool c_valid = !grid->isNodata(value_c, vmultiplier);
    const bool d_valid = !grid->isNodata(value_d, vmultiplier);
    const int countValid =
        static_cast<int>(a_valid) + static_cast<int>(b_valid) +
        static_cast<int>(c_valid) + static_cast<int>(d_valid);
    if (countValid == 4) {
        {
            double weight = 1.0 - grid_x - grid_y + grid_x_y;
            value = value_a * weight;
        }
        {
            double weight = grid_x - grid_x_y;
            value += value_b * weight;
        }
        {
            double weight = grid_y - grid_x_y;
            value += value_c * weight;
        }
        {
            double weight = grid_x_y;
            value += value_d * weight;
        }
    } else if (countValid == 0) {
        proj_context_errno_set(ctx, PROJ_ERR_COORD_TRANSFM_GRID_AT_NODATA);
        value = HUGE_VAL;
    } else {
        double total_weight = 0.0;
        if (a_valid) {
            double weight = 1.0 - grid_x - grid_y + grid_x_y;
            value = value_a * weight;
            total_weight = weight;
        }
        if (b_valid) {
            double weight = grid_x - grid_x_y;
            value += value_b * weight;
            total_weight += weight;
        }
        if (c_valid) {
            double weight = grid_y - grid_x_y;
            value += value_c * weight;
            total_weight += weight;
        }
        if (d_valid) {
            double weight = grid_x_y;
            value += value_d * weight;
            total_weight += weight;
        }
        value /= total_weight;
    }

    return value * vmultiplier;
}

/**********************************************/
ListOfVGrids pj_vgrid_init(PJ *P, const char *gridkey) {
    /**********************************************

      Initizalize and populate gridlist.

        Takes a PJ-object and the plus-parameter
        name that is used in the proj-string to
        specify the grids to load, e.g. "+grids".
        The + should be left out here.

        Returns the number of loaded grids.

    ***********************************************/

    std::string key("s");
    key += gridkey;
    const char *gridnames = pj_param(P->ctx, P->params, key.c_str()).s;
    if (gridnames == nullptr)
        return {};

    auto listOfGridNames = internal::split(std::string(gridnames), ',');
    ListOfVGrids grids;
    for (const auto &gridnameStr : listOfGridNames) {
        const char *gridname = gridnameStr.c_str();
        bool canFail = false;
        if (gridname[0] == '@') {
            canFail = true;
            gridname++;
        }
        auto gridSet = VerticalShiftGridSet::open(P->ctx, gridname);
        if (!gridSet) {
            if (!canFail) {
                if (proj_context_errno(P->ctx) !=
                    PROJ_ERR_OTHER_NETWORK_ERROR) {
                    proj_context_errno_set(
                        P->ctx, PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
                }
                return {};
            }
            proj_context_errno_set(P->ctx,
                                   0); // don't treat as a persistent error
        } else {
            grids.emplace_back(std::move(gridSet));
        }
    }

    return grids;
}

/***********************************************/
double pj_vgrid_value(PJ *P, const ListOfVGrids &grids, PJ_LP lp,
                      double vmultiplier) {
    /***********************************************

      Read grid value at position lp in grids loaded
      with proj_grid_init.

      Returns the grid value of the given coordinate.

    ************************************************/

    double value;

    value = read_vgrid_value(P->ctx, grids, lp, vmultiplier);
    if (pj_log_active(P->ctx, PJ_LOG_TRACE)) {
        proj_log_trace(P, "proj_vgrid_value: (%f, %f) = %f",
                       lp.lam * RAD_TO_DEG, lp.phi * RAD_TO_DEG, value);
    }

    return value;
}

// ---------------------------------------------------------------------------

const GenericShiftGrid *pj_find_generic_grid(const ListOfGenericGrids &grids,
                                             const PJ_LP &input,
                                             GenericShiftGridSet *&gridSetOut) {
    for (const auto &gridset : grids) {
        auto grid = gridset->gridAt(input.lam, input.phi);
        if (grid) {
            gridSetOut = gridset.get();
            return grid;
        }
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

// Used by +proj=deformation and +proj=xyzgridshift to do bilinear interpolation
// on 3 sample values per node.
bool pj_bilinear_interpolation_three_samples(
    PJ_CONTEXT *ctx, const GenericShiftGrid *grid, const PJ_LP &lp, int idx1,
    int idx2, int idx3, double &v1, double &v2, double &v3, bool &must_retry) {
    must_retry = false;
    if (grid->isNullGrid()) {
        v1 = 0.0;
        v2 = 0.0;
        v3 = 0.0;
        return true;
    }

    const auto &extent = grid->extentAndRes();
    if (!extent.isGeographic) {
        pj_log(ctx, PJ_LOG_ERROR,
               "Can only handle grids referenced in a geographic CRS");
        proj_context_errno_set(ctx,
                               PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
        return false;
    }

    // From a input location lp, determine the grid cell into which it falls,
    // by identifying the lower-left x,y of it (ix, iy), and the upper-right
    // (ix2, iy2)

    double grid_x = (lp.lam - extent.west) * extent.invResX;
    // Special case for grids with world extent, and dealing with wrap-around
    if (lp.lam < extent.west) {
        grid_x = (lp.lam + 2 * M_PI - extent.west) * extent.invResX;
    } else if (lp.lam > extent.east) {
        grid_x = (lp.lam - 2 * M_PI - extent.west) * extent.invResX;
    }
    double grid_y = (lp.phi - extent.south) * extent.invResY;
    int ix = static_cast<int>(grid_x);
    int iy = static_cast<int>(grid_y);
    int ix2 = std::min(ix + 1, grid->width() - 1);
    int iy2 = std::min(iy + 1, grid->height() - 1);

    float dx1 = 0.0f, dy1 = 0.0f, dz1 = 0.0f;
    float dx2 = 0.0f, dy2 = 0.0f, dz2 = 0.0f;
    float dx3 = 0.0f, dy3 = 0.0f, dz3 = 0.0f;
    float dx4 = 0.0f, dy4 = 0.0f, dz4 = 0.0f;
    bool error = (!grid->valueAt(ix, iy, idx1, dx1) ||
                  !grid->valueAt(ix, iy, idx2, dy1) ||
                  !grid->valueAt(ix, iy, idx3, dz1) ||
                  !grid->valueAt(ix2, iy, idx1, dx2) ||
                  !grid->valueAt(ix2, iy, idx2, dy2) ||
                  !grid->valueAt(ix2, iy, idx3, dz2) ||
                  !grid->valueAt(ix, iy2, idx1, dx3) ||
                  !grid->valueAt(ix, iy2, idx2, dy3) ||
                  !grid->valueAt(ix, iy2, idx3, dz3) ||
                  !grid->valueAt(ix2, iy2, idx1, dx4) ||
                  !grid->valueAt(ix2, iy2, idx2, dy4) ||
                  !grid->valueAt(ix2, iy2, idx3, dz4));
    if (grid->hasChanged()) {
        must_retry = true;
        return false;
    }
    if (error) {
        return false;
    }

    // Bilinear interpolation
    double frct_lam = grid_x - ix;
    double frct_phi = grid_y - iy;
    double m10 = frct_lam;
    double m11 = m10;
    double m01 = 1. - frct_lam;
    double m00 = m01;
    m11 *= frct_phi;
    m01 *= frct_phi;
    frct_phi = 1. - frct_phi;
    m00 *= frct_phi;
    m10 *= frct_phi;

    v1 = m00 * dx1 + m10 * dx2 + m01 * dx3 + m11 * dx4;
    v2 = m00 * dy1 + m10 * dy2 + m01 * dy3 + m11 * dy4;
    v3 = m00 * dz1 + m10 * dz2 + m01 * dz3 + m11 * dz4;
    return true;
}

NS_PROJ_END

/*****************************************************************************/
PJ_GRID_INFO proj_grid_info(const char *gridname) {
    /******************************************************************************
        Information about a named datum grid.

        Returns PJ_GRID_INFO struct.
    ******************************************************************************/
    PJ_GRID_INFO grinfo;

    /*PJ_CONTEXT *ctx = proj_context_create(); */
    PJ_CONTEXT *ctx = pj_get_default_ctx();
    memset(&grinfo, 0, sizeof(PJ_GRID_INFO));

    const auto fillGridInfo = [&grinfo, ctx,
                               gridname](const NS_PROJ::Grid &grid,
                                         const std::string &format) {
        const auto &extent = grid.extentAndRes();

        /* name of grid */
        strncpy(grinfo.gridname, gridname, sizeof(grinfo.gridname) - 1);

        /* full path of grid */
        if (!pj_find_file(ctx, gridname, grinfo.filename,
                          sizeof(grinfo.filename) - 1)) {
            // Can happen when using a remote grid
            grinfo.filename[0] = 0;
        }

        /* grid format */
        strncpy(grinfo.format, format.c_str(), sizeof(grinfo.format) - 1);

        /* grid size */
        grinfo.n_lon = grid.width();
        grinfo.n_lat = grid.height();

        /* cell size */
        grinfo.cs_lon = extent.resX;
        grinfo.cs_lat = extent.resY;

        /* bounds of grid */
        grinfo.lowerleft.lam = extent.west;
        grinfo.lowerleft.phi = extent.south;
        grinfo.upperright.lam = extent.east;
        grinfo.upperright.phi = extent.north;
    };

    {
        const auto gridSet = NS_PROJ::VerticalShiftGridSet::open(ctx, gridname);
        if (gridSet) {
            const auto &grids = gridSet->grids();
            if (!grids.empty()) {
                const auto &grid = grids.front();
                fillGridInfo(*grid, gridSet->format());
                return grinfo;
            }
        }
    }

    {
        const auto gridSet =
            NS_PROJ::HorizontalShiftGridSet::open(ctx, gridname);
        if (gridSet) {
            const auto &grids = gridSet->grids();
            if (!grids.empty()) {
                const auto &grid = grids.front();
                fillGridInfo(*grid, gridSet->format());
                return grinfo;
            }
        }
    }
    strcpy(grinfo.format, "missing");
    return grinfo;
}

/*****************************************************************************/
PJ_INIT_INFO proj_init_info(const char *initname) {
    /******************************************************************************
        Information about a named init file.

        Maximum length of initname is 64.

        Returns PJ_INIT_INFO struct.

        If the init file is not found all members of the return struct are set
        to the empty string.

        If the init file is found, but the metadata is missing, the value is
        set to "Unknown".
    ******************************************************************************/
    int file_found;
    char param[80], key[74];
    paralist *start, *next;
    PJ_INIT_INFO ininfo;
    PJ_CONTEXT *ctx = pj_get_default_ctx();

    memset(&ininfo, 0, sizeof(PJ_INIT_INFO));

    file_found =
        pj_find_file(ctx, initname, ininfo.filename, sizeof(ininfo.filename));
    if (!file_found || strlen(initname) > 64) {
        if (strcmp(initname, "epsg") == 0 || strcmp(initname, "EPSG") == 0) {
            const char *val;

            proj_context_errno_set(ctx, 0);

            strncpy(ininfo.name, initname, sizeof(ininfo.name) - 1);
            strcpy(ininfo.origin, "EPSG");
            val = proj_context_get_database_metadata(ctx, "EPSG.VERSION");
            if (val) {
                strncpy(ininfo.version, val, sizeof(ininfo.version) - 1);
            }
            val = proj_context_get_database_metadata(ctx, "EPSG.DATE");
            if (val) {
                strncpy(ininfo.lastupdate, val, sizeof(ininfo.lastupdate) - 1);
            }
            return ininfo;
        }

        if (strcmp(initname, "IGNF") == 0) {
            const char *val;

            proj_context_errno_set(ctx, 0);

            strncpy(ininfo.name, initname, sizeof(ininfo.name) - 1);
            strcpy(ininfo.origin, "IGNF");
            val = proj_context_get_database_metadata(ctx, "IGNF.VERSION");
            if (val) {
                strncpy(ininfo.version, val, sizeof(ininfo.version) - 1);
            }
            val = proj_context_get_database_metadata(ctx, "IGNF.DATE");
            if (val) {
                strncpy(ininfo.lastupdate, val, sizeof(ininfo.lastupdate) - 1);
            }
            return ininfo;
        }

        return ininfo;
    }

    /* The initial memset (0) makes strncpy safe here */
    strncpy(ininfo.name, initname, sizeof(ininfo.name) - 1);
    strcpy(ininfo.origin, "Unknown");
    strcpy(ininfo.version, "Unknown");
    strcpy(ininfo.lastupdate, "Unknown");

    strncpy(key, initname, 64); /* make room for ":metadata\0" at the end */
    key[64] = 0;
    memcpy(key + strlen(key), ":metadata", 9 + 1);
    strcpy(param, "+init=");
    /* The +strlen(param) avoids a cppcheck false positive warning */
    strncat(param + strlen(param), key, sizeof(param) - 1 - strlen(param));

    start = pj_mkparam(param);
    pj_expand_init(ctx, start);

    if (pj_param(ctx, start, "tversion").i)
        strncpy(ininfo.version, pj_param(ctx, start, "sversion").s,
                sizeof(ininfo.version) - 1);

    if (pj_param(ctx, start, "torigin").i)
        strncpy(ininfo.origin, pj_param(ctx, start, "sorigin").s,
                sizeof(ininfo.origin) - 1);

    if (pj_param(ctx, start, "tlastupdate").i)
        strncpy(ininfo.lastupdate, pj_param(ctx, start, "slastupdate").s,
                sizeof(ininfo.lastupdate) - 1);

    for (; start; start = next) {
        next = start->next;
        free(start);
    }

    return ininfo;
}
