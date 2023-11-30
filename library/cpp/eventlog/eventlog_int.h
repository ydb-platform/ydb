#pragma once

#include <util/stream/output.h>
#include <util/generic/maybe.h>
#include <util/generic/utility.h>
#include <util/generic/yexception.h>
#include <util/ysaveload.h>

using TEventClass = ui32;
using TEventLogFormat = ui32;
using TEventTimestamp = ui64;

constexpr TStringBuf COMPRESSED_LOG_FRAME_SYNC_DATA =
    "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
    "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
    "\x00\x00\x00\x00\xfe\x00\x00\xff\xff\x00\x00\xff\xff\x00"
    "\x00\xff\xff\x00\x00\xff\xff\x00\x00\xff\xff\x00\x00\xff"
    "\xff\x00\x00\xff\xff\x00\x00\xff"sv;

static_assert(COMPRESSED_LOG_FRAME_SYNC_DATA.size() == 64);

/*
 * Коды форматов логов. Форматом лога считается формат служебных
 * структур лога. К примеру формат заголовка, наличие компрессии, и т.д.
 * Имеет значение только 1 младший байт.
 */

enum EEventLogFormat : TEventLogFormat {
    // Формат версии 1. Используется компрессор LZQ.
    COMPRESSED_LOG_FORMAT_V1 = 1,

    // Формат версии 2. Используется компрессор ZLIB. Добавлены CRC заголовка и данных,
    // поле типа компрессора.
    COMPRESSED_LOG_FORMAT_V2 = 2,

    // Формат версии 3. Используется компрессор ZLIB. В начинке фреймов перед каждым событием добавлен его размер.
    COMPRESSED_LOG_FORMAT_V3 = 3,

    // Lz4hc codec + zlib
    COMPRESSED_LOG_FORMAT_V4 = 4 /* "zlib_lz4" */,

    // zstd
    COMPRESSED_LOG_FORMAT_V5 = 5 /* "zstd" */,
};

TMaybe<TEventLogFormat> ParseEventLogFormat(TStringBuf str);

#pragma pack(push, 1)

struct TCompressedFrameBaseHeader {
    TEventLogFormat Format;
    ui32 Length; // Длина остатка фрейма в байтах, после этого заголовка
    ui32 FrameId;
};

struct TCompressedFrameHeader {
    TEventTimestamp StartTimestamp;
    TEventTimestamp EndTimestamp;
    ui32 UncompressedDatalen; // Длина данных, которые были закомпрессированы
    ui32 PayloadChecksum;     // В логе версии 1 поле не используется
};

struct TCompressedFrameHeader2: public TCompressedFrameHeader {
    ui8 CompressorVersion; // Сейчас не используется
    ui32 HeaderChecksum;
};

#pragma pack(pop)

Y_DECLARE_PODTYPE(TCompressedFrameBaseHeader);
Y_DECLARE_PODTYPE(TCompressedFrameHeader);
Y_DECLARE_PODTYPE(TCompressedFrameHeader2);
