#cython: language_level=3
# Attribute parsing flags
DEF _ATTR_CODEC_MASK = 0x07
DEF _ATTR_CODEC_NONE = 0x00
DEF _ATTR_CODEC_GZIP = 0x01
DEF _ATTR_CODEC_SNAPPY = 0x02
DEF _ATTR_CODEC_LZ4 = 0x03
DEF _ATTR_CODEC_ZSTD = 0x04

DEF _TIMESTAMP_TYPE_MASK = 0x08
DEF _TRANSACTIONAL_MASK = 0x10
DEF _CONTROL_MASK = 0x20

# NOTE: freelists are used based on the assumption, that those will only be
#       temporary objects and actual structs from `aiokafka.structs` will be
#       return to user.
DEF _LEGACY_RECORD_METADATA_FREELIST_SIZE = 20
# Fetcher will only use 1 parser per partition, so this is based on how much
# partitions can be used simultaniously.
DEF _LEGACY_RECORD_BATCH_FREELIST_SIZE = 100
DEF _LEGACY_RECORD_FREELIST_SIZE = 100

DEF _DEFAULT_RECORD_METADATA_FREELIST_SIZE = 20
DEF _DEFAULT_RECORD_BATCH_FREELIST_SIZE = 100
DEF _DEFAULT_RECORD_FREELIST_SIZE = 100
