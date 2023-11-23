#include "lib/compress_base_udf.h"

using namespace NYql::NUdf;

namespace NCompress {
    SIMPLE_MODULE(TCompressModule, EXPORTED_COMPRESS_BASE_UDF);
}

namespace NDecompress {
    SIMPLE_MODULE(TDecompressModule, EXPORTED_DECOMPRESS_BASE_UDF);
}

namespace NTryDecompress {
    SIMPLE_MODULE(TTryDecompressModule, EXPORTED_TRY_DECOMPRESS_BASE_UDF);
}

REGISTER_MODULES(NCompress::TCompressModule, NDecompress::TDecompressModule, NTryDecompress::TTryDecompressModule);
