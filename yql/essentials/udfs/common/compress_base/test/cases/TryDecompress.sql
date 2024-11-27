/* syntax version 1 */
$bad = "Is not compressed!";

SELECT
    TryDecompress::Gzip(Compress::Gzip($bad, 3)) = $bad AS ok_Gzip,
    TryDecompress::Gzip($bad) AS bad_Gzip,
    TryDecompress::Zlib(Compress::Zlib($bad, 3)) = $bad AS ok_Zlib,
    TryDecompress::Zlib($bad) AS bad_Zlib,
    TryDecompress::Brotli(Compress::Brotli($bad, 3)) = $bad AS ok_Brotli,
    TryDecompress::Brotli($bad) AS bad_Brotli,
    TryDecompress::Lzma(Compress::Lzma($bad, 3)) = $bad AS ok_Lzma,
    TryDecompress::Lzma($bad) AS bad_Lzma,
    TryDecompress::BZip2(Compress::BZip2($bad, 3)) = $bad AS ok_BZip2,
    TryDecompress::BZip2($bad) AS bad_BZip2,
    TryDecompress::Snappy(Compress::Snappy($bad)) = $bad AS ok_Snappy,
    TryDecompress::Snappy($bad) AS bad_Snappy,
    TryDecompress::Zstd(Compress::Zstd($bad, 3)) = $bad AS ok_Zstd,
    TryDecompress::Zstd($bad) AS bad_Zstd;

