/* syntax version 1 */
$level = 5;

SELECT
    Ensure(Compress::Gzip(value, $level), Decompress::Gzip(Compress::Gzip(value, $level)) == value, "gzip failed at: " || value) AS gzip,
    Ensure(Compress::Zlib(value, $level), Decompress::Zlib(Compress::Zlib(value, $level)) == value, "zlib failed at: " || value) AS zlib,
    Ensure(Compress::Brotli(value, $level), Decompress::Brotli(Compress::Brotli(value, $level)) == value, "brotli failed at: " || value) AS brotli,
    Ensure(Compress::Lzma(value, $level), Decompress::Lzma(Compress::Lzma(value, $level)) == value, "lzma failed at: " || value) AS lzma,
    Ensure(Compress::BZip2(value, $level), Decompress::BZip2(Compress::BZip2(value, $level)) == value, "bzip2 failed at: " || value) AS bzip2,
    Ensure(Compress::Zstd(value, $level), Decompress::Zstd(Compress::Zstd(value, $level)) == value, "zstd failed at: " || value) AS zstd,
    Ensure(Compress::Snappy(value), Decompress::Snappy(Compress::Snappy(value)) == value, "Snappy failed at: " || value) AS snappy,
FROM Input;
