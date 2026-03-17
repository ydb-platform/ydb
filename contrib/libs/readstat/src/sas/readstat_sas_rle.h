
ssize_t sas_rle_decompress(void *output_buf, size_t output_len,
        const void *input_buf, size_t input_len);
ssize_t sas_rle_compress(void *output_buf, size_t output_len,
        const void *input_buf, size_t input_len);

ssize_t sas_rle_decompressed_len(const void *input_buf, size_t input_len);
ssize_t sas_rle_compressed_len(const void *bytes, size_t len);
