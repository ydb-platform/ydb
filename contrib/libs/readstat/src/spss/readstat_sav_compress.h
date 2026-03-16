enum sav_row_stream_status {
    SAV_ROW_STREAM_NEED_DATA,
    SAV_ROW_STREAM_HAVE_DATA,
    SAV_ROW_STREAM_FINISHED_ROW,
    SAV_ROW_STREAM_FINISHED_ALL
};
struct sav_row_stream_s {
    const unsigned char  *next_in;
    size_t                avail_in;

    unsigned char        *next_out;
    size_t                avail_out;

    uint64_t              missing_value;
    double                bias;

    unsigned char         chunk[8];
    int                   i;
    int                   bswap;

    enum sav_row_stream_status status;
};

size_t sav_compressed_row_bound(size_t uncompressed_length);
size_t sav_compress_row(void *output_row, void *input_row, size_t input_len,
        readstat_writer_t *writer);
void sav_decompress_row(struct sav_row_stream_s *state);
