## AWS C Compression

This is a cross-platform C99 implementation of compression algorithms such as
gzip, and huffman encoding/decoding. Currently only huffman is implemented.

## License

This library is licensed under the Apache 2.0 License.

## Usage

### Building

Note that aws-c-compression has a dependency on aws-c-common:

```
git clone git@github.com:awslabs/aws-c-common.git
cmake -DCMAKE_PREFIX_PATH=<install-path> -DCMAKE_INSTALL_PREFIX=<install-path> -S aws-c-common -B aws-c-common/build
cmake --build aws-c-common/build --target install

git clone git@github.com:awslabs/aws-c-compression.git
cmake -DCMAKE_PREFIX_PATH=<install-path> -DCMAKE_INSTALL_PREFIX=<install-path> -S aws-c-compression -B aws-c-compression/build
cmake --build aws-c-compression/build --target install
```

### Huffman

The Huffman implemention in this library is designed around the concept of a
generic "symbol coder" object, which defines how each symbol (value between 0
and 255) is encoded and decoded. This object looks like this:
```c
typedef struct aws_huffman_code (*aws_huffman_symbol_encoder)(uint8_t symbol, void *userdata);
typedef uint8_t (*aws_huffman_symbol_decoder)(uint32_t bits, uint8_t *symbol, void *userdata);

struct aws_huffman_symbol_coder {
    aws_huffman_symbol_encoder encode;
    aws_huffman_symbol_decoder decode;
    void *userdata;
};
```
These callbacks may be implemented manually, or you may use the included
Huffman coder generator to generate one from a table definition file. The
generator expects to be called with the following arguments:
```shell
$ aws-c-compression-huffman-generator path/to/table.def path/to/generated.c coder_name
```

The table definition file should be in the following format:
```c
/*           sym               bits   code len */
HUFFMAN_CODE(  0,      "1100101110", 0x32e, 10)
HUFFMAN_CODE(  1,      "1100101111", 0x32f, 10)
/* ... */
```
The HUFFMAN_CODE macro expects 4 arguments:
* sym: the symbol value [0-255]
* bits: the bits representing the symbol in string form
* code: the bits representing the symbol in numeric form
* len: the number of bits used to represent the symbol

> #### Note
> This file may also be `#include`d in the following way to generate a static
> list of codes:
> ```c
> /* Provides the HUFFMAN_CODE macro */
> #include <aws/testing/compression/huffman.h>
>
> static struct huffman_test_code_point code_points[] = {
> #include "test_huffman_static_table.def"
> };
> ```

This will emit a c file which exports a function with the following signiture:
```c
struct aws_huffman_symbol_coder *{coder_name}_get_coder();
```
Note that this function does not allocate, but maintains a static instance of
the coder.


An example implementation of this file is provided in
`tests/test_huffman_static_table.def`.


To use the coder, forward declare that function, and pass the result as the
second argument to `aws_huffman_encoder_init` and `aws_huffman_decoder_init`.
```c
struct aws_huffman_encoder encoder;
aws_huffman_encoder_init(&encoder, {coder_name}_get_coder());

struct aws_huffman_decoder decoder;
aws_huffman_decoder_init(&decoder, {coder_name}_get_coder())
```

#### Encoding
```c
/**
 * Encode a symbol buffer into the output buffer.
 *
 * \param[in]       encoder         The encoder object to use
 * \param[in]       to_encode       The symbol buffer to encode
 * \param[in,out]   length          In: The length of to_decode. Out: The number of bytes read from to_encode
 * \param[in]       output          The buffer to write encoded bytes to
 * \param[in,out]   output_size     In: The size of output. Out: The number of bytes written to output
 *
 * \return AWS_OP_SUCCESS if encoding is successful, AWS_OP_ERR the code for the error that occured
 */
int aws_huffman_encode(struct aws_huffman_encoder *encoder, const char *to_encode, size_t *length, uint8_t *output, size_t *output_size);
```
The encoder is built to support partial encoding. This means that if there
isn't enough space in `output`, the encoder will encode as much as possible,
update `length` to indicate how much was consumed, `output_size` won't change,
and `AWS_ERROR_SHORT_BUFFER` will be raised. `aws_huffman_encode` may then be
called again like the following pseudo-code:
```c
void encode_and_send(const char *to_encode, size_t size) {
    while (size > 0) {
        uint8_t output[some_chunk_size];
        size_t output_size = sizeof(output);
        size_t bytes_read = size;
        aws_huffman_encode(encoder, to_encode, &bytes_read, output, &output_size);
        /* AWS_ERROR_SHORT_BUFFER was raised... */
        send_output_to_someone_else(output, output_size);

        to_encode += bytes_read;
        size -= bytes_read;
    }
    /* Be sure to reset the encoder after use */
    aws_huffman_encoder_reset(encoder);
}
```

`aws_huffman_encoder` also has a `uint8_t` field called `eos_padding` that
defines how any unwritten bits in the last byte of output are filled. The most
significant bits will used. For example, if the last byte contains only 3 bits
and `eos_padding` is `0b01010101`, `01010` will be appended to the byte.

#### Decoding
```c
/**
 * Decodes a byte buffer into the provided symbol array.
 *
 * \param[in]       decoder         The decoder object to use
 * \param[in]       to_decode       The encoded byte buffer to read from
 * \param[in,out]   length          In: The length of to_decode. Out: The number of bytes read from to_decode
 * \param[in]       output          The buffer to write decoded symbols to
 * \param[in,out]   output_size     In: The size of output. Out: The number of bytes written to output
 *
 * \return AWS_OP_SUCCESS if encoding is successful, AWS_OP_ERR the code for the error that occured
 */
int aws_huffman_decode(struct aws_huffman_decoder *decoder, const uint8_t *to_decode, size_t *length, char *output, size_t *output_size);
```
The decoder is built to support partial encoding. This means that if there
isn't enough space in `output`, the decoder will decode as much as possible,
update `length` to indicate how much was consumed, `output_size` won't change,
and `AWS_ERROR_SHORT_BUFFER` will be raised. `aws_huffman_decode` may then be
called again like the following pseudo-code:
```c
void decode_and_send(const char *to_decode, size_t size) {
    while (size > 0) {
        uint8_t output[some_chunk_size];
        size_t output_size = sizeof(output);
        size_t bytes_read = size;
        aws_huffman_decode(decoder, to_decode, &bytes_read, output, &output_size);
        /* AWS_ERROR_SHORT_BUFFER was raised... */
        send_output_to_someone_else(output, output_size);

        to_decode += bytes_read;
        size -= bytes_read;
    }
    /* Be sure to reset the decoder after use */
    aws_huffman_decoder_reset(decoder);
}
```

Upon completion of a decode, the most significant bits of
`decoder->working_bits` will contain the final bits of `to_decode` that could
not match a symbol. This is useful for verifying the padding bits of a stream.
For example, to validate that a stream ends in all 1's (like HPACK requires),
you could do the following:
```c
AWS_ASSERT(decoder->working_bits == UINT64_MAX << (64 - decoder->num_bits));
```
