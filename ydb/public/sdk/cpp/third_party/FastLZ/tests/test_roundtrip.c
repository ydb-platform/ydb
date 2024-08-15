/*
  FastLZ - Byte-aligned LZ77 compression library
  Copyright (C) 2005-2020 Ariya Hidayat <ariya.hidayat@gmail.com>

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in
  all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
  THE SOFTWARE.
*/

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "fastlz.h"

/*
 * Workaround for DJGPP to find uint8_t, uint16_t, etc.
 */
#if defined(__MSDOS__) && defined(__GNUC__)
#include <stdint-gcc.h>
#endif

#define LOG
#undef LOG

int compare(const char* name, const uint8_t* a, const uint8_t* b, int size) {
  int bad = 0;
  int i;
  for (i = 0; i < size; ++i) {
    if (a[i] != b[i]) {
      bad = 1;
      printf("Error on %s!\n", name);
      printf("Different at index %d: expecting %02x,actual %02x\n", i, a[i], b[i]);
      break;
    }
  }
  return bad;
}

#if !defined(__MSDOS__)
#define MAX_FILE_SIZE (100 * 1024 * 1024)
#else
#define MAX_FILE_SIZE (32 * 1024 * 1024)
#endif

/* prototype, implemented in refimpl.c */
void REF_Level1_decompress(const uint8_t* input, int length, uint8_t* output);
void REF_Level2_decompress(const uint8_t* input, int length, uint8_t* output);

/*
  Same as test_roundtrip_level1 EXCEPT that the decompression is carried out
  using the highly-simplified, unoptimized vanilla reference decompressor.
*/

void test_ref_decompressor_level1(const char* name, const char* file_name) {
#ifdef LOG
  printf("Processing %s...\n", name);
#endif
  FILE* f = fopen(file_name, "rb");
  if (!f) {
    printf("Error: can not open %s!\n", file_name);
    exit(1);
  }
  fseek(f, 0L, SEEK_END);
  long file_size = ftell(f);
  rewind(f);

#ifdef LOG
  printf("Size is %ld bytes.\n", file_size);
#endif
  if (file_size > MAX_FILE_SIZE) {
    fclose(f);
    printf("%25s %10ld [skipped, file too big]\n", name, file_size);
    return;
  }

  uint8_t* file_buffer = malloc(file_size);
  long read = fread(file_buffer, 1, file_size, f);
  fclose(f);
  if (read != file_size) {
    free(file_buffer);
    printf("Error: only read %ld bytes!\n", read);
    exit(1);
  }

#ifdef LOG
  printf("Compressing. Please wait...\n");
#endif
  uint8_t* compressed_buffer = malloc(1.05 * file_size);
  int compressed_size = fastlz_compress_level(1, file_buffer, file_size, compressed_buffer);
  double ratio = (100.0 * compressed_size) / file_size;
#ifdef LOG
  printf("Compressing was completed: %ld -> %ld (%.2f%%)\n", file_size, compressed_size, ratio);
#endif

#ifdef LOG
  printf("Decompressing. Please wait...\n");
#endif
  uint8_t* uncompressed_buffer = malloc(file_size);
  if (uncompressed_buffer == NULL) {
    printf("%25s %10ld  -> %10d  (%.2f%%)  skipped, can't decompress\n", name, file_size, compressed_size, ratio);
    return;
  }
  memset(uncompressed_buffer, '-', file_size);
  REF_Level1_decompress(compressed_buffer, compressed_size, uncompressed_buffer);
#ifdef LOG
  printf("Comparing. Please wait...\n");
#endif
  int result = compare(file_name, file_buffer, uncompressed_buffer, file_size);
  if (result == 1) {
    free(uncompressed_buffer);
    exit(1);
  }

  free(file_buffer);
  free(compressed_buffer);
  free(uncompressed_buffer);
#ifdef LOG
  printf("OK.\n");
#else
  printf("%25s %10ld  -> %10d  (%.2f%%)\n", name, file_size, compressed_size, ratio);
#endif
}

/*
  Same as test_roundtrip_level2 EXCEPT that the decompression is carried out
  using the highly-simplified, unoptimized vanilla reference decompressor.
*/

void test_ref_decompressor_level2(const char* name, const char* file_name) {
#ifdef LOG
  printf("Processing %s...\n", name);
#endif
  FILE* f = fopen(file_name, "rb");
  if (!f) {
    printf("Error: can not open %s!\n", file_name);
    exit(1);
  }
  fseek(f, 0L, SEEK_END);
  long file_size = ftell(f);
  rewind(f);

#ifdef LOG
  printf("Size is %ld bytes.\n", file_size);
#endif
  if (file_size > MAX_FILE_SIZE) {
    fclose(f);
    printf("%25s %10ld [skipped, file too big]\n", name, file_size);
    return;
  }

  uint8_t* file_buffer = malloc(file_size);
  long read = fread(file_buffer, 1, file_size, f);
  fclose(f);
  if (read != file_size) {
    free(file_buffer);
    printf("Error: only read %ld bytes!\n", read);
    exit(1);
  }

#ifdef LOG
  printf("Compressing. Please wait...\n");
#endif
  uint8_t* compressed_buffer = malloc(1.05 * file_size);
  int compressed_size = fastlz_compress_level(2, file_buffer, file_size, compressed_buffer);
  double ratio = (100.0 * compressed_size) / file_size;
#ifdef LOG
  printf("Compressing was completed: %ld -> %ld (%.2f%%)\n", file_size, compressed_size, ratio);
#endif

#ifdef LOG
  printf("Decompressing. Please wait...\n");
#endif
  uint8_t* uncompressed_buffer = malloc(file_size);
  if (uncompressed_buffer == NULL) {
    printf("%25s %10ld  -> %10d  (%.2f%%)  skipped, can't decompress\n", name, file_size, compressed_size, ratio);
    return;
  }
  memset(uncompressed_buffer, '-', file_size);

  /* intentionally mask out the block tag */
  compressed_buffer[0] = compressed_buffer[0] & 31;

  REF_Level2_decompress(compressed_buffer, compressed_size, uncompressed_buffer);
#ifdef LOG
  printf("Comparing. Please wait...\n");
#endif
  int result = compare(file_name, file_buffer, uncompressed_buffer, file_size);
  if (result == 1) {
    free(uncompressed_buffer);
    exit(1);
  }

  free(file_buffer);
  free(compressed_buffer);
  free(uncompressed_buffer);
#ifdef LOG
  printf("OK.\n");
#else
  printf("%25s %10ld  -> %10d  (%.2f%%)\n", name, file_size, compressed_size, ratio);
#endif
}

/*
  Read the content of the file.
  Compress it first using the Level 1 compressor.
  Decompress the output with Level 1 decompressor.
  Compare the result with the original file content.
*/
void test_roundtrip_level1(const char* name, const char* file_name) {
#ifdef LOG
  printf("Processing %s...\n", name);
#endif
  FILE* f = fopen(file_name, "rb");
  if (!f) {
    printf("Error: can not open %s!\n", file_name);
    exit(1);
  }
  fseek(f, 0L, SEEK_END);
  long file_size = ftell(f);
  rewind(f);

#ifdef LOG
  printf("Size is %ld bytes.\n", file_size);
#endif
  if (file_size > MAX_FILE_SIZE) {
    fclose(f);
    printf("%25s %10ld [skipped, file too big]\n", name, file_size);
    return;
  }

  uint8_t* file_buffer = malloc(file_size);
  long read = fread(file_buffer, 1, file_size, f);
  fclose(f);
  if (read != file_size) {
    free(file_buffer);
    printf("Error: only read %ld bytes!\n", read);
    exit(1);
  }

#ifdef LOG
  printf("Compressing. Please wait...\n");
#endif
  uint8_t* compressed_buffer = malloc(1.05 * file_size);
  int compressed_size = fastlz_compress_level(1, file_buffer, file_size, compressed_buffer);
  double ratio = (100.0 * compressed_size) / file_size;
#ifdef LOG
  printf("Compressing was completed: %ld -> %ld (%.2f%%)\n", file_size, compressed_size, ratio);
#endif

#ifdef LOG
  printf("Decompressing. Please wait...\n");
#endif
  uint8_t* uncompressed_buffer = malloc(file_size);
  if (uncompressed_buffer == NULL) {
    printf("%25s %10ld  -> %10d  (%.2f%%)  skipped, can't decompress\n", name, file_size, compressed_size, ratio);
    return;
  }
  memset(uncompressed_buffer, '-', file_size);
  fastlz_decompress(compressed_buffer, compressed_size, uncompressed_buffer, file_size);
#ifdef LOG
  printf("Comparing. Please wait...\n");
#endif
  int result = compare(file_name, file_buffer, uncompressed_buffer, file_size);
  if (result == 1) {
    free(uncompressed_buffer);
    exit(1);
  }

  free(file_buffer);
  free(compressed_buffer);
  free(uncompressed_buffer);
#ifdef LOG
  printf("OK.\n");
#else
  printf("%25s %10ld  -> %10d  (%.2f%%)\n", name, file_size, compressed_size, ratio);
#endif
}

/*
  Read the content of the file.
  Compress it first using the Level 2 compressor.
  Decompress the output with Level 2 decompressor.
  Compare the result with the original file content.
*/
void test_roundtrip_level2(const char* name, const char* file_name) {
#ifdef LOG
  printf("Processing %s...\n", name);
#endif
  FILE* f = fopen(file_name, "rb");
  if (!f) {
    printf("Error: can not open %s!\n", file_name);
    exit(1);
  }
  fseek(f, 0L, SEEK_END);
  long file_size = ftell(f);
  rewind(f);

#ifdef LOG
  printf("Size is %ld bytes.\n", file_size);
#endif
  if (file_size > MAX_FILE_SIZE) {
    fclose(f);
    printf("%25s %10ld [skipped, file too big]\n", name, file_size);
    return;
  }

  uint8_t* file_buffer = malloc(file_size);
  long read = fread(file_buffer, 1, file_size, f);
  fclose(f);
  if (read != file_size) {
    free(file_buffer);
    printf("Error: only read %ld bytes!\n", read);
    exit(1);
  }

#ifdef LOG
  printf("Compressing. Please wait...\n");
#endif
  uint8_t* compressed_buffer = malloc(1.05 * file_size);
  int compressed_size = fastlz_compress_level(2, file_buffer, file_size, compressed_buffer);
  double ratio = (100.0 * compressed_size) / file_size;
#ifdef LOG
  printf("Compressing was completed: %ld -> %ld (%.2f%%)\n", file_size, compressed_size, ratio);
#endif

#ifdef LOG
  printf("Decompressing. Please wait...\n");
#endif
  uint8_t* uncompressed_buffer = malloc(file_size);
  if (uncompressed_buffer == NULL) {
    free(file_buffer);
    free(compressed_buffer);
    printf("%25s %10ld  -> %10d  (%.2f%%)  skipped, can't decompress OOM\n", name, file_size, compressed_size, ratio);
    exit(1);
    return;
  }
  memset(uncompressed_buffer, '-', file_size);
  fastlz_decompress(compressed_buffer, compressed_size, uncompressed_buffer, file_size);
#ifdef LOG
  printf("Comparing. Please wait...\n");
#endif
  int result = compare(file_name, file_buffer, uncompressed_buffer, file_size);
  if (result == 1) {
    free(uncompressed_buffer);
    exit(1);
  }

  free(file_buffer);
  free(compressed_buffer);
  free(uncompressed_buffer);
#ifdef LOG
  printf("OK.\n");
#else
  printf("%25s %10ld  -> %10d  (%.2f%%)\n", name, file_size, compressed_size, ratio);
#endif
}

int main(int argc, char** argv) {
  const char* default_prefix = "../compression-corpus/";
  const char* names[] = {"canterbury/alice29.txt",
                         "canterbury/asyoulik.txt",
                         "canterbury/cp.html",
                         "canterbury/fields.c",
                         "canterbury/grammar.lsp",
                         "canterbury/kennedy.xls",
                         "canterbury/lcet10.txt",
                         "canterbury/plrabn12.txt",
                         "canterbury/ptt5",
                         "canterbury/sum",
                         "canterbury/xargs.1",
                         "silesia/dickens",
                         "silesia/mozilla",
                         "silesia/mr",
                         "silesia/nci",
                         "silesia/ooffice",
                         "silesia/osdb",
                         "silesia/reymont",
                         "silesia/samba",
                         "silesia/sao",
                         "silesia/webster",
                         "silesia/x-ray",
                         "silesia/xml",
                         "enwik/enwik8.txt"};

  const char* prefix = (argc == 2) ? argv[1] : default_prefix;

  const int count = sizeof(names) / sizeof(names[0]);
  int i;

  printf("Test reference decompressor for Level 1\n\n");
  for (i = 0; i < count; ++i) {
    const char* name = names[i];
    char* filename = malloc(strlen(prefix) + strlen(name) + 1);
    strcpy(filename, prefix);
    strcat(filename, name);
    test_ref_decompressor_level1(name, filename);
    free(filename);
  }
  printf("\n");

  printf("Test reference decompressor for Level 2\n\n");
  for (i = 0; i < count; ++i) {
    const char* name = names[i];
    char* filename = malloc(strlen(prefix) + strlen(name) + 1);
    strcpy(filename, prefix);
    strcat(filename, name);
    test_ref_decompressor_level2(name, filename);
    free(filename);
  }
  printf("\n");

  printf("Test round-trip for Level 1\n\n");
  for (i = 0; i < count; ++i) {
    const char* name = names[i];
    char* filename = malloc(strlen(prefix) + strlen(name) + 1);
    strcpy(filename, prefix);
    strcat(filename, name);
    test_roundtrip_level1(name, filename);
    free(filename);
  }
  printf("\n");

  printf("Test round-trip for Level 2\n\n");
  for (i = 0; i < count; ++i) {
    const char* name = names[i];
    char* filename = malloc(strlen(prefix) + strlen(name) + 1);
    strcpy(filename, prefix);
    strcat(filename, name);
    test_roundtrip_level2(name, filename);
    free(filename);
  }
  printf("\n");

  return 0;
}
