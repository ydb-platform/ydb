#include <stdint.h>

/*
 * Workaround for DJGPP to find uint8_t, uint16_t, etc.
 */
#if defined(__MSDOS__) && defined(__GNUC__)
#include <stdint-gcc.h>
#endif

void REF_Level1_decompress(const uint8_t* input, int length, uint8_t* output) {
  int src = 0;
  int dest = 0;
  while (src < length) {
    int type = input[src] >> 5;
    if (type == 0) {
      /* literal run */
      int run = 1 + input[src];
      src = src + 1;
      while (run > 0) {
        output[dest] = input[src];
        src = src + 1;
        dest = dest + 1;
        run = run - 1;
      }
    } else if (type < 7) {
      /* short match */
      int ofs = 256 * (input[src] & 31) + input[src + 1];
      int len = 2 + (input[src] >> 5);
      src = src + 2;
      int ref = dest - ofs - 1;
      while (len > 0) {
        output[dest] = output[ref];
        ref = ref + 1;
        dest = dest + 1;
        len = len - 1;
      }
    } else {
      /* long match */
      int ofs = 256 * (input[src] & 31) + input[src + 2];
      int len = 9 + input[src + 1];
      src = src + 3;
      int ref = dest - ofs - 1;
      while (len > 0) {
        output[dest] = output[ref];
        ref = ref + 1;
        dest = dest + 1;
        len = len - 1;
      }
    }
  }
}

void REF_Level2_decompress(const uint8_t* input, int length, uint8_t* output) {
  int src = 0;
  int dest = 0;
  while (src < length) {
    int type = input[src] >> 5;
    if (type == 0) {
      /* literal run */
      int run = 1 + input[src];
      src = src + 1;
      while (run > 0) {
        output[dest] = input[src];
        src = src + 1;
        dest = dest + 1;
        run = run - 1;
      }
    } else {
      int next = 2;
      int len = 2 + (input[src] >> 5);
      if (len == 9) {
        /* long match */
        next = next + 1;
        len = len + input[src + 1];
        if (len == 9 + 255) {
          /* Gamma code for match length */
          int nn = input[src + 1];
          while (nn == 255) {
            nn = input[src + next - 1];
            next = next + 1;
            len += nn;
          }
        }
      }

      int ofs = 256 * (input[src] & 31) + input[src + next - 1];
      if (ofs == 8191) {
        /* match from 16-bit distance */
        ofs += 256 * input[src + next] + input[src + next + 1];
        next = next + 2;
      }
      src = src + next;

      int ref = dest - ofs - 1;
      while (len > 0) {
        output[dest] = output[ref];
        ref = ref + 1;
        dest = dest + 1;
        len = len - 1;
      }
    }
  }
}
