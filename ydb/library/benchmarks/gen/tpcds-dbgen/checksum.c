/* 
 * Legal Notice 
 * 
 * This document and associated source code (the "Work") is a part of a 
 * benchmark specification maintained by the TPC. 
 * 
 * The TPC reserves all right, title, and interest to the Work as provided 
 * under U.S. and international laws, including without limitation all patent 
 * and trademark rights therein. 
 * 
 * No Warranty 
 * 
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION 
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE 
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER 
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY, 
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES, 
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR 
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF 
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE. 
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT, 
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT 
 *     WITH REGARD TO THE WORK. 
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO 
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE 
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS 
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT, 
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT 
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD 
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES. 
 * 
 * Contributors:
 * Gradient Systems
 */ 
#define _LARGEFILE64_SOURCE 1
#define _FILE_OFFSET_BITS 64
#include <stdio.h>
#include <stdlib.h>
 
unsigned short GetCCITT (unsigned short crc, unsigned short ch)
{
  static unsigned int i;
 
  ch <<= 8;
  for (i=8; i>0; i--) {
    if ((ch ^ crc) & 0X8000)
      crc = (crc << 1 ) ^ 0x1021;
    else
      crc <<= 1;
    ch <<= 1;
  }
  return (crc);
}
 
int main(int argc, char *argv[]) {
  FILE *fin;
  char *buffer;
  size_t i, j;
  long long int nLF=0, nCR=0, nChar=0, nDelim=0;
  unsigned short crc=0;
 
  if (argc < 2) {
    fin = stdin;
  } else {
    if (( fin = fopen (argv[1], "rb")) == NULL ) {
      fprintf (stderr, "Cannot open %s\n", argv[1]);
      return (1);
    }
  }
 
  if ((buffer = (char *)malloc(32766)) == NULL) {
    fprintf (stderr, "Out of memory\n");
    return (1);
  }
 
  for (;;) {
    i = fread(buffer, 1, 32766, fin);
    if (i == 0) {
      if (feof (fin)) {
        printf("CCITT CRC for %s is %04X; #LF/#CR is %lld/%lld; #Delim is %lld; #Chars is %lld\n",
          argv[1], crc, nLF, nCR, nDelim, nChar);
        return (0);
      }
      else
        continue;
    }
    for (j=0; j<i; j++) {
      ++nChar;
      switch (buffer[j]) {
        case 10: ++nLF; break;
        case 13: ++nCR; break;
        case '|': ++nDelim; crc = GetCCITT (crc, buffer[j]); break;
        default: crc = GetCCITT (crc, buffer[j]); break;
      }
    }
  }
}
