/*********************************************************************
  Blosc - Blocked Shuffling and Compression Library

  Author: Francesc Alted <francesc@blosc.org>
  Creation date: 2018-01-03

  See LICENSE.txt for details about copyright and rights to use.
**********************************************************************/

#ifndef BLOSC_FASTCOPY_H
#define BLOSC_FASTCOPY_H

/* Same semantics than memcpy() */
unsigned char *fastcopy(unsigned char *out, const unsigned char *from, unsigned len);

/* Same as fastcopy() but without overwriting origin or destination when they overlap */
unsigned char* copy_match(unsigned char *out, const unsigned char *from, unsigned len);

#endif //BLOSC_FASTCOPY_H
