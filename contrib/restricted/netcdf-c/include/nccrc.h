/*
Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
See COPYRIGHT for license information.
*/

/*
CRC32 and CRC64 implementations from Mark Adler
*/

#ifndef NCCRC_H
#define NCCRC_H 1

EXTERNL unsigned int NC_crc32(unsigned int crc, const void* buf, unsigned int len);
EXTERNL unsigned long long NC_crc64(unsigned long long crc, void* buf, unsigned int len);

#endif /*NCCRC_H*/
