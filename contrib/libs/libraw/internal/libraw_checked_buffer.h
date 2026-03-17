#pragma once

#include <vector>
#include "../libraw/libraw_types.h"

class checked_buffer_t
{
public:
  // create with internal storage
	checked_buffer_t(short ord, int size);
	checked_buffer_t(short ord, unsigned char *dd, int ss);
	ushort sget2(int offset);
	ushort sget2LL(INT64 offset) { return sget2(int(offset)); }
	void checkoffset(int off);
	unsigned char operator[](int idx);
	unsigned sget4(int offset);
	unsigned sget4LL(INT64 offset) { return sget4(int(offset)); }
	double sgetreal(int type, int offset);
	float sgetrealf(int type, int offset) { return float(sgetreal(type, offset)); }
    unsigned char *data() { return _data; }
 
	int tiff_sget(unsigned save, INT64 *tag_offset, unsigned *tag_id, unsigned *tag_type, INT64 *tag_dataoffset,
		unsigned *tag_datalen, int *tag_dataunitlen);
protected:
  short _order;

 private:
  unsigned char *_data;
  int _len;
  std::vector<unsigned char> storage;
};
