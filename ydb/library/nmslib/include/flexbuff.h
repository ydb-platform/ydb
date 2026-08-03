/**
 * Non-metric Space Library
 *
 * Main developers: Bilegsaikhan Naidan, Leonid Boytsov, Yury Malkov, Ben Frederickson, David Novak
 *
 * For the complete list of contributors and further details see:
 * https://github.com/nmslib/nmslib
 *
 * Copyright (c) 2013-2018
 *
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 */
#include <memory>

#ifndef FLEX_MINIMUM_ALLOC_BUFF_H
#define FLEX_MINIMUM_ALLOC_BUFF_H

/*
 * Allocates an array
 *
 * 1) on the stack, if the number of elements doesn't exceed iMaxStackQty
 * 2) on the heap (and on the stack), if the number of elements does exceed iMaxStackQty
 *
 * One allocation can be relatively expensive (dozens or hundreds CPU cycles). However,
 * the amortized allocation cost can be quite small, if, most of the time, we deal
 * with small data that can be allocated on the stack.
 *
 * A word of caution: 
 *  i) Use small values for iMaxStackQty (so that allocated memory is in the order of 1-4KB or smaller),
 *  ii) The stack has limited size (by default on Linux it's 8 MB). 
 *  iii) Therefore, the more threads you use the more likely the program will crash.
 */
#define DECLARE_FLEXIBLE_BUFF(ElemType, varName, iBuffQty, iMaxStackQty) \
        std::unique_ptr<ElemType[]>     varName ## HeapBuff; \
        ElemType                        varName ## LocalBuff[iMaxStackQty];\
        ElemType*                       varName = varName ## LocalBuff ;\
        if (iBuffQty > iMaxStackQty) {\
            varName ## HeapBuff.reset(new ElemType[iBuffQty]);\
            varName = varName ## HeapBuff .get() ;\
        }

/*

  Some dumb code that can be used to test the above (also, e.g, with valgrind). 
  Unfortunately, you can't do with a unit test.

// Change to 1 to see if the delete was actually called
#if 0
void  operator delete  ( void* ptr   ) { cout << "delete called" << endl; free( ptr ); }
void  operator delete[]( void* ptr   ) { cout << "delete[] called" << endl; free( ptr ); }
#endif


int main(int,char**) {
  // Maximum default stack size is 8192MB or 2048 ints
  // One must be careful here not to exceed this number
  const unsigned ELEM_QTY = 1024 * 32;

  size_t bufQty = ELEM_QTY +1, stackQty = ELEM_QTY ;
  DECLARE_FLEXIBLE_BUFF(int, var1, bufQty, stackQty);
  for (int i = 0; i < bufQty; ++i) var1[i] = 255;

  bufQty = ELEM_QTY ; stackQty = ELEM_QTY ;
  DECLARE_FLEXIBLE_BUFF(int, var2, bufQty, stackQty);
  for (int i = 0; i < bufQty; ++i) var2[i] = 255;

  DECLARE_FLEXIBLE_BUFF(char, var3, 1025, 1024);
  for (int i = 0; i < 1025; ++i) var3[i] = 255;

  DECLARE_FLEXIBLE_BUFF(double, var4, ELEM_QTY * 2, ELEM_QTY);
  for (int i = 0; i < ELEM_QTY * 2; ++i) var4[i] = -1;
}

*/


#endif

