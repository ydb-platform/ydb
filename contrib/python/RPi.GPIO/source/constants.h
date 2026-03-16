/*
Copyright (c) 2013-2021 Ben Croston

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#define PY_PUD_CONST_OFFSET 20
#define PY_EVENT_CONST_OFFSET 30

extern PyObject *high;
extern PyObject *low;
extern PyObject *input;
extern PyObject *output;
extern PyObject *pwm;
extern PyObject *serial;
extern PyObject *i2c;
extern PyObject *spi;
extern PyObject *unknown;
extern PyObject *board;
extern PyObject *bcm;
extern PyObject *pud_off;
extern PyObject *pud_up;
extern PyObject *pud_down;
extern PyObject *rising_edge;
extern PyObject *falling_edge;
extern PyObject *both_edge;

void define_constants(PyObject *module);
