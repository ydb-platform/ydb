/*
 * spidev_module.c - Python bindings for Linux SPI access through spidev
 *
 * MIT License
 *
 * Copyright (C) 2009 Volker Thoms <unconnected@gmx.de>
 * Copyright (C) 2012 Stephen Caudle <scaudle@doceme.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include <Python.h>
#include "structmember.h"
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <linux/spi/spidev.h>
#include <linux/types.h>
#include <sys/ioctl.h>
#include <linux/ioctl.h>

#define _VERSION_ "3.8"
#define SPIDEV_MAXPATH 4096

#define BLOCK_SIZE_CONTROL_FILE "/sys/module/spidev/parameters/bufsiz"
// The xfwr3 function attempts to use large blocks if /sys/module/spidev/parameters/bufsiz setting allows it.
// However where we cannot get a value from that file, we fall back to this safe default.
#define XFER3_DEFAULT_BLOCK_SIZE SPIDEV_MAXPATH
// Largest block size for xfer3 - even if /sys/module/spidev/parameters/bufsiz allows bigger
// blocks, we won't go above this value. As I understand, DMA is not used for anything bigger so why bother.
#define XFER3_MAX_BLOCK_SIZE 65535


#if PY_MAJOR_VERSION < 3
#define PyLong_AS_LONG(val) PyInt_AS_LONG(val)
#define PyLong_AsLong(val) PyInt_AsLong(val)
#endif

// Macros needed for Python 3
#ifndef PyInt_Check
#define PyInt_Check			PyLong_Check
#define PyInt_FromLong	PyLong_FromLong
#define PyInt_AsLong		PyLong_AsLong
#define PyInt_Type			PyLong_Type
#endif

// Maximum block size for xfer3
// Initialised once by get_xfer3_block_size
uint32_t xfer3_block_size = 0;

// Read maximum block size from the /sys/module/spidev/parameters/bufsiz
// In case of any problems reading the number, we fall back to XFER3_DEFAULT_BLOCK_SIZE.
// If number is read ok but it exceeds the XFER3_MAX_BLOCK_SIZE, it will be capped to that value.
// The value is read and cached on the first invocation. Following invocations just return the cached one.
uint32_t get_xfer3_block_size(void) {
	int value;

	// If value was already initialised, just use it
	if (xfer3_block_size != 0) {
		return xfer3_block_size;
	}

	// Start with the default
	xfer3_block_size = XFER3_DEFAULT_BLOCK_SIZE;

	FILE *file = fopen(BLOCK_SIZE_CONTROL_FILE,"r");
	if (file != NULL) {
		if (fscanf(file, "%d", &value) == 1 && value > 0) {
			if (value <= XFER3_MAX_BLOCK_SIZE) {
				xfer3_block_size = value;
			} else {
				xfer3_block_size = XFER3_MAX_BLOCK_SIZE;
			}
		}
		fclose(file);
	}

	return xfer3_block_size;
}

PyDoc_STRVAR(SpiDev_module_doc,
	"This module defines an object type that allows SPI transactions\n"
	"on hosts running the Linux kernel. The host kernel must have SPI\n"
	"support and SPI device interface support.\n"
	"All of these can be either built-in to the kernel, or loaded from\n"
	"modules.\n"
	"\n"
	"Because the SPI device interface is opened R/W, users of this\n"
	"module usually must have root permissions.\n");

typedef struct {
	PyObject_HEAD

	int fd;	/* open file descriptor: /dev/spidevX.Y */
	uint8_t mode;	/* current SPI mode */
	uint8_t bits_per_word;	/* current SPI bits per word setting */
	uint32_t max_speed_hz;	/* current SPI max speed setting in Hz */
	uint8_t read0;	/* read 0 bytes after transfer to lwoer CS if SPI_CS_HIGH */
} SpiDevObject;

static PyObject *
SpiDev_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
	SpiDevObject *self;
	if ((self = (SpiDevObject *)type->tp_alloc(type, 0)) == NULL)
		return NULL;

	self->fd = -1;
	self->mode = 0;
	self->bits_per_word = 0;
	self->max_speed_hz = 0;

	Py_INCREF(self);
	return (PyObject *)self;
}

PyDoc_STRVAR(SpiDev_close_doc,
	"close()\n\n"
	"Disconnects the object from the interface.\n");

static PyObject *
SpiDev_close(SpiDevObject *self)
{
	if ((self->fd != -1) && (close(self->fd) == -1)) {
		PyErr_SetFromErrno(PyExc_IOError);
		return NULL;
	}

	self->fd = -1;
	self->mode = 0;
	self->bits_per_word = 0;
	self->max_speed_hz = 0;

	Py_INCREF(Py_None);
	return Py_None;
}

static void
SpiDev_dealloc(SpiDevObject *self)
{
	PyObject *ref = SpiDev_close(self);
	Py_XDECREF(ref);

	Py_TYPE(self)->tp_free((PyObject *)self);
}

static char *wrmsg_list0 = "Empty argument list.";
static char *wrmsg_listmax = "Argument list size exceeds %d bytes.";
static char *wrmsg_val = "Non-Int/Long value in arguments: %x.";
static char *wrmsg_oom = "Out of memory.";


PyDoc_STRVAR(SpiDev_write_doc,
	"write([values]) -> None\n\n"
	"Write bytes to SPI device.\n");

static PyObject *
SpiDev_writebytes(SpiDevObject *self, PyObject *args)
{
	int		status;
	uint16_t	ii, len;
	uint8_t	buf[SPIDEV_MAXPATH];
	PyObject	*obj;
	PyObject	*seq;
	char	wrmsg_text[4096];

	if (!PyArg_ParseTuple(args, "O:write", &obj))
		return NULL;

	seq = PySequence_Fast(obj, "expected a sequence");
	if (!seq)
		return NULL;

	len = PySequence_Fast_GET_SIZE(seq);
	if (len <= 0) {
		PyErr_SetString(PyExc_TypeError, wrmsg_list0);
		return NULL;
	}

	if (len > SPIDEV_MAXPATH) {
		snprintf(wrmsg_text, sizeof (wrmsg_text) - 1, wrmsg_listmax, SPIDEV_MAXPATH);
		PyErr_SetString(PyExc_OverflowError, wrmsg_text);
		return NULL;
	}

	for (ii = 0; ii < len; ii++) {
		PyObject *val = PySequence_Fast_GET_ITEM(seq, ii);
#if PY_MAJOR_VERSION < 3
		if (PyInt_Check(val)) {
			buf[ii] = (__u8)PyInt_AS_LONG(val);
		} else
#endif
		{
			if (PyLong_Check(val)) {
				buf[ii] = (__u8)PyLong_AS_LONG(val);
			} else {
				snprintf(wrmsg_text, sizeof (wrmsg_text) - 1, wrmsg_val, val);
				PyErr_SetString(PyExc_TypeError, wrmsg_text);
				return NULL;
			}
		}
	}

	Py_DECREF(seq);

	status = write(self->fd, &buf[0], len);

	if (status < 0) {
		PyErr_SetFromErrno(PyExc_IOError);
		return NULL;
	}

	if (status != len) {
		perror("short write");
		return NULL;
	}

	Py_INCREF(Py_None);
	return Py_None;
}

PyDoc_STRVAR(SpiDev_read_doc,
	"read(len) -> [values]\n\n"
	"Read len bytes from SPI device.\n");

static PyObject *
SpiDev_readbytes(SpiDevObject *self, PyObject *args)
{
	uint8_t	rxbuf[SPIDEV_MAXPATH];
	int		status, len, ii;
	PyObject	*list;

	if (!PyArg_ParseTuple(args, "i:read", &len))
		return NULL;

	/* read at least 1 byte, no more than SPIDEV_MAXPATH */
	if (len < 1)
		len = 1;
	else if ((unsigned)len > sizeof(rxbuf))
		len = sizeof(rxbuf);

	memset(rxbuf, 0, sizeof rxbuf);
	status = read(self->fd, &rxbuf[0], len);

	if (status < 0) {
		PyErr_SetFromErrno(PyExc_IOError);
		return NULL;
	}

	if (status != len) {
		perror("short read");
		return NULL;
	}

	list = PyList_New(len);

	for (ii = 0; ii < len; ii++) {
		PyObject *val = PyLong_FromLong((long)rxbuf[ii]);
		PyList_SET_ITEM(list, ii, val);  // Steals reference, no need to Py_DECREF(val)
	}

	return list;
}

static PyObject *
SpiDev_writebytes2_buffer(SpiDevObject *self, Py_buffer *buffer)
{
	int		status;
	Py_ssize_t	remain, block_size, block_start, spi_max_block;

	spi_max_block = get_xfer3_block_size();

	block_start = 0;
	remain = buffer->len;
	while (block_start < buffer->len) {
		block_size = (remain < spi_max_block) ? remain : spi_max_block;

		Py_BEGIN_ALLOW_THREADS
		status = write(self->fd, buffer->buf + block_start, block_size);
		Py_END_ALLOW_THREADS

		if (status < 0) {
			PyErr_SetFromErrno(PyExc_IOError);
			return NULL;
		}

		if (status != block_size) {
			perror("short write");
			return NULL;
		}

		block_start += block_size;
		remain -= block_size;
	}

	Py_INCREF(Py_None);
	return Py_None;
}

static PyObject *
SpiDev_writebytes2_seq_internal(SpiDevObject *self, PyObject *seq, Py_ssize_t len, uint8_t *buf, Py_ssize_t bufsize)
{
	int		status;
	Py_ssize_t	ii, jj, remain, block_size;
	char	wrmsg_text[4096];

	remain = len;
	jj = 0;
	while (remain > 0) {
		block_size = (remain < bufsize) ? remain : bufsize;

		for (ii = 0; ii < block_size; ii++, jj++) {
			PyObject *val = PySequence_Fast_GET_ITEM(seq, jj);
#if PY_MAJOR_VERSION < 3
			if (PyInt_Check(val)) {
				buf[ii] = (__u8)PyInt_AS_LONG(val);
			} else
#endif
			{
				if (PyLong_Check(val)) {
					buf[ii] = (__u8)PyLong_AS_LONG(val);
				} else {
					snprintf(wrmsg_text, sizeof (wrmsg_text) - 1, wrmsg_val, val);
					PyErr_SetString(PyExc_TypeError, wrmsg_text);
					return NULL;
				}
			}
		}

		Py_BEGIN_ALLOW_THREADS
		status = write(self->fd, buf, block_size);
		Py_END_ALLOW_THREADS

		if (status < 0) {
			PyErr_SetFromErrno(PyExc_IOError);
			return NULL;
		}

		if (status != block_size) {
			perror("short write");
			return NULL;
		}

		remain -= block_size;
	}

	Py_INCREF(Py_None);
	return Py_None;
}

// In writebytes2 we try to avoild doing malloc/free on each tiny block.
// So for any transfer below this size we will use on-stack local buffer instead of allocating one on the heap.
#define SMALL_BUFFER_SIZE 128

static PyObject *
SpiDev_writebytes2_seq(SpiDevObject *self, PyObject *seq)
{
	Py_ssize_t	len, bufsize, spi_max_block;
	PyObject	*result = NULL;

	len = PySequence_Fast_GET_SIZE(seq);
	if (len <= 0) {
		PyErr_SetString(PyExc_TypeError, wrmsg_list0);
		return NULL;
	}

	spi_max_block = get_xfer3_block_size();

	bufsize = (len < spi_max_block) ? len : spi_max_block;

	if (bufsize <= SMALL_BUFFER_SIZE) {
		// The data size is very small so we can avoid malloc/free completely
		// by using a small local buffer instead
		uint8_t buf[SMALL_BUFFER_SIZE];
		result = SpiDev_writebytes2_seq_internal(self, seq, len, buf, SMALL_BUFFER_SIZE);
	} else {
		// Large data, need to allocate buffer on heap
		uint8_t	*buf;
		Py_BEGIN_ALLOW_THREADS
		buf = malloc(sizeof(__u8) * bufsize);
		Py_END_ALLOW_THREADS

		if (!buf) {
			PyErr_SetString(PyExc_OverflowError, wrmsg_oom);
			return NULL;
		}

		result = SpiDev_writebytes2_seq_internal(self, seq, len, buf, bufsize);

		Py_BEGIN_ALLOW_THREADS
		free(buf);
		Py_END_ALLOW_THREADS
	}

	return result;
}

PyDoc_STRVAR(SpiDev_writebytes2_doc,
	"writebytes2([values]) -> None\n\n"
	"Write bytes to SPI device.\n"
	"values must be a list or buffer.\n");

static PyObject *
SpiDev_writebytes2(SpiDevObject *self, PyObject *args)
{
	PyObject	*obj, *seq;;
	PyObject	*result = NULL;

	if (!PyArg_ParseTuple(args, "O:writebytes2", &obj)) {
		return NULL;
	}

	// Try using buffer protocol if object supports it.
	if (PyObject_CheckBuffer(obj) && 1) {
		Py_buffer	buffer;
		if (PyObject_GetBuffer(obj, &buffer, PyBUF_SIMPLE) != -1) {
			result = SpiDev_writebytes2_buffer(self, &buffer);
			PyBuffer_Release(&buffer);
			return result;
		}
	}


	// Otherwise, fall back to sequence protocol
	seq = PySequence_Fast(obj, "expected a sequence");
	if (seq == NULL) {
		return NULL;
	}

	result = SpiDev_writebytes2_seq(self, seq);

	Py_DECREF(seq);

	return result;

}

PyDoc_STRVAR(SpiDev_xfer_doc,
	"xfer([values]) -> [values]\n\n"
	"Perform SPI transaction.\n"
	"CS will be released and reactivated between blocks.\n"
	"delay specifies delay in usec between blocks.\n");

static PyObject *
SpiDev_xfer(SpiDevObject *self, PyObject *args)
{
	uint16_t ii, len;
	int status;
	uint16_t delay_usecs = 0;
	uint32_t speed_hz = 0;
	uint8_t bits_per_word = 0;
	PyObject *obj;
	PyObject *seq;
#ifdef SPIDEV_SINGLE
	struct spi_ioc_transfer *xferptr;
	memset(&xferptr, 0, sizeof(xferptr));
#else
	struct spi_ioc_transfer xfer;
	memset(&xfer, 0, sizeof(xfer));
#endif
	uint8_t *txbuf, *rxbuf;
	char	wrmsg_text[4096];

	if (!PyArg_ParseTuple(args, "O|IHB:xfer", &obj, &speed_hz, &delay_usecs, &bits_per_word))
		return NULL;

	seq = PySequence_Fast(obj, "expected a sequence");
	if (!seq) {
		return NULL;
	}

	len = PySequence_Fast_GET_SIZE(seq);
	if (len <= 0) {
		Py_DECREF(seq);
		PyErr_SetString(PyExc_TypeError, wrmsg_list0);
		return NULL;
	}

	if (len > SPIDEV_MAXPATH) {
		snprintf(wrmsg_text, sizeof(wrmsg_text) - 1, wrmsg_listmax, SPIDEV_MAXPATH);
		PyErr_SetString(PyExc_OverflowError, wrmsg_text);
		Py_DECREF(seq);
		return NULL;
	}

	txbuf = malloc(sizeof(__u8) * len);
	rxbuf = malloc(sizeof(__u8) * len);

#ifdef SPIDEV_SINGLE
	xferptr = (struct spi_ioc_transfer*) malloc(sizeof(struct spi_ioc_transfer) * len);

	for (ii = 0; ii < len; ii++) {
		PyObject *val = PySequence_Fast_GET_ITEM(seq, ii);
#if PY_MAJOR_VERSION < 3
		if (PyInt_Check(val)) {
			txbuf[ii] = (__u8)PyInt_AS_LONG(val);
		} else
#endif
		{
			if (PyLong_Check(val)) {
				txbuf[ii] = (__u8)PyLong_AS_LONG(val);
			} else {
				snprintf(wrmsg_text, sizeof(wrmsg_text) - 1, wrmsg_val, val);
				PyErr_SetString(PyExc_TypeError, wrmsg_text);
				free(xferptr);
				free(txbuf);
				free(rxbuf);
				Py_DECREF(seq);
				return NULL;
			}
		}
		xferptr[ii].tx_buf = (unsigned long)&txbuf[ii];
		xferptr[ii].rx_buf = (unsigned long)&rxbuf[ii];
		xferptr[ii].len = 1;
		xferptr[ii].delay_usecs = delay;
		xferptr[ii].speed_hz = speed_hz ? speed_hz : self->max_speed_hz;
		xferptr[ii].bits_per_word = bits_per_word ? bits_per_word : self->bits_per_word;
#ifdef SPI_IOC_WR_MODE32
		xferptr[ii].tx_nbits = 0;
#endif
#ifdef SPI_IOC_RD_MODE32
		xferptr[ii].rx_nbits = 0;
#endif
	}

	status = ioctl(self->fd, SPI_IOC_MESSAGE(len), xferptr);
	free(xferptr);
	if (status < 0) {
		PyErr_SetFromErrno(PyExc_IOError);
		free(txbuf);
		free(rxbuf);
		Py_DECREF(seq);
		return NULL;
	}
#else
	for (ii = 0; ii < len; ii++) {
		PyObject *val = PySequence_Fast_GET_ITEM(seq, ii);
#if PY_MAJOR_VERSION < 3
		if (PyInt_Check(val)) {
			txbuf[ii] = (__u8)PyInt_AS_LONG(val);
		} else
#endif
		{
			if (PyLong_Check(val)) {
				txbuf[ii] = (__u8)PyLong_AS_LONG(val);
			} else {
				snprintf(wrmsg_text, sizeof(wrmsg_text) - 1, wrmsg_val, val);
				PyErr_SetString(PyExc_TypeError, wrmsg_text);
				free(txbuf);
				free(rxbuf);
				Py_DECREF(seq);
				return NULL;
			}
		}
	}

	if (PyTuple_Check(obj)) {
		Py_DECREF(seq);
		seq = PySequence_List(obj);
	}

	xfer.tx_buf = (unsigned long)txbuf;
	xfer.rx_buf = (unsigned long)rxbuf;
	xfer.len = len;
	xfer.delay_usecs = delay_usecs;
	xfer.speed_hz = speed_hz ? speed_hz : self->max_speed_hz;
	xfer.bits_per_word = bits_per_word ? bits_per_word : self->bits_per_word;
#ifdef SPI_IOC_WR_MODE32
	xfer.tx_nbits = 0;
#endif
#ifdef SPI_IOC_RD_MODE32
	xfer.rx_nbits = 0;
#endif

	status = ioctl(self->fd, SPI_IOC_MESSAGE(1), &xfer);
	if (status < 0) {
		PyErr_SetFromErrno(PyExc_IOError);
		free(txbuf);
		free(rxbuf);
		Py_DECREF(seq);
		return NULL;
	}
#endif

	for (ii = 0; ii < len; ii++) {
		PyObject *val = PyLong_FromLong((long)rxbuf[ii]);
		PySequence_SetItem(seq, ii, val);
		Py_DECREF(val); // PySequence_SetItem does not steal reference, must Py_DECREF(val)
	}

	// WA:
	// in CS_HIGH mode CS isn't pulled to low after transfer, but after read
	// reading 0 bytes doesnt matter but brings cs down
	// tomdean:
	// Stop generating an extra CS except in mode CS_HOGH
	if (self->read0 && (self->mode & SPI_CS_HIGH)) status = read(self->fd, &rxbuf[0], 0);

	free(txbuf);
	free(rxbuf);

	if (PyTuple_Check(obj)) {
		PyObject *old = seq;
		seq = PySequence_Tuple(seq);
		Py_DECREF(old);
	}

	return seq;
}


PyDoc_STRVAR(SpiDev_xfer2_doc,
	"xfer2([values]) -> [values]\n\n"
	"Perform SPI transaction.\n"
	"CS will be held active between blocks.\n");

static PyObject *
SpiDev_xfer2(SpiDevObject *self, PyObject *args)
{
	int status;
	uint16_t delay_usecs = 0;
	uint32_t speed_hz = 0;
	uint8_t bits_per_word = 0;
	uint16_t ii, len;
	PyObject *obj;
	PyObject *seq;
	struct spi_ioc_transfer xfer;
	Py_BEGIN_ALLOW_THREADS
	memset(&xfer, 0, sizeof(xfer));
	Py_END_ALLOW_THREADS
	uint8_t *txbuf, *rxbuf;
	char	wrmsg_text[4096];

	if (!PyArg_ParseTuple(args, "O|IHB:xfer2", &obj, &speed_hz, &delay_usecs, &bits_per_word))
		return NULL;

	seq = PySequence_Fast(obj, "expected a sequence");
	if (!seq) {
		return NULL;
	}

	len = PySequence_Fast_GET_SIZE(seq);
	if (len <= 0) {
		Py_DECREF(seq);
		PyErr_SetString(PyExc_TypeError, wrmsg_list0);
		return NULL;
	}

	if (len > SPIDEV_MAXPATH) {
		snprintf(wrmsg_text, sizeof(wrmsg_text) - 1, wrmsg_listmax, SPIDEV_MAXPATH);
		PyErr_SetString(PyExc_OverflowError, wrmsg_text);
		Py_DECREF(seq);
		return NULL;
	}

	Py_BEGIN_ALLOW_THREADS
	txbuf = malloc(sizeof(__u8) * len);
	rxbuf = malloc(sizeof(__u8) * len);
	Py_END_ALLOW_THREADS

	for (ii = 0; ii < len; ii++) {
		PyObject *val = PySequence_Fast_GET_ITEM(seq, ii);
#if PY_MAJOR_VERSION < 3
		if (PyInt_Check(val)) {
			txbuf[ii] = (__u8)PyInt_AS_LONG(val);
		} else
#endif
		{
			if (PyLong_Check(val)) {
				txbuf[ii] = (__u8)PyLong_AS_LONG(val);
			} else {
				snprintf(wrmsg_text, sizeof (wrmsg_text) - 1, wrmsg_val, val);
				PyErr_SetString(PyExc_TypeError, wrmsg_text);
				free(txbuf);
				free(rxbuf);
				Py_DECREF(seq);
				return NULL;
			}
		}
	}

	if (PyTuple_Check(obj)) {
		Py_DECREF(seq);
		seq = PySequence_List(obj);
	}

	Py_BEGIN_ALLOW_THREADS
	xfer.tx_buf = (unsigned long)txbuf;
	xfer.rx_buf = (unsigned long)rxbuf;
	xfer.len = len;
	xfer.delay_usecs = delay_usecs;
	xfer.speed_hz = speed_hz ? speed_hz : self->max_speed_hz;
	xfer.bits_per_word = bits_per_word ? bits_per_word : self->bits_per_word;

	status = ioctl(self->fd, SPI_IOC_MESSAGE(1), &xfer);
	Py_END_ALLOW_THREADS
	if (status < 0) {
		PyErr_SetFromErrno(PyExc_IOError);
		free(txbuf);
		free(rxbuf);
		Py_DECREF(seq);
		return NULL;
	}

	for (ii = 0; ii < len; ii++) {
		PyObject *val = PyLong_FromLong((long)rxbuf[ii]);
		PySequence_SetItem(seq, ii, val);
		Py_DECREF(val); // PySequence_SetItem does not steal reference, must Py_DECREF(val)
	}
	// WA:
	// in CS_HIGH mode CS isnt pulled to low after transfer
	// reading 0 bytes doesn't really matter but brings CS down
	// tomdean:
	// Stop generating an extra CS except in mode CS_HOGH
	if (self->read0 && (self->mode & SPI_CS_HIGH)) status = read(self->fd, &rxbuf[0], 0);

	Py_BEGIN_ALLOW_THREADS
	free(txbuf);
	free(rxbuf);
	Py_END_ALLOW_THREADS


	if (PyTuple_Check(obj)) {
		PyObject *old = seq;
		seq = PySequence_Tuple(seq);
		Py_DECREF(old);
	}

	return seq;
}

PyDoc_STRVAR(SpiDev_xfer3_doc,
	"xfer3([values]) -> [values]\n\n"
	"Perform SPI transaction. Accepts input of arbitrary size.\n"
	"Large blocks will be send as multiple transactions\n"
	"CS will be held active between blocks.\n");

static PyObject *
SpiDev_xfer3(SpiDevObject *self, PyObject *args)
{
	int status;
	uint16_t delay_usecs = 0;
	uint32_t speed_hz = 0;
	uint8_t bits_per_word = 0;
	Py_ssize_t ii, jj, len, block_size, block_start, bufsize;
	PyObject *obj;
	PyObject *seq;
	PyObject *rx_tuple;
	struct spi_ioc_transfer xfer;
	Py_BEGIN_ALLOW_THREADS
	memset(&xfer, 0, sizeof(xfer));
	Py_END_ALLOW_THREADS
	uint8_t *txbuf, *rxbuf;
	char	wrmsg_text[4096];

	if (!PyArg_ParseTuple(args, "O|IHB:xfer3", &obj, &speed_hz, &delay_usecs, &bits_per_word))
		return NULL;

	seq = PySequence_Fast(obj, "expected a sequence");
	if (!seq) {
		return NULL;
	}

	len = PySequence_Fast_GET_SIZE(seq);
	if (len <= 0) {
		Py_DECREF(seq);
		PyErr_SetString(PyExc_TypeError, wrmsg_list0);
		return NULL;
	}

	bufsize = get_xfer3_block_size();
	if (bufsize > len) {
		bufsize = len;
	}

	rx_tuple = PyTuple_New(len);
	if (!rx_tuple) {
		Py_DECREF(seq);
		PyErr_SetString(PyExc_OverflowError, wrmsg_oom);
		return NULL;
	}

	Py_BEGIN_ALLOW_THREADS
	// Allocate tx and rx buffers immediately releasing them if any allocation fails
	if ((txbuf = malloc(sizeof(__u8) * bufsize)) != NULL) {
		if ((rxbuf = malloc(sizeof(__u8) * bufsize)) != NULL) {
			// All good, both buffers allocated
		} else {
			// rxbuf allocation failed while txbuf succeeded
			free(txbuf);
			txbuf = NULL;
		}
	} else {
		// txbuf allocation failed
		rxbuf = NULL;
	}
	Py_END_ALLOW_THREADS
	if (!txbuf || !rxbuf) {
		// Allocation failed. Buffers has been freed already
		Py_DECREF(seq);
		Py_DECREF(rx_tuple);
		PyErr_SetString(PyExc_OverflowError, wrmsg_oom);
		return NULL;
	}


	block_start = 0;
	while (block_start < len) {

		for (ii = 0, jj = block_start; jj < len && ii < bufsize; ii++, jj++) {
			PyObject *val = PySequence_Fast_GET_ITEM(seq, jj);
#if PY_MAJOR_VERSION < 3
			if (PyInt_Check(val)) {
				txbuf[ii] = (__u8)PyInt_AS_LONG(val);
			} else
#endif
			{
				if (PyLong_Check(val)) {
					txbuf[ii] = (__u8)PyLong_AS_LONG(val);
				} else {
					snprintf(wrmsg_text, sizeof (wrmsg_text) - 1, wrmsg_val, val);
					PyErr_SetString(PyExc_TypeError, wrmsg_text);
					free(txbuf);
					free(rxbuf);
					Py_DECREF(rx_tuple);
					Py_DECREF(seq);
					return NULL;
				}
			}
		}

		block_size = ii;

		Py_BEGIN_ALLOW_THREADS
		xfer.tx_buf = (unsigned long)txbuf;
		xfer.rx_buf = (unsigned long)rxbuf;
		xfer.len = block_size;
		xfer.delay_usecs = delay_usecs;
		xfer.speed_hz = speed_hz ? speed_hz : self->max_speed_hz;
		xfer.bits_per_word = bits_per_word ? bits_per_word : self->bits_per_word;

		status = ioctl(self->fd, SPI_IOC_MESSAGE(1), &xfer);
		Py_END_ALLOW_THREADS

		if (status < 0) {
			PyErr_SetFromErrno(PyExc_IOError);
			free(txbuf);
			free(rxbuf);
			Py_DECREF(rx_tuple);
			Py_DECREF(seq);
			return NULL;
		}
		for (ii = 0, jj = block_start; ii < block_size; ii++, jj++) {
			PyObject *val = PyLong_FromLong((long)rxbuf[ii]);
			PyTuple_SetItem(rx_tuple, jj, val);  // Steals reference, no need to Py_DECREF(val)
		}

		block_start += block_size;
	}


	// WA:
	// in CS_HIGH mode CS isnt pulled to low after transfer
	// reading 0 bytes doesn't really matter but brings CS down
	// tomdean:
	// Stop generating an extra CS except in mode CS_HIGH
	if (self->read0 && (self->mode & SPI_CS_HIGH)) status = read(self->fd, &rxbuf[0], 0);

	Py_BEGIN_ALLOW_THREADS
	free(txbuf);
	free(rxbuf);
	Py_END_ALLOW_THREADS

	Py_DECREF(seq);

	return rx_tuple;
}

static int __spidev_set_mode( int fd, __u8 mode) {
	__u8 test;
	if (ioctl(fd, SPI_IOC_WR_MODE, &mode) == -1) {
		PyErr_SetFromErrno(PyExc_IOError);
		return -1;
	}
	if (ioctl(fd, SPI_IOC_RD_MODE, &test) == -1) {
		PyErr_SetFromErrno(PyExc_IOError);
		return -1;
	}
	if (test != mode) {
		PyErr_Format(PyExc_IOError,
			"Attempted to set mode 0x%x but mode 0x%x returned",
			(unsigned int)mode, (unsigned int)test);
		return -1;
	}
	return 0;
}

PyDoc_STRVAR(SpiDev_fileno_doc,
	"fileno() -> integer \"file descriptor\"\n\n"
	"This is needed for lower-level file interfaces, such as os.read().\n");

static PyObject *
SpiDev_fileno(SpiDevObject *self)
{
	PyObject *result = Py_BuildValue("i", self->fd);
	Py_INCREF(result);
	return result;
}

static PyObject *
SpiDev_get_mode(SpiDevObject *self, void *closure)
{
	PyObject *result = Py_BuildValue("i", (self->mode & (SPI_CPHA | SPI_CPOL) ) );
	Py_INCREF(result);
	return result;
}

static PyObject *
SpiDev_get_cshigh(SpiDevObject *self, void *closure)
{
	PyObject *result;

	if (self->mode & SPI_CS_HIGH)
		result = Py_True;
	else
		result = Py_False;

	Py_INCREF(result);
	return result;
}

static PyObject *
SpiDev_get_lsbfirst(SpiDevObject *self, void *closure)
{
	PyObject *result;

	if (self->mode & SPI_LSB_FIRST)
		result = Py_True;
	else
		result = Py_False;

	Py_INCREF(result);
	return result;
}

static PyObject *
SpiDev_get_3wire(SpiDevObject *self, void *closure)
{
	PyObject *result;

	if (self->mode & SPI_3WIRE)
		result = Py_True;
	else
		result = Py_False;

	Py_INCREF(result);
	return result;
}

static PyObject *
SpiDev_get_loop(SpiDevObject *self, void *closure)
{
	PyObject *result;

	if (self->mode & SPI_LOOP)
		result = Py_True;
	else
		result = Py_False;

	Py_INCREF(result);
	return result;
}

static PyObject *
SpiDev_get_no_cs(SpiDevObject *self, void *closure)
{
        PyObject *result;

        if (self->mode & SPI_NO_CS)
                result = Py_True;
        else
                result = Py_False;

        Py_INCREF(result);
        return result;
}


static int
SpiDev_set_mode(SpiDevObject *self, PyObject *val, void *closure)
{
	uint8_t mode, tmp;
	int ret;

	if (val == NULL) {
		PyErr_SetString(PyExc_TypeError,
			"Cannot delete attribute");
		return -1;
	}
#if PY_MAJOR_VERSION < 3
	if (PyInt_Check(val)) {
		mode = PyInt_AS_LONG(val);
	} else
#endif
	{
		if (PyLong_Check(val)) {
			mode = PyLong_AS_LONG(val);
		} else {
			PyErr_SetString(PyExc_TypeError,
				"The mode attribute must be an integer");
			return -1;
		}
	}


	if ( mode > 3 ) {
		PyErr_SetString(PyExc_TypeError,
			"The mode attribute must be an integer"
				 "between 0 and 3.");
		return -1;
	}

	// clean and set CPHA and CPOL bits
	tmp = ( self->mode & ~(SPI_CPHA | SPI_CPOL) ) | mode ;

	ret = __spidev_set_mode(self->fd, tmp);

	if (ret != -1)
		self->mode = tmp;
	return ret;
}

static int
SpiDev_set_cshigh(SpiDevObject *self, PyObject *val, void *closure)
{
	uint8_t tmp;
	int ret;

	if (val == NULL) {
		PyErr_SetString(PyExc_TypeError,
			"Cannot delete attribute");
		return -1;
	}
	else if (!PyBool_Check(val)) {
		PyErr_SetString(PyExc_TypeError,
			"The cshigh attribute must be boolean");
		return -1;
	}

	if (val == Py_True)
		tmp = self->mode | SPI_CS_HIGH;
	else
		tmp = self->mode & ~SPI_CS_HIGH;

	ret = __spidev_set_mode(self->fd, tmp);

	if (ret != -1)
		self->mode = tmp;
	return ret;
}

static int
SpiDev_set_lsbfirst(SpiDevObject *self, PyObject *val, void *closure)
{
	uint8_t tmp;
	int ret;

	if (val == NULL) {
		PyErr_SetString(PyExc_TypeError,
			"Cannot delete attribute");
		return -1;
	}
	else if (!PyBool_Check(val)) {
		PyErr_SetString(PyExc_TypeError,
			"The lsbfirst attribute must be boolean");
		return -1;
	}

	if (val == Py_True)
		tmp = self->mode | SPI_LSB_FIRST;
	else
		tmp = self->mode & ~SPI_LSB_FIRST;

	ret = __spidev_set_mode(self->fd, tmp);

	if (ret != -1)
		self->mode = tmp;
	return ret;
}

static int
SpiDev_set_3wire(SpiDevObject *self, PyObject *val, void *closure)
{
	uint8_t tmp;
	int ret;

	if (val == NULL) {
		PyErr_SetString(PyExc_TypeError,
			"Cannot delete attribute");
		return -1;
	}
	else if (!PyBool_Check(val)) {
		PyErr_SetString(PyExc_TypeError,
			"The 3wire attribute must be boolean");
		return -1;
	}

	if (val == Py_True)
		tmp = self->mode | SPI_3WIRE;
	else
		tmp = self->mode & ~SPI_3WIRE;

	ret = __spidev_set_mode(self->fd, tmp);

	if (ret != -1)
		self->mode = tmp;
	return ret;
}

static int
SpiDev_set_no_cs(SpiDevObject *self, PyObject *val, void *closure)
{
        uint8_t tmp;
	int ret;

        if (val == NULL) {
                PyErr_SetString(PyExc_TypeError,
                        "Cannot delete attribute");
                return -1;
        }
        else if (!PyBool_Check(val)) {
                PyErr_SetString(PyExc_TypeError,
                        "The no_cs attribute must be boolean");
                return -1;
        }

        if (val == Py_True)
                tmp = self->mode | SPI_NO_CS;
        else
                tmp = self->mode & ~SPI_NO_CS;

        ret = __spidev_set_mode(self->fd, tmp);

	if (ret != -1)
		self->mode = tmp;
        return ret;
}


static int
SpiDev_set_loop(SpiDevObject *self, PyObject *val, void *closure)
{
	uint8_t tmp;
	int ret;

	if (val == NULL) {
		PyErr_SetString(PyExc_TypeError,
			"Cannot delete attribute");
		return -1;
	}
	else if (!PyBool_Check(val)) {
		PyErr_SetString(PyExc_TypeError,
			"The loop attribute must be boolean");
		return -1;
	}

	if (val == Py_True)
		tmp = self->mode | SPI_LOOP;
	else
		tmp = self->mode & ~SPI_LOOP;

	ret = __spidev_set_mode(self->fd, tmp);

	if (ret != -1)
		self->mode = tmp;
	return ret;
}

static PyObject *
SpiDev_get_bits_per_word(SpiDevObject *self, void *closure)
{
	PyObject *result = Py_BuildValue("i", self->bits_per_word);
	Py_INCREF(result);
	return result;
}

static int
SpiDev_set_bits_per_word(SpiDevObject *self, PyObject *val, void *closure)
{
	uint8_t bits;

	if (val == NULL) {
		PyErr_SetString(PyExc_TypeError,
			"Cannot delete attribute");
		return -1;
	}
#if PY_MAJOR_VERSION < 3
	if (PyInt_Check(val)) {
		bits = PyInt_AS_LONG(val);
	} else
#endif
	{
		if (PyLong_Check(val)) {
			bits = PyLong_AS_LONG(val);
		} else {
			PyErr_SetString(PyExc_TypeError,
				"The bits_per_word attribute must be an integer");
			return -1;
		}
	}

		if (bits < 8 || bits > 32) {
		PyErr_SetString(PyExc_TypeError,
			"invalid bits_per_word (8 to 32)");
		return -1;
	}

	if (self->bits_per_word != bits) {
		if (ioctl(self->fd, SPI_IOC_WR_BITS_PER_WORD, &bits) == -1) {
			PyErr_SetFromErrno(PyExc_IOError);
			return -1;
		}
		self->bits_per_word = bits;
	}
	return 0;
}

static PyObject *
SpiDev_get_max_speed_hz(SpiDevObject *self, void *closure)
{
	PyObject *result = Py_BuildValue("i", self->max_speed_hz);
	Py_INCREF(result);
	return result;
}

static int
SpiDev_set_max_speed_hz(SpiDevObject *self, PyObject *val, void *closure)
{
	uint32_t max_speed_hz;

	if (val == NULL) {
		PyErr_SetString(PyExc_TypeError,
			"Cannot delete attribute");
		return -1;
	}
#if PY_MAJOR_VERSION < 3
	if (PyInt_Check(val)) {
		max_speed_hz = PyInt_AS_LONG(val);
	} else
#endif
	{
		if (PyLong_Check(val)) {
			max_speed_hz = PyLong_AS_LONG(val);
		} else {
			PyErr_SetString(PyExc_TypeError,
				"The max_speed_hz attribute must be an integer");
			return -1;
		}
	}

	if (self->max_speed_hz != max_speed_hz) {
		if (ioctl(self->fd, SPI_IOC_WR_MAX_SPEED_HZ, &max_speed_hz) == -1) {
			PyErr_SetFromErrno(PyExc_IOError);
			return -1;
		}
		self->max_speed_hz = max_speed_hz;
	}
	return 0;
}

static PyObject *
SpiDev_get_read0(SpiDevObject *self, void *closure)
{
	PyObject *result = (self->read0 == 1) ? Py_True : Py_False;
	Py_INCREF(result);
	return result;
}

static int
SpiDev_set_read0(SpiDevObject *self, PyObject *val, void *closure)
{
	if (val == NULL) {
		PyErr_SetString(PyExc_TypeError,
			"Cannot delete attribute");
		return -1;
	}
	else if (!PyBool_Check(val)) {
		PyErr_SetString(PyExc_TypeError,
			"The read0 attribute must be boolean");
		return -1;
	}

	self->read0 = (val == Py_True) ? 1 : 0;

	return 0;
}

static PyGetSetDef SpiDev_getset[] = {
	{"mode", (getter)SpiDev_get_mode, (setter)SpiDev_set_mode,
			"SPI mode as two bit pattern of \n"
			"Clock Polarity  and Phase [CPOL|CPHA]\n"
			"min: 0b00 = 0 max: 0b11 = 3\n"},
	{"cshigh", (getter)SpiDev_get_cshigh, (setter)SpiDev_set_cshigh,
			"CS active high\n"},
	{"threewire", (getter)SpiDev_get_3wire, (setter)SpiDev_set_3wire,
			"SI/SO signals shared\n"},
	{"lsbfirst", (getter)SpiDev_get_lsbfirst, (setter)SpiDev_set_lsbfirst,
			"LSB first\n"},
	{"loop", (getter)SpiDev_get_loop, (setter)SpiDev_set_loop,
			"loopback configuration\n"},
	{"no_cs", (getter)SpiDev_get_no_cs, (setter)SpiDev_set_no_cs,
			"disable chip select\n"},
	{"bits_per_word", (getter)SpiDev_get_bits_per_word, (setter)SpiDev_set_bits_per_word,
			"bits per word\n"},
	{"max_speed_hz", (getter)SpiDev_get_max_speed_hz, (setter)SpiDev_set_max_speed_hz,
			"maximum speed in Hz\n"},
	{"read0", (getter)SpiDev_get_read0, (setter)SpiDev_set_read0,
			"Read 0 bytes after transfer to lower CS if cshigh == True\n"},
	{NULL},
};

static PyObject *
SpiDev_open_dev(SpiDevObject *self, char *dev_path)
{
	uint8_t tmp8;
	uint32_t tmp32;
	if ((self->fd = open(dev_path, O_RDWR, 0)) == -1) {
		PyErr_SetFromErrno(PyExc_IOError);
		return NULL;
	}
	if (ioctl(self->fd, SPI_IOC_RD_MODE, &tmp8) == -1) {
		PyErr_SetFromErrno(PyExc_IOError);
		return NULL;
	}
	self->mode = tmp8;
	if (ioctl(self->fd, SPI_IOC_RD_BITS_PER_WORD, &tmp8) == -1) {
		PyErr_SetFromErrno(PyExc_IOError);
		return NULL;
	}
	self->bits_per_word = tmp8;
	if (ioctl(self->fd, SPI_IOC_RD_MAX_SPEED_HZ, &tmp32) == -1) {
		PyErr_SetFromErrno(PyExc_IOError);
		return NULL;
	}
	self->max_speed_hz = tmp32;

	Py_INCREF(Py_None);
	return Py_None;
}


PyDoc_STRVAR(SpiDev_open_path_doc,
	"open_path(spidev_path)\n\n"
	"Connects the object to the specified SPI device.\n"
	"open_path(X) will open the spidev character device <X> (following symbolic links if necessary).\n");

static PyObject *
SpiDev_open_path(SpiDevObject *self, PyObject *args, PyObject *kwds)
{
	static char *kwlist[] = {"path", NULL};
	PyObject *py_dev_path;
	char *dev_path;
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "O&:open", kwlist, PyUnicode_FSConverter, &py_dev_path))
		return NULL;
	if (py_dev_path == NULL)
		return NULL;
	dev_path = PyBytes_AsString(py_dev_path);
	if (dev_path == NULL)
		return NULL;
	return SpiDev_open_dev(self, dev_path);
}


PyDoc_STRVAR(SpiDev_open_doc,
	"open(bus, device)\n\n"
	"Connects the object to the specified SPI device.\n"
	"open(X,Y) will open /dev/spidev<X>.<Y>\n");

static PyObject *
SpiDev_open(SpiDevObject *self, PyObject *args, PyObject *kwds)
{
	int bus, device;
	char path[SPIDEV_MAXPATH];
	static char *kwlist[] = {"bus", "device", NULL};
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "ii:open", kwlist, &bus, &device))
		return NULL;
	if (snprintf(path, SPIDEV_MAXPATH, "/dev/spidev%d.%d", bus, device) >= SPIDEV_MAXPATH) {
		PyErr_SetString(PyExc_OverflowError,
			"Bus and/or device number is invalid.");
		return NULL;
	}
	return SpiDev_open_dev(self, path);
}

static int
SpiDev_init(SpiDevObject *self, PyObject *args, PyObject *kwds)
{
	int bus = -1;
	int client = -1;
	static char *kwlist[] = {"bus", "client", NULL};

	if (!PyArg_ParseTupleAndKeywords(args, kwds, "|ii:__init__",
			kwlist, &bus, &client))
		return -1;

	if (bus >= 0) {
		SpiDev_open(self, args, kwds);
		if (PyErr_Occurred())
			return -1;
	}

	return 0;
}


PyDoc_STRVAR(SpiDevObjectType_doc,
	"SpiDev([bus],[client]) -> SPI\n\n"
	"Return a new SPI object that is (optionally) connected to the\n"
	"specified SPI device interface.\n");

static
PyObject *SpiDev_enter(PyObject *self, PyObject *args)
{
    if (!PyArg_ParseTuple(args, ""))
        return NULL;

    Py_INCREF(self);
    return self;
}

static
PyObject *SpiDev_exit(SpiDevObject *self, PyObject *args)
{

    PyObject *exc_type = 0;
    PyObject *exc_value = 0;
    PyObject *traceback = 0;
    if (!PyArg_UnpackTuple(args, "__exit__", 3, 3, &exc_type, &exc_value,
                           &traceback)) {
        return 0;
    }

    SpiDev_close(self);
    Py_RETURN_FALSE;
}

static PyMethodDef SpiDev_methods[] = {
	{"open", (PyCFunction)SpiDev_open, METH_VARARGS | METH_KEYWORDS,
		SpiDev_open_doc},
	{"open_path", (PyCFunction)SpiDev_open_path, METH_VARARGS | METH_KEYWORDS,
		SpiDev_open_path_doc},
	{"close", (PyCFunction)SpiDev_close, METH_NOARGS,
		SpiDev_close_doc},
	{"fileno", (PyCFunction)SpiDev_fileno, METH_NOARGS,
		SpiDev_fileno_doc},
	{"readbytes", (PyCFunction)SpiDev_readbytes, METH_VARARGS,
		SpiDev_read_doc},
	{"writebytes", (PyCFunction)SpiDev_writebytes, METH_VARARGS,
		SpiDev_write_doc},
	{"writebytes2", (PyCFunction)SpiDev_writebytes2, METH_VARARGS,
		SpiDev_writebytes2_doc},
	{"xfer", (PyCFunction)SpiDev_xfer, METH_VARARGS,
		SpiDev_xfer_doc},
	{"xfer2", (PyCFunction)SpiDev_xfer2, METH_VARARGS,
		SpiDev_xfer2_doc},
	{"xfer3", (PyCFunction)SpiDev_xfer3, METH_VARARGS,
		SpiDev_xfer3_doc},
	{"__enter__", (PyCFunction)SpiDev_enter, METH_VARARGS,
		NULL},
	{"__exit__", (PyCFunction)SpiDev_exit, METH_VARARGS,
		NULL},
	{NULL},
};

static PyTypeObject SpiDevObjectType = {
#if PY_MAJOR_VERSION >= 3
	PyVarObject_HEAD_INIT(NULL, 0)
#else
	PyObject_HEAD_INIT(NULL)
	0,				/* ob_size */
#endif
	"SpiDev",			/* tp_name */
	sizeof(SpiDevObject),		/* tp_basicsize */
	0,				/* tp_itemsize */
	(destructor)SpiDev_dealloc,	/* tp_dealloc */
	0,				/* tp_print */
	0,				/* tp_getattr */
	0,				/* tp_setattr */
	0,				/* tp_compare */
	0,				/* tp_repr */
	0,				/* tp_as_number */
	0,				/* tp_as_sequence */
	0,				/* tp_as_mapping */
	0,				/* tp_hash */
	0,				/* tp_call */
	0,				/* tp_str */
	0,				/* tp_getattro */
	0,				/* tp_setattro */
	0,				/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
	SpiDevObjectType_doc,		/* tp_doc */
	0,				/* tp_traverse */
	0,				/* tp_clear */
	0,				/* tp_richcompare */
	0,				/* tp_weaklistoffset */
	0,				/* tp_iter */
	0,				/* tp_iternext */
	SpiDev_methods,			/* tp_methods */
	0,				/* tp_members */
	SpiDev_getset,			/* tp_getset */
	0,				/* tp_base */
	0,				/* tp_dict */
	0,				/* tp_descr_get */
	0,				/* tp_descr_set */
	0,				/* tp_dictoffset */
	(initproc)SpiDev_init,		/* tp_init */
	0,				/* tp_alloc */
	SpiDev_new,			/* tp_new */
};

static PyMethodDef SpiDev_module_methods[] = {
	{NULL}
};

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef moduledef = {
	PyModuleDef_HEAD_INIT,
	"spidev",
	SpiDev_module_doc,
	-1,
	SpiDev_module_methods,
	NULL,
	NULL,
	NULL,
	NULL,
};
#else
#ifndef PyMODINIT_FUNC	/* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif
#endif

#if PY_MAJOR_VERSION >= 3
PyMODINIT_FUNC
PyInit_spidev(void)
#else
void initspidev(void)
#endif
{
	PyObject* m;

	if (PyType_Ready(&SpiDevObjectType) < 0)
#if PY_MAJOR_VERSION >= 3
		return NULL;
#else
		return;
#endif

#if PY_MAJOR_VERSION >= 3
	m = PyModule_Create(&moduledef);
	PyObject *version = PyUnicode_FromString(_VERSION_);
#else
	m = Py_InitModule3("spidev", SpiDev_module_methods, SpiDev_module_doc);
	PyObject *version = PyString_FromString(_VERSION_);
#endif

	PyObject *dict = PyModule_GetDict(m);
	PyDict_SetItemString(dict, "__version__", version);
	Py_DECREF(version);

	Py_INCREF(&SpiDevObjectType);
	PyModule_AddObject(m, "SpiDev", (PyObject *)&SpiDevObjectType);

#if PY_MAJOR_VERSION >= 3
	return m;
#endif
}
