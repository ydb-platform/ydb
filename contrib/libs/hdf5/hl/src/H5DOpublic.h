/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://www.hdfgroup.org/licenses.               *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#ifndef H5DOpublic_H
#define H5DOpublic_H

#ifdef __cplusplus
extern "C" {
#endif

/** \page H5DO_UG The HDF5 High Level Optimizations
 * @todo Under Construction
 */

/**\defgroup H5DO HDF5 Optimizations APIs (H5DO)
 *
 * <em>Bypassing default HDF5 behavior in order to optimize for specific
 * use cases (H5DO)</em>
 *
 * HDF5 functions described is this section are implemented in the HDF5 High-level
 * library as optimized functions. These functions generally require careful setup
 * and testing as they enable an application to bypass portions of the HDF5
 * library's I/O pipeline for performance purposes.
 *
 * These functions are distributed in the standard HDF5 distribution and are
 * available any time the HDF5 High-level library is available.
 *
 * - \ref H5DOappend
 *    \n Appends data to a dataset along a specified dimension.
 * - \ref H5DOread_chunk
 *   \n Reads a raw data chunk directly from a dataset in a file into a buffer (DEPRECATED)
 * - \ref H5DOwrite_chunk
 *   \n  Writes a raw data chunk from a buffer directly to a dataset in a file (DEPRECATED)
 *
 */

/*-------------------------------------------------------------------------
 *
 * "Optimized dataset" routines.
 *
 *-------------------------------------------------------------------------
 */

/**
 * --------------------------------------------------------------------------
 * \ingroup H5DO
 *
 * \brief Appends data to a dataset along a specified dimension.
 *
 * \param[in] dset_id   Dataset identifier
 * \param[in] dxpl_id   Dataset transfer property list identifier
 * \param[in] axis      Dataset Dimension (0-based) for the append
 * \param[in] extension Number of elements to append for the
 *                      axis-th dimension
 * \param[in] memtype   The memory datatype identifier
 * \param[in] buf       Buffer with data for the append
 *
 * \return \herr_t
 *
 * \details The H5DOappend() routine extends a dataset by \p extension
 *          number of elements along a dimension specified by a
 *          dimension \p axis and writes \p buf of elements to the
 *          dataset. Dimension \p axis is 0-based. Elements’ type
 *          is described by \p memtype.
 *
 *          This routine combines calling H5Dset_extent(),
 *          H5Sselect_hyperslab(), and H5Dwrite() into a single routine
 *          that simplifies application development for the common case
 *          of appending elements to an existing dataset.
 *
 *          For a multi-dimensional dataset, appending to one dimension
 *          will write a contiguous hyperslab over the other dimensions.
 *          For example, if a 3-D dataset has dimension sizes (3, 5, 8),
 *          extending the 0th dimension (currently of size 3) by 3 will
 *          append 3*5*8 = 120 elements (which must be pointed to by the
 *          \p buffer parameter) to the dataset, making its final
 *          dimension sizes (6, 5, 8).
 *
 *          If a dataset has more than one unlimited dimension, any of
 *          those dimensions may be appended to, although only along
 *          one dimension per call to H5DOappend().
 *
 * \since   1.10.0
 *
 */
H5_HLDLL herr_t H5DOappend(hid_t dset_id, hid_t dxpl_id, unsigned axis, size_t extension, hid_t memtype,
                           const void *buf);

/* Symbols defined for compatibility with previous versions of the HDF5 API.
 *
 * Use of these symbols is deprecated.
 */
#ifndef H5_NO_DEPRECATED_SYMBOLS

/* Compatibility wrappers for functionality moved to H5D */

/**
 * --------------------------------------------------------------------------
 * \ingroup H5DO
 *
 * \brief Writes a raw data chunk from a buffer directly to a dataset in a file.
 *
 * \param[in] dset_id       Identifier for the dataset to write to
 * \param[in] dxpl_id       Transfer property list identifier for
 *                          this I/O operation
 * \param[in] filters       Mask for identifying the filters in use
 * \param[in] offset        Logical position of the chunk's first element
 *                          in the dataspace
 * \param[in] data_size     Size of the actual data to be written in bytes
 * \param[in] buf           Buffer containing data to be written to the chunk
 *
 * \return \herr_t
 *
 * \deprecated This function was deprecated in favor of the function
 *             H5Dwrite_chunk() of HDF5-1.10.3.
 *             The functionality of H5DOwrite_chunk() was moved
 *             to H5Dwrite_chunk().
 * \deprecated For compatibility, this API call has been left as a stub which
 *             simply calls H5Dwrite_chunk(). New code should use H5Dwrite_chunk().
 *
 * \details The H5DOwrite_chunk() writes a raw data chunk as specified by its
 *          logical \p offset in a chunked dataset \p dset_id from the application
 *          memory buffer \p buf to the dataset in the file. Typically, the data
 *          in \p buf is preprocessed in memory by a custom transformation, such as
 *          compression. The chunk will bypass the library's internal data
 *          transfer pipeline, including filters, and will be written directly to the file.
 *
 *          \p dxpl_id is a data transfer property list identifier.
 *
 *          \p filters is a mask providing a record of which filters are used
 *          with the chunk. The default value of the mask is zero (\c 0),
 *          indicating that all enabled filters are applied. A filter is skipped
 *          if the bit corresponding to the filter's position in the pipeline
 *          (<tt>0 ≤ position < 32</tt>) is turned on. This mask is saved
 *          with the chunk in the file.
 *
 *          \p offset is an array specifying the logical position of the first
 *          element of the chunk in the dataset's dataspace. The length of the
 *          offset array must equal the number of dimensions, or rank, of the
 *          dataspace. The values in \p offset must not exceed the dimension limits
 *          and must specify a point that falls on a dataset chunk boundary.
 *
 *          \p data_size is the size in bytes of the chunk, representing the number of
 *          bytes to be read from the buffer \p buf. If the data chunk has been
 *          precompressed, \p data_size should be the size of the compressed data.
 *
 *          \p buf is the memory buffer containing data to be written to the chunk in the file.
 *
 * \attention   Exercise caution when using H5DOread_chunk() and H5DOwrite_chunk(),
 *              as they read and write data chunks directly in a file.
 *              H5DOwrite_chunk() bypasses hyperslab selection, the conversion of data
 *              from one datatype to another, and the filter pipeline to write the chunk.
 *              Developers should have experience with these processes before
 *              using this function. Please see
 *              <a href="https://portal.hdfgroup.org/display/HDF5/Using+the+Direct+Chunk+Write+Function">
 *              Using the Direct Chunk Write Function</a>
 *              for more information.
 *
 * \note    H5DOread_chunk() and H5DOwrite_chunk() are not
 *          supported under parallel and do not support variable length types.
 *
 * \par Example
 * The following code illustrates the use of H5DOwrite_chunk to write
 * an entire dataset, chunk by chunk:
 * \snippet H5DO_examples.c H5DOwrite
 *
 * \version 1.10.3  Function deprecated in favor of H5Dwrite_chunk.
 *
 * \since   1.8.11
 */
H5_HLDLL herr_t H5DOwrite_chunk(hid_t dset_id, hid_t dxpl_id, uint32_t filters, const hsize_t *offset,
                                size_t data_size, const void *buf);

/**
 * --------------------------------------------------------------------------
 * \ingroup H5DO
 *
 * \brief Reads a raw data chunk directly from a dataset in a file into a buffer.
 *
 * \param[in] dset_id           Identifier for the dataset to be read
 * \param[in] dxpl_id           Transfer property list identifier for
 *                              this I/O operation
 * \param[in] offset            Logical position of the chunk's first
                                element in the dataspace
 * \param[in,out] filters       Mask for identifying the filters used
 *                              with the chunk
 * \param[in] buf               Buffer containing the chunk read from
 *                              the dataset
 *
 * \return \herr_t
 *
 * \deprecated This function was deprecated in favor of the function
 *             H5Dread_chunk() as of HDF5-1.10.3.
 *             In HDF5 1.10.3, the functionality of H5DOread_chunk()
 *             was moved to H5Dread_chunk().
 * \deprecated For compatibility, this API call has been left as a stub which
 *             simply calls H5Dread_chunk().  New code should use H5Dread_chunk().
 *
 * \details The H5DOread_chunk() reads a raw data chunk as specified
 *          by its logical \p offset in a chunked dataset \p dset_id
 *          from the dataset in the file into the application memory
 *          buffer \p buf. The data in \p buf is read directly from the file
 *          bypassing the library's internal data transfer pipeline,
 *          including filters.
 *
 *          \p dxpl_id is a data transfer property list identifier.
 *
 *          The mask \p filters indicates which filters are used with the
 *          chunk when written. A zero value indicates that all enabled filters
 *          are applied on the chunk. A filter is skipped if the bit corresponding
 *          to the filter's position in the pipeline
 *          (<tt>0 ≤ position < 32</tt>) is turned on.
 *
 *          \p offset is an array specifying the logical position of the first
 *          element of the chunk in the dataset's dataspace. The length of the
 *          offset array must equal the number of dimensions, or rank, of the
 *          dataspace. The values in \p offset must not exceed the dimension
 *          limits and must specify a point that falls on a dataset chunk boundary.
 *
 *          \p buf is the memory buffer containing the chunk read from the dataset
 *          in the file.
 *
 * \par Example
 * The following code illustrates the use of H5DOread_chunk()
 * to read a chunk from a dataset:
 * \snippet H5DO_examples.c H5DOread
 *
 * \version 1.10.3  Function deprecated in favor of H5Dread_chunk.
 *
 * \since   1.10.2, 1.8.19
 */
H5_HLDLL herr_t H5DOread_chunk(hid_t dset_id, hid_t dxpl_id, const hsize_t *offset, uint32_t *filters /*out*/,
                               void *buf /*out*/);

#endif /* H5_NO_DEPRECATED_SYMBOLS */

#ifdef __cplusplus
}
#endif

#endif
