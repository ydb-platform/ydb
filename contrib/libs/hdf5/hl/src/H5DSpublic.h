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

#ifndef H5DSpublic_H
#define H5DSpublic_H

#define DIMENSION_SCALE_CLASS "DIMENSION_SCALE"
#define DIMENSION_LIST        "DIMENSION_LIST"
#define REFERENCE_LIST        "REFERENCE_LIST"
#define DIMENSION_LABELS      "DIMENSION_LABELS"

/**
 * \brief Prototype for H5DSiterate_scales() operator
 *
 */
//! <!-- [H5DS_iterate_t_snip] -->
typedef herr_t (*H5DS_iterate_t)(hid_t dset, unsigned dim, hid_t scale, void *visitor_data);
//! <!-- [H5DS_iterate_t_snip] -->

#ifdef __cplusplus
extern "C" {
#endif

/** \page H5DS_UG The HDF5 High Level Dimension Scales
 * @todo Under Construction
 */

/**\defgroup H5DS HDF5 Dimension Scales APIs (H5DS)
 *
 * <em>Creating and manipulating HDF5 datasets that are associated with
 * the dimension of another HDF5 dataset (H5DS)</em>
 *
 * \note \Bold{Programming hints:}
 * \note To use any of these functions or subroutines,
 *       you must first include the relevant include file (C) or
 *       module (Fortran) in your application.
 * \note The following line includes the HDF5 Dimension Scale package,
 *       H5DS, in C applications:
 *       \code #include "hdf5_hl.h" \endcode
 * \note This line includes the H5DS module in Fortran applications:
 *       \code use h5ds \endcode
 *
 * - \ref H5DSwith_new_ref
 *   \n Determines if new references are used with dimension scales.
 * - \ref H5DSattach_scale
 *   \n Attach dimension scale dsid to dimension idx of dataset did.
 * - \ref H5DSdetach_scale
 *   \n Detach dimension scale dsid from the dimension idx of Dataset did.
 * - \ref H5DSget_label
 *   \n Read the label for dimension idx of did into buffer label.
 * - \ref H5DSget_num_scales
 *   \n Determines how many Dimension Scales are attached
 *      to dimension idx of did.
 * - \ref H5DSget_scale_name
 *   \n Retrieves name of scale did into buffer name.
 * - \ref H5DSis_attached
 *   \n Report if dimension scale dsid is currently attached
 *      to dimension idx of dataset did.
 * - \ref H5DSis_scale
 *   \n Determines whether dset is a Dimension Scale.
 * - \ref H5DSiterate_scales
 *   \n Iterates the operation visitor through the scales
 *      attached to dimension dim.
 * - \ref H5DSset_label
 *   \n Set label for the dimension idx of did to the value label.
 * - \ref H5DSset_scale
 *   \n Convert dataset dsid to a dimension scale,
 *      with optional name, dimname.
 *
 */

/* THIS IS A NEW ROUTINE NOT ON OLD PORTAL */
/**
 *  --------------------------------------------------------------------------
 *  \ingroup H5DS
 *
 *  \brief Determines if new references are used with dimension scales.
 *
 *  \param[in] obj_id        Object identifier
 *  \param[out] with_new_ref New references are used or not
 *
 *  \return \herr_t
 *
 *  \details H5DSwith_new_ref() takes any object identifier and checks
 *           if new references are used for dimension scales. Currently,
 *           new references are used when non-native VOL connector is
 *           used or when H5_DIMENSION_SCALES_WITH_NEW_REF is set up
 *           via configure option.
 *
 */
H5_HLDLL herr_t H5DSwith_new_ref(hid_t obj_id, hbool_t *with_new_ref);

/**
 * --------------------------------------------------------------------------
 * \ingroup H5DS
 *
 * \brief Attach dimension scale \p dsid to dimension \p idx of
 *        dataset did.
 *
 * \param[in] did   The dataset
 * \param[in] dsid  The scale to be attached
 * \param[in] idx   The dimension of \p did that \p dsid is associated with
 *
 * \return \herr_t
 *
 * \details Define Dimension Scale \p dsid to be associated with
 *          dimension \p idx of dataset \p did.
 *
 *          Entries are created in the #DIMENSION_LIST and
 *          #REFERENCE_LIST attributes, as defined in section 4.2 of
 *          <a href="https://support.hdfgroup.org/HDF5/doc/HL/H5DS_Spec.pdf">
 *          HDF5 Dimension Scale Specification</a>.
 *
 *          Fails if:
 *          - Bad arguments
 *          - If \p dsid is not a Dimension Scale
 *          - If \p did is a Dimension Scale
 *            (A Dimension Scale cannot have scales.)
 *
 * \note The Dimension Scale \p dsid can be attached to the
 *       same dimension more than once, which has no effect.
 */
H5_HLDLL herr_t H5DSattach_scale(hid_t did, hid_t dsid, unsigned int idx);

/**
 * --------------------------------------------------------------------------
 * \ingroup H5DS
 *
 * \brief Detach dimension scale \p dsid from the dimension \p idx of dataset \p did.
 *
 * \param[in] did   The dataset
 * \param[in] dsid  The scale to be detached
 * \param[in] idx   The dimension of \p did to detach
 *
 * \return \herr_t
 *
 * \details If possible, deletes association of Dimension Scale \p dsid with
 *          dimension \p idx of dataset \p did. This deletes the entries in the
 *          #DIMENSION_LIST and #REFERENCE_LIST attributes,
 *          as defined in section 4.2 of
 *          <a href="https://support.hdfgroup.org/HDF5/doc/HL/H5DS_Spec.pdf">
 *          HDF5 Dimension Scale Specification</a>.
 *
 *          Fails if:
 *          - Bad arguments
 *          - The dataset \p did or \p dsid do not exist
 *          - The \p dsid is not a Dimension Scale
 *          - \p dsid is not attached to \p did
 *
 * \note A scale may be associated with more than dimension of the
 *       same dataset. If so, the detach operation only deletes one
 *       of the associations, for \p did.
 *
 */
H5_HLDLL herr_t H5DSdetach_scale(hid_t did, hid_t dsid, unsigned int idx);

/**
 * --------------------------------------------------------------------------
 * \ingroup H5DS
 *
 * \brief Convert dataset \p dsid to a dimension scale,
 *        with optional name, \p dimname.
 *
 * \param[in] dsid      The dataset to be made a Dimemsion Scale
 * \param[in] dimname   The dimension name (optional), NULL if the
 *                      dimension has no name.
 *
 * \return \herr_t
 *
 * \details The dataset \p dsid is converted to a Dimension Scale dataset,
 *          as defined above. Creates the CLASS attribute, set to the value
 *          "DIMENSION_SCALE" and an empty #REFERENCE_LIST attribute,
 *          as described in
 *          <a href="https://support.hdfgroup.org/HDF5/doc/HL/H5DS_Spec.pdf">
 *          HDF5 Dimension Scale Specification</a>.
 *          (PDF, see section 4.2).
 *
 *          If \p dimname is specified, then an attribute called NAME
 *          is created, with the value \p dimname.
 *
 *          Fails if:
 *          - Bad arguments
 *          - If \p dsid is already a scale
 *          - If \p dsid is a dataset which already has dimension scales
 *
 *          If the dataset was created with the Table, Image, or Palette interface [9],
 *          it is not recommended to convert to a Dimension Scale.
 *          (These Datasets will have a CLASS Table, Image, or Palette.)
 *
 * \todo what is [9] after Palette interface?
 */
H5_HLDLL herr_t H5DSset_scale(hid_t dsid, const char *dimname);

/**
 * --------------------------------------------------------------------------
 * \ingroup H5DS
 *
 * \brief Determines how many Dimension Scales are attached
 *        to dimension \p idx of \p did.
 *
 * \param[in] did   The dataset to query
 * \param[in] idx   The dimension of \p did to query
 *
 * \return Returns the number of Dimension Scales associated
 *         with \p did, if successful, otherwise returns a
 *         negative value.
 *
 * \details H5DSget_num_scales() determines how many Dimension
 *          Scales are attached to dimension \p idx of
 *          dataset \p did.
 *
 */
H5_HLDLL int H5DSget_num_scales(hid_t did, unsigned int idx);

/**
 * --------------------------------------------------------------------------
 * \ingroup H5DS
 *
 * \brief Set label for the dimension \p idx of \p did
 *        to the value \p label.
 *
 * \param[in] did   The dataset
 * \param[in] idx   The dimension
 * \param[in] label The label
 *
 * \return  \herr_t
 *
 * \details Sets the #DIMENSION_LABELS for dimension \p idx of
 *          dataset \p did. If the dimension had a label,
 *          the new value replaces the old.
 *
 *          Fails if:
 *          - Bad arguments
 *
 */
H5_HLDLL herr_t H5DSset_label(hid_t did, unsigned int idx, const char *label);

/**
 * --------------------------------------------------------------------------
 * \ingroup H5DS
 *
 * \brief Read the label for dimension \p idx of \p did into buffer \p label.
 *
 * \param[in] did       The dataset
 * \param[in] idx       The dimension
 * \param[out] label    The label
 * \param[in] size      The length of the label buffer
 *
 * \return  Upon success, size of label or zero if no label found.
 *          Negative if fail.
 *
 * \details Returns the value of the #DIMENSION_LABELS for
 *          dimension \p idx of dataset \p did, if set.
 *          Up to \p size characters of the name are copied into
 *          the buffer \p label.  If the label is longer than
 *          \p size, it will be truncated to fit.  The parameter
 *          \p size is set to the size of the returned \p label.
 *
 *          If \p did has no label, the return value of
 *          \p label is NULL.
 *
 *          Fails if:
 *          - Bad arguments
 *
 */
H5_HLDLL ssize_t H5DSget_label(hid_t did, unsigned int idx, char *label, size_t size);

/**
 * --------------------------------------------------------------------------
 * \ingroup H5DS
 *
 * \brief Retrieves name of scale \p did into buffer \p name.
 *
 * \param[in] did       Dimension scale identifier
 * \param[out] name     Buffer to contain the returned name
 * \param[in] size      Size in bytes, of the \p name buffer
 *
 * \return  Upon success, the length of the scale name or zero if no name found.
 *          Negative if fail.
 *
 * \details H5DSget_scale_name() retrieves the name attribute
 *          for scale \p did.
 *
 *          Up to \p size characters of the scale name are returned
 *          in \p name; additional characters, if any, are not returned
 *          to the user application.
 *
 *          If the length of the name, which determines the required value of
 *          \p size, is unknown, a preliminary H5DSget_scale_name() call can
 *          be made by setting \p name to NULL. The return value of this call
 *          will be the size of the scale name; that value plus one (1) can then
 *          be assigned to \p size for a second H5DSget_scale_name() call,
 *          which will retrieve the actual name.  (The value passed in with the
 *          parameter \p size must be one greater than size in bytes of the actual
 *          name in order to accommodate the null terminator;
 *          if \p size is set to the exact size of the name, the last byte
 *          passed back will contain the null terminator and the last character
 *          will be missing from the name passed back to the calling application.)
 */
H5_HLDLL ssize_t H5DSget_scale_name(hid_t did, char *name, size_t size);

/**
 * --------------------------------------------------------------------------
 * \ingroup H5DS
 *
 * \brief Determines whether \p did is a Dimension Scale.
 *
 * \param[in] did   The dataset to query
 *
 * \return  \htri_t
 *
 * \details H5DSis_scale() determines if \p did is a Dimension Scale,
 *          i.e., has class="DIMENSION_SCALE").
 *
 */
H5_HLDLL htri_t H5DSis_scale(hid_t did);

/**
 * --------------------------------------------------------------------------
 * \ingroup H5DS
 *
 * \brief Iterates the operation visitor through the scales
 *        attached to dimension \p dim.
 *
 * \param[in]       did             The dataset
 * \param[in]       dim             The dimension of dataset \p did
 * \param[in,out]   idx             Input the index to start iterating,
 *                                  output the next index to visit.
 *                                  If NULL, start at the first position.
 * \param[in]       visitor         The visitor function
 * \param[in]       visitor_data    Arbitrary data to pass to the
 *                                  visitor function
 *
 * \return  Returns the return value of the last operator if it was
 *          non-zero, or zero if all scales were processed.
 *
 * \details H5DSiterate_scales() iterates over the scales attached to
 *          dimension \p dim of dataset \p did. For each scale in the
 *          list, the \p visitor_data and some additional information,
 *          specified below, are passed to the \p visitor function.
 *          The iteration begins with the \p idx object in the
 *          group and the next element to be processed by the operator
 *          is returned in \p idx. If \p idx is NULL, then the
 *          iterator starts at the first group member; since no
 *          stopping point is returned in this case,
 *          the iterator cannot be restarted if one of the calls
 *          to its operator returns non-zero.
 *
 *          The prototype for \ref H5DS_iterate_t is:
 *          \snippet this H5DS_iterate_t_snip
 *
 *          The operation receives the Dimension Scale dataset
 *          identifier, \p scale, and the pointer to the operator
 *          data passed in to H5DSiterate_scales(), \p visitor_data.
 *
 *          The return values from an operator are:
 *
 *          - Zero causes the iterator to continue, returning zero
 *            when all group members have been processed.
 *          - Positive causes the iterator to immediately return that
 *            positive value, indicating short-circuit success.
 *            The iterator can be restarted at the next group member.
 *          - Negative causes the iterator to immediately return
 *            that value, indicating failure. The iterator can be
 *            restarted at the next group member.
 *
 *          H5DSiterate_scales() assumes that the scales of the
 *          dimension identified by \p dim remain unchanged through
 *          the iteration. If the membership changes during the iteration,
 *          the function's behavior is undefined.
 */
H5_HLDLL herr_t H5DSiterate_scales(hid_t did, unsigned int dim, int *idx, H5DS_iterate_t visitor,
                                   void *visitor_data);

/**
 * --------------------------------------------------------------------------
 * \ingroup H5DS
 *
 * \brief Report if dimension scale \p dsid is currently attached to
 *        dimension \p idx of dataset \p did.
 *
 * \param[in] did   The dataset
 * \param[in] dsid  The scale to be attached
 * \param[in] idx   The dimension of \p did that \p dsid is associated with
 *
 * \return  \htri_t
 *
 * \details Report if dimension scale \p dsid is currently attached to
 *          dimension \p idx of dataset \p did.
 *
 *          Fails if:
 *          - Bad arguments
 *          - If \p dsid is not a Dimension Scale
 *          - The \p dsid is not a Dimension Scale
 *          - If \p did is a Dimension Scale (A Dimension Scale cannot have scales.)
 *
 */
H5_HLDLL htri_t H5DSis_attached(hid_t did, hid_t dsid, unsigned int idx);

#ifdef __cplusplus
}
#endif

#endif
