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

/*
 * Purpose: This file contains declarations which define macros for the
 *          H5A package.  Including this header means that the source file
 *          is part of the H5A package.
 */
#ifndef H5Amodule_H
#define H5Amodule_H

/* Define the proper control macros for the generic FUNC_ENTER/LEAVE and error
 *      reporting macros.
 */
#define H5A_MODULE
#define H5_MY_PKG     H5A
#define H5_MY_PKG_ERR H5E_ATTR

/** \page H5A_UG HDF5 Attributes
 *
 * \section sec_attribute HDF5 Attributes
 *
 * An HDF5 attribute is a small metadata object describing the nature and/or intended usage of a primary data
 * object. A primary data object may be a dataset, group, or committed datatype.
 *
 * \subsection subsec_attribute_intro Introduction
 *
 * Attributes are assumed to be very small as data objects go, so storing them as standard HDF5 datasets would
 * be quite inefficient. HDF5 attributes are therefore managed through a special attributes interface,
 * \ref H5A, which is designed to easily attach attributes to primary data objects as small datasets
 * containing metadata information and to minimize storage requirements.
 *
 * Consider, as examples of the simplest case, a set of laboratory readings taken under known temperature and
 * pressure conditions of 18.0 degrees Celsius and 0.5 atmospheres, respectively. The temperature and pressure
 * stored as attributes of the dataset could be described as the following name/value pairs:
 *     \li temp=18.0
 *     \li pressure=0.5
 *
 * While HDF5 attributes are not standard HDF5 datasets, they have much in common:
 * \li An attribute has a user-defined dataspace and the included metadata has a user-assigned datatype
 * \li Metadata can be of any valid HDF5 datatype
 * \li Attributes are addressed by name
 *
 * But there are some very important differences:
 * \li There is no provision for special storage such as compression or chunking
 * \li There is no partial I/O or sub-setting capability for attribute data
 * \li Attributes cannot be shared
 * \li Attributes cannot have attributes
 * \li Being small, an attribute is stored in the object header of the object it describes and is thus
 * attached directly to that object
 *
 * \subsection subsec_error_H5A Attribute Function Summaries
 * @see H5A reference manual
 *
 * \subsection subsec_attribute_program Programming Model for Attributes
 *
 * The figure below shows the UML model for an HDF5 attribute and its associated dataspace and datatype.
 * <table>
 * <tr>
 * <td>
 * \image html UML_Attribute.jpg "The UML model for an HDF5 attribute"
 * </td>
 * </tr>
 * </table>
 *
 * Creating an attribute is similar to creating a dataset. To create an attribute, the application must
 * specify the object to which the attribute is attached, the datatype and dataspace of the attribute
 * data, and the attribute creation property list.
 *
 * The following steps are required to create and write an HDF5 attribute:
 * \li Obtain the object identifier for the attribute's primary data object
 * \li Define the characteristics of the attribute and specify the attribute creation property list
 * <ul> <li> Define the datatype</li>
 * <li> Define the dataspace</li>
 * <li> Specify the attribute creation property list</li></ul>
 * \li Create the attribute
 * \li Write the attribute data (optional)
 * \li Close the attribute (and datatype, dataspace, and attribute creation property list, if necessary)
 * \li Close the primary data object (if appropriate)
 *
 * The following steps are required to open and read/write an existing attribute. Since HDF5 attributes
 *  allow no partial I/O, you need specify only the attribute and the attribute's memory datatype to read it:
 * \li Obtain the object identifier for the attribute's primary data object
 * \li Obtain the attribute's name or index
 * \li Open the attribute
 * \li Get attribute dataspace and datatype (optional)
 * \li Specify the attribute's memory type
 * \li Read and/or write the attribute data
 * \li Close the attribute
 * \li Close the primary data object (if appropriate)
 *
 * <table>
 * <tr><th>Create</th><th>Update</th></tr>
 * <tr valign="top">
 *   <td>
 *   \snippet{lineno} H5A_examples.c create
 *   </td>
 *   <td>
 *   \snippet{lineno} H5A_examples.c update
 *   </td>
 * <tr><th>Read</th><th>Delete</th></tr>
 * <tr valign="top">
 *   <td>
 *   \snippet{lineno} H5A_examples.c read
 *   </td>
 *   <td>
 *   \snippet{lineno} H5A_examples.c delete
 *   </td>
 * </tr>
 * </table>
 *
 * \subsection subsec_attribute_work Working with Attributes
 *
 * \subsubsection subsubsec_attribute_work_struct The Structure of an Attribute
 *
 * An attribute has two parts: name and value(s).
 *
 * HDF5 attributes are sometimes discussed as name/value pairs in the form name=value.
 *
 * An attribute's name is a null-terminated ASCII or UTF-8 character string. Each attribute attached to an
 * object has a unique name.
 *
 * The value portion of the attribute contains one or more data elements of the same datatype.
 *
 * HDF5 attributes have all the characteristics of HDF5 datasets except that there is no partial I/O
 * capability. In other words, attributes can be written and read only in full with no sub-setting.
 *
 * \subsubsection subsubsec_attribute_work_create Creating, Writing, and Reading Attributes
 *
 * If attributes are used in an HDF5 file, these functions will be employed: \ref H5Acreate, \ref H5Awrite,
 * and \ref H5Aread. \ref H5Acreate and \ref H5Awrite are used together to place the attribute in the file. If
 * an attribute is to be used and is not currently in memory, \ref H5Aread generally comes into play
 * usually in concert with one each of the H5Aget_* and H5Aopen_* functions.
 *
 * To create an attribute, call H5Acreate:
 * \code
 *      hid_t H5Acreate (hid_t loc_id, const char *name,
 *                      hid_t type_id, hid_t space_id, hid_t create_plist,
 *                      hid_t access_plist)
 * \endcode
 * loc_id identifies the object (dataset, group, or committed datatype) to which the attribute is to be
 * attached. name, type_id, space_id, and create_plist convey, respectively, the attribute's name, datatype,
 * dataspace, and attribute creation property list. The attribute's name must be locally unique: it must be
 * unique within the context of the object to which it is attached.
 *
 * \ref H5Acreate creates the attribute in memory. The attribute does not exist in the file until
 * \ref H5Awrite writes it there.
 *
 * To write or read an attribute, call H5Awrite or H5Aread, respectively:
 * \code
 *      herr_t H5Awrite (hid_t attr_id, hid_t mem_type_id, const void *buf)
 *      herr_t H5Aread (hid_t attr_id, hid_t mem_type_id, void *buf)
 * \endcode
 * attr_id identifies the attribute while mem_type_id identifies the in-memory datatype of the attribute data.
 *
 * \ref H5Awrite writes the attribute data from the buffer buf to the file. \ref H5Aread reads attribute data
 * from the file into buf.
 *
 * The HDF5 Library converts the metadata between the in-memory datatype, mem_type_id, and the in-file
 * datatype, defined when the attribute was created, without user intervention.
 *
 * \subsubsection subsubsec_attribute_work_access Accessing Attributes by Name or Index
 *
 * Attributes can be accessed by name or index value. The use of an index value makes it possible to iterate
 * through all of the attributes associated with a given object.
 *
 * To access an attribute by its name, use the \ref H5Aopen_by_name function. \ref H5Aopen_by_name returns an
 * attribute identifier that can then be used by any function that must access an attribute such as \ref
 * H5Aread. Use the function \ref H5Aget_name to determine an attribute's name.
 *
 * To access an attribute by its index value, use the \ref H5Aopen_by_idx function. To determine an attribute
 * index value when it is not already known, use the H5Oget_info function. \ref H5Aopen_by_idx is generally
 * used in the course of opening several attributes for later access. Use \ref H5Aiterate if the intent is to
 * perform the same operation on every attribute attached to an object.
 *
 * \subsubsection subsubsec_attribute_work_info Obtaining Information Regarding an Object's Attributes
 *
 * In the course of working with HDF5 attributes, one may need to obtain any of several pieces of information:
 * \li An attribute name
 * \li The dataspace of an attribute
 * \li The datatype of an attribute
 * \li The number of attributes attached to an object
 *
 * To obtain an attribute's name, call H5Aget_name with an attribute identifier, attr_id:
 * \code
 *      ssize_t H5Aget_name (hid_t attr_id, size_t buf_size, char *buf)
 * \endcode
 * As with other attribute functions, attr_id identifies the attribute; buf_size defines the size of the
 * buffer; and buf is the buffer to which the attribute's name will be read.
 *
 * If the length of the attribute name, and hence the value required for buf_size, is unknown, a first call
 * to \ref H5Aget_name will return that size. If the value of buf_size used in that first call is too small,
 * the name will simply be truncated in buf. A second \ref H5Aget_name call can then be used to retrieve the
 * name in an appropriately-sized buffer.
 *
 * To determine the dataspace or datatype of an attribute, call \ref H5Aget_space or \ref H5Aget_type,
 * respectively: \code hid_t H5Aget_space (hid_t attr_id) hid_t H5Aget_type (hid_t attr_id) \endcode \ref
 * H5Aget_space returns the dataspace identifier for the attribute attr_id. \ref H5Aget_type returns the
 * datatype identifier for the attribute attr_id.
 *
 * To determine the number of attributes attached to an object, use the \ref H5Oget_info function. The
 * function signature is below. \code herr_t H5Oget_info( hid_t object_id, H5O_info_t *object_info  ) \endcode
 * The number of attributes will be returned in the object_info buffer. This is generally the preferred first
 * step in determining attribute index values. If the call returns N, the attributes attached to the object
 * object_id have index values of 0 through N-1.
 *
 * \subsubsection subsubsec_attribute_work_iterate Iterating across an Object's Attributes
 *
 * It is sometimes useful to be able to perform the identical operation across all of the attributes attached
 * to an object. At the simplest level, you might just want to open each attribute. At a higher level, you
 * might wish to perform a rather complex operation on each attribute as you iterate across the set.
 *
 * To iterate an operation across the attributes attached to an object, one must make a series of calls to
 * \ref H5Aiterate
 * \code
 *      herr_t H5Aiterate (hid_t obj_id, H5_index_t index_type,
 *                        H5_iter_order_t order, hsize_t *n, H5A_operator2_t op,
 *                        void *op_data)
 * \endcode
 * \ref H5Aiterate successively marches across all of the attributes attached to the object specified in
 * loc_id, performing the operation(s) specified in op_func with the data specified in op_data on each
 * attribute.
 *
 * When \ref H5Aiterate is called, index contains the index of the attribute to be accessed in this call. When
 * \ref H5Aiterate returns, index will contain the index of the next attribute. If the returned index is the
 * null pointer, then all attributes have been processed, and the iterative process is complete.
 *
 * op_func is a user-defined operation that adheres to the \ref H5A_operator_t prototype. This prototype and
 * certain requirements imposed on the operator's behavior are described in the \ref H5Aiterate entry in the
 * \ref RM.
 *
 * op_data is also user-defined to meet the requirements of op_func. Beyond providing a parameter with which
 * to pass this data, HDF5 provides no tools for its management and imposes no restrictions.
 *
 * \subsubsection subsubsec_attribute_work_delete Deleting an Attribute
 *
 * Once an attribute has outlived its usefulness or is no longer appropriate, it may become necessary to
 * delete it.
 *
 * To delete an attribute, call \ref H5Adelete
 * \code
 *      herr_t H5Adelete (hid_t loc_id, const char *name)
 * \endcode
 * \ref H5Adelete removes the attribute name from the group, dataset, or committed datatype specified in
 * loc_id.
 *
 * \ref H5Adelete must not be called if there are any open attribute identifiers on the object loc_id. Such a
 * call can cause the internal attribute indexes to change; future writes to an open attribute would then
 * produce unintended results.
 *
 * \subsubsection subsubsec_attribute_work_close Closing an Attribute
 *
 * As is the case with all HDF5 objects, once access to an attribute it is no longer needed, that attribute
 * must be closed. It is best practice to close it as soon as practicable; it is mandatory that it be closed
 * prior to the H5close call closing the HDF5 Library.
 *
 * To close an attribute, call \ref H5Aclose
 * \code
 *      herr_t H5Aclose (hid_t attr_id)
 * \endcode
 * \ref H5Aclose closes the specified attribute by terminating access to its identifier, attr_id.
 *
 * \subsection subsec_attribute_special Special Issues
 *
 * Some special issues for attributes are discussed below.
 *
 * <h4>Large Numbers of Attributes Stored in Dense Attribute Storage</h4>
 *
 * The dense attribute storage scheme was added in version 1.8 so that datasets, groups, and committed
 * datatypes that have large numbers of attributes could be processed more quickly.
 *
 * Attributes start out being stored in an object's header. This is known as compact storage. For more
 * information, see "Storage Strategies."
 *
 * As the number of attributes grows, attribute-related performance slows. To improve performance, dense
 * attribute storage can be initiated with the H5Pset_attr_phase_change function. See the HDF5 Reference
 * Manual for more information.
 *
 * When dense attribute storage is enabled, a threshold is defined for the number of attributes kept in
 * compact storage. When the number is exceeded, the library moves all of the attributes into dense storage
 * at another location. The library handles the movement of attributes and the pointers between the locations
 * automatically. If some of the attributes are deleted so that the number falls below the threshold, then
 * the attributes are moved back to compact storage by the library.
 *
 * The improvements in performance from using dense attribute storage are the result of holding attributes
 * in a heap and indexing the heap with a B-tree.
 *
 * Note that there are some disadvantages to using dense attribute storage. One is that this is a new feature.
 * Datasets, groups, and committed datatypes that use dense storage cannot be read by applications built with
 * earlier versions of the library. Another disadvantage is that attributes in dense storage cannot be
 * compressed.
 *
 * <h4>Large Attributes Stored in Dense Attribute Storage</h4>
 *
 * We generally consider the maximum size of an attribute to be 64K bytes. The library has two ways of storing
 * attributes larger than 64K bytes: in dense attribute storage or in a separate dataset. Using dense
 * attribute storage is described in this section, and storing in a separate dataset is described in the next
 * section.
 *
 * To use dense attribute storage to store large attributes, set the number of attributes that will be stored
 * in compact storage to 0 with the H5Pset_attr_phase_change function. This will force all attributes to be
 * put into dense attribute storage and will avoid the 64KB size limitation for a single attribute in compact
 * attribute storage.
 *
 * The example code below illustrates how to create a large attribute that will be kept in dense storage.
 *
 * <table>
 * <tr><th>Create</th></tr>
 * <tr valign="top">
 *   <td>
 *   \snippet{lineno} H5A_examples.c create
 *   </td>
 * </tr>
 * </table>
 *
 * <h4>Large Attributes Stored in a Separate Dataset</h4>
 *
 * In addition to dense attribute storage (see above), a large attribute can be stored in a separate dataset.
 * In the figure below, DatasetA holds an attribute that is too large for the object header in Dataset1. By
 * putting a pointer to DatasetA as an attribute in Dataset1, the attribute becomes available to those
 * working with Dataset1.
 * This way of handling large attributes can be used in situations where backward compatibility is important
 * and where compression is important. Applications built with versions before 1.8.x can read large
 * attributes stored in separate datasets. Datasets can be compressed while attributes cannot.
 * <table>
 * <tr>
 * <td>
 * \image html Shared_Attribute.jpg "A large or shared HDF5 attribute and its associated dataset(s)"
 * </td>
 * </tr>
 * </table>
 * Note: In the figure above, DatasetA is an attribute of Dataset1 that is too large to store in Dataset1's
 * header. DatasetA is associated with Dataset1 by means of an object reference pointer attached as an
 * attribute to Dataset1. The attribute in DatasetA can be shared among multiple datasets by means of
 * additional object reference pointers attached to additional datasets.
 *
 * <h4>Shared Attributes</h4>
 *
 * Attributes written and managed through the \ref H5A interface cannot be shared. If shared attributes are
 * required, they must be handled in the manner described above for large attributes and illustrated in
 * the figure above.
 *
 * <h4>Attribute Names</h4>
 *
 * While any ASCII or UTF-8 character may be used in the name given to an attribute, it is usually wise
 * to avoid the following kinds of characters:
 * \li Commonly used separators or delimiters such as slash, backslash, colon, and semi-colon (\, /, :, ;)
 * \li Escape characters
 * \li Wild cards such as asterisk and question mark (*, ?)
 * NULL can be used within a name, but HDF5 names are terminated with a NULL: whatever comes after the NULL
 * will be ignored by HDF5.
 *
 * The use of ASCII or UTF-8 characters is determined by the character encoding property. See
 * #H5Pset_char_encoding in the \ref RM.
 *
 * <h4>No Special I/O or Storage</h4>
 *
 * HDF5 attributes have all the characteristics of HDF5 datasets except the following:
 * \li  Attributes are written and read only in full: there is no provision for partial I/O or sub-setting
 * \li  No special storage capability is provided for attributes: there is no compression or chunking, and
 *      attributes are not extendable
 *
 * Previous Chapter \ref sec_dataspace - Next Chapter \ref sec_error
 *
 * \defgroup H5A Attributes (H5A)
 *
 * An HDF5 attribute is a small metadata object describing the nature and/or intended usage of a primary data
 * object. A primary data object may be a dataset, group, or committed datatype.
 *
 * @see sec_attribute
 *
 */

#endif /* H5Amodule_H */
