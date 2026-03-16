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
 *          H5P package.  Including this header means that the source file
 *          is part of the H5P package.
 */
#ifndef H5Pmodule_H
#define H5Pmodule_H

/* Define the proper control macros for the generic FUNC_ENTER/LEAVE and error
 *      reporting macros.
 */
#define H5P_MODULE
#define H5_MY_PKG     H5P
#define H5_MY_PKG_ERR H5E_PLIST

/** \page H5P_UG  Properties and Property Lists in HDF5
 *
 * \section sec_plist Properties and Property Lists in HDF5
 *
 * HDF5 property lists are the main vehicle to configure the
 * behavior of HDF5 API functions.
 *
 * Typically, property lists are created by instantiating one of the built-in
 * or user-defined property list classes. After adding suitable properties,
 * property lists are used when opening or creating HDF5 items, or when reading
 * or writing data. Property lists can be modified by adding or changing
 * properties. Property lists are deleted by closing the associated handles.
 *
 * \subsection subsec_plist_intro Introduction
 *
 * HDF5 properties and property lists make it possible to shape or modify an HDF5 file, group,
 * dataset, attribute, committed datatype, or even an I/O stream, in a number of ways. For example,
 * you can do any of the following:
 * \li Customize the storage layout of a file to suit a project or task.
 * \li Create a chunked dataset.
 * \li Apply compression or filters to raw data.
 * \li Use either ASCII or UTF-8 character encodings.
 * \li Create missing groups on the fly.
 * \li Switch between serial and parallel I/O.
 * \li Create consistency within a single file or across an international project.
 *
 * Some properties enable an HDF5 application to take advantage of the capabilities of a specific
 * computing environment while others make a file more compact; some speed the reading or
 * writing of data while others enable more record-keeping at a per-object level. HDF5 offers
 * nearly one hundred specific properties that can be used in literally thousands of combinations to
 * maximize the usability of HDF5-stored data.
 *
 * At the most basic level, a property list is a collection of properties, represented by name/value
 * pairs that can be passed to various HDF5 functions, usually modifying default settings. A
 * property list inherits a set of properties and values from a property list class. But that statement
 * hardly provides a complete picture; in the rest of this section and in the next section,
 * \ref subsec_plist_class , we will discuss these things in much more detail.
 * After reading that material, the reader should have a reasonably complete understanding of how
 * properties and property lists can be used in HDF5 applications.
 *
 * <table>
 * <tr>
 * <td>
 * \image html PropListEcosystem.gif "The HDF5 property environment"
 * </td>
 * </tr>
 * </table>
 *
 * The remaining sections in this chapter discuss the following topics:
 * \li What are properties, property lists, and property list classes?
 * \li Property list programming model
 * \li Generic property functions
 * \li Summary listings of property list functions
 * \li Additional resources
 *
 * The discussions and function listings in this chapter focus on general property operations, object
 * and link properties, and related functions.
 *
 * File, group, dataset, datatype, and attribute properties are discussed in the chapters devoted to
 * those features, where that information will be most convenient to users. For example, \ref sec_dataset
 * discusses dataset creation property lists and functions, dataset access property lists and
 * functions, and dataset transfer property lists and functions. This chapter does not duplicate those
 * discussions.
 *
 * Generic property operations are an advanced feature and are beyond the scope of this guide.
 *
 * This chapter assumes an understanding of the following chapters of this \ref UG
 * \li \ref sec_data_model
 * \li \ref sec_program
 *
 * \subsection subsec_plist_class Property List Classes, Property Lists, and Properties
 *
 * HDF5 property lists and the property list interface \ref H5P provide a mechanism for storing
 * characteristics of objects in an HDF5 file and economically passing them around in an HDF5
 * application. In this capacity, property lists significantly reduce the burden of additional function
 * parameters throughout the HDF5 API. Another advantage of property lists is that features can
 * often be added to HDF5 by adding only property list functions to the API; this is particularly true
 * when all other requirements of the feature can be accomplished internally to the library.
 *
 * For instance, a file creation operation needs to know several things about a file, such as the size
 * of the userblock or the sizes of various file data structures. Bundling this information as a
 * property list simplifies the interface by reducing the number of parameters to the function
 * \ref H5Fcreate.
 *
 * As illustrated in the figure above ("The HDF5 property environment"), the HDF5 property
 * environment is a three-level hierarchy:
 * \li Property list classes
 * \li Property lists
 * \li Properties
 *
 * The following subsections discuss property list classes, property lists, and properties in more detail.
 *
 * \subsubsection subsubsec_plist_class Property List Classes
 *
 * A property list class defines the roles that property lists of that class can play. Each class includes
 * all properties that are valid for that class with each property set to its default value. HDF5 offers
 * a property lists class for each of the following situations.
 *
 * <table>
 * <caption align=top id="table_plist">Property list classes in HDF5</caption>
 * <tr><th>Property List Class</th><th></th><th>For further discussion</th></tr>
 * <tr valign="top">
 *   <td>
 *   File creation (FCPL)
 *   </td>
 *   <td>
 *   \ref H5P_FILE_CREATE
 *   </td>
 *   <td>
 *   See various sections of \ref sec_file
 *   </td>
 * <tr valign="top">
 *   <td>
 * File access (FAPL)
 *   </td>
 *   <td>
 * \ref H5P_FILE_ACCESS
 *   </td>
 *   <td>
 * Used only as \ref H5P_DEFAULT.
 *   </td>
 * </tr>
 * <tr valign="top">
 *   <td>
 * File mount (FMPL)
 *   </td>
 *   <td>
 * \ref H5P_FILE_MOUNT
 *   </td>
 *   <td>
 * For more information, see \ref FileMountProps "File Mount Properties"
 *   </td>
 * </tr>
 * <tr valign="top">
 *   <td>
 * Object creation (OCPL)
 *   </td>
 *   <td>
 * \ref H5P_OBJECT_CREATE
 *   </td>
 *   <td>
 * See \ref OCPL
 *   </td>
 * </tr>
 * <tr valign="top">
 *   <td>
 * Object copy (OCPYPL)
 *   </td>
 *   <td>
 * \ref H5P_OBJECT_COPY
 *   </td>
 *   <td>
 *
 *   </td>
 * </tr>
 * <tr valign="top">
 *   <td>
 * Group creation (GCPL)
 *   </td>
 *   <td>
 * \ref H5P_GROUP_CREATE
 *   </td>
 *   <td>
 * See \ref subsec_group_program
 *   </td>
 * </tr>
 * <tr valign="top">
 *   <td>
 * Group access (GAPL)
 *   </td>
 *   <td>
 * \ref H5P_GROUP_ACCESS
 *   </td>
 *   <td>
 *
 *   </td>
 * </tr>
 * <tr valign="top">
 *   <td>
 * Link creation (LCPL)
 *   </td>
 *   <td>
 * \ref H5P_LINK_CREATE
 *   </td>
 *   <td>
 * See examples in \ref subsec_plist_program and \ref LCPL
 *   </td>
 * </tr>
 * <tr valign="top">
 *   <td>
 * Link access (LAPL)
 *   </td>
 *   <td>
 * \ref H5P_LINK_ACCESS
 *   </td>
 *   <td>
 *
 *   </td>
 * </tr>
 * <tr valign="top">
 *   <td>
 * Dataset creation (DCPL)
 *   </td>
 *   <td>
 * \ref H5P_DATASET_CREATE
 *   </td>
 *   <td>
 * See \ref subsec_dataset_program
 *   </td>
 * </tr>
 * <tr valign="top">
 *   <td>
 * Dataset access (DAPL)
 *   </td>
 *   <td>
 * \ref H5P_DATASET_ACCESS
 *   </td>
 *   <td>
 *
 *   </td>
 * </tr>
 * <tr valign="top">
 *   <td>
 * Dataset transfer (DXPL)
 *   </td>
 *   <td>
 * \ref H5P_DATASET_XFER
 *   </td>
 *   <td>
 *
 *   </td>
 * </tr>
 * <tr valign="top">
 *   <td>
 * Datatype creation (TCPL)
 *   </td>
 *   <td>
 * \ref H5P_DATATYPE_CREATE
 *   </td>
 *   <td>
 * See various sections of \ref sec_datatype
 *   </td>
 * </tr>
 * <tr valign="top">
 *   <td>
 * String creation (STRCPL)
 *   </td>
 *   <td>
 * \ref H5P_STRING_CREATE
 *   </td>
 *   <td>
 * See \ref subsec_dataset_program and \ref subsec_datatype_program
 *   </td>
 * </tr>
 * <tr valign="top">
 *   <td>
 * Attribute creation (ACPL)
 *   </td>
 *   <td>
 * \ref H5P_ATTRIBUTE_CREATE
 *   </td>
 *   <td>
 * See \ref subsec_attribute_work.
 *   </td>
 * </tr>
 * </table>
 *
 * Note: In the table above, the abbreviations to the right of each property list class name in this
 * table are widely used in both HDF5 programmer documentation and HDF5 source code. For
 * example, \ref FCPL (FCPL) is the file creation property list, \ref OCPL (OCPL) is the object creation
 * property list, \ref OCPYPL (OCPYPL) is object copy property list, and \ref STRCPL (STRCPL) is the string
 * creation property list. These abbreviations may appear in either uppercase or lowercase.
 *
 * The “HDF5 property list class inheritance hierarchy” figure, immediately following, illustrates
 * the inheritance hierarchy of HDF5's property list classes. Properties are defined at the root of the
 * HDF5 property environment (\ref PLCR in the figure below). Property list
 * classes then inherit properties from that root, either directly or indirectly through a parent class.
 * In every case, a property list class inherits only the properties relevant to its role. For example,
 * the \ref OCPL (OCPL) inherits all properties that are relevant to the
 * creation of any object while the \ref GCPL (GCPL) inherits only those
 * properties that are relevant to group creation.
 *
 * <table>
 * <tr>
 * <td>
 * \image html PropListClassInheritance.gif "HDF5 property list class inheritance hierarchy"
 * </td>
 * </tr>
 * </table>
 * Note: In the figure above, property list classes displayed in black are directly accessible through
 * the programming interface; the root of the property environment and the \ref STRCPL and \ref OCPL
 * property list classes, in gray above, are not user-accessible. The red empty set symbol indicates
 * that the \ref FMPL (FMPL) is an empty class; that is, it has no set table
 * properties. For more information, see \ref FileMountProps "File Mount Properties". Abbreviations
 * used in this figure are defined in the preceding table, \ref table_plist "Property list classes in HDF5".
 *
 * \subsubsection subsubsec_plist_lists Property Lists
 *
 * A property list is a collection of related properties that are used together in specific
 * circumstances. A new property list created from a property list class inherits the properties of the
 * property list class and each property's default value. A fresh dataset creation property list, for
 * example, includes all of the HDF5 properties relevant to the creation of a new dataset.
 *
 * Property lists are implemented as containers holding a collection of name/value pairs. Each pair
 * specifies a property name and a value for the property. A property list usually contains
 * information for one to many properties.
 *
 * HDF5's default property values are designed to be reasonable for general use cases. Therefore,
 * an application can often use a property list without modification. On the other hand, adjusting
 * property list settings is a routine action and there are many reasons for an application to do so.
 *
 * A new property list may either be derived from a property list class or copied from an existing
 * property list. When a property list is created from a property list class, it contains all the
 * properties that are relevant to the class, with each property set to its default value. A new
 * property list created by copying an existing property list will contain the same properties and
 * property values as the original property list. In either case, the property values can be changed as
 * needed through the HDF5 API.
 *
 * Property lists can be freely reused to create consistency. For example, a single set of file, group,
 * and dataset creation property lists might be created at the beginning of a project and used to
 * create hundreds, thousands, even millions, of consistent files, file structures, and datasets over
 * the project's life. When such consistency is important to a project, this is an economical means
 * of providing it.
 *
 * \subsubsection subsubsec_plist_props Properties
 *
 * A property is the basic element of the property list hierarchy. HDF5 offers nearly one hundred
 * properties controlling things ranging from file access rights, to the storage layout of a dataset,
 * through optimizing the use of a parallel computing environment.
 *
 * Further examples include the following:
 * <table>
 * <tr><th>Purpose</th><th>Examples</th><th>Property List</th></tr>
 * <tr valign="top">
 *   <td>
 *   Specify the driver to be used to open a file
 *   </td>
 *   <td>
 *   A POSIX driver or an MPI IO driver
 *   </td>
 *   <td>
 *   \ref FAPL
 *   </td>
 * <tr valign="top">
 *   <td>
 * Specify filters to be applied to a dataset
 *   </td>
 *   <td>
 * Gzip compression or checksum evaluation
 *   </td>
 *   <td>
 * \ref DCPL
 *   </td>
 * </tr>
 * <tr valign="top">
 *   <td>
 * Specify whether to record key times associated with an object
 *   </td>
 *   <td>
 * Creation time and/or last-modified time
 *   </td>
 *   <td>
 * \ref OCPL
 *   </td>
 * </tr>
 * <tr valign="top">
 *   <td>
 * Specify the access mode for a file opened via an external link
 *   </td>
 *   <td>
 * Read-only or read-write
 *   </td>
 *   <td>
 * \ref LAPL
 *   </td>
 * </tr>
 * </table>
 *
 * Each property is initialized with a default value. For each property, there are one or more
 * dedicated H5Pset_*calls that can be used to change that value.
 *
 * <h4>Creation, access, and transfer properties:</h4>
 *
 * Properties fall into one of several major categories: creation properties, access properties, and
 * transfer properties.
 *
 * Creation properties control permanent object characteristics. These characteristics must be
 * established when an object is created, cannot change through the life of the object (they are
 * immutable), and the property setting usually has a permanent presence in the file.
 *
 *  <table>
 *     <caption align=top>Examples of creation properties include:</caption>
 *     <tr>
 *       <td>
 * <p>
 * Whether a dataset is stored in a compact, contiguous, or chunked layout <br />
 * <br />
 * The default for this dataset creation property (\ref H5Pset_layout) is that a dataset is
 * stored in a contiguous block. This works well for datasets with a known size limit that
 * will fit easily in system memory. <br />
 * <br />
 * A chunked layout is important if a dataset is to be compressed, to enable extending
 * the dataset's size, or to enable caching during I/O. <br />
 * <br />
 * A compact layout is suitable only for very small datasets because the raw data is
 * stored in the object header.
 * </p>
 *       </td>
 *     </tr>
 *     <tr>
 *       <td>
 * <p>
 * Creation of intermediate groups when adding an object to an HDF5 file<br />
 * <br />
 * This link creation property, \ref H5Pset_create_intermediate_group, enables an
 * application to add an object in a file without having to know that the group or group
 * hierarchy containing that object already exists. With this property set, HDF5
 * automatically creates missing groups. If this property is not set, an application must
 * verify that each group in the path exists, and create those that do not, before creating
 * the new object; if any group is missing, the create operation will fail.
 * </p>
 *       </td>
 *     </tr>
 *     <tr>
 *       <td>
 * <p>
 * Whether an HDF5 file is a single file or a set of tightly related files that form a virtual
 * HDF5 file<br />
 * <br />
 * Certain file creation properties enable the application to select one of several file
 * layouts. Examples of the available layouts include a standard POSIX-compliant
 * layout (\ref H5Pset_fapl_sec2), a family of files (\ref H5Pset_fapl_family), and a split file
 * layout that separates raw data and metadata into separate files (\ref H5Pset_fapl_split).
 * These and other file layout options are discussed in \ref subsec_file_alternate_drivers.
 * </p>
 *       </td>
 *     </tr>
 *     <tr>
 *       <td>
 * <p>
 * To enable error detection when creating a dataset<br />
 * <br />
 * In settings where data integrity is vulnerable, it may be desirable to set
 * checksumming when datasets are created (\ref H5Pset_fletcher32). A subsequent
 * application will then have a means to verify data integrity when reading the dataset.
 * </p>
 *       </td>
 *     </tr>
 *   </table>
 *
 * Access properties control transient object characteristics. These characteristics may change with
 * the circumstances under which an object is accessed.
 *
 *  <table>
 *     <caption align=top>Examples of access properties include:</caption>
 *     <tr>
 *       <td>
 * <p>
 * The driver used to open a file<br />
 * <br />
 * For example, a file might be created with the MPI I/O driver (\ref H5Pset_fapl_mpio)
 * during high-speed data acquisition in a parallel computing environment. The same
 * file might later be analyzed in a serial computing environment with I/O access
 * handled through the serial POSIX driver (\ref H5Pset_fapl_sec2).
 * </p>
 *       </td>
 *     </tr>
 *     <tr>
 *       <td>
 * <p>
 * Optimization settings in specialized environments<br />
 * <br />
 * Optimizations differ across computing environments and according to the needs of
 * the task being performed, so are transient by nature.
 * </p>
 *       </td>
 *     </tr>
 *   </table>
 *
 * Transfer properties apply only to datasets and control transient aspects of data I/O. These
 * characteristics may change with the circumstances under which data is accessed.
 *
 *  <table>
 *     <caption align=top>Examples of dataset transfer properties include:</caption>
 *     <tr>
 *       <td>
 * <p>
 * To enable error detection when reading a dataset<br />
 * <br />
 * If checksumming has been set on a dataset (with \ref H5Pset_fletcher32, in the dataset
 * creation property list), an application reading that dataset can choose whether to check
 * for data integrity (\ref H5Pset_edc_check).
 * </p>
 *       </td>
 *     </tr>
 *     <tr>
 *       <td>
 * <p>
 * Various properties to optimize chunked data I/O on parallel computing systems<br />
 * <br />
 * HDF5 provides several properties for tuning I/O of chunked datasets in a parallel
 * computing environment (\ref H5Pset_dxpl_mpio_chunk_opt, \ref H5Pset_dxpl_mpio_chunk_opt_num,
 * \ref H5Pset_dxpl_mpio_chunk_opt_ratio, and \ref H5Pget_mpio_actual_chunk_opt_mode).<br />
 * <br />
 * Optimal settings differ due to the characteristics of a computing environment and due
 * to an application's data access patterns; even when working with the same file, these
 * settings might change for every application and every platform.
 * </p>
 *       </td>
 *     </tr>
 *   </table>
 *
 * \subsection subsec_plist_program Programming Model for Properties and Property Lists
 *
 * The programming model for HDF5 property lists is actually quite simple:
 * \li Create a property list.
 * \li Modify the property list, if required.
 * \li Use the property list.
 * \li Close the property list.
 *
 * There are nuances, of course, but that is the basic process.
 *
 * In some cases, you will not have to define property lists at all. If the default property settings are
 * sufficient for your application, you can tell HDF5 to use the default property list.
 *
 * The following sections first discuss the use of default property lists, then each step of the
 * programming model, and finally a few less frequently used property list operations.
 *
 * \subsubsection subsubsec_plist_default Using Default Property Lists
 *
 * Default property lists can simplify many routine HDF5 tasks because you do not always have to
 * create every property list you use.
 *
 * An application that would be well-served by HDF5's default property settings can use the default
 * property lists simply by substituting the value \ref H5P_DEFAULT for a property list identifier.
 * HDF5 will then apply the default property list for the appropriate property list class.
 *
 * For example, the function \ref H5Dcreate2 calls for a link creation property list, a dataset creation
 * property list, and a dataset access property list. If the default properties are suitable for a dataset,
 * this call can be made as
 * \code
 *     dset_id = H5Dcreate2( loc_id, name, dtype_id, space_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );
 * \endcode
 * HDF5 will then apply the default link creation, dataset creation, and dataset access property lists
 * correctly.
 *
 * Of course, you would not want to do this without considering where it is appropriate, as there
 * may be unforeseen consequences. Consider, for example, the use of chunked datasets. Optimal
 * chunking is quite dependent on the makeup of the dataset and the most common access patterns,
 * both of which must be taken into account in setting up the size and shape of chunks.
 *
 * \subsubsection subsubsec_plist_basic Basic Steps of the Programming Model
 *
 * The steps of the property list programming model are described in the sub-sections below.
 *
 * <h4>Create a Property List</h4>
 *
 * A new property list can be created either as an instance of a property list class or by copying an
 * existing property list. Consider the following examples. A new dataset creation property list is
 * first created "from scratch" with \ref H5Pcreate. A second dataset creation property list is then
 * created by copying the first one with \ref H5Pcopy.
 *
 * \code
 *     dcplA_id = H5Pcreate (H5P_DATASET_CREATE);
 * \endcode
 *
 * The new dataset creation property list is created as an instance of the property list class
 * \ref H5P_DATASET_CREATE.
 *
 * The new dataset creation property list's identifier is returned in dcplA_id and the property list is
 * initialized with default dataset creation property values.
 *
 * A list of valid classes appears in the table \ref table_plist "Property list classes in HDF5".
 *
 * \code
 *     dcplB_id = H5Pcopy (dcplA_id);
 * \endcode
 *
 * A new dataset creation property list, dcplB_id, is created as a copy of dcplA_id and is initialized
 * with dataset creation property values currently in dcplA_id.
 *
 * At this point, dcplA_id and dcplB_id are identical; they will both contain any modified property
 * values that were changed in dcplA_id before dcplB_id was created. They may, however, diverge
 * as additional property values are reset in each.
 *
 * While we are creating property lists, let's create a link creation property list; we will need this
 * property list when the new dataset is linked into the file below:
 * \code
 *     lcplAB_id = H5Pcreate (H5P_LINK_CREATE);
 * \endcode
 *
 * <h4>Change Property Values</h4>
 *
 * This section describes how to set property values.
 *
 * Later in this section, the dataset creation property lists dcplA_id and dcplB_id created in the
 * section above will be used respectively to create chunked and contiguous datasets. To set this up,
 * we must set the layout property in each property list. The following example sets dcplA_id for
 * chunked datasets and dcplB_id for contiguous datasets:
 * \code
 *     error = H5Pset_layout (dcplA_id, H5D_CHUNKED);
 *     error = H5Pset_layout (dcplB_id, H5D_CONTIGUOUS);
 * \endcode
 *
 * Since dcplA_id specifies a chunked layout, we must also set the number of dimensions and the
 * size of the chunks. The example below specifies that datasets created with dcplA_id will be
 * 3-dimensional and that the chunk size will be 100 in each dimension:
 * \code
 *     error = H5Pset_chunk (dcplA_id, 3, [100,100,100]);
 * \endcode
 *
 * These datasets will be created with UTF-8 encoded names. To accomplish that, the following
 * example sets the character encoding property in the link creation property list to create link
 * names with UTF-8 encoding:
 * \code
 *     error = H5Pset_char_encoding (lcplAB_id, H5T_CSET_UTF8);
 * \endcode
 *
 * dcplA_id can now be used to create chunked datasets and dcplB_id to create contiguous datasets.
 * And with the use of lcplAB_id, they will be created with UTF-8 encoded names.
 *
 * <h4>Use the Property List</h4>
 *
 * Once the required property lists have been created, they can be used to control various HDF5
 * processes. For illustration, consider dataset creation.
 *
 * Assume that the datatype dtypeAB and the dataspaces dspaceA and dspaceB have been defined
 * and that the location identifier locAB_id specifies the group AB in the current HDF5 file. We
 * have already created the required link creation and dataset creation property lists.
 * For the sake of illustration, we assume that the default dataset access property list meets our application
 * requirements. The following calls would create the datasets dsetA and dsetB in the group AB.
 * The raw data in dsetA will be contiguous while dsetB raw data will be chunked; both datasets
 * will have UTF-8 encoded link names:
 *
 * \code
 *     dsetA_id = H5Dcreate2( locAB_id, dsetA, dtypeAB, dspaceA_id,
 *                lcplAB_id, dcplA_id, H5P_DEFAULT );
 *     dsetB_id = H5Dcreate2( locAB_id, dsetB, dtypeAB, dspaceB_id,
 *                lcplAB_id, dcplB_id, H5P_DEFAULT );
 * \endcode
 *
 * <h4>Close the Property List</h4>
 *
 * Generally, creating or opening anything in an HDF5 file results in an HDF5 identifier. These
 * identifiers are of HDF5 type hid_t and include things like file identifiers, often expressed as
 * file_id; dataset identifiers, dset_id; and property list identifiers, plist_id. To reduce the risk of
 * memory leaks, all of these identifiers must be closed once they are no longer needed.
 *
 * Property list identifiers are no exception to this rule, and \ref H5Pclose is used for this purpose. The
 * calls immediately following would close the property lists created and used in the examples above.
 *
 * \code
 *     error = H5Pclose (dcplA_id);
 *     error = H5Pclose (dcplB_id);
 *     error = H5Pclose (lcplAB_id);
 * \endcode
 *
 * \subsubsection subsubsec_plist_additional Additional Property List Operations
 *
 * A few property list operations fall outside of the programming model described above. This
 * section describes those operations.
 *
 * <h4>Query the Class of an Existing Property List</h4>
 *
 * Occasionally an application will have a property list but not know the corresponding property list
 * class. A call such as in the following example will retrieve the unknown class of a known property list:
 * \code
 *     PList_Class = H5Pget_class (dcplA_id);
 * \endcode
 *
 * Upon this function's return, PList_Class will contain the value \ref H5P_DATASET_CREATE indicating that
 * dcplA_id is a dataset creation property list.

 * <h4>Determine Current Creation Property List Settings in an Existing Object</h4>
 *
 * After a file has been created, another application may work on the file without knowing how the
 * creation properties for the file were set up. Retrieving these property values is often unnecessary;
 * HDF5 can read the data and knows how to deal with any properties it encounters.
 *
 * But sometimes an application must do something that requires knowing the creation property
 * settings. HDF5 makes the acquisition of this information fairly straight-forward; for each
 * property setting call, H5Pset_*, there is a corresponding H5Pget_*call to retrieve the property's
 * current setting.
 *
 * Consider the following examples which illustrate the determination of dataset layout and chunking settings:
 *
 * The application must first identify the creation property list with the appropriate get creation property
 * list call. There is one such call for each kind of object.
 *
 * \ref H5Dget_create_plist will return a property list identifier for the creation property list that was
 * used to create the dataset. Call it DCPL1_id.
 *
 * \ref H5Pset_layout sets a dataset's layout to be compact, contiguous, or chunked.
 *
 * \ref H5Pget_layout called with DCPL1_id will return the dataset's layout,
 * either \ref H5D_COMPACT, \ref H5D_CONTIGUOUS, or \ref H5D_CHUNKED.
 *
 * \ref H5Pset_chunk sets the rank of a dataset, that is the number of dimensions it will have, and the
 * maximum size of each dimension.
 *
 * \ref H5Pget_chunk, also called with DCPL1_id, will return the rank of the dataset and the maximum
 * size of each dimension.
 *
 * If a creation property value has not been explicitly set, these H5Pget_calls will return the
 * property's default value.
 *
 * <h4>Determine Access Property Settings</h4>
 *
 * Access property settings are quite different from creation properties. Since access property
 * settings are not retained in an HDF5 file or object, there is normally no knowledge of the settings
 * that were used in the past. On the other hand, since access properties do not affect characteristics
 * of the file or object, this is not normally an issue. For more information, see "Access and
 * Creation Property Exceptions."
 *
 * One circumstance under which an application might need to determine access property settings
 * might be when a file or object is already open but the application does not know the property list
 * settings. In that case, the application can use the appropriate get access property list
 * call to retrieve a property list identifier. For example, if the dataset dsetA
 * from the earlier examples is still open, the following call would return an identifier for the dataset
 * access property list in use:
 * \code
 *     dsetA_dacpl_id = H5Dget_access_plist( dsetA_id );
 * \endcode
 *
 * The application could then use the returned property list identifier to analyze the property settings
 *
 * \subsection subsec_plist_generic Generic Properties Interface and User-defined Properties
 *
 * HDF5's generic property interface provides tools for managing the entire property hierarchy and
 * for the creation and management of user-defined property lists and properties. This interface also
 * makes it possible for an application or a driver to create, modify, and manage custom properties,
 * property lists, and property list classes. A comprehensive list of functions for this interface
 * appears under "Generic Property Operations (Advanced)" in the "H5P: Property List Interface"
 * section of the \ref RM.
 *
 * Further discussion of HDF5's generic property interface and user-defined properties and
 * property lists is beyond the scope of this document.
 *
 * \subsection subsec_plist_H5P Property List Function Summaries
 *
 * General property functions, generic property functions and macros, property functions that are
 * used with multiple types of objects, and object and link property functions are listed below.
 *
 * Property list functions that apply to a specific type of object are listed in the chapter that
 * discusses that object. For example, the \ref sec_dataset chapter has two property list function listings:
 * one for dataset creation property list functions and one for dataset access property list functions.
 * As has been stated, this chapter is not intended to describe every property list function.
 *
 * \ref H5P reference manual
 *
 * \subsection subsec_plist_resources Additional Property List Resources
 * Property lists are ubiquitous in an HDF5 environment and are therefore discussed in many places
 * in HDF5 documentation. The following sections and listings in the \ref UG are of
 * particular interest:
 * \li In the \ref sec_data_model chapter, see \ref subsubsec_data_model_abstract_plist.
 * \li In the \ref sec_file chapter, see the following sections and listings:
 * <ul> <li>\ref subsec_file_creation_access</li>
 * <li>\ref subsec_file_property_lists</li>
 * <li>\ref subsubsec_file_examples_props</li>
 * <li>\ref subsubsec_file_examples_access</li>
 * <li>\ref dcpl_table_tag "Dataset creation property list functions (H5P)"</li>
 * <li>\ref fapl_table_tag "File access property list functions (H5P)"</li>
 * <li>\ref fd_pl_table_tag "File driver property list functions (H5P)"</li></ul>
 * \li In the \ref sec_attribute chapter, see "Attribute creation property list functions (H5P)".
 * \li In the \ref sec_group chapter, see "Group creation property list functions (H5P)".
 * \li Property lists are discussed throughout \ref sec_dataset.
 *
 * All property list functions are described in the \ref H5P section of the
 * \ref RM. The function index at the top of the page provides a categorized listing
 * grouped by property list class. Those classes are listed below:
 * \li \ref FCPL
 * \li \ref FAPL
 * \li \ref GCPL
 * \li \ref DCPL
 * \li \ref DAPL
 * \li \ref DXPL
 * \li \ref LCPL
 * \li \ref LAPL
 * \li \ref OCPL
 * \li \ref OCPYPL
 *
 * Additional categories not related to the class structure are as follows:
 * \li General property list operations
 * \li Generic property list functions
 *
 * The general property functions can be used with any property list; the generic property functions
 * constitute an advanced feature.
 *
 * The in-memory file image feature of HDF5 uses property lists in a manner that differs
 * substantially from their use elsewhere in HDF5. Those who plan to use in-memory file images
 * must study "File Image Operations" (PDF) in the Advanced Topics in HDF5collection.
 *
 * \subsection subsec_plist_notes Notes
 *
 * \anchor FileMountProps <h4>File Mount Properties</h4>
 *
 * While the file mount property list class \ref H5P_FILE_MOUNT is a valid HDF5 property list class,
 * no file mount properties are defined by the HDF5 Library. References to a file mount property
 * list should always be expressed as \ref H5P_DEFAULT, meaning the default file mount property list.
 *
 * <h4>Access and Creation Property Exceptions</h4>
 *
 * There are a small number of exceptions to the rule that creation properties are always retained in
 * a file or object and access properties are never retained.
 *
 * The following properties are file access properties but they are not transient; they have
 * permanent and different effects on a file. They could be validly classified as file creation
 * properties as they must be set at creation time to properly create the file. But they are access
 * properties because they must also be set when a file is reopened to properly access the file.
 * <table>
 * <tr><th>Property</th><th>Related function</th></tr>
 * <tr valign="top">
 *   <td>
 *   Family file driver
 *   </td>
 *   <td>
 *   \ref H5Pset_fapl_family
 *   </td>
 * </tr>
 * <tr valign="top">
 *   <td>
 *   Split file driver
 *   </td>
 *   <td>
 *   \ref H5Pset_fapl_split
 *   </td>
 * </tr>
 * <tr valign="top">
 *   <td>
 *   Core file driver
 *   </td>
 *   <td>
 *   \ref H5Pset_fapl_core
 *   </td>
 * </tr>
 * </table>
 *
 * The following is a link creation property, but it is not relevant after an object has been created
 * and is not retained in the file or object.
 * <table>
 * <tr><th>Property</th><th>Related function</th></tr>
 * <tr valign="top">
 *   <td>
 *   Create missing intermediate groups
 *   </td>
 *   <td>
 *   \ref H5Pset_create_intermediate_group
 *   </td>
 * </tr>
 * </table>
 *
 * Previous Chapter \ref sec_error - Next Chapter \ref sec_vol
 *
 * \defgroup H5P Property Lists (H5P)
 *
 * Use the functions in this module to manage HDF5 property lists and property
 * list classes. HDF5 property lists are the main vehicle to configure the
 * behavior of HDF5 API functions.
 *
 * Typically, property lists are created by instantiating one of the built-in
 * or user-defined property list classes. After adding suitable properties,
 * property lists are used when opening or creating HDF5 items, or when reading
 * or writing data. Property lists can be modified by adding or changing
 * properties. Property lists are deleted by closing the associated handles.
 *
 * \ref PLCR
 * \snippet{doc} tables/propertyLists.dox plcr_table
 *
 * \ref PLCR
 * \snippet{doc} tables/propertyLists.dox plcra_table
 *
 * \ref PLCR / \ref OCPL / \ref GCPL
 * \snippet{doc} tables/propertyLists.dox fcpl_table
 *
 * \ref PLCR
 * \snippet{doc} tables/propertyLists.dox fapl_table
 * \snippet{doc} tables/propertyLists.dox fd_pl_table
 *
 * \ref PLCR
 * \snippet{doc} tables/propertyLists.dox lapl_table
 *
 * \ref PLCR / \ref OCPL
 * \snippet{doc} tables/propertyLists.dox dcpl_table
 *
 * \ref PLCR / \ref LAPL
 * \snippet{doc} tables/propertyLists.dox dapl_table
 *
 * \ref PLCR / \ref OCPL
 * \snippet{doc} tables/propertyLists.dox gcpl_table
 *
 * \ref PLCR / \ref LAPL
 * \snippet{doc} tables/propertyLists.dox gapl_table
 *
 * \ref PLCR
 * \snippet{doc} tables/propertyLists.dox ocpl_table
 *
 * \ref PLCR
 * \snippet{doc} tables/propertyLists.dox ocpypl_table
 *
 * \ref PLCR
 * \snippet{doc} tables/propertyLists.dox strcpl_table
 *
 * \ref PLCR / \ref STRCPL
 * \snippet{doc} tables/propertyLists.dox lcpl_table
 *
 * \ref PLCR / \ref STRCPL
 * \snippet{doc} tables/propertyLists.dox acpl_table
 *
 *
 * \defgroup STRCPL String Creation Properties
 * \ingroup H5P
 * Currently, there are only two creation properties that you can use to control
 * the creation of HDF5 attributes and links. The first creation property, the
 * choice of a character encoding, applies to both attributes and links.
 * The second creation property applies to links only, and advises the library
 * to automatically create missing intermediate groups when creating new objects.
 *
 * \snippet{doc} tables/propertyLists.dox strcpl_table
 *
 * \defgroup LCPL Link Creation Properties
 * \ingroup STRCPL
 * This creation property applies to links only, and advises the library
 * to automatically create missing intermediate groups when creating new objects.
 *
 * \snippet{doc} tables/propertyLists.dox lcpl_table
 *
 * @see STRCPL
 *
 * \defgroup ACPL Attribute Creation Properties
 * \ingroup STRCPL
 * The creation property, the choice of a character encoding, applies to attributes.
 *
 * \snippet{doc} tables/propertyLists.dox acpl_table
 *
 * @see STRCPL
 *
 * \defgroup LAPL Link Access Properties
 * \ingroup H5P
 *
 * \snippet{doc} tables/propertyLists.dox lapl_table
 *
 * \defgroup DAPL Dataset Access Properties
 * \ingroup LAPL
 * Use dataset access properties to modify the default behavior of the HDF5
 * library when accessing datasets. The properties include adjusting the size
 * of the chunk cache, providing prefixes for external content and virtual
 * dataset file paths, and controlling flush behavior, etc. These properties
 * are \Emph{not} persisted with datasets, and can be adjusted at runtime before
 * a dataset is created or opened.
 *
 * \snippet{doc} tables/propertyLists.dox dapl_table
 *
 * \defgroup DCPL Dataset Creation Properties
 * \ingroup OCPL
 * Use dataset creation properties to control aspects of dataset creation such
 * as fill time, storage layout, compression methods, etc.
 * Unlike dataset access and transfer properties, creation properties \Emph{are}
 * stored with the dataset, and cannot be changed once a dataset has been
 * created.
 *
 * \snippet{doc} tables/propertyLists.dox dcpl_table
 *
 * \defgroup DXPL Dataset Transfer Properties
 * \ingroup H5P
 * Use dataset transfer properties to customize certain aspects of reading
 * and writing datasets such as transformations, MPI-IO I/O mode, error
 * detection, etc. These properties are \Emph{not} persisted with datasets,
 * and can be adjusted at runtime before a dataset is read or written.
 *
 * \snippet{doc} tables/propertyLists.dox dxpl_table
 *
 * \defgroup FAPL File Access Properties
 * \ingroup H5P
 * Use file access properties to modify the default behavior of the HDF5
 * library when accessing files. The properties include selecting a virtual
 * file driver (VFD), configuring the metadata cache (MDC), control
 * file locking, etc. These properties are \Emph{not} persisted with files, and
 * can be adjusted at runtime before a file is created or opened.
 *
 * \snippet{doc} tables/propertyLists.dox fapl_table
 * \snippet{doc} tables/propertyLists.dox fd_pl_table
 *
 * \defgroup FCPL File Creation Properties
 * \ingroup GCPL
 * Use file creation properties to control aspects of file creation such
 * as setting a file space management strategy or creating a user block.
 * Unlike file access properties, creation properties \Emph{are}
 * stored with the file, and cannot be changed once a file has been
 * created.
 *
 * \snippet{doc} tables/propertyLists.dox fcpl_table
 *
 * \defgroup GAPL Group Access Properties
 * \ingroup LAPL
 * The functions in this section can be applied to group property lists.
 *
 * \snippet{doc} tables/propertyLists.dox gapl_table
 *
 * \defgroup GCPL Group Creation Properties
 * \ingroup OCPL
 * Use group creation properties to control aspects of group creation such
 * as storage layout, compression, and link creation order tracking.
 * Unlike file access properties, creation properties \Emph{are}
 * stored with the group, and cannot be changed once a group has been
 * created.
 *
 * \snippet{doc} tables/propertyLists.dox gcpl_table
 *
 * \defgroup PLCR Property List Class Root
 * \ingroup H5P
 * Use the functions in this module to manage HDF5 property lists.
 *
 * \snippet{doc} tables/propertyLists.dox plcr_table
 *
 * \defgroup PLCRA Property List Class Root (Advanced)
 * \ingroup H5P
 * You can create and customize user-defined property list classes using the
 * functions described below. Arbitrary user-defined properties can also
 * be inserted into existing property lists as so-called temporary properties.
 *
 * \snippet{doc} tables/propertyLists.dox plcra_table
 *
 * \defgroup OCPL Object Creation Properties
 * \ingroup H5P
 *
 * \snippet{doc} tables/propertyLists.dox ocpl_table
 *
 * \defgroup OCPYPL Object Copy Properties
 * \ingroup H5P
 *
 * \snippet{doc} tables/propertyLists.dox ocpypl_table
 *
 * \defgroup FMPL File Mount Properties
 * \ingroup H5P
 * Empty property class.
 *
 *
 * \defgroup TCPL Datatype Creation Properties
 * \ingroup OCPL
 * TCPL isn't supported yet.
 *
 *
 * \defgroup TAPL Datatype Access Properties
 * \ingroup LAPL
 * TAPL isn't supported yet.
 *
 *
 *
 */

#endif /* H5Pmodule_H */
