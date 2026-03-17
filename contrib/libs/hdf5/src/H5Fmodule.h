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
 *          H5F package.  Including this header means that the source file
 *          is part of the H5F package.
 */
#ifndef H5Fmodule_H
#define H5Fmodule_H

/* Define the proper control macros for the generic FUNC_ENTER/LEAVE and error
 *      reporting macros.
 */
#define H5F_MODULE
#define H5_MY_PKG     H5F
#define H5_MY_PKG_ERR H5E_FILE

/** \page H5F_UG  The HDF5 File
 *
 * \section sec_file The HDF5 File
 * \subsection subsec_file_intro Introduction
 * The purpose of this chapter is to describe how to work with HDF5 data files.
 *
 * If HDF5 data is to be written to or read from a file, the file must first be explicitly created or
 * opened with the appropriate file driver and access privileges. Once all work with the file is
 * complete, the file must be explicitly closed.
 *
 * This chapter discusses the following:
 * \li File access modes
 * \li Creating, opening, and closing files
 * \li The use of file creation property lists
 * \li The use of file access property lists
 * \li The use of low-level file drivers
 *
 * This chapter assumes an understanding of the material presented in the data model chapter. For
 * more information, @see @ref sec_data_model.
 *
 * \subsection subsec_file_access_modes File Access Modes
 * There are two issues regarding file access:
 * <ul><li>What should happen when a new file is created but a file of the same name already
 * exists? Should the create action fail, or should the existing file be overwritten?</li>
 * <li>Is a file to be opened with read-only or read-write access?</li></ul>
 *
 * Four access modes address these concerns. Two of these modes can be used with #H5Fcreate, and
 * two modes can be used with #H5Fopen.
 * \li #H5Fcreate accepts #H5F_ACC_EXCL or #H5F_ACC_TRUNC
 * \li #H5Fopen accepts #H5F_ACC_RDONLY or #H5F_ACC_RDWR
 *
 * The access modes are described in the table below.
 *
 * <table>
 * <caption>Access flags and modes</caption>
 * <tr>
 * <th>Access Flag</th>
 * <th>Resulting Access Mode</th>
 * </tr>
 * <tr>
 * <td>#H5F_ACC_EXCL</td>
 * <td>If the file already exists, #H5Fcreate fails. If the file does not exist,
 * it is created and opened with read-write access. (Default)</td>
 * </tr>
 * <tr>
 * <td>#H5F_ACC_TRUNC</td>
 * <td>If the file already exists, the file is opened with read-write access,
 * and new data will overwrite any existing data. If the file does not exist,
 * it is created and opened with read-write access.</td>
 * </tr>
 * <tr>
 * <td>#H5F_ACC_RDONLY</td>
 * <td>An existing file is opened with read-only access. If the file does not
 * exist, #H5Fopen fails. (Default)</td>
 * </tr>
 * <tr>
 * <td>#H5F_ACC_RDWR</td>
 * <td>An existing file is opened with read-write access. If the file does not
 * exist, #H5Fopen fails.</td>
 * </tr>
 * </table>
 *
 * By default, #H5Fopen opens a file for read-only access; passing #H5F_ACC_RDWR allows
 * read-write access to the file.
 *
 * By default, #H5Fcreate fails if the file already exists; only passing #H5F_ACC_TRUNC allows
 * the truncating of an existing file.
 *
 * \subsection subsec_file_creation_access File Creation and File Access Properties
 * File creation and file access property lists control the more complex aspects of creating and
 * accessing files.
 *
 * File creation property lists control the characteristics of a file such as the size of the userblock,
 * a user-definable data block; the size of data address parameters; properties of the B-trees that are
 * used to manage the data in the file; and certain HDF5 Library versioning information.
 *
 * For more information, @see @ref subsubsec_file_property_lists_props.
 *
 * This section has a more detailed discussion of file creation properties. If you have no special
 * requirements for these file characteristics, you can simply specify #H5P_DEFAULT for the default
 * file creation property list when a file creation property list is called for.
 *
 * File access property lists control properties and means of accessing a file such as data alignment
 * characteristics, metadata block and cache sizes, data sieve buffer size, garbage collection
 * settings, and parallel I/O. Data alignment, metadata block and cache sizes, and data sieve buffer
 * size are factors in improving I/O performance.
 *
 * For more information, @see @ref subsubsec_file_property_lists_access.
 *
 * This section has a more detailed discussion of file access properties. If you have no special
 * requirements for these file access characteristics, you can simply specify #H5P_DEFAULT for the
 * default file access property list when a file access property list is called for.
 *
 * <table>
 * <caption>Figure 10 - More sample file structures</caption>
 * <tr>
 * <td>
 * \image html UML_FileAndProps.gif "UML model for an HDF5 file and its property lists"
 * </td>
 * </tr>
 * </table>
 *
 * \subsection subsec_file_drivers Low-level File Drivers
 * The concept of an HDF5 file is actually rather abstract: the address space for what is normally
 * thought of as an HDF5 file might correspond to any of the following at the storage level:
 * \li Single file on a standard file system
 * \li Multiple files on a standard file system
 * \li Multiple files on a parallel file system
 * \li Block of memory within an application's memory space
 * \li More abstract situations such as virtual files
 *
 * This HDF5 address space is generally referred to as an HDF5 file regardless of its organization at
 * the storage level.
 *
 * HDF5 accesses a file (the address space) through various types of low-level file drivers. The
 * default HDF5 file storage layout is as an unbuffered permanent file which is a single, contiguous
 * file on local disk. Alternative layouts are designed to suit the needs of a variety of systems,
 * environments, and applications.
 *
 * \subsection subsec_file_program_model Programming Model for Files
 * Programming models for creating, opening, and closing HDF5 files are described in the
 * sub-sections below.
 *
 * \subsubsection subsubsec_file_program_model_create Creating a New File
 * The programming model for creating a new HDF5 file can be summarized as follows:
 * \li Define the file creation property list
 * \li Define the file access property list
 * \li Create the file
 *
 * First, consider the simple case where we use the default values for the property lists. See the
 * example below.
 *
 * <em>Creating an HDF5 file using property list defaults</em>
 * \code
 *   file_id = H5Fcreate ("SampleFile.h5", H5F_ACC_EXCL, H5P_DEFAULT, H5P_DEFAULT)
 * \endcode
 *
 * Note: The example above specifies that #H5Fcreate should fail if SampleFile.h5 already exists.
 *
 * A more complex case is shown in the example below. In this example, we define file creation
 * and access property lists (though we do not assign any properties), specify that #H5Fcreate
 * should fail if SampleFile.h5 already exists, and create a new file named SampleFile.h5. The example
 * does not specify a driver, so the default driver, #H5FD_SEC2, will be used.
 *
 * <em>Creating an HDF5 file using property lists</em>
 * \code
 *   fcplist_id = H5Pcreate (H5P_FILE_CREATE)
 *   <...set desired file creation properties...>
 *   faplist_id = H5Pcreate (H5P_FILE_ACCESS)
 *   <...set desired file access properties...>
 *   file_id = H5Fcreate ("SampleFile.h5", H5F_ACC_EXCL, fcplist_id, faplist_id)
 * \endcode
 * Notes:
 * 1. A root group is automatically created in a file when the file is first created.
 *
 * 2. File property lists, once defined, can be reused when another file is created within the same
 * application.
 *
 * \subsubsection subsubsec_file_program_model_open Opening an Existing File
 * The programming model for opening an existing HDF5 file can be summarized as follows:
 * <ul><li>Define or modify the file access property list including a low-level file driver (optional)</li>
 * <li>Open the file</li></ul>
 *
 * The code in the example below shows how to open an existing file with read-only access.
 *
 * <em>Opening an HDF5 file</em>
 * \code
 *   faplist_id = H5Pcreate (H5P_FILE_ACCESS)
 *   status = H5Pset_fapl_stdio (faplist_id)
 *   file_id = H5Fopen ("SampleFile.h5", H5F_ACC_RDONLY, faplist_id)
 * \endcode
 *
 * \subsubsection subsubsec_file_program_model_close Closing a File
 * The programming model for closing an HDF5 file is very simple:
 * \li Close file
 *
 * We close SampleFile.h5 with the code in the example below.
 *
 * <em>Closing an HDF5 file</em>
 * \code
 *   status = H5Fclose (file_id)
 * \endcode
 * Note that #H5Fclose flushes all unwritten data to storage and that file_id is the identifier returned
 * for SampleFile.h5 by #H5Fopen.
 *
 * More comprehensive discussions regarding all of these steps are provided below.
 *
 * \subsection subsec_file_h5dump Using h5dump to View a File
 * h5dump is a command-line utility that is included in the HDF5 distribution. This program
 * provides a straight-forward means of inspecting the contents of an HDF5 file. You can use
 * h5dump to verify that a program is generating the intended HDF5 file. h5dump displays ASCII
 * output formatted according to the HDF5 DDL grammar.
 *
 * The following h5dump command will display the contents of SampleFile.h5:
 * \code
 *   h5dump SampleFile.h5
 * \endcode
 *
 * If no datasets or groups have been created in and no data has been written to the file, the output
 * will look something like the following:
 * \code
 *    HDF5 "SampleFile.h5" {
 *    GROUP "/" {
 *    }
 *    }
 * \endcode
 *
 * Note that the root group, indicated above by /, was automatically created when the file was created.
 *
 * h5dump is described on the
 * <a href="https://portal.hdfgroup.org/display/HDF5/h5dump">Tools</a>
 * page under
 * <a href="https://portal.hdfgroup.org/display/HDF5/Libraries+and+Tools+Reference">
 * Libraries and Tools Reference</a>.
 * The HDF5 DDL grammar is described in the document \ref DDLBNF110.
 *
 * \subsection subsec_file_summary File Function Summaries
 * General library (\ref H5 functions and macros), (\ref H5F functions), file related
 * (\ref H5P functions), and file driver (\ref H5P functions) are listed below.
 *
 * <table>
 * <caption>General library functions and macros</caption>
 * <tr>
 * <th>Function</th>
 * <th>Purpose</th>
 * </tr>
 * <tr>
 * <td>#H5check_version</td>
 * <td>Verifies that HDF5 library versions are consistent.</td>
 * </tr>
 * <tr>
 * <td>#H5close</td>
 * <td>Flushes all data to disk, closes all open identifiers, and cleans up memory.</td>
 * </tr>
 * <tr>
 * <td>#H5dont_atexit</td>
 * <td>Instructs the library not to install the atexit cleanup routine.</td>
 * </tr>
 * <tr>
 * <td>#H5garbage_collect</td>
 * <td>Performs garbage collection (GC) on all free-lists of all types.</td>
 * </tr>
 * <tr>
 * <td>#H5get_libversion</td>
 * <td>Returns the HDF library release number.</td>
 * </tr>
 * <tr>
 * <td>#H5open</td>
 * <td>Initializes the HDF5 library.</td>
 * </tr>
 * <tr>
 * <td>#H5set_free_list_limits</td>
 * <td>Sets free-list size limits.</td>
 * </tr>
 * <tr>
 * <td>#H5_VERSION_GE</td>
 * <td>Determines whether the version of the library being used is greater than or equal
 * to the specified version.</td>
 * </tr>
 * <tr>
 * <td>#H5_VERSION_LE</td>
 * <td>Determines whether the version of the library being used is less than or equal
 * to the specified version.</td>
 * </tr>
 * </table>
 *
 * <table>
 * <caption>File functions </caption>
 * <tr>
 * <th>Function</th>
 * <th>Purpose</th>
 * </tr>
 * <tr>
 * <td>#H5Fclear_elink_file_cache</td>
 * <td>Clears the external link open file cache for a file.</td>
 * </tr>
 * <tr>
 * <td>#H5Fclose</td>
 * <td>Closes HDF5 file.</td>
 * </tr>
 * <tr>
 * <td>#H5Fcreate</td>
 * <td>Creates new HDF5 file.</td>
 * </tr>
 * <tr>
 * <td>#H5Fflush</td>
 * <td>Flushes data to HDF5 file on storage medium.</td>
 * </tr>
 * <tr>
 * <td>#H5Fget_access_plist</td>
 * <td>Returns a file access property list identifier.</td>
 * </tr>
 * <tr>
 * <td>#H5Fget_create_plist</td>
 * <td>Returns a file creation property list identifier.</td>
 * </tr>
 * <tr>
 * <td>#H5Fget_file_image</td>
 * <td>Retrieves a copy of the image of an existing, open file.</td>
 * </tr>
 * <tr>
 * <td>#H5Fget_filesize</td>
 * <td>Returns the size of an HDF5 file.</td>
 * </tr>
 * <tr>
 * <td>#H5Fget_freespace</td>
 * <td>Returns the amount of free space in a file.</td>
 * </tr>
 * <tr>
 * <td>#H5Fget_info</td>
 * <td>Returns global information for a file.</td>
 * </tr>
 * <tr>
 * <td>#H5Fget_intent</td>
 * <td>Determines the read/write or read-only status of a file.</td>
 * </tr>
 * <tr>
 * <td>#H5Fget_mdc_config</td>
 * <td>Obtains current metadata cache configuration for target file.</td>
 * </tr>
 * <tr>
 * <td>#H5Fget_mdc_hit_rate</td>
 * <td>Obtains target file's metadata cache hit rate.</td>
 * </tr>
 * <tr>
 * <td>#H5Fget_mdc_size</td>
 * <td>Obtains current metadata cache size data for specified file.</td>
 * </tr>
 * <tr>
 * <td>#H5Fget_mpi_atomicity</td>
 * <td>Retrieves the atomicity mode in use.</td>
 * </tr>
 * <tr>
 * <td>#H5Fget_name</td>
 * <td>Retrieves the name of the file to which the object belongs.</td>
 * </tr>
 * <tr>
 * <td>#H5Fget_obj_count</td>
 * <td>Returns the number of open object identifiers for an open file.</td>
 * </tr>
 * <tr>
 * <td>#H5Fget_obj_ids</td>
 * <td>Returns a list of open object identifiers.</td>
 * </tr>
 * <tr>
 * <td>#H5Fget_vfd_handle</td>
 * <td>Returns pointer to the file handle from the virtual file driver.</td>
 * </tr>
 * <tr>
 * <td>#H5Fis_hdf5</td>
 * <td>Determines whether a file is in the HDF5 format.</td>
 * </tr>
 * <tr>
 * <td>#H5Fmount</td>
 * <td>Mounts a file.</td>
 * </tr>
 * <tr>
 * <td>#H5Fopen</td>
 * <td>Opens an existing HDF5 file.</td>
 * </tr>
 * <tr>
 * <td>#H5Freopen</td>
 * <td>Returns a new identifier for a previously-opened HDF5 file.</td>
 * </tr>
 * <tr>
 * <td>#H5Freset_mdc_hit_rate_stats</td>
 * <td>Resets hit rate statistics counters for the target file.</td>
 * </tr>
 * <tr>
 * <td>#H5Fset_mdc_config</td>
 * <td>Configures metadata cache of target file.</td>
 * </tr>
 * <tr>
 * <td>#H5Fset_mpi_atomicity</td>
 * <td>Sets the MPI atomicity mode.</td>
 * </tr>
 * <tr>
 * <td>#H5Funmount</td>
 * <td>Unmounts a file.</td>
 * </tr>
 * </table>
 *
 * \anchor fcpl_table_tag File creation property list functions (H5P)
 * \snippet{doc} tables/propertyLists.dox fcpl_table
 *
 * \anchor fapl_table_tag File access property list functions (H5P)
 * \snippet{doc} tables/propertyLists.dox fapl_table
 *
 * \anchor fd_pl_table_tag File driver property list functions (H5P)
 * \snippet{doc} tables/propertyLists.dox fd_pl_table
 *
 *
 * \subsection subsec_file_create Creating or Opening an HDF5 File
 * This section describes in more detail how to create and how to open files.
 *
 * New HDF5 files are created and opened with #H5Fcreate; existing files are opened with
 * #H5Fopen. Both functions return an object identifier which must eventually be released by calling
 * #H5Fclose.
 *
 * To create a new file, call #H5Fcreate:
 * \code
 *   hid_t H5Fcreate (const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id)
 * \endcode
 *
 * #H5Fcreate creates a new file named name in the current directory. The file is opened with read
 * and write access; if the #H5F_ACC_TRUNC flag is set, any pre-existing file of the same name in
 * the same directory is truncated. If #H5F_ACC_TRUNC is not set or #H5F_ACC_EXCL is set and
 * if a file of the same name exists, #H5Fcreate will fail.
 *
 * The new file is created with the properties specified in the property lists fcpl_id and fapl_id.
 * fcpl is short for file creation property list. fapl is short for file access property list. Specifying
 * #H5P_DEFAULT for either the creation or access property list will use the library's default
 * creation or access properties.
 *
 * If #H5Fcreate successfully creates the file, it returns a file identifier for the new file. This
 * identifier will be used by the application any time an object identifier, an OID, for the file is
 * required. Once the application has finished working with a file, the identifier should be released
 * and the file closed with #H5Fclose.
 *
 * To open an existing file, call #H5Fopen:
 * \code
 *   hid_t H5Fopen (const char *name, unsigned flags, hid_t fapl_id)
 * \endcode
 *
 * #H5Fopen opens an existing file with read-write access if #H5F_ACC_RDWR is set and read-only
 * access if #H5F_ACC_RDONLY is set.
 *
 * fapl_id is the file access property list identifier. Alternatively, #H5P_DEFAULT indicates that the
 * application relies on the default I/O access parameters. Creating and changing access property
 * lists is documented further below.
 *
 * A file can be opened more than once via multiple #H5Fopen calls. Each such call returns a unique
 * file identifier and the file can be accessed through any of these file identifiers as long as they
 * remain valid. Each of these file identifiers must be released by calling #H5Fclose when it is no
 * longer needed.
 *
 * For more information, @see @ref subsubsec_file_property_lists_access.
 * For more information, @see @ref subsec_file_property_lists.
 *
 * \subsection subsec_file_closes Closing an HDF5 File
 * #H5Fclose both closes a file and releases the file identifier returned by #H5Fopen or #H5Fcreate.
 * #H5Fclose must be called when an application is done working with a file; while the HDF5
 * Library makes every effort to maintain file integrity, failure to call #H5Fclose may result in the
 * file being abandoned in an incomplete or corrupted state.
 *
 * To close a file, call #H5Fclose:
 * \code
 *   herr_t H5Fclose (hid_t file_id)
 * \endcode
 * This function releases resources associated with an open file. After closing a file, the file
 * identifier, file_id, cannot be used again as it will be undefined.
 *
 * #H5Fclose fulfills three purposes: to ensure that the file is left in an uncorrupted state, to ensure
 * that all data has been written to the file, and to release resources. Use #H5Fflush if you wish to
 * ensure that all data has been written to the file but it is premature to close it.
 *
 * Note regarding serial mode behavior: When #H5Fclose is called in serial mode, it closes the file
 * and terminates new access to it, but it does not terminate access to objects that remain
 * individually open within the file. That is, if #H5Fclose is called for a file but one or more objects
 * within the file remain open, those objects will remain accessible until they are individually
 * closed. To illustrate, assume that a file, fileA, contains a dataset, data_setA, and that both are
 * open when #H5Fclose is called for fileA. data_setA will remain open and accessible, including
 * writable, until it is explicitly closed. The file will be automatically and finally closed once all
 * objects within it have been closed.
 *
 * Note regarding parallel mode behavior: Once #H5Fclose has been called in parallel mode, access
 * is no longer available to any object within the file.
 *
 * \subsection subsec_file_property_lists File Property Lists
 * Additional information regarding file structure and access are passed to #H5Fcreate and
 * #H5Fopen through property list objects. Property lists provide a portable and extensible method of
 * modifying file properties via simple API functions. There are two kinds of file-related property
 * lists:
 * \li File creation property lists
 * \li File access property lists
 *
 * In the following sub-sections, we discuss only one file creation property, userblock size, in detail
 * as a model for the user. Other file creation and file access properties are mentioned and defined
 * briefly, but the model is not expanded for each; complete syntax, parameter, and usage
 * information for every property list function is provided in the \ref H5P
 * section of the HDF5 Reference Manual.
 *
 * For more information, @see @ref sec_plist.
 *
 * \subsubsection subsubsec_file_property_lists_create Creating a Property List
 * If you do not wish to rely on the default file creation and access properties, you must first create
 * a property list with #H5Pcreate.
 * \code
 *   hid_t H5Pcreate (hid_t cls_id)
 * \endcode
 * cls_id is the type of property list being created. In this case, the appropriate values are
 * #H5P_FILE_CREATE for a file creation property list and #H5P_FILE_ACCESS for a file access
 * property list.
 *
 * Thus, the following calls create a file creation property list and a file access property list with
 * identifiers fcpl_id and fapl_id, respectively:
 * \code
 *   fcpl_id = H5Pcreate (H5P_FILE_CREATE)
 *   fapl_id = H5Pcreate (H5P_FILE_ACCESS)
 * \endcode
 *
 * Once the property lists have been created, the properties themselves can be modified via the
 * functions described in the following sub-sections.
 *
 * \subsubsection subsubsec_file_property_lists_props File Creation Properties
 * File creation property lists control the file metadata, which is maintained in the superblock of the
 * file. These properties are used only when a file is first created.
 *
 * <h4>Userblock Size</h4>
 * \code
 *   herr_t H5Pset_userblock (hid_t plist, hsize_t size)
 *   herr_t H5Pget_userblock (hid_t plist, hsize_t *size)
 * \endcode
 *
 * The userblock is a fixed-length block of data located at the beginning of the file and is ignored
 * by the HDF5 library. This block is specifically set aside for any data or information that
 * developers determine to be useful to their applications but that will not be used by the HDF5
 * library. The size of the userblock is defined in bytes and may be set to any power of two with a
 * minimum size of 512 bytes. In other words, userblocks might be 512, 1024, or 2048 bytes in
 * size.
 *
 * This property is set with #H5Pset_userblock and queried via #H5Pget_userblock. For example, if
 * an application needed a 4K userblock, then the following function call could be used:
 * \code
 *   status = H5Pset_userblock(fcpl_id, 4096)
 * \endcode
 *
 * The property list could later be queried with:
 * \code
 *   status = H5Pget_userblock(fcpl_id, size)
 * \endcode
 * and the value 4096 would be returned in the parameter size.
 *
 * Other properties, described below, are set and queried in exactly the same manner. Syntax and
 * usage are detailed in the @ref H5P section of the HDF5 Reference Manual.
 *
 * <h4>Offset and Length Sizes</h4>
 * This property specifies the number of bytes used to store the offset and length of objects in the
 * HDF5 file. Values of 2, 4, and 8 bytes are currently supported to accommodate 16-bit, 32-bit,
 * and 64-bit file address spaces.
 *
 * These properties are set and queried via #H5Pset_sizes and #H5Pget_sizes.
 *
 * <h4>Symbol Table Parameters</h4>
 * The size of symbol table B-trees can be controlled by setting the 1/2-rank and 1/2-node size
 * parameters of the B-tree.
 *
 * These properties are set and queried via #H5Pset_sym_k and #H5Pget_sym_k
 *
 * <h4>Indexed Storage Parameters</h4>
 * The size of indexed storage B-trees can be controlled by setting the 1/2-rank and 1/2-node size
 * parameters of the B-tree.
 *
 * These properties are set and queried via #H5Pset_istore_k and #H5Pget_istore_k.
 *
 * <h4>Version Information</h4>
 * Various objects in an HDF5 file may over time appear in different versions. The HDF5 Library
 * keeps track of the version of each object in the file.
 *
 * Version information is retrieved via #H5Pget_version.
 *
 * \subsubsection subsubsec_file_property_lists_access File Access Properties
 * This section discusses file access properties that are not related to the low-level file drivers. File
 * drivers are discussed separately later in this chapter.
 * For more information, @see @ref subsec_file_alternate_drivers.
 *
 * File access property lists control various aspects of file I/O and structure.
 *
 * <h4>Data Alignment</h4>
 * Sometimes file access is faster if certain data elements are aligned in a specific manner. This can
 * be controlled by setting alignment properties via the #H5Pset_alignment function. There are two
 * values involved:
 * \li A threshold value
 * \li An alignment interval
 *
 * Any allocation request at least as large as the threshold will be aligned on an address that is a
 * multiple of the alignment interval.
 *
 * <h4>Metadata Block Allocation Size</h4>
 * Metadata typically exists as very small chunks of data; storing metadata elements in a file
 * without blocking them can result in hundreds or thousands of very small data elements in the
 * file. This can result in a highly fragmented file and seriously impede I/O. By blocking metadata
 * elements, these small elements can be grouped in larger sets, thus alleviating both problems.
 *
 * #H5Pset_meta_block_size sets the minimum size in bytes of metadata block allocations.
 * #H5Pget_meta_block_size retrieves the current minimum metadata block allocation size.
 *
 * <h4>Metadata Cache</h4>
 * Metadata and raw data I/O speed are often governed by the size and frequency of disk reads and
 * writes. In many cases, the speed can be substantially improved by the use of an appropriate
 * cache.
 *
 * #H5Pset_cache sets the minimum cache size for both metadata and raw data and a preemption
 * value for raw data chunks. #H5Pget_cache retrieves the current values.
 *
 * <h4>Data Sieve Buffer Size</h4>
 * Data sieve buffering is used by certain file drivers to speed data I/O and is most commonly when
 * working with dataset hyperslabs. For example, using a buffer large enough to hold several pieces
 * of a dataset as it is read in for hyperslab selections will boost performance noticeably.
 *
 * #H5Pset_sieve_buf_size sets the maximum size in bytes of the data sieve buffer.
 * #H5Pget_sieve_buf_size retrieves the current maximum size of the data sieve buffer.
 *
 * <h4>Garbage Collection References</h4>
 * Dataset region references and other reference types use space in an HDF5 file's global heap. If
 * garbage collection is on (1) and the user passes in an uninitialized value in a reference structure,
 * the heap might become corrupted. When garbage collection is off (0), however, and the user reuses
 * a reference, the previous heap block will be orphaned and not returned to the free heap
 * space. When garbage collection is on, the user must initialize the reference structures to 0 or risk
 * heap corruption.
 *
 * #H5Pset_gc_references sets the garbage collecting references flag.
 *
 * \subsection subsec_file_alternate_drivers Alternate File Storage Layouts and Low-level File Drivers
 * The concept of an HDF5 file is actually rather abstract: the address space for what is normally
 * thought of as an HDF5 file might correspond to any of the following:
 * \li Single file on standard file system
 * \li Multiple files on standard file system
 * \li Multiple files on parallel file system
 * \li Block of memory within application's memory space
 * \li More abstract situations such as virtual files
 *
 * This HDF5 address space is generally referred to as an HDF5 file regardless of its organization at
 * the storage level.
 *
 * HDF5 employs an extremely flexible mechanism called the virtual file layer, or VFL, for file
 * I/O. A full understanding of the VFL is only necessary if you plan to write your own drivers
 * @see \ref VFL in the HDF5 Technical Notes.
 *
 * For our
 * purposes here, it is sufficient to know that the low-level drivers used for file I/O reside in the
 * VFL, as illustrated in the following figure. Note that H5FD_STREAM is not available with 1.8.x
 * and later versions of the library.
 *
 * <table>
 * <tr>
 * <td>
 * \image html VFL_Drivers.gif "I/O path from application to VFL and low-level drivers to storage"
 * </td>
 * </tr>
 * </table>
 *
 * As mentioned above, HDF5 applications access HDF5 files through various low-level file
 * drivers. The default driver for that layout is the POSIX driver (also known as the SEC2 driver),
 * #H5FD_SEC2. Alternative layouts and drivers are designed to suit the needs of a variety of
 * systems, environments, and applications. The drivers are listed in the table below.
 *
 * \snippet{doc} tables/fileDriverLists.dox supported_file_driver_table
 *
 * For more information, see the HDF5 Reference Manual entries for the function calls shown in
 * the column on the right in the table above.
 *
 * Note that the low-level file drivers manage alternative file storage layouts. Dataset storage
 * layouts (chunking, compression, and external dataset storage) are managed independently of file
 * storage layouts.
 *
 * If an application requires a special-purpose low-level driver, the VFL provides a public API for
 * creating one. For more information on how to create a driver,
 * @see @ref VFL in the HDF5 Technical Notes.
 *
 * \subsubsection subsubsec_file_alternate_drivers_id Identifying the Previously‐used File Driver
 * When creating a new HDF5 file, no history exists, so the file driver must be specified if it is to be
 * other than the default.
 *
 * When opening existing files, however, the application may need to determine which low-level
 * driver was used to create the file. The function #H5Pget_driver is used for this purpose. See the
 * example below.
 *
 * <em>Identifying a driver</em>
 * \code
 *   hid_t H5Pget_driver (hid_t fapl_id)
 * \endcode
 *
 * #H5Pget_driver returns a constant identifying the low-level driver for the access property list
 * fapl_id. For example, if the file was created with the POSIX (aka SEC2) driver,
 * #H5Pget_driver returns #H5FD_SEC2.
 *
 * If the application opens an HDF5 file without both determining the driver used to create the file
 * and setting up the use of that driver, the HDF5 Library will examine the superblock and the
 * driver definition block to identify the driver.
 * See the <a href="https://docs.hdfgroup.org/hdf5/develop/_s_p_e_c.html">HDF5 File Format Specification</a>
 * for detailed descriptions of the superblock and the driver definition block.
 *
 * \subsubsection subsubsec_file_alternate_drivers_sec2 The POSIX (aka SEC2) Driver
 * The POSIX driver, #H5FD_SEC2, uses functions from section 2 of the POSIX manual to access
 * unbuffered files stored on a local file system. This driver is also known as the SEC2 driver. The
 * HDF5 Library buffers metadata regardless of the low-level driver, but using this driver prevents
 * data from being buffered again by the lowest layers of the library.
 *
 * The function #H5Pset_fapl_sec2 sets the file access properties to use the POSIX driver. See the
 * example below.
 *
 * <em>Using the POSIX, aka SEC2, driver</em>
 * \code
 *   herr_t H5Pset_fapl_sec2 (hid_t fapl_id)
 * \endcode
 *
 * Any previously-defined driver properties are erased from the property list.
 *
 * Additional parameters may be added to this function in the future. Since there are no additional
 * variable settings associated with the POSIX driver, there is no H5Pget_fapl_sec2 function.
 *
 * \subsubsection subsubsec_file_alternate_drivers_direct The Direct Driver
 * The Direct driver, #H5FD_DIRECT, functions like the POSIX driver except that data is written to
 * or read from the file synchronously without being cached by the system.
 *
 * The functions #H5Pset_fapl_direct and #H5Pget_fapl_direct are used to manage file access properties.
 * See the example below.
 *
 * <em>Using the Direct driver</em>
 * \code
 *   herr_t H5Pset_fapl_direct(hid_t fapl_id, size_t alignment, size_t block_size, size_t cbuf_size)
 *   herr_t H5Pget_fapl_direct(hid_t fapl_id, size_t *alignment, size_t *block_size, size_t *cbuf_size)
 * \endcode
 *
 * #H5Pset_fapl_direct sets the file access properties to use the Direct driver; any previously defined
 * driver properties are erased from the property list. #H5Pget_fapl_direct retrieves the file access
 * properties used with the Direct driver. fapl_id is the file access property list identifier.
 * alignment is the memory alignment boundary. block_size is the file system block size.
 * cbuf_size is the copy buffer size.
 *
 * Additional parameters may be added to this function in the future.
 *
 * \subsubsection subsubsec_file_alternate_drivers_log The Log Driver
 * The Log driver, #H5FD_LOG, is designed for situations where it is necessary to log file access
 * activity.
 *
 * The function #H5Pset_fapl_log is used to manage logging properties. See the example below.
 *
 * <em>Logging file access</em>
 * \code
 *   herr_t H5Pset_fapl_log (hid_t fapl_id, const char *logfile, unsigned int flags, size_t buf_size)
 * \endcode
 *
 * #H5Pset_fapl_log sets the file access property list to use the Log driver. File access characteristics
 * are identical to access via the POSIX driver. Any previously defined driver properties are erased
 * from the property list.
 *
 * Log records are written to the file logfile.
 *
 * The logging levels set with the verbosity parameter are shown in the table below.
 *
 * <table>
 * <caption>Logging levels</caption>
 * <tr>
 * <th>Level</th>
 * <th>Comments</th>
 * </tr>
 * <tr>
 * <td>0</td>
 * <td>Performs no logging.</td>
 * </tr>
 * <tr>
 * <td>1</td>
 * <td>Records where writes and reads occur in the file.</td>
 * </tr>
 * <tr>
 * <td>2</td>
 * <td>Records where writes and reads occur in the file and what kind of data is written
 * at each location. This includes raw data or any of several types of metadata
 * (object headers, superblock, B-tree data, local headers, or global headers).</td>
 * </tr>
 * </table>
 *
 * There is no H5Pget_fapl_log function.
 *
 * Additional parameters may be added to this function in the future.
 *
 * \subsubsection subsubsec_file_alternate_drivers_win The Windows Driver
 * The Windows driver, #H5FD_WINDOWS, was modified in HDF5-1.8.8 to be a wrapper of the
 * POSIX driver, #H5FD_SEC2. In other words, if the Windows drivers is used, any file I/O will
 * instead use the functionality of the POSIX driver. This change should be transparent to all user
 * applications. The Windows driver used to be the default driver for Windows systems. The
 * POSIX driver is now the default.
 *
 * The function #H5Pset_fapl_windows sets the file access properties to use the Windows driver.
 * See the example below.
 *
 * <em>Using the Windows driver</em>
 * \code
 *   herr_t H5Pset_fapl_windows (hid_t fapl_id)
 * \endcode
 *
 * Any previously-defined driver properties are erased from the property list.
 *
 * Additional parameters may be added to this function in the future. Since there are no additional
 * variable settings associated with the POSIX driver, there is no H5Pget_fapl_windows function.
 *
 * \subsubsection subsubsec_file_alternate_drivers_stdio The STDIO Driver
 * The STDIO driver, #H5FD_STDIO, accesses permanent files in a local file system like the
 * POSIX driver does. The STDIO driver also has an additional layer of buffering beneath the
 * HDF5 Library.
 *
 * The function #H5Pset_fapl_stdio sets the file access properties to use the STDIO driver. See the
 * example below.
 *
 * <em>Using the STDIO driver</em>
 * \code
 *   herr_t H5Pset_fapl_stdio (hid_t fapl_id)
 * \endcode
 *
 * Any previously defined driver properties are erased from the property list.
 *
 * Additional parameters may be added to this function in the future. Since there are no additional
 * variable settings associated with the STDIO driver, there is no H5Pget_fapl_stdio function.
 *
 * \subsubsection subsubsec_file_alternate_drivers_mem The Memory (aka Core) Driver
 * There are several situations in which it is reasonable, sometimes even required, to maintain a file
 * entirely in system memory. You might want to do so if, for example, either of the following
 * conditions apply:
 * <ul><li>Performance requirements are so stringent that disk latency is a limiting factor</li>
 * <li>You are working with small, temporary files that will not be retained and, thus,
 * need not be written to storage media</li></ul>
 *
 * The Memory driver, #H5FD_CORE, provides a mechanism for creating and managing such in memory files.
 * The functions #H5Pset_fapl_core and #H5Pget_fapl_core manage file access
 * properties. See the example below.
 *
 * <em>Managing file access for in-memory files</em>
 * \code
 *   herr_t H5Pset_fapl_core (hid_t access_properties, size_t block_size, bool backing_store)
 *   herr_t H5Pget_fapl_core (hid_t access_properties, size_t *block_size), bool *backing_store)
 * \endcode
 *
 * #H5Pset_fapl_core sets the file access property list to use the Memory driver; any previously
 * defined driver properties are erased from the property list.
 *
 * Memory for the file will always be allocated in units of the specified block_size.
 *
 * The backing_store Boolean flag is set when the in-memory file is created.
 * backing_store indicates whether to write the file contents to disk when the file is closed. If
 * backing_store is set to 1 (true), the file contents are flushed to a file with the same name as the
 * in-memory file when the file is closed or access to the file is terminated in memory. If
 * backing_store is set to 0 (false), the file is not saved.
 *
 * The application is allowed to open an existing file with the #H5FD_CORE driver. While using
 * #H5Fopen to open an existing file, if backing_store is set to 1 and the flag for #H5Fopen is set to
 * #H5F_ACC_RDWR, changes to the file contents will be saved to the file when the file is closed.
 * If backing_store is set to 0 and the flag for #H5Fopen is set to #H5F_ACC_RDWR, changes to the
 * file contents will be lost when the file is closed. If the flag for #H5Fopen is set to
 * #H5F_ACC_RDONLY, no change to the file will be allowed either in memory or on file.
 *
 * If the file access property list is set to use the Memory driver, #H5Pget_fapl_core will return
 * block_size and backing_store with the relevant file access property settings.
 *
 * Note the following important points regarding in-memory files:
 * <ul><li>Local temporary files are created and accessed directly from memory without ever
 *  being written to disk</li>
 * <li>Total file size must not exceed the available virtual memory</li>
 * <li>Only one HDF5 file identifier can be opened for the file, the identifier returned by
 *  #H5Fcreate or #H5Fopen</li>
 * <li>The changes to the file will be discarded when access is terminated unless
 *  backing_store is set to 1</li></ul>
 *
 * Additional parameters may be added to these functions in the future.
 *
 * @see <a href="https://portal.hdfgroup.org/display/HDF5/HDF5+File+Image+Operations">
 * HDF5 File Image Operations</a>
 * section for information on more advanced usage of the Memory file driver, and
 * @see <a href="http://www.hdfgroup.org/HDF5/doc/Advanced/ModifiedRegionWrites/ModifiedRegionWrites.pdf">
 * Modified Region Writes</a>
 * section for information on how to set write operations so that only modified regions are written
 * to storage.
 *
 * \subsubsection subsubsec_file_alternate_drivers_family The Family Driver
 * HDF5 files can become quite large, and this can create problems on systems that do not support
 * files larger than 2 gigabytes. The HDF5 file family mechanism is designed to solve the problems
 * this creates by splitting the HDF5 file address space across several smaller files. This structure
 * does not affect how metadata and raw data are stored: they are mixed in the address space just as
 * they would be in a single, contiguous file.
 *
 * HDF5 applications access a family of files via the Family driver, #H5FD_FAMILY. The
 * functions #H5Pset_fapl_family and #H5Pget_fapl_family are used to manage file family
 * properties. See the example below.
 *
 * <em>Managing file family properties</em>
 * \code
 *   herr_t H5Pset_fapl_family (hid_t fapl_id,
 *   hsize_t memb_size, hid_t member_properties)
 *   herr_t H5Pget_fapl_family (hid_t fapl_id,
 *   hsize_t *memb_size, hid_t *member_properties)
 * \endcode
 *
 * Each member of the family is the same logical size though the size and disk storage reported by
 * file system listing tools may be substantially smaller. Examples of file system listing tools are
 * \code
 *  ls -l
 * \endcode
 * on a Unix system or the detailed folder listing on an Apple or Microsoft Windows
 * system. The name passed to #H5Fcreate or #H5Fopen should include a printf(3c)-style integer
 * format specifier which will be replaced with the family member number. The first family
 * member is numbered zero (0).
 *
 * #H5Pset_fapl_family sets the access properties to use the Family driver; any previously defined
 * driver properties are erased from the property list. member_properties will serve as the file
 * access property list for each member of the file family. memb_size specifies the logical size, in
 * bytes, of each family member. memb_size is used only when creating a new file or truncating an
 * existing file; otherwise the member size is determined by the size of the first member of the
 * family being opened. Note: If the size of the off_t type is four bytes, the maximum family
 * member size is usually 2^31-1 because the byte at offset 2,147,483,647 is generally inaccessible.
 *
 * #H5Pget_fapl_family is used to retrieve file family properties. If the file access property list is set
 * to use the Family driver, member_properties will be returned with a pointer to a copy of the
 * appropriate member access property list. If memb_size is non-null, it will contain the logical
 * size, in bytes, of family members.
 *
 * Additional parameters may be added to these functions in the future.
 *
 * <h4>Unix Tools and an HDF5 Utility</h4>
 * It occasionally becomes necessary to repartition a file family. A command-line utility for this
 * purpose, h5repart, is distributed with the HDF5 library.
 *
 * \code
 * h5repart [-v] [-b block_size[suffix]] [-m member_size[suffix]] source destination
 * \endcode
 *
 * h5repart repartitions an HDF5 file by copying the source file or file family to the destination file
 * or file family, preserving holes in the underlying UNIX files. Families are used for the source
 * and/or destination if the name includes a printf-style integer format such as %d. The -v switch
 * prints input and output file names on the standard error stream for progress monitoring, -b sets
 * the I/O block size (the default is 1KB), and -m sets the output member size if the destination is a
 * family name (the default is 1GB). block_size and member_size may be suffixed with the letters
 * g, m, or k for GB, MB, or KB respectively.
 *
 * The h5repart utility is described on the Tools page of the HDF5 Reference Manual.
 *
 * An existing HDF5 file can be split into a family of files by running the file through split(1) on a
 * UNIX system and numbering the output files. However, the HDF5 Library is lazy about
 * extending the size of family members, so a valid file cannot generally be created by
 * concatenation of the family members.
 *
 * Splitting the file and rejoining the segments by concatenation (split(1) and cat(1) on UNIX
 * systems) does not generate files with holes; holes are preserved only through the use of h5repart.
 *
 * \subsubsection subsubsec_file_alternate_drivers_multi The Multi Driver
 * In some circumstances, it is useful to separate metadata from raw data and some types of
 * metadata from other types of metadata. Situations that would benefit from use of the Multi driver
 * include the following:
 * <ul><li>In networked situations where the small metadata files can be kept on local disks but
 * larger raw data files must be stored on remote media</li>
 * <li>In cases where the raw data is extremely large</li>
 * <li>In situations requiring frequent access to metadata held in RAM while the raw data
 * can be efficiently held on disk</li></ul>
 *
 * In either case, access to the metadata is substantially easier with the smaller, and possibly more
 * localized, metadata files. This often results in improved application performance.
 *
 * The Multi driver, #H5FD_MULTI, provides a mechanism for segregating raw data and different
 * types of metadata into multiple files. The functions #H5Pset_fapl_multi and
 * #H5Pget_fapl_multi are used to manage access properties for these multiple files. See the example
 * below.
 *
 * <em>Managing access properties for multiple files</em>
 * \code
 *   herr_t H5Pset_fapl_multi (hid_t fapl_id, const H5FD_mem_t *memb_map, const hid_t *memb_fapl,
 *                             const char * const *memb_name, const haddr_t *memb_addr,
 *                             bool relax)
 *  herr_t H5Pget_fapl_multi (hid_t fapl_id, const H5FD_mem_t *memb_map, const hid_t *memb_fapl,
 *                             const char **memb_name, const haddr_t *memb_addr, bool *relax)
 * \endcode
 *
 * #H5Pset_fapl_multi sets the file access properties to use the Multi driver; any previously defined
 * driver properties are erased from the property list. With the Multi driver invoked, the application
 * will provide a base name to #H5Fopen or #H5Fcreate. The files will be named by that base name as
 * modified by the rule indicated in memb_name. File access will be governed by the file access
 * property list memb_properties.
 *
 * See #H5Pset_fapl_multi and #H5Pget_fapl_multi in the HDF5 Reference Manual for descriptions
 * of these functions and their usage.
 *
 * Additional parameters may be added to these functions in the future.
 *
 * \subsubsection subsubsec_file_alternate_drivers_split The Split Driver
 * The Split driver is a limited case of the Multi driver where only two files are
 * created. One file holds metadata, and the other file holds raw data.
 * The function #H5Pset_fapl_split is used to manage Split file access properties. See the example
 * below.
 *
 * <em>Managing access properties for split files</em>
 * \code
 *   herr_t H5Pset_fapl_split (hid_t access_properties, const char *meta_extension,
 *                             hid_t meta_properties,const char *raw_extension, hid_t raw_properties)
 * \endcode
 *
 * #H5Pset_fapl_split sets the file access properties to use the Split driver; any previously defined
 * driver properties are erased from the property list.
 *
 * With the Split driver invoked, the application will provide a base file name such as file_name to
 * #H5Fcreate or #H5Fopen. The metadata and raw data files in storage will then be named
 * file_name.meta_extension and file_name.raw_extension, respectively. For example, if
 * meta_extension is defined as .meta and raw_extension is defined as .raw, the final filenames will
 * be file_name.meta and file_name.raw.
 *
 * Each file can have its own file access property list. This allows the creative use of other lowlevel
 * file drivers. For instance, the metadata file can be held in RAM and accessed via the
 * Memory driver while the raw data file is stored on disk and accessed via the POSIX driver.
 * Metadata file access will be governed by the file access property list in meta_properties. Raw
 * data file access will be governed by the file access property list in raw_properties.
 *
 * Additional parameters may be added to these functions in the future. Since there are no
 * additional variable settings associated with the Split driver, there is no H5Pget_fapl_split
 * function.
 *
 * \subsubsection subsubsec_file_alternate_drivers_par The Parallel Driver
 * Parallel environments require a parallel low-level driver. HDF5's default driver for parallel
 * systems is called the Parallel driver, #H5FD_MPIO. This driver uses the MPI standard for both
 * communication and file I/O.
 *
 * The functions #H5Pset_fapl_mpio and #H5Pget_fapl_mpio are used to manage file access
 * properties for the #H5FD_MPIO driver. See the example below.
 *
 * <em>Managing parallel file access properties</em>
 * \code
 *   herr_t H5Pset_fapl_mpio (hid_t fapl_id, MPI_Comm comm, MPI_info info)
 *   herr_t H5Pget_fapl_mpio (hid_t fapl_id, MPI_Comm *comm, MPI_info *info)
 * \endcode
 *
 * The file access properties managed by #H5Pset_fapl_mpio and retrieved by
 * #H5Pget_fapl_mpio are the MPI communicator, comm, and the MPI info object, info. comm and
 * info are used for file open. info is an information object much like an HDF5 property list. Both
 * are defined in MPI_FILE_OPEN of MPI-2.
 *
 * The communicator and the info object are saved in the file access property list fapl_id.
 * fapl_id can then be passed to MPI_FILE_OPEN to create and/or open the file.
 *
 * #H5Pset_fapl_mpio and #H5Pget_fapl_mpio are available only in the parallel HDF5 Library and
 * are not collective functions. The Parallel driver is available only in the parallel HDF5 Library.
 *
 * Additional parameters may be added to these functions in the future.
 *
 * \subsection subsec_file_examples Code Examples for Opening and Closing Files
 * \subsubsection subsubsec_file_examples_trunc Example Using the H5F_ACC_TRUNC Flag
 * The following example uses the #H5F_ACC_TRUNC flag when it creates a new file. The default
 * file creation and file access properties are also used. Using #H5F_ACC_TRUNC means the
 * function will look for an existing file with the name specified by the function. In this case, that
 * name is FILE. If the function does not find an existing file, it will create one. If it does find an
 * existing file, it will empty the file in preparation for a new set of data. The identifier for the
 * "new" file will be passed back to the application program.
 * For more information, @see @ref subsec_file_access_modes.
 *
 * <em>Creating a file with default creation and access properties</em>
 * \code
 *   hid_t file; // identifier
 *
 *   // Create a new file using H5F_ACC_TRUNC access, default
 *   // file creation properties, and default file access
 *   // properties.
 *   file = H5Fcreate(FILE, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
 *
 *   // Close the file.
 *   status = H5Fclose(file);
 * \endcode
 *
 * \subsubsection subsubsec_file_examples_props Example with the File Creation Property List
 * The example below shows how to create a file with 64-bit object offsets and lengths.
 *
 * <em>Creating a file with 64-bit offsets</em>
 * \code
 *   hid_t create_plist;
 *   hid_t file_id;
 *
 *   create_plist = H5Pcreate(H5P_FILE_CREATE);
 *   H5Pset_sizes(create_plist, 8, 8);
 *   file_id = H5Fcreate(“test.h5”, H5F_ACC_TRUNC, create_plist, H5P_DEFAULT);
 *   .
 *   .
 *   .
 *
 *   H5Fclose(file_id);
 * \endcode
 *
 * \subsubsection subsubsec_file_examples_access Example with the File Access Property List
 * This example shows how to open an existing file for independent datasets access by MPI parallel
 * I/O:
 *
 * <em>Opening an existing file for parallel I/O</em>
 * \code
 *   hid_t access_plist;
 *   hid_t file_id;
 *
 *   access_plist = H5Pcreate(H5P_FILE_ACCESS);
 *   H5Pset_fapl_mpi(access_plist, MPI_COMM_WORLD, MPI_INFO_NULL);
 *
 *   // H5Fopen must be called collectively
 *   file_id = H5Fopen(“test.h5”, H5F_ACC_RDWR, access_plist);
 *   .
 *   .
 *   .
 *
 *   // H5Fclose must be called collectively
 *   H5Fclose(file_id);
 * \endcode
 *
 * \subsection subsec_file_multiple Working with Multiple HDF5 Files
 * Multiple HDF5 files can be associated so that the files can be worked with as though all the
 * information is in a single HDF5 file. A temporary association can be set up by means of the
 * #H5Fmount function. A permanent association can be set up by means of the external link
 * function #H5Lcreate_external.
 *
 * The purpose of this section is to describe what happens when the #H5Fmount function is used to
 * mount one file on another.
 *
 * When a file is mounted on another, the mounted file is mounted at a group, and the root group of
 * the mounted file takes the place of that group until the mounted file is unmounted or until the
 * files are closed.
 *
 * The figure below shows two files before one is mounted on the other. File1 has two groups and
 * three datasets. The group that is the target of the A link has links, Z and Y, to two of the datasets.
 * The group that is the target of the B link has a link, W, to the other dataset. File2 has three
 * groups and three datasets. The groups in File2 are the targets of the AA, BB, and CC links. The
 * datasets in File2 are the targets of the ZZ, YY, and WW links.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Files_fig3.gif "Two separate files"
 * </td>
 * </tr>
 * </table>
 *
 * The figure below shows the two files after File2 has been mounted File1 at the group that is the
 * target of the B link.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Files_fig4.gif "File2 mounted on File1"
 * </td>
 * </tr>
 * </table>
 *
 * Note: In the figure above, the dataset that is the target of the W link is not shown. That dataset is
 * masked by the mounted file.
 *
 * If a file is mounted on a group that has members, those members are hidden until the mounted
 * file is unmounted. There are two ways around this if you need to work with a group member.
 * One is to mount the file on an empty group. Another is to open the group member before you
 * mount the file. Opening the group member will return an identifier that you can use to locate the
 * group member.
 *
 * The example below shows how #H5Fmount might be used to mount File2 onto File1.
 *
 * <em>Using H5Fmount</em>
 * \code
 *   status = H5Fmount(loc_id, "/B", child_id, plist_id)
 * \endcode
 *
 * Note: In the code example above, loc_id is the file identifier for File1, /B is the link path to the
 * group where File2 is mounted, child_id is the file identifier for File2, and plist_id is a property
 * list identifier.
 * For more information, @see @ref sec_group.
 *
 * See the entries for #H5Fmount, #H5Funmount, and #H5Lcreate_external in the HDF5 Reference Manual.
 *
 * Previous Chapter \ref sec_program - Next Chapter \ref sec_group
 *
 */

/**
 * \defgroup H5F Files (H5F)
 *
 * Use the functions in this module to manage HDF5 files.
 *
 * In the code snippets below, we show the skeletal life cycle of an HDF5 file,
 * when creating a new file (left) or when opening an existing file (right).
 * File creation is essentially controlled through \ref FCPL, and file access to
 * new and existing files is controlled through \ref FAPL. The file \c name and
 * creation or access \c mode control the interaction with the underlying
 * storage such as file systems.
 *
 * <table>
 * <tr><th>Create</th><th>Read</th></tr>
 * <tr valign="top">
 *   <td>
 *   \snippet{lineno} H5F_examples.c create
 *   </td>
 *   <td>
 *   \snippet{lineno} H5F_examples.c read
 *   </td>
 * </tr>
 * <tr><th>Update</th><th>Delete</th></tr>
 * <tr valign="top">
 *   <td>
 *   \snippet{lineno} H5F_examples.c update
 *   </td>
 *   <td>
 *   \snippet{lineno} H5F_examples.c delete
 *   </td>
 * </tr>
 * </table>
 *
 * In addition to general file management functions, there are three categories
 * of functions that deal with advanced file management tasks and use cases:
 * 1. The control of the HDF5 \ref MDC
 * 2. The use of (MPI-) \ref PH5F HDF5
 * 3. The \ref SWMR pattern
 *
 * \defgroup MDC Metadata Cache
 * \ingroup H5F
 * \defgroup PH5F Parallel
 * \ingroup H5F
 * \defgroup SWMR Single Writer Multiple Readers
 * \ingroup H5F
 */

#endif /* H5Fmodule_H */
