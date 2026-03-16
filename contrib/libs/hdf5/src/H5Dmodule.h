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
 *          H5D package.  Including this header means that the source file
 *          is part of the H5D package.
 */
#ifndef H5Dmodule_H
#define H5Dmodule_H

/* Define the proper control macros for the generic FUNC_ENTER/LEAVE and error
 *      reporting macros.
 */
#define H5D_MODULE
#define H5_MY_PKG     H5D
#define H5_MY_PKG_ERR H5E_DATASET

/** \page H5D_UG HDF5 Datasets
 *
 * \section sec_dataset HDF5 Datasets
 *
 * \subsection subsec_dataset_intro Introduction
 *
 * An HDF5 dataset is an object composed of a collection of data elements, or raw data, and
 * metadata that stores a description of the data elements, data layout, and all other information
 * necessary to write, read, and interpret the stored data. From the viewpoint of the application the
 * raw data is stored as a one-dimensional or multi-dimensional array of elements (the raw data),
 * those elements can be any of several numerical or character types, small arrays, or even
 * compound types similar to C structs. The dataset object may have attribute objects. See the
 * figure below.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Dsets_fig1.gif "Application view of a dataset"
 * </td>
 * </tr>
 * </table>
 *
 * A dataset object is stored in a file in two parts: a header and a data array. The header contains
 * information that is needed to interpret the array portion of the dataset, as well as metadata (or
 * pointers to metadata) that describes or annotates the dataset. Header information includes the
 * name of the object, its dimensionality, its number-type, information about how the data itself is
 * stored on disk (the storage layout), and other information used by the library to speed up access
 * to the dataset or maintain the file's integrity.
 *
 * The HDF5 dataset interface, comprising the @ref H5D functions, provides a mechanism for managing
 * HDF5 datasets including the transfer of data between memory and disk and the description of
 * dataset properties.
 *
 * A dataset is used by other HDF5 APIs, either by name or by an identifier. For more information,
 * \see \ref api-compat-macros.
 *
 * \subsubsection subsubsec_dataset_intro_link Link/Unlink
 * A dataset can be added to a group with one of the H5Lcreate calls, and deleted from a group with
 * #H5Ldelete. The link and unlink operations use the name of an object, which may be a dataset.
 * The dataset does not have to open to be linked or unlinked.
 *
 * \subsubsection subsubsec_dataset_intro_obj Object Reference
 * A dataset may be the target of an object reference. The object reference is created by
 * #H5Rcreate with the name of an object which may be a dataset and the reference type
 * #H5R_OBJECT. The dataset does not have to be open to create a reference to it.
 *
 * An object reference may also refer to a region (selection) of a dataset. The reference is created
 * with #H5Rcreate and a reference type of #H5R_DATASET_REGION.
 *
 * An object reference can be accessed by a call to #H5Rdereference. When the reference is to a
 * dataset or dataset region, the #H5Rdereference call returns an identifier to the dataset just as if
 * #H5Dopen has been called.
 *
 * \subsubsection subsubsec_dataset_intro_attr Adding Attributes
 * A dataset may have user-defined attributes which are created with #H5Acreate and accessed
 * through the @ref H5A API. To create an attribute for a dataset, the dataset must be open, and the
 * identifier is passed to #H5Acreate. The attributes of a dataset are discovered and opened using
 * #H5Aopen_name, #H5Aopen_idx, or #H5Aiterate; these functions use the identifier of the dataset.
 * An attribute can be deleted with #H5Adelete which also uses the identifier of the dataset.
 *
 * \subsection subsec_dataset_function Dataset Function Summaries
 * Functions that can be used with datasets (@ref H5D functions) and property list functions that can
 * used with datasets (@ref H5P functions) are listed below.
 *
 * <table>
 * <caption>Dataset functions</caption>
 * <tr>
 * <th>Function</th>
 * <th>Purpose</th>
 * </tr>
 * <tr>
 * <td>#H5Dcreate</td>
 * <td>Creates a dataset at the specified location. The
 * C function is a macro: \see \ref api-compat-macros.</td>
 * </tr>
 * <tr>
 * <td>#H5Dcreate_anon</td>
 * <td>Creates a dataset in a file without linking it into the file structure.</td>
 * </tr>
 * <tr>
 * <td>#H5Dopen</td>
 * <td>Opens an existing dataset. The C function is a macro: \see \ref api-compat-macros.</td>
 * </tr>
 * <tr>
 * <td>#H5Dclose</td>
 * <td>Closes the specified dataset.</td>
 * </tr>
 * <tr>
 * <td>#H5Dget_space</td>
 * <td>Returns an identifier for a copy of the dataspace for a dataset.</td>
 * </tr>
 * <tr>
 * <td>#H5Dget_space_status</td>
 * <td>Determines whether space has been allocated for a dataset.</td>
 * </tr>
 * <tr>
 * <td>#H5Dget_type</td>
 * <td>Returns an identifier for a copy of the datatype for a dataset.</td>
 * </tr>
 * <tr>
 * <td>#H5Dget_create_plist</td>
 * <td>Returns an identifier for a copy of the dataset creation property list for a dataset.</td>
 * </tr>
 * <tr>
 * <td>#H5Dget_access_plist</td>
 * <td>Returns the dataset access property list associated with a dataset.</td>
 * </tr>
 * <tr>
 * <td>#H5Dget_offset</td>
 * <td>Returns the dataset address in a file.</td>
 * </tr>
 * <tr>
 * <td>#H5Dget_storage_size</td>
 * <td>Returns the amount of storage required for a dataset.</td>
 * </tr>
 * <tr>
 * <td>#H5Dvlen_get_buf_size</td>
 * <td>Determines the number of bytes required to store variable-length (VL) data.</td>
 * </tr>
 * <tr>
 * <td>#H5Dvlen_reclaim</td>
 * <td>Reclaims VL datatype memory buffers.</td>
 * </tr>
 * <tr>
 * <td>#H5Dread</td>
 * <td>Reads raw data from a dataset into a buffer.</td>
 * </tr>
 * <tr>
 * <td>#H5Dwrite</td>
 * <td>Writes raw data from a buffer to a dataset.</td>
 * </tr>
 * <tr>
 * <td>#H5Diterate</td>
 * <td>Iterates over all selected elements in a dataspace.</td>
 * </tr>
 * <tr>
 * <td>#H5Dgather</td>
 * <td>Gathers data from a selection within a memory buffer.</td>
 * </tr>
 * <tr>
 * <td>#H5Dscatter</td>
 * <td>Scatters data into a selection within a memory buffer.</td>
 * </tr>
 * <tr>
 * <td>#H5Dfill</td>
 * <td>Fills dataspace elements with a fill value in a memory buffer.</td>
 * </tr>
 * <tr>
 * <td>#H5Dset_extent</td>
 * <td>Changes the sizes of a dataset's dimensions.</td>
 * </tr>
 * </table>
 *
 * \anchor dcpl_table_tag Dataset creation property list functions (H5P)
 * \snippet{doc} tables/propertyLists.dox dcpl_table
 *
 * \anchor dapl_table_tag Dataset access property list functions (H5P)
 * \snippet{doc} tables/propertyLists.dox dapl_table
 *
 * \subsection subsec_dataset_program Programming Model for Datasets
 * This section explains the programming model for datasets.
 *
 * \subsubsection subsubsec_dataset_program_general General Model
 *
 * The programming model for using a dataset has three main phases:
 * \li Obtain access to the dataset
 * \li Operate on the dataset using the dataset identifier returned at access
 * \li Release the dataset
 *
 * These three phases or steps are described in more detail below the figure.
 *
 * A dataset may be opened several times and operations performed with several different
 * identifiers to the same dataset. All the operations affect the dataset although the calling program
 * must synchronize if necessary to serialize accesses.
 *
 * Note that the dataset remains open until every identifier is closed. The figure below shows the
 * basic sequence of operations.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Dsets_fig2.gif "Dataset programming sequence"
 * </td>
 * </tr>
 * </table>
 *
 * Creation and data access operations may have optional parameters which are set with property
 * lists. The general programming model is:
 * \li Create property list of appropriate class (dataset create, dataset transfer)
 * \li Set properties as needed; each type of property has its own format and datatype
 * \li Pass the property list as a parameter of the API call
 *
 * The steps below describe the programming phases or steps for using a dataset.
 * <h4>Step 1. Obtain Access</h4>
 * A new dataset is created by a call to #H5Dcreate. If successful, the call returns an identifier for the
 * newly created dataset.
 *
 * Access to an existing dataset is obtained by a call to #H5Dopen. This call returns an identifier for
 * the existing dataset.
 *
 * An object reference may be dereferenced to obtain an identifier to the dataset it points to.
 *
 * In each of these cases, the successful call returns an identifier to the dataset. The identifier is
 * used in subsequent operations until the dataset is closed.
 *
 * <h4>Step 2. Operate on the Dataset</h4>
 * The dataset identifier can be used to write and read data to the dataset, to query and set
 * properties, and to perform other operations such as adding attributes, linking in groups, and
 * creating references.
 *
 * The dataset identifier can be used for any number of operations until the dataset is closed.
 *
 * <h4>Step 3. Close the Dataset</h4>
 * When all operations are completed, the dataset identifier should be closed with a call to
 * #H5Dclose. This releases the dataset.
 *
 * After the identifier is closed, it cannot be used for further operations.
 *
 * \subsubsection subsubsec_dataset_program_create Create Dataset
 *
 * A dataset is created and initialized with a call to #H5Dcreate. The dataset create operation sets
 * permanent properties of the dataset:
 * \li Name
 * \li Dataspace
 * \li Datatype
 * \li Storage properties
 *
 * These properties cannot be changed for the life of the dataset, although the dataspace may be
 * expanded up to its maximum dimensions.
 *
 * <h4>Name</h4>
 * A dataset name is a sequence of alphanumeric ASCII characters. The full name would include a
 * tracing of the group hierarchy from the root group of the file. An example is
 * /rootGroup/groupA/subgroup23/dataset1. The local name or relative name within the lowest-
 * level group containing the dataset would include none of the group hierarchy. An example is
 * Dataset1.
 *
 * <h4>Dataspace</h4>
 * The dataspace of a dataset defines the number of dimensions and the size of each dimension. The
 * dataspace defines the number of dimensions, and the maximum dimension sizes and current size
 * of each dimension. The maximum dimension size can be a fixed value or the constant
 * #H5S_UNLIMITED, in which case the actual dimension size can be changed with calls to
 * #H5Dset_extent, up to the maximum set with the maxdims parameter in the #H5Screate_simple
 * call that established the dataset's original dimensions. The maximum dimension size is set when
 * the dataset is created and cannot be changed.
 *
 * <h4>Datatype</h4>
 * Raw data has a datatype which describes the layout of the raw data stored in the file. The
 * datatype is set when the dataset is created and can never be changed. When data is transferred to
 * and from the dataset, the HDF5 library will assure that the data is transformed to and from the
 * stored format.
 *
 * <h4>Storage Properties</h4>
 * Storage properties of the dataset are set when it is created. The required inputs table below shows
 * the categories of storage properties. The storage properties cannot be changed after the dataset is
 * created.
 *
 * <h4>Filters</h4>
 * When a dataset is created, optional filters are specified. The filters are added to the data transfer
 * pipeline when data is read or written. The standard library includes filters to implement
 * compression, data shuffling, and error detection code. Additional user-defined filters may also be
 * used.
 *
 * The required filters are stored as part of the dataset, and the list may not be changed after the
 * dataset is created. The HDF5 library automatically applies the filters whenever data is
 * transferred.
 *
 * <h4>Summary</h4>
 *
 * A newly created dataset has no attributes and no data values. The dimensions, datatype, storage
 * properties, and selected filters are set. The table below lists the required inputs, and the second
 * table below lists the optional inputs.
 *
 * <table>
 * <caption>Required inputs</caption>
 * <tr>
 * <th>Required Inputs</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>Dataspace</td>
 * <td>The shape of the array.</td>
 * </tr>
 * <tr>
 * <td>Datatype</td>
 * <td>The layout of the stored elements.</td>
 * </tr>
 * <tr>
 * <td>Name</td>
 * <td>The name of the dataset in the group.</td>
 * </tr>
 * </table>
 *
 * <table>
 * <caption>Optional inputs</caption>
 * <tr>
 * <th>Optional Inputs</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>Storage Layout</td>
 * <td>How the data is organized in the file including chunking.</td>
 * </tr>
 * <tr>
 * <td>Fill Value</td>
 * <td>The behavior and value for uninitialized data.</td>
 * </tr>
 * <tr>
 * <td>External Storage</td>
 * <td>Option to store the raw data in an external file.</td>
 * </tr>
 * <tr>
 * <td>Filters</td>
 * <td>Select optional filters to be applied. One of the filters that might be applied is compression.</td>
 * </tr>
 * </table>
 *
 * <h4>Example</h4>
 * To create a new dataset, go through the following general steps:
 * \li Set dataset characteristics (optional where default settings are acceptable)
 * \li Datatype
 * \li Dataspace
 * \li Dataset creation property list
 * \li Create the dataset
 * \li Close the datatype, dataspace, and property list (as necessary)
 * \li Close the dataset
 *
 * Example 1 below shows example code to create an empty dataset. The dataspace is 7 x 8, and the
 * datatype is a big-endian integer. The dataset is created with the name “dset1” and is a member of
 * the root group, “/”.
 *
 * <em> Example 1. Create an empty dataset</em>
 * \code
 *   hid_t dataset, datatype, dataspace;
 *
 *   // Create dataspace: Describe the size of the array and create the dataspace for fixed-size dataset.
 *   dimsf[0] = 7;
 *   dimsf[1] = 8;
 *   dataspace = H5Screate_simple(2, dimsf, NULL);
 *
 *   // Define datatype for the data in the file.
 *   // For this example, store little-endian integer numbers.
 *   datatype = H5Tcopy(H5T_NATIVE_INT);
 *   status = H5Tset_order(datatype, H5T_ORDER_LE);
 *
 *   // Create a new dataset within the file using defined
 *   // dataspace and datatype. No properties are set.
 *   dataset = H5Dcreate(file, "/dset", datatype, dataspace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
 *   H5Dclose(dataset);
 *   H5Sclose(dataspace);
 *   H5Tclose(datatype);
 * \endcode
 *
 * Example 2, below, shows example code to create a similar dataset with a fill value of ‘-1’. This
 * code has the same steps as in the example above, but uses a non-default property list. A file
 * creation property list is created, and then the fill value is set to the desired value. Then the
 * property list is passed to the #H5Dcreate call.
 *
 * <em> Example 2. Create a dataset with fill value set</em>
 * \code
 *   hid_t plist;  // property list
 *   hid_t dataset, datatype, dataspace;
 *   int   fillval = -1;
 *
 *   dimsf[0] = 7;
 *   dimsf[1] = 8;
 *   dataspace = H5Screate_simple(2, dimsf, NULL);
 *   datatype = H5Tcopy(H5T_NATIVE_INT);
 *   status = H5Tset_order(datatype, H5T_ORDER_LE);
 *
 *   // Example of Dataset Creation property list: set fill value to '-1'
 *   plist = H5Pcreate(H5P_DATASET_CREATE);
 *   status = H5Pset_fill_value(plist, datatype, &fillval);
 *
 *   // Same as above, but use the property list
 *   dataset = H5Dcreate(file, "/dset", datatype, dataspace, H5P_DEFAULT, plist, H5P_DEFAULT);
 *   H5Dclose(dataset);
 *   H5Sclose(dataspace);
 *   H5Tclose(datatype);
 *   H5Pclose(plist);
 * \endcode
 *
 * After this code is executed, the dataset has been created and written to the file. The data array is
 * uninitialized. Depending on the storage strategy and fill value options that have been selected,
 * some or all of the space may be allocated in the file, and fill values may be written in the file.
 *
 * \subsubsection subsubsec_dataset_program_transfer Data Transfer Operations on a Dataset
 * Data is transferred between memory and the raw data array of the dataset through #H5Dwrite and
 * #H5Dread operations. A data transfer has the following basic steps:
 * \li 1. Allocate and initialize memory space as needed
 * \li 2. Define the datatype of the memory elements
 * \li 3. Define the elements to be transferred (a selection, or all the elements)
 * \li 4. Set data transfer properties (including parameters for filters or file drivers) as needed
 * \li 5. Call the @ref H5D API
 *
 * Note that the location of the data in the file, the datatype of the data in the file, the storage
 * properties, and the filters do not need to be specified because these are stored as a permanent part
 * of the dataset. A selection of elements from the dataspace is specified; the selected elements may
 * be the whole dataspace.
 *
 * The following figure shows a diagram of a write operation which
 * transfers a data array from memory to a dataset in the file (usually on disk). A read operation has
 * similar parameters with the data flowing the other direction.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Dsets_fig3.gif "A write operation"
 * </td>
 * </tr>
 * </table>
 *
 * <h4>Memory Space</h4>
 * The calling program must allocate sufficient memory to store the data elements to be transferred.
 * For a write (from memory to the file), the memory must be initialized with the data to be written
 * to the file. For a read, the memory must be large enough to store the elements that will be read.
 * The amount of storage needed can be computed from the memory datatype (which defines the
 * size of each data element) and the number of elements in the selection.
 *
 * <h4>Memory Datatype</h4>
 * The memory layout of a single data element is specified by the memory datatype. This specifies
 * the size, alignment, and byte order of the element as well as the datatype class. Note that the
 * memory datatype must be the same datatype class as the file, but may have different byte order
 * and other properties. The HDF5 Library automatically transforms data elements between the
 * source and destination layouts. For more information, \ref sec_datatype.
 *
 * For a write, the memory datatype defines the layout of the data to be written; an example is IEEE
 * floating-point numbers in native byte order. If the file datatype (defined when the dataset is
 * created) is different but compatible, the HDF5 Library will transform each data element when it
 * is written. For example, if the file byte order is different than the native byte order, the HDF5
 * library will swap the bytes.
 *
 * For a read, the memory datatype defines the desired layout of the data to be read. This must be
 * compatible with the file datatype, but should generally use native formats such as byte orders.
 * The HDF5 library will transform each data element as it is read.
 *
 * <h4>Selection</h4>
 * The data transfer will transfer some or all of the elements of the dataset depending on the
 * dataspace selection. The selection has two dataspace objects: one for the source, and one for the
 * destination. These objects describe which elements of the dataspace to be transferred. Some
 * (partial I/O) or all of the data may be transferred. Partial I/O is defined by defining hyperslabs or
 * lists of elements in a dataspace object.
 *
 * The dataspace selection for the source defines the indices of the elements to be read or written.
 * The two selections must define the same number of points, but the order and layout may be
 * different. The HDF5 Library automatically selects and distributes the elements according to the
 * selections. It might, for example, perform a scatter-gather or sub-set of the data.
 *
 * <h4>Data Transfer Properties</h4>
 * For some data transfers, additional parameters should be set using the transfer property list. The
 * table below lists the categories of transfer properties. These properties set parameters for the
 * HDF5 Library and may be used to pass parameters for optional filters and file drivers. For
 * example, transfer properties are used to select independent or collective operation when using
 * MPI-I/O.
 *
 * <table>
 * <caption>Categories of transfer properties</caption>
 * <tr>
 * <th>Properties</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>Library parameters</td>
 * <td>Internal caches, buffers, B-Trees, etc.</td>
 * </tr>
 * <tr>
 * <td>Memory management</td>
 * <td>Variable-length memory management, data overwrite</td>
 * </tr>
 * <tr>
 * <td>File driver management</td>
 * <td>Parameters for file drivers</td>
 * </tr>
 * <tr>
 * <td>Filter management</td>
 * <td>Parameters for filters</td>
 * </tr>
 * </table>
 *
 * <h4>Data Transfer Operation (Read or Write)</h4>
 * The data transfer is done by calling #H5Dread or #H5Dwrite with the parameters described above.
 * The HDF5 Library constructs the required pipeline, which will scatter-gather, transform
 * datatypes, apply the requested filters, and use the correct file driver.
 *
 * During the data transfer, the transformations and filters are applied to each element of the data in
 * the required order until all the data is transferred.
 *
 * <h4>Summary</h4>
 * To perform a data transfer, it is necessary to allocate and initialize memory, describe the source
 * and destination, set required and optional transfer properties, and call the \ref H5D API.
 *
 * <h4>Examples</h4>
 * The basic procedure to write to a dataset is the following:
 * \li Open the dataset.
 * \li Set the dataset dataspace for the write (optional if dataspace is #H5S_ALL).
 * \li Write data.
 * \li Close the datatype, dataspace, and property list (as necessary).
 * \li Close the dataset.
 *
 * Example 3 below shows example code to write a 4 x 6 array of integers. In the example, the data
 * is initialized in the memory array dset_data. The dataset has already been created in the file, so it
 * is opened with H5Dopen.
 *
 * The data is written with #H5Dwrite. The arguments are the dataset identifier, the memory
 * datatype (#H5T_NATIVE_INT), the memory and file selections (#H5S_ALL in this case: the
 * whole array), and the default (empty) property list. The last argument is the data to be
 * transferred.
 *
 * <em> Example 3. Write an array of integers</em>
 * \code
 *   hid_t file_id, dataset_id; // identifiers
 *   herr_t status;
 *   int i, j, dset_data[4][6];
 *
 *   // Initialize the dataset.
 *   for (i = 0; i < 4; i++)
 *      for (j = 0; j < 6; j++)
 *         dset_data[i][j] = i * 6 + j + 1;
 *
 *   // Open an existing file.
 *   file_id = H5Fopen("dset.h5", H5F_ACC_RDWR, H5P_DEFAULT);
 *
 *   // Open an existing dataset.
 *   dataset_id = H5Dopen(file_id, "/dset", H5P_DEFAULT);
 *
 *   // Write the entire dataset, using 'dset_data': memory type is 'native int'
 *   // write the entire dataspace to the entire dataspace, no transfer properties
 *   status = H5Dwrite(dataset_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, dset_data);
 *
 *   status = H5Dclose(dataset_id);
 * \endcode
 *
 * Example 4 below shows a similar write except for setting a non-default value for the transfer
 * buffer. The code is the same as Example 3, but a transfer property list is created, and the desired
 * buffer size is set. The #H5Dwrite function has the same arguments, but uses the property list to set
 * the buffer.
 *
 * <em> Example 4. Write an array using a property list</em>
 * \code
 *   hid_t file_id, dataset_id;
 *   hid_t xferplist;
 *   herr_t status;
 *   int i, j, dset_data[4][6];
 *
 *   file_id = H5Fopen("dset.h5", H5F_ACC_RDWR, H5P_DEFAULT);
 *   dataset_id = H5Dopen(file_id, "/dset", H5P_DEFAULT);
 *
 *   // Example: set type conversion buffer to 64MB
 *   xferplist = H5Pcreate(H5P_DATASET_XFER);
 *   status = H5Pset_buffer( xferplist, 64 * 1024 *1024, NULL, NULL);
 *
 *   // Write the entire dataset, using 'dset_data': memory type is 'native int'
 *   write the entire dataspace to the entire dataspace, set the buffer size with the property list
 *   status = H5Dwrite(dataset_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, xferplist, dset_data);
 *
 *   status = H5Dclose(dataset_id);
 * \endcode
 *
 * The basic procedure to read from a dataset is the following:
 * \li Define the memory dataspace of the read (optional if dataspace is #H5S_ALL).
 * \li Open the dataset.
 * \li Get the dataset dataspace (if using #H5S_ALL above).
 *
 * Else define dataset dataspace of read.
 * \li Define the memory datatype (optional).
 * \li Define the memory buffer.
 * \li Open the dataset.
 * \li Read data.
 * \li Close the datatype, dataspace, and property list (as necessary).
 * \li Close the dataset.
 *
 * The example below shows code that reads a 4 x 6 array of integers from a dataset called “dset1”.
 * First, the dataset is opened. The #H5Dread call has parameters:
 * \li The dataset identifier (from #H5Dopen)
 * \li The memory datatype (#H5T_NATIVE_INT)
 * \li The memory and file dataspace (#H5S_ALL, the whole array)
 * \li A default (empty) property list
 * \li The memory to be filled
 *
 * <em> Example 5. Read an array from a dataset</em>
 * \code
 *   hid_t file_id, dataset_id;
 *   herr_t status;
 *   int i, j, dset_data[4][6];
 *
 *   // Open an existing file.
 *   file_id = H5Fopen("dset.h5", H5F_ACC_RDWR, H5P_DEFAULT);
 *
 *   // Open an existing dataset.
 *   dataset_id = H5Dopen(file_id, "/dset", H5P_DEFAULT);
 *
 *   // read the entire dataset, into 'dset_data': memory type is 'native int'
 *   // read the entire dataspace to the entire dataspace, no transfer properties,
 *   status = H5Dread(dataset_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, dset_data);
 *
 *   status = H5Dclose(dataset_id);
 * \endcode
 *
 * \subsubsection subsubsec_dataset_program_read Retrieve the Properties of a Dataset
 * The functions listed below allow the user to retrieve information regarding a dataset including
 * the datatype, the dataspace, the dataset creation property list, and the total stored size of the data.
 *
 * <table>
 * <caption>Retrieve dataset information</caption>
 * <tr>
 * <th>Query Function</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>H5Dget_space</td>
 * <td>Retrieve the dataspace of the dataset as stored in the file.</td>
 * </tr>
 * <tr>
 * <td>H5Dget_type</td>
 * <td>Retrieve the datatype of the dataset as stored in the file.</td>
 * </tr>
 * <tr>
 * <td>H5Dget_create_plist</td>
 * <td>Retrieve the dataset creation properties.</td>
 * </tr>
 * <tr>
 * <td>H5Dget_storage_size</td>
 * <td>Retrieve the total bytes for all the data of the dataset.</td>
 * </tr>
 * <tr>
 * <td>H5Dvlen_get_buf_size</td>
 * <td>Retrieve the total bytes for all the variable-length data of the dataset.</td>
 * </tr>
 * </table>
 *
 * The example below illustrates how to retrieve dataset information.
 *
 * <em> Example 6. Retrieve dataset</em>
 * \code
 *   hid_t file_id, dataset_id;
 *   hid_t dspace_id, dtype_id, plist_id;
 *   herr_t status;
 *
 *   // Open an existing file.
 *   file_id = H5Fopen("dset.h5", H5F_ACC_RDWR, H5P_DEFAULT);
 *
 *   // Open an existing dataset.
 *   dataset_id = H5Dopen(file_id, "/dset", H5P_DEFAULT);
 *   dspace_id = H5Dget_space(dataset_id);
 *   dtype_id = H5Dget_type(dataset_id);
 *   plist_id = H5Dget_create_plist(dataset_id);
 *
 *   // use the objects to discover the properties of the dataset
 *   status = H5Dclose(dataset_id);
 * \endcode
 *
 * \subsection subsec_dataset_transfer Data Transfer
 * The HDF5 library implements data transfers through a pipeline which implements data
 * transformations (according to the datatype and selections), chunking (as requested), and I/O
 * operations using different mechanisms (file drivers). The pipeline is automatically configured by
 * the HDF5 library. Metadata is stored in the file so that the correct pipeline can be constructed to
 * retrieve the data. In addition, optional filters such as compression may be added to the standard
 * pipeline.
 *
 * The figure below illustrates data layouts for different layers of an application using HDF5. The
 * application data is organized as a multidimensional array of elements. The HDF5 format
 * specification defines the stored layout of the data and metadata. The storage layout properties
 * define the organization of the abstract data. This data is written to and read from some storage
 * medium.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Dsets_fig4.gif "Data layouts in an application"
 * </td>
 * </tr>
 * </table>
 *
 * The last stage of a write (and first stage of a read) is managed by an HDF5 file driver module.
 * The virtual file layer of the HDF5 Library implements a standard interface to alternative I/O
 * methods, including memory (AKA “core”) files, single serial file I/O, multiple file I/O, and
 * parallel I/O. The file driver maps a simple abstract HDF5 file to the specific access methods.
 *
 * The raw data of an HDF5 dataset is conceived to be a multidimensional array of data elements.
 * This array may be stored in the file according to several storage strategies:
 * \li Contiguous
 * \li Chunked
 * \li Compact
 *
 * The storage strategy does not affect data access methods except that certain operations may be
 * more or less efficient depending on the storage strategy and the access patterns.
 *
 * Overall, the data transfer operations (#H5Dread and #H5Dwrite) work identically for any storage
 * method, for any file driver, and for any filters and transformations. The HDF5 library
 * automatically manages the data transfer process. In some cases, transfer properties should or
 * must be used to pass additional parameters such as MPI/IO directives when using the parallel file
 * driver.
 *
 * \subsubsection subsubsec_dataset_transfer_pipe The Data Pipeline
 * When data is written or read to or from an HDF5 file, the HDF5 library passes the data through a
 * sequence of processing steps which are known as the HDF5 data pipeline. This data pipeline
 * performs operations on the data in memory such as byte swapping, alignment, scatter-gather, and
 * hyperslab selections. The HDF5 library automatically determines which operations are needed
 * and manages the organization of memory operations such as extracting selected elements from a
 * data block. The data pipeline modules operate on data buffers: each module processes a buffer
 * and passes the transformed buffer to the next stage.
 *
 * The table below lists the stages of the data pipeline. The figure below the table shows the order
 * of processing during a read or write.
 *
 * <table>
 * <caption>Stages of the data pipeline</caption>
 * <tr>
 * <th>Layers</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>I/O initiation</td>
 * <td>Initiation of HDF5 I/O activities (#H5Dwrite and #H5Dread) in a user's application program.</td>
 * </tr>
 * <tr>
 * <td>Memory hyperslab operation</td>
 * <td>Data is scattered to (for read), or gathered from (for write) the application's memory buffer
 * (bypassed if no datatype conversion is needed).</td>
 * </tr>
 * <tr>
 * <td>Datatype conversion</td>
 * <td>Datatype is converted if it is different between memory and storage (bypassed if no datatype
 * conversion is needed).</td>
 * </tr>
 * <tr>
 * <td>File hyperslab operation</td>
 * <td>Data is gathered from (for read), or scattered to (for write) to file space in memory (bypassed
 * if no datatype conversion is needed).</td>
 * </tr>
 * <tr>
 * <td>Filter pipeline</td>
 * <td>Data is processed by filters when it passes. Data can be modified and restored here (bypassed
 * if no datatype conversion is needed, no filter is enabled, or dataset is not chunked).</td>
 * </tr>
 * <tr>
 * <td>Virtual File Layer</td>
 * <td>Facilitate easy plug-in file drivers such as MPIO or POSIX I/O.</td>
 * </tr>
 * <tr>
 * <td>Actual I/O</td>
 * <td>Actual file driver used by the library such as MPIO or STDIO.</td>
 * </tr>
 * </table>
 *
 * <table>
 * <tr>
 * <td>
 * \image html Dsets_fig5.gif "The processing order in the data pipeline"
 * </td>
 * </tr>
 * </table>
 *
 * The HDF5 library automatically applies the stages as needed.
 *
 * When the memory dataspace selection is other than the whole dataspace, the memory hyperslab
 * stage scatters/gathers the data elements between the application memory (described by the
 * selection) and a contiguous memory buffer for the pipeline. On a write, this is a gather operation;
 * on a read, this is a scatter operation.
 *
 * When the memory datatype is different from the file datatype, the datatype conversion stage
 * transforms each data element. For example, if data is written from 32-bit big-endian memory,
 * and the file datatype is 32-bit little-endian, the datatype conversion stage will swap the bytes of
 * every element. Similarly, when data is read from the file to native memory, byte swapping will
 * be applied automatically when needed.
 *
 * The file hyperslab stage is similar to the memory hyperslab stage, but is managing the
 * arrangement of the elements according to the dataspace selection. When data is read, data
 * elements are gathered from the data blocks from the file to fill the contiguous buffers which are
 * then processed by the pipeline. When data is read, the elements from a buffer are scattered to the
 * data blocks of the file.
 *
 * \subsubsection subsubsec_dataset_transfer_filter Data Pipeline Filters
 * In addition to the standard pipeline, optional stages, called filters, can be inserted in the pipeline.
 * The standard distribution includes optional filters to implement compression and error checking.
 * User applications may add custom filters as well.
 *
 * The HDF5 library distribution includes or employs several optional filters. These are listed in the
 * table below. The filters are applied in the pipeline between the virtual file layer and the file
 * hyperslab operation. See the figure above. The application can use any number of filters in any
 * order.
 *
 * <table>
 * <caption>Data pipeline filters</caption>
 * <tr>
 * <th>Filter</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>gzip compression</td>
 * <td>Data compression using zlib.</td>
 * </tr>
 * <tr>
 * <td>Szip compression</td>
 * <td>Data compression using the Szip library. See The HDF Group website for more information
 * regarding the Szip filter.</td>
 * </tr>
 * <tr>
 * <td>N-bit compression</td>
 * <td>Data compression using an algorithm specialized for n-bit datatypes.</td>
 * </tr>
 * <tr>
 * <td>Scale-offset compression</td>
 * <td>Data compression using a “scale and offset” algorithm.</td>
 * </tr>
 * <tr>
 * <td>Shuffling</td>
 * <td>To improve compression performance, data is regrouped by its byte position in the data
 * unit. In other words, the 1st, 2nd, 3rd, and 4th bytes of integers are stored together
 * respectively.</td>
 * </tr>
 * <tr>
 * <td>Fletcher32</td>
 * <td>Fletcher32 checksum for error-detection.</td>
 * </tr>
 * </table>
 *
 * Filters may be used only for chunked data and are applied to chunks of data between the file
 * hyperslab stage and the virtual file layer. At this stage in the pipeline, the data is organized as
 * fixed-size blocks of elements, and the filter stage processes each chunk separately.
 *
 * Filters are selected by dataset creation properties, and some behavior may be controlled by data
 * transfer properties. The library determines what filters must be applied and applies them in the
 * order in which they were set by the application. That is, if an application calls
 * #H5Pset_shuffle and then #H5Pset_deflate when creating a dataset's creation property list, the
 * library will apply the shuffle filter first and then the deflate filter.
 *
 * For more information,
 * \li @see @ref subsubsec_dataset_filters_nbit
 * \li @see @ref subsubsec_dataset_filters_scale
 *
 * \subsubsection subsubsec_dataset_transfer_drive File Drivers
 * I/O is performed by the HDF5 virtual file layer. The file driver interface writes and reads blocks
 * of data; each driver module implements the interface using different I/O mechanisms. The table
 * below lists the file drivers currently supported. Note that the I/O mechanisms are separated from
 * the pipeline processing: the pipeline and filter operations are identical no matter what data access
 * mechanism is used.
 *
 * \snippet{doc} tables/propertyLists.dox lcpl_table
 *
 * Each file driver writes/reads contiguous blocks of bytes from a logically contiguous address
 * space. The file driver is responsible for managing the details of the different physical storage
 * methods.
 *
 * In serial environments, everything above the virtual file layer tends to work identically no matter
 * what storage method is used.
 *
 * Some options may have substantially different performance depending on the file driver that is
 * used. In particular, multi-file and parallel I/O may perform considerably differently from serial
 * drivers depending on chunking and other settings.
 *
 * \subsubsection subsubsec_dataset_transfer_props Data Transfer Properties to Manage the Pipeline
 * Data transfer properties set optional parameters that control parts of the data pipeline. The
 * function listing below shows transfer properties that control the behavior of the library.
 *
 * \snippet{doc} tables/fileDriverLists.dox file_driver_table
 *
 * Some filters and file drivers require or use additional parameters from the application program.
 * These can be passed in the data transfer property list. The table below shows file driver property
 * list functions.
 *
 * <table>
 * <caption>File driver property list functions</caption>
 * <tr>
 * <th>C Function</th>
 * <th>Purpose</th>
 * </tr>
 * <tr>
 * <td>#H5Pset_dxpl_mpio</td>
 * <td>Control the MPI I/O transfer mode (independent or collective) during data I/O operations.</td>
 * </tr>
 * <tr>
 * <td>#H5Pset_small_data_block_size</td>
 * <td>Reserves blocks of size bytes for the contiguous storage of the raw data portion of small
 * datasets. The HDF5 Library then writes the raw data from small datasets to this reserved space
 * which reduces unnecessary discontinuities within blocks of metadata and improves
 * I/O performance.</td>
 * </tr>
 * <tr>
 * <td>#H5Pset_edc_check</td>
 * <td>Disable/enable EDC checking for read. When selected, EDC is always written.</td>
 * </tr>
 * </table>
 *
 * The transfer properties are set in a property list which is passed as a parameter of the #H5Dread or
 * #H5Dwrite call. The transfer properties are passed to each pipeline stage. Each stage may use or
 * ignore any property in the list. In short, there is one property list that contains all the properties.
 *
 * \subsubsection subsubsec_dataset_transfer_store Storage Strategies
 * The raw data is conceptually a multi-dimensional array of elements that is stored as a contiguous
 * array of bytes. The data may be physically stored in the file in several ways. The table below lists
 * the storage strategies for a dataset.
 *
 * <table>
 * <caption> Dataset storage strategies</caption>
 * <tr>
 * <th>Storage Strategy</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>Contiguous</td>
 * <td>The dataset is stored as one continuous array of bytes.</td>
 * </tr>
 * <tr>
 * <td>Chunked </td>
 * <td>The dataset is stored as fixed-size chunks.</td>
 * </tr>
 * <tr>
 * <td>Compact</td>
 * <td>A small dataset is stored in the metadata header.</td>
 * </tr>
 * </table>
 *
 * The different storage strategies do not affect the data transfer operations of the dataset: reads and
 * writes work the same for any storage strategy.
 *
 * These strategies are described in the following sections.
 *
 * <h4>Contiguous</h4>
 * A contiguous dataset is stored in the file as a header and a single continuous array of bytes. See
 * the figure below. In the case of a multi-dimensional array, the data is serialized in row major order. By
 * default, data is stored contiguously.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Dsets_fig6.gif "Contiguous data storage"
 * </td>
 * </tr>
 * </table>
 *
 * Contiguous storage is the simplest model. It has several limitations. First, the dataset must be a
 * fixed-size: it is not possible to extend the limit of the dataset or to have unlimited dimensions. In
 * other words, if the number of dimensions of the array might change over time, then chunking
 * storage must be used instead of contiguous. Second, because data is passed through the pipeline
 * as fixed-size blocks, compression and other filters cannot be used with contiguous data.
 *
 * <h4>Chunked</h4>
 * The data of a dataset may be stored as fixed-size chunks. A chunk is a hyper-
 * rectangle of any shape. When a dataset is chunked, each chunk is read or written as a single I/O
 * operation, and individually passed from stage to stage of the data pipeline.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Dsets_fig7.gif "Chunked data storage"
 * </td>
 * </tr>
 * </table>
 *
 * Chunks may be any size and shape that fits in the dataspace of the dataset. For example, a three
 * dimensional dataspace can be chunked as 3-D cubes, 2-D planes, or 1-D lines. The chunks may
 * extend beyond the size of the dataspace. For example, a 3 x 3 dataset might by chunked in 2 x 2
 * chunks. Sufficient chunks will be allocated to store the array, and any extra space will not be
 * accessible. So, to store the 3 x 3 array, four 2 x 2 chunks would be allocated with 5 unused
 * elements stored.
 *
 * Chunked datasets can be unlimited in any direction and can be compressed or filtered.
 *
 * Since the data is read or written by chunks, chunking can have a dramatic effect on performance
 * by optimizing what is read and written. Note, too, that for specific access patterns such as
 * parallel I/O, decomposition into chunks can have a large impact on performance.
 *
 * Two restrictions have been placed on chunk shape and size:
 * <ul><li>  The rank of a chunk must be less than or equal to the rank of the dataset</li>
 * <li>  Chunk size cannot exceed the size of a fixed-size dataset; for example, a dataset consisting of
 * a 5 x 4 fixed-size array cannot be defined with 10 x 10 chunks</li></ul>
 *
 * <h4>Compact</h4>
 * For contiguous and chunked storage, the dataset header information and data are stored in two
 * (or more) blocks. Therefore, at least two I/O operations are required to access the data: one to
 * access the header, and one (or more) to access data. For a small dataset, this is considerable
 * overhead.
 *
 * A small dataset may be stored in a continuous array of bytes in the header block using the
 * compact storage option. This dataset can be read entirely in one operation which retrieves the
 * header and data. The dataset must fit in the header. This may vary depending on the metadata
 * that is stored. In general, a compact dataset should be approximately 30 KB or less total size.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Dsets_fig8.gif "Compact data storage"
 * </td>
 * </tr>
 * </table>
 *
 * \subsubsection subsubsec_dataset_transfer_partial Partial I/O Sub‐setting and Hyperslabs
 * Data transfers can write or read some of the data elements of the dataset. This is controlled by
 * specifying two selections: one for the source and one for the destination. Selections are specified
 * by creating a dataspace with selections.
 *
 * Selections may be a union of hyperslabs or a list of points. A hyperslab is a contiguous hyper-
 * rectangle from the dataspace. Selected fields of a compound datatype may be read or written. In
 * this case, the selection is controlled by the memory and file datatypes.
 *
 * Summary of procedure:
 * \li 1. Open the dataset
 * \li 2. Define the memory datatype
 * \li 3. Define the memory dataspace selection and file dataspace selection
 * \li 4. Transfer data (#H5Dread or #H5Dwrite)
 *
 * For more information,
 * @see @ref sec_dataspace
 *
 * \subsection subsec_dataset_allocation Allocation of Space in the File
 * When a dataset is created, space is allocated in the file for its header and initial data. The amount
of space allocated when the dataset is created depends on the storage properties. When the
dataset is modified (data is written, attributes added, or other changes), additional storage may be
allocated if necessary.
 *
 * <table>
 * <caption>Initial dataset size</caption>
 * <tr>
 * <th>Object</th>
 * <th>Size</th>
 * </tr>
 * <tr>
 * <td>Header</td>
 * <td>Variable, but typically around 256 bytes at the creation of a simple dataset with a simple
 * datatype.</td>
 * </tr>
 * <tr>
 * <td>Data</td>
 * <td>Size of the data array (number of elements x size of element). Space allocated in
 * the file depends on the storage strategy and the allocation strategy.</td>
 * </tr>
 * </table>
 *
 * <h4>Header</h4>
 * A dataset header consists of one or more header messages containing persistent metadata
 * describing various aspects of the dataset. These records are defined in the HDF5 File Format
 * Specification. The amount of storage required for the metadata depends on the metadata to be
 * stored. The table below summarizes the metadata.
 *
 * <table>
 * <caption>Metadata storage sizes</caption>
 * <tr>
 * <th>Header Information</th>
 * <th>Approximate Storage Size</th>
 * </tr>
 * <tr>
 * <td>Datatype (required)</td>
 * <td>Bytes or more. Depends on type.</td>
 * </tr>
 * <tr>
 * <td>Dataspace (required)</td>
 * <td>Bytes or more. Depends on number of dimensions and hsize_t.</td>
 * </tr>
 * <tr>
 * <td>Layout (required)</td>
 * <td>Points to the stored data. Bytes or more. Depends on hsize_t and number of dimensions.</td>
 * </tr>
 * <tr>
 * <td>Filters</td>
 * <td>Depends on the number of filters. The size of the filter message depends on the name and
 * data that will be passed.</td>
 * </tr>
 * </table>
 *
 * The header blocks also store the name and values of attributes, so the total storage depends on
 * the number and size of the attributes.
 *
 * In addition, the dataset must have at least one link, including a name, which is stored in the file
 * and in the group it is linked from.
 *
 * The different storage strategies determine when and how much space is allocated for the data
 * array. See the discussion of fill values below for a detailed explanation of the storage allocation.
 *
 * <h4>Contiguous Storage</h4>
 * For a continuous storage option, the data is stored in a single, contiguous block in the file. The
 * data is nominally a fixed-size, (number of elements x size of element). The figure below shows
 * an example of a two dimensional array stored as a contiguous dataset.
 *
 * Depending on the fill value properties, the space may be allocated when the dataset is created or
 * when first written (default), and filled with fill values if specified. For parallel I/O, by default the
 * space is allocated when the dataset is created.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Dsets_fig9.gif "A two dimensional array stored as a contiguous dataset"
 * </td>
 * </tr>
 * </table>
 *
 * <h4>Chunked Storage</h4>
 * For chunked storage, the data is stored in one or more chunks. Each chunk is a continuous block
 * in the file, but chunks are not necessarily stored contiguously. Each chunk has the same size. The
 * data array has the same nominal size as a contiguous array (number of elements x size of
 * element), but the storage is allocated in chunks, so the total size in the file can be larger than the
 * nominal size of the array. See the figure below.
 *
 * If a fill value is defined, each chunk will be filled with the fill value. Chunks must be allocated
 * when data is written, but they may be allocated when the file is created, as the file expands, or
 * when data is written.
 *
 * For serial I/O, by default chunks are allocated incrementally, as data is written to the chunk. For
 * a sparse dataset, chunks are allocated only for the parts of the dataset that are written. In this
 * case, if the dataset is extended, no storage is allocated.
 *
 * For parallel I/O, by default chunks are allocated when the dataset is created or extended with fill
 * values written to the chunk.
 *
 * In either case, the default can be changed using fill value properties. For example, using serial
 * I/O, the properties can select to allocate chunks when the dataset is created.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Dsets_fig10.gif "A two dimensional array stored in chunks"
 * </td>
 * </tr>
 * </table>
 *
 * <h4>Changing Dataset Dimensions</h4>
 * #H5Dset_extent is used to change the current dimensions of the dataset within the limits of the
 * dataspace. Each dimension can be extended up to its maximum or unlimited. Extending the
 * dataspace may or may not allocate space in the file and may or may not write fill values, if they
 * are defined. See the example code below.
 *
 * The dimensions of the dataset can also be reduced. If the sizes specified are smaller than the
 * dataset's current dimension sizes, #H5Dset_extent will reduce the dataset's dimension sizes to the
 * specified values. It is the user's responsibility to ensure that valuable data is not lost;
 * #H5Dset_extent does not check.
 *
 * <em>Using #H5Dset_extent to increase the size of a dataset</em>
 * \code
 *   hid_t file_id, dataset_id;
 *   herr_t status;
 *   size_t newdims[2];
 *
 *   // Open an existing file.
 *   file_id = H5Fopen("dset.h5", H5F_ACC_RDWR, H5P_DEFAULT);
 *
 *   // Open an existing dataset.
 *   dataset_id = H5Dopen(file_id, "/dset", H5P_DEFAULT);
 *
 *   // Example: dataset is 2 x 3, each dimension is UNLIMITED
 *   // extend to 2 x 7
 *   newdims[0] = 2;
 *   newdims[1] = 7;
 *   status = H5Dset_extent(dataset_id, newdims);
 *
 *   // dataset is now 2 x 7
 *
 *   status = H5Dclose(dataset_id);
 * \endcode
 *
 * \subsubsection subsubsec_dataset_allocation_store Storage Allocation in the File: Early, Incremental, Late
 * The HDF5 Library implements several strategies for when storage is allocated if and when it is
 * filled with fill values for elements not yet written by the user. Different strategies are
 * recommended for different storage layouts and file drivers. In particular, a parallel program
 * needs storage allocated during a collective call (for example, create or extend), while serial
 * programs may benefit from delaying the allocation until the data is written.
 *
 * Two file creation properties control when to allocate space, when to write the fill value, and the
 * actual fill value to write.
 *
 * <h4>When to Allocate Space</h4>
 * The table below shows the options for when data is allocated in the file. Early allocation is done
 * during the dataset create call. Certain file drivers (especially MPI-I/O and MPI-POSIX) require
 * space to be allocated when a dataset is created, so all processors will have the correct view of the
 * data.
 *
 * <table>
 * <caption>File storage allocation options</caption>
 * <tr>
 * <th>Strategy</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>Early</td>
 * <td>Allocate storage for the dataset immediately when the dataset is created.</td>
 * </tr>
 * <tr>
 * <td>Late</td>
 * <td>Defer allocating space for storing the dataset until the dataset is written.</td>
 * </tr>
 * <tr>
 * <td>Incremental</td>
 * <td>Defer allocating space for storing each chunk until the chunk is written.</td>
 * </tr>
 * <tr>
 * <td>Default</td>
 * <td>Use the strategy (Early, Late, or Incremental) for the storage method and
 * access method. This is the recommended strategy.</td>
 * </tr>
 * </table>
 *
 * Late allocation is done at the time of the first write to dataset. Space for the whole dataset is
 * allocated at the first write.
 *
 * Incremental allocation (chunks only) is done at the time of the first write to the chunk. Chunks
 * that have never been written are not allocated in the file. In a sparsely populated dataset, this
 * option allocates chunks only where data is actually written.
 *
 * The “Default” property selects the option recommended as appropriate for the storage method
 * and access method. The defaults are shown in the table below. Note that Early allocation is
 * recommended for all Parallel I/O, while other options are recommended as the default for serial
 * I/O cases.
 *
 * <table>
 * <caption>Default storage options</caption>
 * <tr>
 * <th>Storage Type</th>
 * <th>Serial I/O</th>
 * <th>Parallel I/O</th>
 * </tr>
 * <tr>
 * <td>Contiguous</td>
 * <td>Late</td>
 * <td>Early</td>
 * </tr>
 * <tr>
 * <td>Chunked</td>
 * <td>Incremental</td>
 * <td>Early</td>
 * </tr>
 * <tr>
 * <td>Compact</td>
 * <td>Early</td>
 * <td>Early</td>
 * </tr>
 * </table>
 *
 * <h4>When to Write the Fill Value</h4>
 * The second property is when to write the fill value. The possible values are “Never” and
 * “Allocation”. The table below shows these options.
 *
 * <table>
 * <caption>When to write fill values</caption>
 * <tr>
 * <th>When</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>Never</td>
 * <td>Fill value will never be written.</td>
 * </tr>
 * <tr>
 * <td>Allocation</td>
 * <td>Fill value is written when space is allocated. (Default for chunked and contiguous
 * data storage.)</td>
 * </tr>
 * </table>
 *
 * <h4>What Fill Value to Write</h4>
 * The third property is the fill value to write. The table below shows the values. By default, the
 * data is filled with zeros. The application may choose no fill value (Undefined). In this case,
 * uninitialized data may have random values. The application may define a fill value of an
 * appropriate type. For more information, @see @ref subsec_datatype_fill.
 *
 * <table>
 * <caption>Fill values to write</caption>
 * <tr>
 * <th>What to Write</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>Default</td>
 * <td>By default, the library fills allocated space with zeros.</td>
 * </tr>
 * <tr>
 * <td>Undefined</td>
 * <td>Allocated space is filled with random values.</td>
 * </tr>
 * <tr>
 * <td>User-defined</td>
 * <td>The application specifies the fill value.</td>
 * </tr>
 * </table>
 *
 * Together these three properties control the library's behavior. The table below summarizes the
 * possibilities during the dataset create-write-close cycle.
 *
 * <table>
 * <caption>Storage allocation and fill summary</caption>
 * <tr>
 * <th>When to allocate space</th>
 * <th>When to write fill value</th>
 * <th>What fill value to write</th>
 * <th>Library create-write-close behavior</th>
 * </tr>
 * <tr>
 * <td>Early</td>
 * <td>Never</td>
 * <td>-</td>
 * <td>Library allocates space when dataset is created, but never writes a fill value to dataset. A read
 * of unwritten data returns undefined values.</td>
 * </tr>
 * <tr>
 * <td>Late</td>
 * <td>Never</td>
 * <td>-</td>
 * <td>Library allocates space when dataset is written to, but never writes a fill value to the dataset. A
 * read of unwritten data returns undefined values.</td>
 * </tr>
 * <tr>
 * <td>Incremental</td>
 * <td>Never</td>
 * <td>-</td>
 * <td>Library allocates space when a dataset or chunk (whichever is the smallest unit of space)
 * is written to, but it never writes a fill value to a dataset or a chunk. A read of unwritten data
 * returns undefined values.</td>
 * </tr>
 * <tr>
 * <td>-</td>
 * <td>Allocation</td>
 * <td>Undefined</td>
 * <td>Error on creating the dataset. The dataset is not created.</td>
 * </tr>
 * <tr>
 * <td>Early</td>
 * <td>Allocation</td>
 * <td>Default or User-defined</td>
 * <td>Allocate space for the dataset when the dataset is created. Write the fill value (default or
 * user-defined) to the entire dataset when the dataset is created.</td>
 * </tr>
 * <tr>
 * <td>Late</td>
 * <td>Allocation</td>
 * <td>Default or User-define</td>
 * <td>Allocate space for the dataset when the application first writes data values to the dataset.
 * Write the fill value to the entire dataset before writing application data values.</td>
 * </tr>
 * <tr>
 * <td>Incremental</td>
 * <td>Allocation</td>
 * <td>Default or User-define</td>
 * <td>Allocate space for the dataset when the application first writes data values to the dataset or
 * chunk (whichever is the smallest unit of space). Write the fill value to the entire dataset
 * or chunk before writing application data values.</td>
 * </tr>
 * </table>
 *
 * During the #H5Dread function call, the library behavior depends on whether space has been
 * allocated, whether the fill value has been written to storage, how the fill value is defined, and
 * when to write the fill value. The table below summarizes the different behaviors.
 *
 * <table>
 * <caption>H5Dread summary</caption>
 * <tr>
 * <th>Is space allocated in the file?</th>
 * <th>What is the fill value?</th>
 * <th>When to write the fill value?</th>
 * <th>Library read behavior</th>
 * </tr>
 * <tr>
 * <td>No</td>
 * <td>Undefined</td>
 * <td>anytime</td>
 * <td>Error. Cannot create this dataset.</td>
 * </tr>
 * <tr>
 * <td>No</td>
 * <td>Default or User-define</td>
 * <td>anytime</td>
 * <td>Fill the memory buffer with the fill value.</td>
 * </tr>
 * <tr>
 * <td>Yes</td>
 * <td>Undefined</td>
 * <td>anytime</td>
 * <td>Return data from storage (dataset). Trash is possible if the application has not written data
 * to the portion of the dataset being read.</td>
 * </tr>
 * <tr>
 * <td>Yes</td>
 * <td>Default or User-define</td>
 * <td>Never</td>
 * <td>Return data from storage (dataset). Trash is possible if the application has not written data
 * to the portion of the dataset being read.</td>
 * </tr>
 * <tr>
 * <td>Yes</td>
 * <td>Default or User-define</td>
 * <td>Allocation</td>
 * <td>Return data from storage (dataset).</td>
 * </tr>
 * </table>
 *
 * There are two cases to consider depending on whether the space in the file has been allocated
 * before the read or not. When space has not yet been allocated and if a fill value is defined, the
 * memory buffer will be filled with the fill values and returned. In other words, no data has been
 * read from the disk. If space has been allocated, the values are returned from the stored data. The
 * unwritten elements will be filled according to the fill value.
 *
 * \subsubsection subsubsec_dataset_allocation_delete Deleting a Dataset from a File and Reclaiming Space
 * HDF5 does not at this time provide an easy mechanism to remove a dataset from a file or to
 * reclaim the storage space occupied by a deleted object.
 *
 * Removing a dataset and reclaiming the space it used can be done with the #H5Ldelete function
 * and the h5repack utility program. With the H5Ldelete function, links to a dataset can be removed
 * from the file structure. After all the links have been removed, the dataset becomes inaccessible to
 * any application and is effectively removed from the file. The way to recover the space occupied
 * by an unlinked dataset is to write all of the objects of the file into a new file. Any unlinked object
 * is inaccessible to the application and will not be included in the new file. Writing objects to a
 * new file can be done with a custom program or with the h5repack utility program.
 *
 * For more information, @see @ref sec_group
 *
 * \subsubsection subsubsec_dataset_allocation_release Releasing Memory Resources
 * The system resources required for HDF5 objects such as datasets, datatypes, and dataspaces
 * should be released once access to the object is no longer needed. This is accomplished via the
 * appropriate close function. This is not unique to datasets but a general requirement when
 * working with the HDF5 Library; failure to close objects will result in resource leaks.
 *
 * In the case where a dataset is created or data has been transferred, there are several objects that
 * must be closed. These objects include datasets, datatypes, dataspaces, and property lists.
 *
 * The application program must free any memory variables and buffers it allocates. When
 * accessing data from the file, the amount of memory required can be determined by calculating
 * the size of the memory datatype and the number of elements in the memory selection.
 *
 * Variable-length data are organized in two or more areas of memory. For more information,
 * \see \ref h4_vlen_datatype "Variable-length Datatypes".
 *
 * When writing data, the application creates an array of
 * vl_info_t which contains pointers to the elements. The elements might be, for example, strings.
 * In the file, the variable-length data is stored in two parts: a heap with the variable-length values
 * of the data elements and an array of vl_info_t elements. When the data is read, the amount of
 * memory required for the heap can be determined with the #H5Dvlen_get_buf_size call.
 *
 * The data transfer property may be used to set a custom memory manager for allocating variable-
 * length data for a #H5Dread. This is set with the #H5Pset_vlen_mem_manager call.
 * To free the memory for variable-length data, it is necessary to visit each element, free the
 * variable-length data, and reset the element. The application must free the memory it has
 * allocated. For memory allocated by the HDF5 Library during a read, the #H5Dvlen_reclaim
 * function can be used to perform this operation.
 *
 * \subsubsection subsubsec_dataset_allocation_ext External Storage Properties
 * The external storage format allows data to be stored across a set of non-HDF5 files. A set of
 * segments (offsets and sizes) in one or more files is defined as an external file list, or EFL, and
 * the contiguous logical addresses of the data storage are mapped onto these segments. Currently,
 * only the #H5D_CONTIGUOUS storage format allows external storage. External storage is
 * enabled by a dataset creation property. The table below shows the API.
 *
 * <table>
 * <caption>External storage API</caption>
 * <tr>
 * <th>Function</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>#H5Pset_external</td>
 * <td>This function adds a new segment to the end of the external file list of the specified dataset
 * creation property list. The segment begins a byte offset of file name and continues for size
 * bytes. The space represented by this segment is adjacent to the space already represented by
 * the external file list. The last segment in a file list may have the size #H5F_UNLIMITED, in
 * which case the external file may be of unlimited size and no more files can be added to the
 * external files list.</td>
 * </tr>
 * <tr>
 * <td>#H5Pget_external_count</td>
 * <td>Calling this function returns the number of segments in an external file list. If the dataset
 * creation property list has no external data, then zero is returned.</td>
 * </tr>
 * <tr>
 * <td>#H5Pget_external</td>
 * <td>This is the counterpart for the #H5Pset_external function. Given a dataset creation
 * property list and a zero-based index into that list, the file name, byte offset, and segment
 * size are returned through non-null arguments. At most name_size characters are copied into
 * the name argument which is not null terminated if the file name is longer than the
 * supplied name buffer (this is similar to strncpy()).</td>
 * </tr>
 * </table>
 *
 * The figure below shows an example of how a contiguous, one-dimensional dataset is partitioned
 * into three parts and each of those parts is stored in a segment of an external file. The top
 * rectangle represents the logical address space of the dataset while the bottom rectangle represents
 * an external file.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Dsets_fig11.gif "External file storage"
 * </td>
 * </tr>
 * </table>
 *
 * The example below shows code that defines the external storage for the example. Note that the
 * segments are defined in order of the logical addresses they represent, not their order within the
 * external file. It would also have been possible to put the segments in separate files. Care should
 * be taken when setting up segments in a single file since the library does not automatically check
 * for segments that overlap.
 *
 * <em>External storage</em>
 * \code
 *   plist = H5Pcreate (H5P_DATASET_CREATE);
 *   H5Pset_external (plist, "velocity.data", 3000, 1000);
 *   H5Pset_external (plist, "velocity.data", 0, 2500);
 *   H5Pset_external (plist, "velocity.data", 4500, 1500);
 * \endcode
 *
 * The figure below shows an example of how a contiguous, two-dimensional dataset is partitioned
 * into three parts and each of those parts is stored in a separate external file. The top rectangle
 * represents the logical address space of the dataset while the bottom rectangles represent external
 * files.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Dsets_fig12.gif "Partitioning a 2-D dataset for external storage"
 * </td>
 * </tr>
 * </table>
 *
 * The example below shows code for the partitioning described above. In this example, the library
 * maps the multi-dimensional array onto a linear address space as defined by the HDF5 format
 * specification, and then maps that address space into the segments defined in the external file list.
 *
 * <em>Partitioning a 2-D dataset for external storage</em>
 * \code
 *   plist = H5Pcreate (H5P_DATASET_CREATE);
 *   H5Pset_external (plist, "scan1.data", 0, 24);
 *   H5Pset_external (plist, "scan2.data", 0, 24);
 *   H5Pset_external (plist, "scan3.data", 0, 16);
 * \endcode
 *
 * The segments of an external file can exist beyond the end of the (external) file. The library reads
 * that part of a segment as zeros. When writing to a segment that exists beyond the end of a file,
 * the external file is automatically extended. Using this feature, one can create a segment (or set of
 * segments) which is larger than the current size of the dataset. This allows the dataset to be
 * extended at a future time (provided the dataspace also allows the extension).
 *
 * All referenced external data files must exist before performing raw data I/O on the dataset. This
 * is normally not a problem since those files are being managed directly by the application or
 * indirectly through some other library. However, if the file is transferred from its original context,
 * care must be taken to assure that all the external files are accessible in the new location.
 *
 * \subsection subsec_dataset_filters Using HDF5 Filters
 * This section describes in detail how to use the n-bit, scale-offset filters and szip filters.
 *
 * \subsubsection subsubsec_dataset_filters_nbit Using the N‐bit Filter
 * N-bit data has n significant bits, where n may not correspond to a precise number of bytes. On
 * the other hand, computing systems and applications universally, or nearly so, run most efficiently
 * when manipulating data as whole bytes or multiple bytes.
 *
 * Consider the case of 12-bit integer data. In memory, that data will be handled in at least 2 bytes,
 * or 16 bits, and on some platforms in 4 or even 8 bytes. The size of such a dataset can be
 * significantly reduced when written to disk if the unused bits are stripped out.
 *
 * The n-bit filter is provided for this purpose, packing n-bit data on output by stripping off all
 * unused bits and unpacking on input, restoring the extra bits required by the computational
 * processor.
 *
 * <h4>N-bit Datatype</h4>
 * An n-bit datatype is a datatype of n significant bits. Unless it is packed, an n-bit datatype is
 * presented as an n-bit bitfield within a larger-sized value. For example, a 12-bit datatype might be
 * presented as a 12-bit field in a 16-bit, or 2-byte, value.
 *
 * Currently, the datatype classes of n-bit datatype or n-bit field of a compound datatype or an array
 * datatype are limited to integer or floating-point.
 *
 * The HDF5 user can create an n-bit datatype through a series of function calls. For example, the
 * following calls create a 16-bit datatype that is stored in a 32-bit value with a 4-bit offset:
 * \code
 *   hid_t nbit_datatype = H5Tcopy(H5T_STD_I32LE);
 *   H5Tset_precision(nbit_datatype, 16);
 *   H5Tset_offset(nbit_datatype, 4);
 * \endcode
 *
 * In memory, one value of the above example n-bit datatype would be stored on a little-endian
 * machine as follows:
 * <table>
 * <tr>
 * <th>byte 3</th>
 * <th>byte 2</th>
 * <th>byte 1</th>
 * <th>byte 0</th>
 * </tr>
 * <tr>
 *   <td> ???????? </td>
 *   <td> ????SPPP </td>
 *   <td> PPPPPPPP </td>
 *   <td> PPPP???? </td>
 * </tr>
 * <tr>
 * <td colspan="4">
 * <em>Note: Key: S - sign bit, E - exponent bit, M - mantissa bit, ? - padding bit. Sign bit is
 *  included in signed integer datatype precision.</em>
 * </td>
 * </tr>
 * </table>
 *
 * <h4>N-bit Filter</h4>
 * When data of an n-bit datatype is stored on disk using the n-bit filter, the filter packs the data by
 * stripping off the padding bits; only the significant bits are retained and stored. The values on disk
 * will appear as follows:
 * <table>
 * <tr>
 * <th>1st value</th>
 * <th>2nd value</th>
 * <th>nth value</th>
 * </tr>
 * <tr>
 * <td>SPPPPPPP PPPPPPPP</td>
 * <td>SPPPPPPP PPPPPPPP</td>
 * <td>...</td>
 * </tr>
 * <tr>
 * <td colspan="3">
 * <em>Note: Key: S - sign bit, E - exponent bit, M - mantissa bit, ? - padding bit. Sign bit is
 *  included in signed integer datatype precision.</em>
 * </td>
 * </tr>
 * </table>
 *
 * <h4>How Does the N-bit Filter Work?</h4>
 * The n-bit filter always compresses and decompresses according to dataset properties supplied by
 * the HDF5 library in the datatype, dataspace, or dataset creation property list.
 *
 * The dataset datatype refers to how data is stored in an HDF5 file while the memory datatype
 * refers to how data is stored in memory. The HDF5 library will do datatype conversion when
 * writing data in memory to the dataset or reading data from the dataset to memory if the memory
 * datatype differs from the dataset datatype. Datatype conversion is performed by HDF5 library
 * before n-bit compression and after n-bit decompression.
 *
 * The following sub-sections examine the common cases:
 * \li N-bit integer conversions
 * \li N-bit floating-point conversions
 *
 * <h4>N-bit Integer Conversions</h4>
 * Integer data with a dataset of integer datatype of less than full precision and a memory datatype
 * of #H5T_NATIVE_INT, provides the simplest application of the n-bit filter.
 *
 * The precision of #H5T_NATIVE_INT is 8 multiplied by sizeof(int). This value, the size of an
 * int in bytes, differs from platform to platform; we assume a value of 4 for the following
 * illustration. We further assume the memory byte order to be little-endian.
 *
 * In memory, therefore, the precision of #H5T_NATIVE_INT is 32 and the offset is 0. One value of
 * #H5T_NATIVE_INT is laid out in memory as follows:
 * <table>
 * <tr>
 * <td>
 * \image html Dsets_NbitInteger1.gif "H5T_NATIVE_INT in memory"<br />
 * <em>Note: Key: S - sign bit, E - exponent bit, M - mantissa bit, ? - padding bit. Sign bit is
 *  included in signed integer datatype precision.</em>
 * </td>
 * </tr>
 * </table>
 *
 * Suppose the dataset datatype has a precision of 16 and an offset of 4. After HDF5 converts
 * values from the memory datatype to the dataset datatype, it passes something like the following
 * to the n-bit filter for compression:
 * <table>
 * <tr>
 * <td>
 * \image html Dsets_NbitInteger2.gif "Passed to the n-bit filter"<br />
 * <em>Note: Key: S - sign bit, E - exponent bit, M - mantissa bit, ? - padding bit. Sign bit is
 *  included in signed integer datatype precision.</em>
 * </td>
 * </tr>
 * </table>
 *
 * Notice that only the specified 16 bits (15 significant bits and the sign bit) are retained in the
 * conversion. All other significant bits of the memory datatype are discarded because the dataset
 * datatype calls for only 16 bits of precision. After n-bit compression, none of these discarded bits,
 * known as padding bits will be stored on disk.
 *
 * <h4>N-bit Floating-point Conversions</h4>
 * Things get more complicated in the case of a floating-point dataset datatype class. This sub-
 * section provides an example that illustrates the conversion from a memory datatype of
 * #H5T_NATIVE_FLOAT to a dataset datatype of class floating-point.
 *
 * As before, let the #H5T_NATIVE_FLOAT be 4 bytes long, and let the memory byte order be
 * little-endian. Per the IEEE standard, one value of #H5T_NATIVE_FLOAT is laid out in memory
 * as follows:
 * <table>
 * <tr>
 * <td>
 * \image html Dsets_NbitFloating1.gif "H5T_NATIVE_FLOAT in memory"<br />
 * <em>Note: Key: S - sign bit, E - exponent bit, M - mantissa bit, ? - padding bit. Sign bit is
 *  included in floating-point datatype precision.</em>
 * </td>
 * </tr>
 * </table>
 *
 * Suppose the dataset datatype has a precision of 20, offset of 7, mantissa size of 13, mantissa
 * position of 7, exponent size of 6, exponent position of 20, and sign position of 26. For more
 * information, @see @ref subsubsec_datatype_program_define.
 *
 * After HDF5 converts values from the memory datatype to the dataset datatype, it passes
 * something like the following to the n-bit filter for compression:
 * <table>
 * <tr>
 * <td>
 * \image html Dsets_NbitFloating2.gif "Passed to the n-bit filter"<br />
 * <em>Note: Key: S - sign bit, E - exponent bit, M - mantissa bit, ? - padding bit. Sign bit is
 *  included in floating-point datatype precision.</em>
 * </td>
 * </tr>
 * </table>
 *
 * The sign bit and truncated mantissa bits are not changed during datatype conversion by the
 * HDF5 library. On the other hand, the conversion of the 8-bit exponent to a 6-bit exponent is a
 * little tricky:
 *
 * The bias for the new exponent in the n-bit datatype is:
 * <code>
 *   2<sup>(n-1)</sup>-1
 * </code>
 *
 * The following formula is used for this exponent conversion:<br />
 * <code>
 *   exp8 - (2<sup>(8-1)</sup> -1) = exp6 - (2<sup>(6-1)</sup>-1) = actual exponent value
 * </code><br />
 * where exp8 is the stored decimal value as represented by the 8-bit exponent, and exp6 is the
 * stored decimal value as represented by the 6-bit exponent.
 *
 * In this example, caution must be taken to ensure that, after conversion, the actual exponent value
 * is within the range that can be represented by a 6-bit exponent. For example, an 8-bit exponent
 * can represent values from -127 to 128 while a 6-bit exponent can represent values only from -31
 * to 32.
 *
 * <h4>N-bit Filter Behavior</h4>
 * The n-bit filter was designed to treat the incoming data byte by byte at the lowest level. The
 * purpose was to make the n-bit filter as generic as possible so that no pointer cast related to the
 * datatype is needed.
 *
 * Bitwise operations are employed for packing and unpacking at the byte level.
 *
 * Recursive function calls are used to treat compound and array datatypes.
 *
 * <h4>N-bit Compression</h4>
 * The main idea of n-bit compression is to use a loop to compress each data element in a chunk.
 * Depending on the datatype of each element, the n-bit filter will call one of four functions. Each
 * of these functions performs one of the following tasks:
 * \li Compress a data element of a no-op datatype
 * \li Compress a data element of an atomic datatype
 * \li Compress a data element of a compound datatype
 * \li Compress a data element of an array datatype
 *
 * No-op datatypes: The n-bit filter does not actually compress no-op datatypes. Rather, it copies
 * the data buffer of the no-op datatype from the non-compressed buffer to the proper location in
 * the compressed buffer; the compressed buffer has no holes. The term “compress” is used here
 * simply to distinguish this function from the function that performs the reverse operation during
 * decompression.
 *
 * Atomic datatypes: The n-bit filter will find the bytes where significant bits are located and try to
 * compress these bytes, one byte at a time, using a loop. At this level, the filter needs the following
 * information:
 * <ul><li>The byte offset of the beginning of the current data element with respect to the
 * beginning of the input data buffer</li>
 * <li>Datatype size, precision, offset, and byte order</li></ul>
 *
 * The n-bit filter compresses from the most significant byte containing significant bits to the least
 * significant byte. For big-endian data, therefore, the loop index progresses from smaller to larger
 * while for little-endian, the loop index progresses from larger to smaller.
 *
 * In the extreme case of when the n-bit datatype has full precision, this function copies the content
 * of the entire non-compressed datatype to the compressed output buffer.
 *
 * Compound datatypes: The n-bit filter will compress each data member of the compound
 * datatype. If the member datatype is of an integer or floating-point datatype, the n-bit filter will
 * call the function described above. If the member datatype is of a no-op datatype, the filter will
 * call the function described above. If the member datatype is of a compound datatype, the filter
 * will make a recursive call to itself. If the member datatype is of an array datatype, the filter will
 * call the function described below.
 *
 * Array datatypes: The n-bit filter will use a loop to compress each array element in the array. If
 * the base datatype of array element is of an integer or floating-point datatype, the n-bit filter will
 * call the function described above. If the base datatype is of a no-op datatype, the filter will call
 * the function described above. If the base datatype is of a compound datatype, the filter will call
 * the function described above. If the member datatype is of an array datatype, the filter will make
 * a recursive call of itself.
 *
 * <h4>N-bit Decompression</h4>
 * The n-bit decompression algorithm is very similar to n-bit compression. The only difference is
 * that at the byte level, compression packs out all padding bits and stores only significant bits into
 * a continuous buffer (unsigned char) while decompression unpacks significant bits and inserts
 * padding bits (zeros) at the proper positions to recover the data bytes as they existed before
 * compression.
 *
 * <h4>Storing N-bit Parameters to Array cd_value[]</h4>
 * All of the information, or parameters, required by the n-bit filter are gathered and stored in the
 * array cd_values[] by the private function H5Z__set_local_nbit and are passed to another private
 * function, H5Z__filter_nbit, by the HDF5 Library.
 * These parameters are as follows:
 * \li Parameters related to the datatype
 * \li The number of elements within the chunk
 * \li A flag indicating whether compression is needed
 *
 * The first and second parameters can be obtained using the HDF5 dataspace and datatype
 * interface calls.
 *
 * A compound datatype can have members of array or compound datatype. An array datatype's
 * base datatype can be a complex compound datatype. Recursive calls are required to set
 * parameters for these complex situations.
 *
 * Before setting the parameters, the number of parameters should be calculated to dynamically
 * allocate the array cd_values[], which will be passed to the HDF5 Library. This also requires
 * recursive calls.
 *
 * For an atomic datatype (integer or floating-point), parameters that will be stored include the
 * datatype's size, endianness, precision, and offset.
 *
 * For a no-op datatype, only the size is required.
 *
 * For a compound datatype, parameters that will be stored include the datatype's total size and
 * number of members. For each member, its member offset needs to be stored. Other parameters
 * for members will depend on the respective datatype class.
 *
 * For an array datatype, the total size parameter should be stored. Other parameters for the array's
 * base type depend on the base type's datatype class.
 *
 * Further, to correctly retrieve the parameter for use of n-bit compression or decompression later,
 * parameters for distinguishing between datatype classes should be stored.
 *
 * <h4>Implementation</h4>
 * Three filter callback functions were written for the n-bit filter:
 * \li H5Z__can_apply_nbit
 * \li H5Z__set_local_nbit
 * \li H5Z__filter_nbit
 *
 * These functions are called internally by the HDF5 library. A number of utility functions were
 * written for the function H5Z__set_local_nbit. Compression and decompression functions were
 * written and are called by function H5Z__filter_nbit. All these functions are included in the file
 * H5Znbit.c.
 *
 * The public function #H5Pset_nbit is called by the application to set up the use of the n-bit filter.
 * This function is included in the file H5Pdcpl.c. The application does not need to supply any
 * parameters.
 *
 * <h4>How N-bit Parameters are Stored</h4>
 * A scheme of storing parameters required by the n-bit filter in the array cd_values[] was
 * developed utilizing recursive function calls.
 *
 * Four private utility functions were written for storing the parameters associated with atomic
 * (integer or floating-point), no-op, array, and compound datatypes:
 * \li H5Z__set_parms_atomic
 * \li H5Z__set_parms_array
 * \li H5Z__set_parms_nooptype
 * \li H5Z__set_parms_compound
 *
 * The scheme is briefly described below.
 *
 * First, assign a numeric code for datatype class atomic (integer or float), no-op, array, and
 * compound datatype. The code is stored before other datatype related parameters are stored.
 *
 * The first three parameters of cd_values[] are reserved for:
 * \li 1. The number of valid entries in the array cd_values[]
 * \li 2. A flag indicating whether compression is needed
 * \li 3. The number of elements in the chunk
 *
 * Throughout the balance of this explanation, i represents the index of cd_values[].
 * In the function H5Z__set_local_nbit:
 * <ul><li>1. i = 2</li>
 * <li>2. Get the number of elements in the chunk and store in cd_value[i]; increment i</li>
 * <li>3. Get the class of the datatype:
 *   <ul><li>For an integer or floating-point datatype, call H5Z__set_parms_atomic</li>
 *   <li>For an array datatype, call H5Z__set_parms_array</li>
 *   <li>For a compound datatype, call H5Z__set_parms_compound</li>
 *   <li>For none of the above, call H5Z__set_parms_noopdatatype</li></ul></li>
 * <li>4. Store i in cd_value[0] and flag in cd_values[1]</li></ul>
 *
 * In the function H5Z__set_parms_atomic:
 * \li 1. Store the assigned numeric code for the atomic datatype in cd_value[i]; increment i
 * \li 2. Get the size of the atomic datatype and store in cd_value[i]; increment i
 * \li 3. Get the order of the atomic datatype and store in cd_value[i]; increment i
 * \li 4. Get the precision of the atomic datatype and store in cd_value[i]; increment i
 * \li 5. Get the offset of the atomic datatype and store in cd_value[i]; increment i
 * \li 6. Determine the need to do compression at this point
 *
 * In the function H5Z__set_parms_nooptype:
 * \li 1. Store the assigned numeric code for the no-op datatype in cd_value[i]; increment i
 * \li 2. Get the size of the no-op datatype and store in cd_value[i]; increment i
 *
 * In the function H5Z__set_parms_array:
 * <ul><li>1. Store the assigned numeric code for the array datatype in cd_value[i]; increment i</li>
 * <li>2. Get the size of the array datatype and store in cd_value[i]; increment i</li>
 * <li>3. Get the class of the array's base datatype.
 *   <ul><li>For an integer or floating-point datatype, call H5Z__set_parms_atomic</li>
 *   <li>For an array datatype, call H5Z__set_parms_array</li>
 *   <li>For a compound datatype, call H5Z__set_parms_compound</li>
 *   <li>If none of the above, call H5Z__set_parms_noopdatatype</li></ul></li></ul>
 *
 * In the function H5Z__set_parms_compound:
 * <ul><li>1. Store the assigned numeric code for the compound datatype in cd_value[i]; increment i</li>
 * <li>2. Get the size of the compound datatype and store in cd_value[i]; increment i</li>
 * <li>3. Get the number of members and store in cd_values[i]; increment i</li>
 * <li>4. For each member
 *   <ul><li>Get the member offset and store in cd_values[i]; increment i</li>
 *   <li>Get the class of the member datatype</li>
 *   <li>For an integer or floating-point datatype, call H5Z__set_parms_atomic</li>
 *   <li>For an array datatype, call H5Z__set_parms_array</li>
 *   <li>For a compound datatype, call H5Z__set_parms_compound</li>
 *   <li>If none of the above, call H5Z__set_parms_noopdatatype</li></ul></li></ul>
 *
 * <h4>N-bit Compression and Decompression Functions</h4>
 * The n-bit compression and decompression functions above are called by the private HDF5
 * function H5Z__filter_nbit. The compress and decompress functions retrieve the n-bit parameters
 * from cd_values[] as it was passed by H5Z__filter_nbit. Parameters are retrieved in exactly the
 * same order in which they are stored and lower-level compression and decompression functions
 * for different datatype classes are called.
 *
 * N-bit compression is not implemented in place. Due to the difficulty of calculating actual output
 * buffer size after compression, the same space as that of the input buffer is allocated for the output
 * buffer as passed to the compression function. However, the size of the output buffer passed by
 * reference to the compression function will be changed (smaller) after the compression is
 * complete.
 *
 * <h4>Usage Examples</h4>
 *
 * The following code example illustrates the use of the n-bit filter for writing and reading n-bit
 * integer data.
 *
 * <em>N-bit compression for integer data</em>
 * \code
 *   #include "hdf5.h"
 *   #include "stdlib.h"
 *   #include "math.h"
 *
 *   #define H5FILE_NAME "nbit_test_int.h5"
 *   #define DATASET_NAME "nbit_int"
 *   #define NX 200
 *   #define NY 300
 *   #define CH_NX 10
 *   #define CH_NY 15
 *
 *   int main(void)
 *   {
 *     hid_t   file, dataspace, dataset, datatype, mem_datatype, dset_create_props;
 *     hsize_t dims[2], chunk_size[2];
 *     int     orig_data[NX][NY];
 *     int     new_data[NX][NY];
 *     int     i, j;
 *     size_t  precision, offset;
 *
 *     // Define dataset datatype (integer), and set precision, offset
 *     datatype = H5Tcopy(H5T_NATIVE_INT);
 *     precision = 17; // precision includes sign bit
 *     if(H5Tset_precision(datatype,precision) < 0) {
 *       printf("Error: fail to set precision\n");
 *       return -1;
 *     }
 *     offset = 4;
 *     if(H5Tset_offset(datatype,offset) < 0) {
 *       printf("Error: fail to set offset\n");
 *       return -1;
 *     }
 *
 *     // Copy to memory datatype
 *     mem_datatype = H5Tcopy(datatype);
 *
 *     // Set order of dataset datatype
 *     if(H5Tset_order(datatype, H5T_ORDER_BE) < 0) {
 *       printf("Error: fail to set endianness\n");
 *       return -1;
 *     }
 *
 *     // Initialize data buffer with random data within correct
 *     // range corresponding to the memory datatype's precision
 *     // and offset.
 *     for (i = 0; i < NX; i++)
 *       for (j = 0; j < NY; j++)
 *         orig_data[i][j] = rand() % (int)pow(2, precision-1) << offset;
 *
 *     // Describe the size of the array.
 *     dims[0] = NX;
 *     dims[1] = NY;
 *     if((dataspace = H5Screate_simple (2, dims, NULL)) < 0) {
 *       printf("Error: fail to create dataspace\n");
 *       return -1;
 *     }
 *
 *     // Create a new file using read/write access, default file
 *     // creation properties, and default file access properties.
 *     if((file = H5Fcreate (H5FILE_NAME, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
 *       printf("Error: fail to create file\n");
 *       return -1;
 *     }
 *
 *     // Set the dataset creation property list to specify that
 *     // the raw data is to be partitioned into 10 x 15 element
 *     // chunks and that each chunk is to be compressed.
 *     chunk_size[0] = CH_NX;
 *     chunk_size[1] = CH_NY;
 *     if((dset_create_props = H5Pcreate (H5P_DATASET_CREATE)) < 0) {
 *       printf("Error: fail to create dataset property\n");
 *       return -1;
 *     }
 *     if(H5Pset_chunk (dset_create_props, 2, chunk_size) < 0) {
 *       printf("Error: fail to set chunk\n");
 *       return -1;
 *     }
 *
 *     // Set parameters for n-bit compression; check the description
 *     // of the H5Pset_nbit function in the HDF5 Reference Manual
 *     // for more information.
 *     if(H5Pset_nbit (dset_create_props) < 0) {
 *       printf("Error: fail to set nbit filter\n");
 *       return -1;
 *     }
 *
 *     // Create a new dataset within the file. The datatype
 *     // and dataspace describe the data on disk, which may
 *     // be different from the format used in the application's
 *     // memory.
 *     if((dataset = H5Dcreate(file, DATASET_NAME, datatype, dataspace,
 *                             H5P_DEFAULT, dset_create_props, H5P_DEFAULT)) < 0) {
 *       printf("Error: fail to create dataset\n");
 *       return -1;
 *     }
 *
 *     // Write the array to the file. The datatype and dataspace
 *     // describe the format of the data in the 'orig_data' buffer.
 *     // The raw data is translated to the format required on disk,
 *     // as defined above. We use default raw data transfer
 *     // properties.
 *     if(H5Dwrite (dataset, mem_datatype, H5S_ALL, H5S_ALL, H5P_DEFAULT, orig_data) < 0) {
 *       printf("Error: fail to write to dataset\n");
 *       return -1;
 *     }
 *     H5Dclose (dataset);
 *
 *     if((dataset = H5Dopen(file, DATASET_NAME, H5P_DEFAULT)) < 0) {
 *       printf("Error: fail to open dataset\n");
 *       return -1;
 *     }
 *
 *     // Read the array. This is similar to writing data,
 *     // except the data flows in the opposite direction.
 *     // Note: Decompression is automatic.
 *     if(H5Dread (dataset, mem_datatype, H5S_ALL, H5S_ALL, H5P_DEFAULT, new_data) < 0) {
 *       printf("Error: fail to read from dataset\n");
 *       return -1;
 *     }
 *
 *     H5Tclose (datatype);
 *     H5Tclose (mem_datatype);
 *     H5Dclose (dataset);
 *     H5Sclose (dataspace);
 *     H5Pclose (dset_create_props);
 *     H5Fclose (file);
 *
 *     return 0;
 *   }
 * \endcode
 *
 * The following code example illustrates the use of the n-bit filter for writing and reading n-bit
 * floating-point data.
 *
 * <em>N-bit compression for floating-point data</em>
 * \code
 *   #include "hdf5.h"
 *
 *   #define H5FILE_NAME "nbit_test_float.h5"
 *   #define DATASET_NAME "nbit_float"
 *   #define NX 2
 *   #define NY 5
 *   #define CH_NX 2
 *   #define CH_NY 5
 *
 *   int main(void)
 *   {
 *     hid_t   file, dataspace, dataset, datatype, dset_create_props;
 *     hsize_t dims[2], chunk_size[2];
 *
 *     // orig_data[] are initialized to be within the range that
 *     // can be represented by dataset datatype (no precision
 *     // loss during datatype conversion)
 *     //
 *     float orig_data[NX][NY] = {{188384.00, 19.103516,-1.0831790e9, -84.242188, 5.2045898},
 *                                {-49140.000, 2350.2500, -3.2110596e-1, 6.4998865e-5, -0.0000000}};
 *     float new_data[NX][NY];
 *     size_t precision, offset;
 *
 *     // Define single-precision floating-point type for dataset
 *     //---------------------------------------------------------------
 *     // size=4 byte, precision=20 bits, offset=7 bits,
 *     // mantissa size=13 bits, mantissa position=7,
 *     // exponent size=6 bits, exponent position=20,
 *     // exponent bias=31.
 *     // It can be illustrated in little-endian order as:
 *     // (S - sign bit, E - exponent bit, M - mantissa bit, ? - padding bit)
 *     //
 *     //     3       2        1        0
 *     // ?????SEE EEEEMMMM MMMMMMMM M???????
 *     //
 *     // To create a new floating-point type, the following
 *     // properties must be set in the order of
 *     // set fields -> set offset -> set precision -> set size.
 *     // All these properties must be set before the type can
 *     // function. Other properties can be set anytime. Derived
 *     // type size cannot be expanded bigger than original size
 *     // but can be decreased. There should be no holes
 *     // among the significant bits. Exponent bias usually
 *     // is set 2^(n-1)-1, where n is the exponent size.
 *     //---------------------------------------------------------------
 *     datatype = H5Tcopy(H5T_IEEE_F32BE);
 *     if(H5Tset_fields(datatype, 26, 20, 6, 7, 13) < 0) {
 *       printf("Error: fail to set fields\n");
 *       return -1;
 *     }
 *     offset = 7;
 *     if(H5Tset_offset(datatype,offset) < 0) {
 *       printf("Error: fail to set offset\n");
 *       return -1;
 *     }
 *     precision = 20;
 *     if(H5Tset_precision(datatype,precision) < 0) {
 *       printf("Error: fail to set precision\n");
 *       return -1;
 *     }
 *     if(H5Tset_size(datatype, 4) < 0) {
 *       printf("Error: fail to set size\n");
 *       return -1;
 *     }
 *     if(H5Tset_ebias(datatype, 31) < 0) {
 *       printf("Error: fail to set exponent bias\n");
 *       return -1;
 *     }
 *
 *     // Describe the size of the array.
 *     dims[0] = NX;
 *     dims[1] = NY;
 *     if((dataspace = H5Screate_simple (2, dims, NULL)) < 0) {
 *       printf("Error: fail to create dataspace\n");
 *       return -1;
 *     }
 *
 *     // Create a new file using read/write access, default file
 *     // creation properties, and default file access properties.
 *     if((file = H5Fcreate (H5FILE_NAME, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
 *       printf("Error: fail to create file\n");
 *       return -1;
 *     }
 *
 *     // Set the dataset creation property list to specify that
 *     // the raw data is to be partitioned into 2 x 5 element
 *     // chunks and that each chunk is to be compressed.
 *     chunk_size[0] = CH_NX;
 *     chunk_size[1] = CH_NY;
 *     if((dset_create_props = H5Pcreate (H5P_DATASET_CREATE)) < 0) {
 *       printf("Error: fail to create dataset property\n");
 *       return -1;
 *     }
 *     if(H5Pset_chunk (dset_create_props, 2, chunk_size) < 0) {
 *       printf("Error: fail to set chunk\n");
 *       return -1;
 *     }
 *
 *     // Set parameters for n-bit compression; check the description
 *     // of the H5Pset_nbit function in the HDF5 Reference Manual
 *     // for more information.
 *     if(H5Pset_nbit (dset_create_props) < 0) {
 *       printf("Error: fail to set nbit filter\n");
 *       return -1;
 *     }
 *
 *     // Create a new dataset within the file. The datatype
 *     // and dataspace describe the data on disk, which may
 *     // be different from the format used in the application's memory.
 *     if((dataset = H5Dcreate(file, DATASET_NAME, datatype, dataspace, H5P_DEFAULT,
 *                             dset_create_plists, H5P_DEFAULT)) < 0) {
 *       printf("Error: fail to create dataset\n");
 *       return -1;
 *     }
 *
 *     // Write the array to the file. The datatype and dataspace
 *     // describe the format of the data in the 'orig_data' buffer.
 *     // The raw data is translated to the format required on disk,
 *     // as defined above. We use default raw data transfer properties.
 *     if(H5Dwrite (dataset, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, orig_data) < 0) {
 *       printf("Error: fail to write to dataset\n");
 *       return -1;
 *     }
 *     H5Dclose (dataset);
 *     if((dataset = H5Dopen(file, DATASET_NAME, H5P_DEFAULT))<0) {
 *       printf("Error: fail to open dataset\n");
 *       return -1;
 *     }
 *
 *     // Read the array. This is similar to writing data,
 *     // except the data flows in the opposite direction.
 *     // Note: Decompression is automatic.
 *     if(H5Dread (dataset, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, new_data) < 0) {
 *       printf("Error: fail to read from dataset\n");
 *       return -1;
 *     }
 *     H5Tclose (datatype);
 *     H5Dclose (dataset);
 *     H5Sclose (dataspace);
 *     H5Pclose (dset_create_props);
 *     H5Fclose (file);
 *
 *     return 0
 *   }
 * \endcode
 *
 * <h4>Limitations</h4>
 * Because the array cd_values[] has to fit into an object header message of 64K, the n-bit filter has
 * an upper limit on the number of n-bit parameters that can be stored in it. To be conservative, a
 * maximum of 4K is allowed for the number of parameters.
 *
 * The n-bit filter currently only compresses n-bit datatypes or fields derived from integer or
 * floating-point datatypes. The n-bit filter assumes padding bits of zero. This may not be true since
 * the HDF5 user can set padding bit to be zero, one, or leave the background alone. However, it is
 * expected the n-bit filter will be modified to adjust to such situations.
 *
 * The n-bit filter does not have a way to handle the situation where the fill value of a dataset is
 * defined and the fill value is not of an n-bit datatype although the dataset datatype is.
 *
 * \subsubsection subsubsec_dataset_filters_scale Using the Scale‐offset Filter
 * Generally speaking, scale-offset compression performs a scale and/or offset operation on each
 * data value and truncates the resulting value to a minimum number of bits (minimum-bits) before
 * storing it.
 *
 * The current scale-offset filter supports integer and floating-point datatypes only. For the floating-
 * point datatype, float and double are supported, but long double is not supported.
 *
 * Integer data compression uses a straight-forward algorithm. Floating-point data compression
 * adopts the GRiB data packing mechanism which offers two alternate methods: a fixed minimum-
 * bits method, and a variable minimum-bits method. Currently, only the variable minimum-bits
 * method is implemented.
 *
 * Like other I/O filters supported by the HDF5 library, applications using the scale-offset filter
 * must store data with chunked storage.
 *
 * Integer type: The minimum-bits of integer data can be determined by the filter. For example, if
 * the maximum value of data to be compressed is 7065 and the minimum value is 2970. Then the
 * “span” of dataset values is equal to (max-min+1), which is 4676. If no fill value is defined for the
 * dataset, the minimum-bits is: ceiling(log2(span)) = 12. With fill value set, the minimum-bits is:
 * ceiling(log2(span+1)) = 13.
 *
 * HDF5 users can also set the minimum-bits. However, if the user gives a minimum-bits that is
 * less than that calculated by the filter, the compression will be lossy.
 *
 * Floating-point type: The basic idea of the scale-offset filter for the floating-point type is to
 * transform the data by some kind of scaling to integer data, and then to follow the procedure of
 * the scale-offset filter for the integer type to do the data compression. Due to the data
 * transformation from floating-point to integer, the scale-offset filter is lossy in nature.
 *
 * Two methods of scaling the floating-point data are used: the so-called D-scaling and E-scaling.
 * D-scaling is more straightforward and easy to understand. For HDF5 1.8 release, only the
 * D-scaling method had been implemented.
 *
 * <h4>Design</h4>
 * Before the filter does any real work, it needs to gather some information from the HDF5 Library
 * through API calls. The parameters the filter needs are:
 * \li The minimum-bits of the data value
 * \li The number of data elements in the chunk
 * \li The datatype class, size, sign (only for integer type), byte order, and fill value if defined
 *
 * Size and sign are needed to determine what kind of pointer cast to use when retrieving values
 * from the data buffer.
 *
 * The pipeline of the filter can be divided into four parts: (1)pre-compression; (2)compression;
 * (3)decompression; (4)post-decompression.
 *
 * Depending on whether a fill value is defined or not, the filter will handle pre-compression and
 * post-decompression differently.
 *
 * The scale-offset filter only needs the memory byte order, size of datatype, and minimum-bits for
 * compression and decompression.
 *
 * Since decompression has no access to the original data, the minimum-bits and the minimum
 * value need to be stored with the compressed data for decompression and post-decompression.
 *
 * <h4>Integer Type</h4>
 * Pre-compression: During pre-compression minimum-bits is calculated if it is not set by the user.
 * For more information on how minimum-bits are calculated, @see @ref subsubsec_dataset_filters_nbit.
 *
 * If the fill value is defined, finding the maximum and minimum values should ignore the data
 * element whose value is equal to the fill value.
 *
 * If no fill value is defined, the value of each data element is subtracted by the minimum value
 * during this stage.
 *
 * If the fill value is defined, the fill value is assigned to the maximum value. In this way minimum-
 * bits can represent a data element whose value is equal to the fill value and subtracts the
 * minimum value from a data element whose value is not equal to the fill value.
 *
 * The fill value (if defined), the number of elements in a chunk, the class of the datatype, the size
 * of the datatype, the memory order of the datatype, and other similar elements will be stored in
 * the HDF5 object header for the post-decompression usage.
 *
 * After pre-compression, all values are non-negative and are within the range that can be stored by
 * minimum-bits.
 *
 * Compression: All modified data values after pre-compression are packed together into the
 * compressed data buffer. The number of bits for each data value decreases from the number of
 * bits of integer (32 for most platforms) to minimum-bits. The value of minimum-bits and the
 * minimum value are added to the data buffer and the whole buffer is sent back to the library. In
 * this way, the number of bits for each modified value is no more than the size of minimum-bits.
 *
 * Decompression: In this stage, the number of bits for each data value is resumed from minimum-
 * bits to the number of bits of integer.
 *
 * Post-decompression: For the post-decompression stage, the filter does the opposite of what it
 * does during pre-compression except that it does not calculate the minimum-bits or the minimum
 * value. These values were saved during compression and can be retrieved through the resumed
 * data buffer. If no fill value is defined, the filter adds the minimum value back to each data
 * element.
 *
 * If the fill value is defined, the filter assigns the fill value to the data element whose value is equal
 * to the maximum value that minimum-bits can represent and adds the minimum value back to
 * each data element whose value is not equal to the maximum value that minimum-bits can
 * represent.
 *
 * @anchor h4_float_datatype <h4>Floating-point Type</h4>
 * The filter will do data transformation from floating-point type to integer type and then handle the
 * data by using the procedure for handling the integer data inside the filter. Insignificant bits of
 * floating-point data will be cut off during data transformation, so this filter is a lossy compression
 * method.
 *
 * There are two scaling methods: D-scaling and E-scaling. The HDF5 1.8 release only supports D-
 * scaling. D-scaling is short for decimal scaling. E-scaling should be similar conceptually. In order
 * to transform data from floating-point to integer, a scale factor is introduced. The minimum value
 * will be calculated. Each data element value will subtract the minimum value. The modified data
 * will be multiplied by 10 (Decimal) to the power of scale_factor, and only the integer part will be
 * kept and manipulated through the routines for the integer type of the filter during pre-
 * compression and compression. Integer data will be divided by 10 to the power of scale_factor to
 * transform back to floating-point data during decompression and post-decompression. Each data
 * element value will then add the minimum value, and the floating-point data are resumed.
 * However, the resumed data will lose some insignificant bits compared with the original value.
 *
 * For example, the following floating-point data are manipulated by the filter, and the D-scaling
 * factor is 2.
 * <em>{104.561, 99.459, 100.545, 105.644}</em>
 *
 * The minimum value is 99.459, each data element subtracts 99.459, the modified data is
 * <em>{5.102, 0, 1.086, 6.185}</em>
 *
 * Since the D-scaling factor is 2, all floating-point data will be multiplied by 10^2 with this result:
 * <em>{510.2, 0, 108.6, 618.5}</em>
 *
 * The digit after decimal point will be rounded off, and then the set looks like:
 * <em>{510, 0, 109, 619}</em>
 *
 * After decompression, each value will be divided by 10^2 and will be added to the offset 99.459.
 * The floating-point data becomes
 * <em>{104.559, 99.459, 100.549, 105.649}</em>
 *
 * The relative error for each value should be no more than 5* (10^(D-scaling factor +1)).
 * D-scaling sometimes is also referred as a variable minimum-bits method since for different datasets
 * the minimum-bits to represent the same decimal precision will vary. The data value is modified
 * to 2 to power of scale_factor for E-scaling. E-scaling is also called fixed-bits method since for
 * different datasets the minimum-bits will always be fixed to the scale factor of E-scaling.
 * Currently, HDF5 ONLY supports the D-scaling (variable minimum-bits) method.
 *
 * <h4>Implementation</h4>
 * The scale-offset filter implementation was written and included in the file H5Zscaleoffset.c.
 * Function #H5Pset_scaleoffset was written and included in the file “H5Pdcpl.c”. The HDF5 user
 * can supply minimum-bits by calling function #H5Pset_scaleoffset.
 *
 * The scale-offset filter was implemented based on the design outlined in this section. However,
 * the following factors need to be considered:
 * <ol><li>
 * The filter needs the appropriate cast pointer whenever it needs to retrieve data values.
 * </li>
 * <li>
 * The HDF5 Library passes to the filter the to-be-compressed data in the format of the dataset
 * datatype, and the filter passes back the decompressed data in the same format. If a fill value is
 * defined, it is also in dataset datatype format. For example, if the byte order of the dataset data-
 * type is different from that of the memory datatype of the platform, compression or decompression performs
 * an endianness conversion of data buffer. Moreover, it should be aware that
 * memory byte order can be different during compression and decompression.
 * </li>
 * <li>
 * The difference of endianness and datatype between file and memory should be considered
 * when saving and retrieval of minimum-bits, minimum value, and fill value.
 * <li>
 * If the user sets the minimum-bits to full precision of the datatype, no operation is needed at
 * the filter side. If the full precision is a result of calculation by the filter, then the minimum-bits
 * needs to be saved for decompression but no compression or decompression is needed (only a
 * copy of the input buffer is needed).</li>
 * <li>
 * If by calculation of the filter, the minimum-bits is equal to zero, special handling is needed.
 * Since it means all values are the same, no compression or decompression is needed. But the
 * minimum-bits and minimum value still need to be saved during compression.</li>
 * <li>
 * For floating-point data, the minimum value of the dataset should be calculated at first. Each
 * data element value will then subtract the minimum value to obtain the “offset” data. The offset
 * data will then follow the steps outlined above in the discussion of floating-point types to do data
 * transformation to integer and rounding. For more information, @see @ref h4_float_datatype.
 * </li></ol>
 *
 * <h4>Usage Examples</h4>
 * The following code example illustrates the use of the scale-offset filter for writing and reading
 * integer data.
 *
 * <em>Scale-offset compression integer data</em>
 * \code
 *   #include "hdf5.h"
 *   #include "stdlib.h"
 *
 *   #define H5FILE_NAME "scaleoffset_test_int.h5"
 *   #define DATASET_NAME "scaleoffset_int"
 *   #define NX 200
 *   #define NY 300
 *   #define CH_NX 10
 *   #define CH_NY 15
 *   int main(void)
 *   {
 *     hid_t   file, dataspace, dataset, datatype, dset_create_props;
 *     hsize_t dims[2], chunk_size[2];
 *     int     orig_data[NX][NY];
 *     int     new_data[NX][NY];
 *     int     i, j, fill_val;
 *
 *     // Define dataset datatype
 *     datatype = H5Tcopy(H5T_NATIVE_INT);
 *
 *     // Initialize data buffer
 *     for (i=0; i < NX; i++)
 *       for (j=0; j < NY; j++)
 *         orig_data[i][j] = rand() % 10000;
 *
 *     // Describe the size of the array.
 *     dims[0] = NX;
 *     dims[1] = NY;
 *     if((dataspace = H5Screate_simple (2, dims, NULL)) < 0) {
 *       printf("Error: fail to create dataspace\n");
 *       return -1;
 *     }
 *
 *     // Create a new file using read/write access, default file
 *     // creation properties, and default file access properties.
 *     if((file = H5Fcreate (H5FILE_NAME, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
 *       printf("Error: fail to create file\n");
 *       return -1;
 *     }
 *
 *     // Set the dataset creation property list to specify that
 *     // the raw data is to be partitioned into 10 x 15 element
 *     // chunks and that each chunk is to be compressed.
 *     chunk_size[0] = CH_NX;
 *     chunk_size[1] = CH_NY;
 *     if((dset_create_props = H5Pcreate (H5P_DATASET_CREATE)) < 0) {
 *       printf("Error: fail to create dataset property\n");
 *       return -1;
 *     }
 *     if(H5Pset_chunk (dset_create_props, 2, chunk_size) < 0) {
 *       printf("Error: fail to set chunk\n");
 *       return -1;
 *     }
 *
 *     // Set the fill value of dataset
 *     fill_val = 10000;
 *     if (H5Pset_fill_value(dset_create_props, H5T_NATIVE_INT, &fill_val)<0) {
 *       printf("Error: can not set fill value for dataset\n");
 *       return -1;
 *     }
 *
 *     // Set parameters for scale-offset compression. Check the
 *     // description of the H5Pset_scaleoffset function in the
 *     // HDF5 Reference Manual for more information.
 *     if(H5Pset_scaleoffset (dset_create_props, H5Z_SO_INT, H5Z_SO_INT_MINIMUMBITS_DEFAULT) < 0) {
 *       printf("Error: fail to set scaleoffset filter\n");
 *       return -1;
 *     }
 *
 *     // Create a new dataset within the file. The datatype
 *     // and dataspace describe the data on disk, which may
 *     // or may not be different from the format used in the
 *     // application's memory. The link creation and
 *     // dataset access property list parameters are passed
 *     // with default values.
 *     if((dataset = H5Dcreate (file, DATASET_NAME, datatype, dataspace, H5P_DEFAULT,
 *                              dset_create_props, H5P_DEFAULT)) < 0) {
 *       printf("Error: fail to create dataset\n");
 *       return -1;
 *     }
 *
 *     // Write the array to the file. The datatype and dataspace
 *     // describe the format of the data in the 'orig_data' buffer.
 *     // We use default raw data transfer properties.
 *     if(H5Dwrite (dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, orig_data) < 0) {
 *       printf("Error: fail to write to dataset\n");
 *       return -1;
 *     }
 *
 *     H5Dclose (dataset);
 *
 *     if((dataset = H5Dopen(file, DATASET_NAME, H5P_DEFAULT)) < 0) {
 *       printf("Error: fail to open dataset\n");
 *       return -1;
 *     }
 *
 *     // Read the array. This is similar to writing data,
 *     // except the data flows in the opposite direction.
 *     // Note: Decompression is automatic.
 *     if(H5Dread (dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, new_data) < 0) {
 *       printf("Error: fail to read from dataset\n");
 *       return -1;
 *     }
 *
 *     H5Tclose (datatype);
 *     H5Dclose (dataset);
 *     H5Sclose (dataspace);
 *     H5Pclose (dset_create_props);
 *     H5Fclose (file);
 *
 *     return 0;
 *   }
 * \endcode
 *
 * The following code example illustrates the use of the scale-offset filter (set for variable
 * minimum-bits method) for writing and reading floating-point data.
 *
 * <em>Scale-offset compression floating-point data</em>
 * \code
 *   #include "hdf5.h"
 *   #include "stdlib.h"
 *
 *   #define H5FILE_NAME "scaleoffset_test_float_Dscale.h5"
 *   #define DATASET_NAME "scaleoffset_float_Dscale"
 *   #define NX 200
 *   #define NY 300
 *   #define CH_NX 10
 *   #define CH_NY 15
 *
 *   int main(void)
 *   {
 *     hid_t   file, dataspace, dataset, datatype, dset_create_props;
 *     hsize_t dims[2], chunk_size[2];
 *     float   orig_data[NX][NY];
 *     float   new_data[NX][NY];
 *     float   fill_val;
 *     int     i, j;
 *
 *     // Define dataset datatype
 *     datatype = H5Tcopy(H5T_NATIVE_FLOAT);
 *
 *     // Initialize data buffer
 *     for (i=0; i < NX; i++)
 *       for (j=0; j < NY; j++)
 *         orig_data[i][j] = (rand() % 10000) / 1000.0;
 *
 *     // Describe the size of the array.
 *     dims[0] = NX;
 *     dims[1] = NY;
 *     if((dataspace = H5Screate_simple (2, dims, NULL)) < 0) {
 *       printf("Error: fail to create dataspace\n");
 *       return -1;
 *     }
 *
 *     // Create a new file using read/write access, default file
 *     // creation properties, and default file access properties.
 *     if((file = H5Fcreate (H5FILE_NAME, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
 *       printf("Error: fail to create file\n");
 *       return -1;
 *     }
 *
 *     // Set the dataset creation property list to specify that
 *     // the raw data is to be partitioned into 10 x 15 element
 *     // chunks and that each chunk is to be compressed.
 *     chunk_size[0] = CH_NX;
 *     chunk_size[1] = CH_NY;
 *     if((dset_create_props = H5Pcreate (H5P_DATASET_CREATE)) < 0) {
 *      printf("Error: fail to create dataset property\n");
 *      return -1;
 *     }
 *     if(H5Pset_chunk (dset_create_props, 2, chunk_size) < 0) {
 *       printf("Error: fail to set chunk\n");
 *       return -1;
 *     }
 *
 *     // Set the fill value of dataset
 *     fill_val = 10000.0;
 *     if (H5Pset_fill_value(dset_create_props, H5T_NATIVE_FLOAT, &fill_val) < 0) {
 *       printf("Error: can not set fill value for dataset\n");
 *       return -1;
 *     }
 *
 *     // Set parameters for scale-offset compression; use variable
 *     // minimum-bits method, set decimal scale factor to 3. Check
 *     // the description of the H5Pset_scaleoffset function in the
 *     // HDF5 Reference Manual for more information.
 *     if(H5Pset_scaleoffset (dset_create_props, H5Z_SO_FLOAT_DSCALE, 3) < 0) {
 *       printf("Error: fail to set scaleoffset filter\n");
 *       return -1;
 *     }
 *
 *     // Create a new dataset within the file. The datatype
 *     // and dataspace describe the data on disk, which may
 *     // or may not be different from the format used in the
 *     // application's memory.
 *     if((dataset = H5Dcreate (file, DATASET_NAME, datatype, dataspace, H5P_DEFAULT,
 *                              dset_create_props, H5P_DEFAULT)) < 0) {
 *       printf("Error: fail to create dataset\n");
 *       return -1;
 *     }
 *
 *     // Write the array to the file. The datatype and dataspace
 *     // describe the format of the data in the 'orig_data' buffer.
 *     // We use default raw data transfer properties.
 *     if(H5Dwrite (dataset, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, orig_data) < 0) {
 *       printf("Error: fail to write to dataset\n");
 *       return -1;
 *     }
 *
 *     H5Dclose (dataset);
 *
 *     if((dataset = H5Dopen(file, DATASET_NAME, H5P_DEFAULT)) < 0) {
 *       printf("Error: fail to open dataset\n");
 *       return -1;
 *     }
 *
 *     // Read the array. This is similar to writing data,
 *     // except the data flows in the opposite direction.
 *     // Note: Decompression is automatic.
 *     if(H5Dread (dataset, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, new_data) < 0) {
 *       printf("Error: fail to read from dataset\n");
 *       return -1;
 *     }
 *
 *     H5Tclose (datatype);
 *     H5Dclose (dataset);
 *     H5Sclose (dataspace);
 *     H5Pclose (dset_create_props);
 *     H5Fclose (file);
 *
 *     return 0;
 *   }
 * \endcode
 *
 * <h4>Limitations</h4>
 * For floating-point data handling, there are some algorithmic limitations to the GRiB data packing
 * mechanism:
 * <ol><li>
 * Both the E-scaling and D-scaling methods are lossy compression
 * </li>
 * <li>
 * For the D-scaling method, since data values have been rounded to integer values (positive)
 * before truncating to the minimum-bits, their range is limited by the maximum value that can be
 * represented by the corresponding unsigned integer type (the same size as that of the floating-
 * point type)
 * </li></ol>
 *
 * <h4>Suggestions</h4>
 * The following are some suggestions for using the filter for floating-point data:
 * <ol><li>
 * It is better to convert the units of data so that the units are within certain common range (for
 * example, 1200m to 1.2km)
 * </li>
 * <li>
 * If data values to be compressed are very near to zero, it is strongly recommended that the
 * user sets the fill value away from zero (for example, a large positive number); if the user does
 * nothing, the HDF5 library will set the fill value to zero, and this may cause undesirable
 * compression results
 * </li>
 * <li>
 * Users are not encouraged to use a very large decimal scale factor (for example, 100) for the
 * D-scaling method; this can cause the filter not to ignore the fill value when finding maximum
 * and minimum values, and they will get a much larger minimum-bits (poor compression)
 * </li></ol>
 *
 * \subsubsection subsubsec_dataset_filters_szip Using the Szip Filter
 * See The HDF Group website for further information regarding the Szip filter.
 *
 * Previous Chapter \ref sec_group - Next Chapter \ref sec_datatype
 *
 */

/**
 * \defgroup H5D Datasets (H5D)
 *
 * Use the functions in this module to manage HDF5 datasets, including the
 * transfer of data between memory and disk and the description of dataset
 * properties. Datasets are used by other HDF5 APIs and referenced either by
 * name or by a handle. Such handles can be obtained by either creating or
 * opening the dataset.
 *
 * Typical stages in the HDF5 dataset life cycle are shown below in introductory
 * examples.
 *
 * <table>
 * <tr><th>Create</th><th>Read</th></tr>
 * <tr valign="top">
 *   <td>
 *   \snippet{lineno} H5D_examples.c create
 *   </td>
 *   <td>
 *   \snippet{lineno} H5D_examples.c read
 *   </td>
 * <tr><th>Update</th><th>Delete</th></tr>
 * <tr valign="top">
 *   <td>
 *   \snippet{lineno} H5D_examples.c update
 *   </td>
 *   <td>
 *   \snippet{lineno} H5D_examples.c delete
 *   </td>
 * </tr>
 * </table>
 *
 */

#endif /* H5Dmodule_H */
