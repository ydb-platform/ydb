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
 * Purpose:    This file contains declarations which define macros for the
 *        H5 package.  Including this header means that the source file
 *        is part of the H5 package.
 */
#ifndef H5module_H
#define H5module_H

/* Define the proper control macros for the generic FUNC_ENTER/LEAVE and error
 *      reporting macros.
 */
#define H5_MODULE
#define H5_MY_PKG     H5
#define H5_MY_PKG_ERR H5E_LIB

/** \page H5DM_UG The HDF5 Data Model and File Structure
 *
 * \section sec_data_model The HDF5 Data Model and File Structure
 * \subsection subsec_data_model_intro Introduction
 * The Hierarchical Data Format (HDF) implements a model for managing and storing data. The
 * model includes an abstract data model and an abstract storage model (the data format), and
 * libraries to implement the abstract model and to map the storage model to different storage
 * mechanisms. The HDF5 library provides a programming interface to a concrete implementation
 * of the abstract models. The library also implements a model of data transfer, an efficient
 * movement of data from one stored representation to another stored representation. The figure
 * below illustrates the relationships between the models and implementations. This chapter
 * explains these models in detail.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Dmodel_fig1.gif "HDF5 models and implementations"
 * </td>
 * </tr>
 * </table>
 *
 * The <em>Abstract Data Model</em> is a conceptual model of data, data types, and data organization. The
 * abstract data model is independent of storage medium or programming environment. The
 * <em>Storage Model</em> is a standard representation for the objects of the abstract data model. The
 * <a href="https://docs.hdfgroup.org/hdf5/develop/_s_p_e_c.html">HDF5 File Format Specification</a>
 * defines the storage model.
 *
 * The <em>Programming Model</em> is a model of the computing environment and includes platforms from
 * small single systems to large multiprocessors and clusters. The programming model manipulates
 * (instantiates, populates, and retrieves) objects from the abstract data model.
 *
 * The <em>Library</em> is the concrete implementation of the programming model. The library exports the
 * HDF5 APIs as its interface. In addition to implementing the objects of the abstract data model,
 * the library manages data transfers from one stored form to another. Data transfer examples
 * include reading from disk to memory and writing from memory to disk.
 *
 * <em>Stored Data</em> is the concrete implementation of the storage model. The <em>Storage Model</em>
 * is mapped to several storage mechanisms including single disk files, multiple files (family of files),
 * and memory representations.
 *
 * The HDF5 library is a C module that implements the programming model and abstract data
 * model. The HDF5 library calls the operating system or other storage management software (for
 * example, the MPI/IO Library) to store and retrieve persistent data. The HDF5 library may also
 * link to other software such as filters for compression. The HDF5 library is linked to an
 * application program which may be written in C, C++, Fortran, or Java. The application program
 * implements problem specific algorithms and data structures and calls the HDF5 library to store
 * and retrieve data. The figure below shows the dependencies of these modules.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Dmodel_fig2.gif "The library, the application program, and other modules"
 * </td>
 * </tr>
 * </table>
 *
 * It is important to realize that each of the software components manages data using models and
 * data structures that are appropriate to the component. When data is passed between layers
 * (during storage or retrieval), it is transformed from one representation to another. The figure
 * below suggests some of the kinds of data structures used in the different layers.
 *
 * The <em>Application Program</em> uses data structures that represent the problem and algorithms
 * including variables, tables, arrays, and meshes among other data structures. Depending on its
 * design and function, an application may have quite a few different kinds of data structures and
 * different numbers and sizes of objects.
 *
 * The <em>HDF5 Library</em> implements the objects of the HDF5 abstract data model. Some of these
 * objects include groups, datasets, and attributes. The application program maps the application
 * data structures to a hierarchy of HDF5 objects. Each application will create a mapping best
 * suited to its purposes.
 *
 * The objects of the HDF5 abstract data model are mapped to the objects of the HDF5 storage
 * model, and stored in a storage medium. The stored objects include header blocks, free lists, data
 * blocks, B-trees, and other objects. Each group or dataset is stored as one or more header and data
 * blocks.
 * @see <a href="https://docs.hdfgroup.org/hdf5/develop/_s_p_e_c.html">HDF5 File Format Specification</a>
 * for more information on how these objects are organized. The HDF5 library can also use other
 * libraries and modules such as compression.
 *
 * <table>
 * <caption>Data structures in different layers</caption>
 * <tr>
 * <td>
 * \image html Dmodel_fig3_a.gif
 * </td>
 * <td>
 * \image html Dmodel_fig2.gif
 * </td>
 * <td>
 * \image html Dmodel_fig3_c.gif
 * </td>
 * </tr>
 * </table>
 *
 * The important point to note is that there is not necessarily any simple correspondence between
 * the objects of the application program, the abstract data model, and those of the Format
 * Specification. The organization of the data of application program, and how it is mapped to the
 * HDF5 abstract data model is up to the application developer. The application program only
 * needs to deal with the library and the abstract data model. Most applications need not consider
 * any details of the
 * <a href="https://docs.hdfgroup.org/hdf5/develop/_s_p_e_c.html">HDF5 File Format Specification</a>
 * or the details of how objects of abstract data model are translated to and from storage.
 *
 * \subsection subsec_data_model_abstract The Abstract Data Model
 * The abstract data model (ADM) defines concepts for defining and describing complex data
 * stored in files. The ADM is a very general model which is designed to conceptually cover many
 * specific models. Many different kinds of data can be mapped to objects of the ADM, and
 * therefore stored and retrieved using HDF5. The ADM is not, however, a model of any particular
 * problem or application domain. Users need to map their data to the concepts of the ADM.
 *
 * The key concepts include:
 * <ul><li>@ref subsubsec_data_model_abstract_file - a contiguous string of bytes in a computer
 * store (memory, disk, etc.), and the bytes represent zero or more objects of the model</li>
 * <li>@ref subsubsec_data_model_abstract_group - a collection of objects (including groups)</li>
 * <li>@ref subsubsec_data_model_abstract_dataset - a multidimensional array of data elements with
 * attributes and other metadata</li>
 * <li>@ref subsubsec_data_model_abstract_space - a description of the dimensions of a multidimensional
 * array</li>
 * <li>@ref subsubsec_data_model_abstract_type - a description of a specific class of data element
 * including its storage layout as a pattern of bits</li>
 * <li>@ref subsubsec_data_model_abstract_attr - a named data value associated with a group,
 * dataset, or named datatype</li>
 * <li>@ref subsubsec_data_model_abstract_plist - a collection of parameters (some permanent and
 * some transient) controlling options in the library</li>
 * <li>@ref subsubsec_data_model_abstract_link - the way objects are connected</li></ul>
 *
 * These key concepts are described in more detail below.
 *
 * \subsubsection subsubsec_data_model_abstract_file File
 * Abstractly, an HDF5 file is a container for an organized collection of objects. The objects are
 * groups, datasets, and other objects as defined below. The objects are organized as a rooted,
 * directed graph. Every HDF5 file has at least one object, the root group. See the figure below. All
 * objects are members of the root group or descendants of the root group.
 *
 * <table>
 * <caption>The HDF5 file</caption>
 * <tr>
 * <td>
 * \image html Dmodel_fig4_b.gif
 * </td>
 * </tr>
 * <tr>
 * <td>
 * \image html Dmodel_fig4_a.gif
 * </td>
 * </tr>
 * </table>
 *
 * HDF5 objects have a unique identity within a single HDF5 file and can be accessed only by their
 * names within the hierarchy of the file. HDF5 objects in different files do not necessarily have
 * unique identities, and it is not possible to access a permanent HDF5 object except through a file.
 * For more information, see \ref subsec_data_model_structure.
 *
 * When the file is created, the file creation properties specify settings for the file. The file creation
 * properties include version information and parameters of global data structures. When the file is
 * opened, the file access properties specify settings for the current access to the file. File access
 * properties include parameters for storage drivers and parameters for caching and garbage
 * collection. The file creation properties are set permanently for the life of the file, and the file
 * access properties can be changed by closing and reopening the file.
 *
 * An HDF5 file can be “mounted” as part of another HDF5 file. This is analogous to Unix file
 * system mounts. The root of the mounted file is attached to a group in the mounting file, and all
 * the contents can be accessed as if the mounted file were part of the mounting file.
 *
 * @see @ref sec_file.
 *
 * \subsubsection subsubsec_data_model_abstract_group Group
 * An HDF5 group is analogous to a file system directory. Abstractly, a group contains zero or
 * more objects, and every object must be a member of at least one group. The root group is a
 * special case; it may not be a member of any group.
 *
 * Group membership is actually implemented via link objects. See the figure below. A link object
 * is owned by a group and points to a named object. Each link has a name, and each link points to
 * exactly one object. Each named object has at least one and possibly many links to it.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Dmodel_fig5.gif "Group membership via link objects"
 * </td>
 * </tr>
 * </table>
 *
 * There are three classes of named objects: group, dataset, and committed (named) datatype. See
 * the figure below. Each of these objects is the member of at least one group, and this means there
 * is at least one link to it.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Dmodel_fig6.gif "Classes of named objects"
 * </td>
 * </tr>
 * </table>
 *
 * @see @ref sec_group.
 *
 * \subsubsection subsubsec_data_model_abstract_dataset Dataset
 * An HDF5 dataset is a multidimensional (rectangular) array of data elements. See the figure
 * below. The shape of the array (number of dimensions, size of each dimension) is described by
 * the dataspace object (described in the next section below).
 *
 * A data element is a single unit of data which may be a number, a character, an array of numbers
 * or characters, or a record of heterogeneous data elements. A data element is a set of bits. The
 * layout of the bits is described by the datatype (see below).
 *
 * The dataspace and datatype are set when the dataset is created, and they cannot be changed for
 * the life of the dataset. The dataset creation properties are set when the dataset is created. The
 * dataset creation properties include the fill value and storage properties such as chunking and
 * compression. These properties cannot be changed after the dataset is created.
 *
 * The dataset object manages the storage and access to the data. While the data is conceptually a
 * contiguous rectangular array, it is physically stored and transferred in different ways depending
 * on the storage properties and the storage mechanism used. The actual storage may be a set of
 * compressed chunks, and the access may be through different storage mechanisms and caches.
 * The dataset maps between the conceptual array of elements and the actual stored data.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Dmodel_fig7_b.gif "The dataset"
 * </td>
 * </tr>
 * </table>
 *
 * @see @ref sec_dataset.
 *
 * \subsubsection subsubsec_data_model_abstract_space Dataspace
 * The HDF5 dataspace describes the layout of the elements of a multidimensional array.
 * Conceptually, the array is a hyper-rectangle with one to 32 dimensions. HDF5 dataspaces can be
 * extendable. Therefore, each dimension has a current size and a maximum size, and the maximum
 * may be unlimited. The dataspace describes this hyper-rectangle: it is a list of dimensions with
 * the current and maximum (or unlimited) sizes. See the figure below.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Dmodel_fig8.gif "The dataspace"
 * </td>
 * </tr>
 * </table>
 *
 * Dataspace objects are also used to describe hyperslab selections from a dataset. Any subset of the
 * elements of a dataset can be selected for read or write by specifying a set of hyperslabs. A
 * non-rectangular region can be selected by the union of several (rectangular) dataspaces.
 *
 * @see @ref sec_dataspace.
 *
 * \subsubsection subsubsec_data_model_abstract_type Datatype
 * The HDF5 datatype object describes the layout of a single data element. A data element is a
 * single element of the array; it may be a single number, a character, an array of numbers or
 * carriers, or other data. The datatype object describes the storage layout of this data.
 *
 * Data types are categorized into 11 classes of datatype. Each class is interpreted according to a set
 * of rules and has a specific set of properties to describe its storage. For instance, floating point
 * numbers have exponent position and sizes which are interpreted according to appropriate
 * standards for number representation. Thus, the datatype class tells what the element means, and
 * the datatype describes how it is stored.
 *
 * The figure below shows the classification of datatypes. Atomic datatypes are indivisible. Each
 * may be a single object such as a number or a string. Composite datatypes are composed of
 * multiple elements of atomic datatypes. In addition to the standard types, users can define
 * additional datatypes such as a 24-bit integer or a 16-bit float.
 * A dataset or attribute has a single datatype object associated with it. See Figure 7 above. The
 * datatype object may be used in the definition of several objects, but by default, a copy of the
 * datatype object will be private to the dataset.
 *
 * Optionally, a datatype object can be stored in the HDF5 file. The datatype is linked into a group,
 * and therefore given a name. A committed datatype (formerly called a named datatype) can be
 * opened and used in any way that a datatype object can be used.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Dmodel_fig9.gif "Datatype classifications"
 * </td>
 * </tr>
 * </table>
 *
 * @see @ref sec_datatype.
 *
 * \subsubsection subsubsec_data_model_abstract_attr Attribute
 * Any HDF5 named data object (group, dataset, or named datatype) may have zero or more user
 * defined attributes. Attributes are used to document the object. The attributes of an object are
 * stored with the object.
 *
 * An HDF5 attribute has a name and data. The data portion is similar in structure to a dataset: a
 * dataspace defines the layout of an array of data elements, and a datatype defines the storage
 * layout and interpretation of the elements See the figure below.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Dmodel_fig10.gif "Attribute data elements"
 * </td>
 * </tr>
 * </table>
 *
 * In fact, an attribute is very similar to a dataset with the following limitations:
 * <ul><li>An attribute can only be accessed via the object</li>
 * <li>Attribute names are significant only within the object</li>
 * <li>An attribute should be a small object</li>
 * <li>The data of an attribute must be read or written in a single access (partial reading or
 * writing is not allowed)</li>
 * <li>Attributes do not have attributes</li></ul>
 *
 * Note that the value of an attribute can be an object reference. A shared attribute or an attribute
 * that is a large array can be implemented as a reference to a dataset.
 *
 * The name, dataspace, and datatype of an attribute are specified when it is created and cannot be
 * changed over the life of the attribute. An attribute can be opened by name, by index, or by
 * iterating through all the attributes of the object.
 *
 * @see @ref sec_attribute.
 *
 * \subsubsection subsubsec_data_model_abstract_plist Property List
 * HDF5 has a generic property list object. Each list is a collection of name-value pairs. Each class
 * of property list has a specific set of properties. Each property has an implicit name, a datatype,
 * and a value. See the figure below. A property list object is created and used in ways similar to
 * the other objects of the HDF5 library.
 *
 * Property Lists are attached to the object in the library, and they can be used by any part of the
 * library. Some properties are permanent (for example, the chunking strategy for a dataset), others
 * are transient (for example, buffer sizes for data transfer). A common use of a Property List is to
 * pass parameters from the calling program to a VFL driver or a module of the pipeline.
 *
 * Property lists are conceptually similar to attributes. Property lists are information relevant to the
 * behavior of the library while attributes are relevant to the user's data and application.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Dmodel_fig11_b.gif "The property list"
 * </td>
 * </tr>
 * </table>
 *
 * Property lists are used to control optional behavior for file creation, file access, dataset creation,
 * dataset transfer (read, write), and file mounting. Some property list classes are shown in the table
 * below. Details of the different property lists are explained in the relevant sections of this
 * document.
 *
 * <table>
 * <caption>Property list classes and their usage</caption>
 * <tr>
 * <th>Property List Class</th>
 * <th>Used</th>
 * <th>Examples</th>
 * </tr>
 * <tr>
 * <td>#H5P_FILE_CREATE</td>
 * <td>Properties for file creation.</td>
 * <td>Set size of user block.</td>
 * </tr>
 * <tr>
 * <td>#H5P_FILE_ACCESS</td>
 * <td>Properties for file access.</td>
 * <td>Set parameters for VFL driver. An example is MPI I/O. </td>
 * </tr>
 * <tr>
 * <td>#H5P_DATASET_CREATE</td>
 * <td>Properties for dataset creation.</td>
 * <td>Set chunking, compression, or fill value.</td>
 * </tr>
 * <tr>
 * <td>#H5P_DATASET_XFER</td>
 * <td>Properties for raw data transfer (read and write).</td>
 * <td>Tune buffer sizes or memory management.</td>
 * </tr>
 * <tr>
 * <td>#H5P_FILE_MOUNT</td>
 * <td>Properties for file mounting.</td>
 * <td></td>
 * </tr>
 * </table>
 *
 * @see @ref sec_plist.
 *
 * \subsubsection subsubsec_data_model_abstract_link Link
 * This section is under construction.
 *
 * \subsection subsec_data_model_storage The HDF5 Storage Model
 * \subsubsection subsubsec_data_model_storage_spec The Abstract Storage Model: the HDF5 Format Specification
 * The <a href="https://docs.hdfgroup.org/hdf5/develop/_s_p_e_c.html">HDF5 File Format Specification</a>
 * defines how HDF5 objects and data are mapped to a linear
 * address space. The address space is assumed to be a contiguous array of bytes stored on some
 * random access medium. The format defines the standard for how the objects of the abstract data
 * model are mapped to linear addresses. The stored representation is self-describing in the sense
 * that the format defines all the information necessary to read and reconstruct the original objects
 * of the abstract data model.
 *
 * The HDF5 File Format Specification is organized in three parts:
 * <ul><li>Level 0: File signature and super block</li>
 * <li>Level 1: File infrastructure</li>
 * <ul><li>Level 1A: B-link trees and B-tree nodes</li>
 * <li>Level 1B: Group</li>
 * <li>Level 1C: Group entry</li>
 * <li>Level 1D: Local heaps</li>
 * <li>Level 1E: Global heap</li>
 * <li>Level 1F: Free-space index</li></ul>
 * <li>Level 2: Data object</li>
 * <ul><li>Level 2A: Data object headers</li>
 * <li>Level 2B: Shared data object headers</li>
 * <li>Level 2C: Data object data storage</li></ul></ul>
 *
 * The Level 0 specification defines the header block for the file. Header block elements include a
 * signature, version information, key parameters of the file layout (such as which VFL file drivers
 * are needed), and pointers to the rest of the file. Level 1 defines the data structures used
 * throughout the file: the B-trees, heaps, and groups. Level 2 defines the data structure for storing
 * the data objects and data. In all cases, the data structures are completely specified so that every
 * bit in the file can be faithfully interpreted.
 *
 * It is important to realize that the structures defined in the HDF5 file format are not the same as
 * the abstract data model: the object headers, heaps, and B-trees of the file specification are not
 * represented in the abstract data model. The format defines a number of objects for managing the
 * storage including header blocks, B-trees, and heaps. The HDF5 File Format Specification defines
 * how the abstract objects (for example, groups and datasets) are represented as headers, B-tree
 * blocks, and other elements.
 *
 * The HDF5 library implements operations to write HDF5 objects to the linear format and to read
 * from the linear format to create HDF5 objects. It is important to realize that a single HDF5
 * abstract object is usually stored as several objects. A dataset, for example, might be stored in a
 * header and in one or more data blocks, and these objects might not be contiguous on the hard
 * disk.
 *
 * \subsubsection subsubsec_data_model_storage_imple Concrete Storage Model
 * The HDF5 file format defines an abstract linear address space. This can be implemented in
 * different storage media such as a single file or multiple files on disk or in memory. The HDF5
 * Library defines an open interface called the Virtual File Layer (VFL). The VFL allows different
 * concrete storage models to be selected.
 *
 * The VFL defines an abstract model, an API for random access storage, and an API to plug in
 * alternative VFL driver modules. The model defines the operations that the VFL driver must and
 * may support, and the plug-in API enables the HDF5 library to recognize the driver and pass it
 * control and data.
 *
 * A number of VFL drivers have been defined in the HDF5 library. Some work with a single file,
 * and some work with multiple files split in various ways. Some work in serial computing
 * environments, and some work in parallel computing environments. Most work with disk copies
 * of HDF5 files, but one works with a memory copy. These drivers are listed in the
 * \ref table_file_drivers "Supported file drivers" table.
 *
 * @see @ref subsec_file_alternate_drivers.
 *
 * Each driver isolates the details of reading and writing storage so that the rest of the HDF5 library
 * and user program can be almost the same for different storage methods. The exception to this
 * rule is that some VFL drivers need information from the calling application. This information is
 * passed using property lists. For example, the Parallel driver requires certain control information
 * that must be provided by the application.
 *
 * \subsection subsec_data_model_structure The Structure of an HDF5 File
 * \subsubsection subsubsec_data_model_structure_file Overall File Structure
 * An HDF5 file is organized as a rooted, directed graph. Named data objects are the nodes of the
 * graph, and links are the directed arcs. Each arc of the graph has a name, and the root group has
 * the name “/”. Objects are created and then inserted into the graph with the link operation which
 * creates a named link from a group to the object. For example, the figure below illustrates the
 * structure of an HDF5 file when one dataset is created. An object can be the target of more than
 * one link. The names on the links must be unique within each group, but there may be many links
 * with the same name in different groups. Link names are unambiguous: some ancestor will have a
 * different name, or they are the same object. The graph is navigated with path names similar to
 * Unix file systems. An object can be opened with a full path starting at the root group or with a
 * relative path and a starting node (group). Note that all paths are relative to a single HDF5 file. In
 * this sense, an HDF5 file is analogous to a single Unix file system.
 *
 * <table>
 * <caption>An HDF5 file with one dataset</caption>
 * <tr>
 * <td>
 * \image html Dmodel_fig12_a.gif
 * </td>
 * <td>
 * \image html Dmodel_fig12_b.gif
 * </td>
 * </tr>
 * </table>
 *
 * Note: In the figure above are two figures. The top figure represents a newly created file with one
 * group, /. In the bottom figure, a dataset called /dset1 has been created.
 *
 * It is important to note that, just like the Unix file system, HDF5 objects do not have names. The
 * names are associated with paths. An object has a unique (within the file) object identifier, but a
 * single object may have many names because there may be many paths to the same object. An
 * object can be renamed (moved to another group) by adding and deleting links. In this case, the
 * object itself never moves. For that matter, membership in a group has no implication for the
 * physical location of the stored object.
 *
 * Deleting a link to an object does not necessarily delete the object. The object remains available
 * as long as there is at least one link to it. After all the links to an object are deleted, it can no
 * longer be opened although the storage may or may not be reclaimed.
 *
 * It is important to realize that the linking mechanism can be used to construct very complex
 * graphs of objects. For example, it is possible for an object to be shared between several groups
 * and even to have more than one name in the same group. It is also possible for a group to be a
 * member of itself or to be in a “cycle” in the graph. An example of a cycle is where a child is the
 * parent of one of its own ancestors.
 *
 * \subsubsection subsubsec_data_model_structure_path HDF5 Path Names and Navigation
 * The structure of the file constitutes the name space for the objects in the file. A path name is a
 * string of components separated by ‘/’. Each component is the name of a link or the special
 * character “.” for the current group. Link names (components) can be any string of ASCII
 * characters not containing ‘/’ (except the string “.” which is reserved). However, users are advised
 * to avoid the use of punctuation and non-printing characters because they may create problems for
 * other software. The figure below gives a BNF grammar for HDF5 path names.
 *
 * <em>A BNF grammar for path names</em>
 * \code
 *   PathName ::= AbsolutePathName | RelativePathName
 *   Separator ::= "/" ["/"]*
 *   AbsolutePathName ::= Separator [ RelativePathName ]
 *   RelativePathName ::= Component [ Separator RelativePathName ]*
 *   Component ::= "." | Name
 *   Name ::= Character+ - {"."}
 *   Character ::= {c: c in {{ legal ASCII characters } - {'/'}}
 * \endcode
 *
 * An object can always be addressed by a full or absolute path which would start at the root group.
 * As already noted, a given object can have more than one full path name. An object can also be
 * addressed by a relative path which would start at a group and include the path to the object.
 *
 * The structure of an HDF5 file is “self-describing.” This means that it is possible to navigate the
 * file to discover all the objects in the file. Basically, the structure is traversed as a graph starting at
 * one node and recursively visiting the nodes of the graph.
 *
 * \subsubsection subsubsec_data_model_structure_example Examples of HDF5 File Structures
 * The figures below show some possible HDF5 file structures with groups and datasets. The first
 * figure shows the structure of a file with three groups. The second shows a dataset created in
 * “/group1”. The third figure shows the structure after a dataset called dset2 has been added to the
 * root group. The fourth figure shows the structure after another group and dataset have been
 * added.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Dmodel_fig14_a.gif "An HDF5 file structure with groups"
 * </td>
 * </tr>
 * </table>
 *
 * Note: The figure above shows three groups; /group1 and /group2 are members of the root group.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Dmodel_fig14_b.gif "An HDF5 file structure with groups and a dataset"
 * </td>
 * </tr>
 * </table>
 *
 * Note: The figure above shows that a dataset has been created in /group1: /group1/dset1.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Dmodel_fig14_c.gif " An HDF5 file structure with groups and datasets"
 * </td>
 * </tr>
 * </table>
 *
 * Note: In the figure above, another dataset has been added as a member of the root group: /dset2.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Dmodel_fig14_c.gif " Another HDF5 file structure with groups and datasets"
 * </td>
 * </tr>
 * </table>
 *
 * Note: In the figure above, another group and dataset have been added reusing object names:
 * <em>/group2/group2/dset2</em>.
 * <ol><li>HDF5 requires random access to the linear address space. For this reason it is not
 * well suited for some data media such as streams.</li>
 * <li>It could be said that HDF5 extends the organizing concepts of a file system to the internal
 * structure of a single file.</li>
 * <li>As of HDF5-1.4, the storage used for an object is reclaimed, even if all links are
 * deleted.</li></ol>
 *
 * Next Chapter \ref sec_program
 *
 */

/** \page H5_UG The HDF5 Library and Programming Model
 *
 * \section sec_program The HDF5 Library and Programming Model
 * \subsection subsec_program_intro Introduction
 * The HDF5 library implements the HDF5 abstract data model and storage model. These models
 * were described in the preceding chapter.
 *
 * Two major objectives of the HDF5 products are to provide tools that can be used on as many
 * computational platforms as possible (portability), and to provide a reasonably object-oriented
 * data model and programming interface.
 *
 * To be as portable as possible, the HDF5 library is implemented in portable C. C is not an
 * object-oriented language, but the library uses several mechanisms and conventions to implement an
 * object model.
 *
 * One mechanism the HDF5 library uses is to implement the objects as data structures. To refer to
 * an object, the HDF5 library implements its own pointers. These pointers are called identifiers.
 * An identifier is then used to invoke operations on a specific instance of an object. For example,
 * when a group is opened, the API returns a group identifier. This identifier is a reference to that
 * specific group and will be used to invoke future operations on that group. The identifier is valid
 * only within the context it is created and remains valid until it is closed or the file is closed. This
 * mechanism is essentially the same as the mechanism that C++ or other object-oriented languages
 * use to refer to objects except that the syntax is C.
 *
 * Similarly, object-oriented languages collect all the methods for an object in a single name space.
 * An example is the methods of a C++ class. The C language does not have any such mechanism,
 * but the HDF5 library simulates this through its API naming convention. API function names
 * begin with a common prefix that is related to the class of objects that the function operates on.
 * The table below lists the HDF5 objects and the standard prefixes used by the corresponding
 * HDF5 APIs. For example, functions that operate on datatype objects all have names beginning
 * with H5T.
 *
 * <table>
 * <caption>Access flags and modes</caption>
 * <tr>
 * <th>Prefix</th>
 * <th>Operates on</th>
 * </tr>
 * <tr>
 * <td>@ref H5A</td>
 * <td>Attributes</td>
 * </tr>
 * <tr>
 * <td>@ref H5D</td>
 * <td>Datasets</td>
 * </tr>
 * <tr>
 * <td>@ref H5E</td>
 * <td>Error reports</td>
 * </tr>
 * <tr>
 * <td>@ref H5F</td>
 * <td>Files</td>
 * </tr>
 * <tr>
 * <td>@ref H5G</td>
 * <td>Groups</td>
 * </tr>
 * <tr>
 * <td>@ref H5I</td>
 * <td>Identifiers</td>
 * </tr>
 * <tr>
 * <td>@ref H5L</td>
 * <td>Links</td>
 * </tr>
 * <tr>
 * <td>@ref H5O</td>
 * <td>Objects</td>
 * </tr>
 * <tr>
 * <td>@ref H5P</td>
 * <td>Property lists</td>
 * </tr>
 * <tr>
 * <td>@ref H5R</td>
 * <td>References</td>
 * </tr>
 * <tr>
 * <td>@ref H5S</td>
 * <td>Dataspaces</td>
 * </tr>
 * <tr>
 * <td>@ref H5T</td>
 * <td>Datatypes</td>
 * </tr>
 * <tr>
 * <td>@ref H5Z</td>
 * <td>Filters</td>
 * </tr>
 * </table>
 *
 * \subsection subsec_program_model The HDF5 Programming Model
 * In this section we introduce the HDF5 programming model by means of a series of short code
 * samples. These samples illustrate a broad selection of common HDF5 tasks. More details are
 * provided in the following chapters and in the HDF5 Reference Manual.
 *
 * \subsubsection subsubsec_program_model_create Creating an HDF5 File
 * Before an HDF5 file can be used or referred to in any manner, it must be explicitly created or
 * opened. When the need for access to a file ends, the file must be closed. The example below
 * provides a C code fragment illustrating these steps. In this example, the values for the file
 * creation property list and the file access property list are set to the defaults #H5P_DEFAULT.
 *
 * <em>Creating and closing an HDF5 file</em>
 * \code
 *   hid_t file; // declare file identifier
 *
 *   // Create a new file using H5F_ACC_TRUNC to truncate and overwrite
 *   // any file of the same name, default file creation properties, and
 *   // default file access properties. Then close the file.
 *   file = H5Fcreate(FILE, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
 *   status = H5Fclose(file);
 * \endcode
 *
 * Note: If there is a possibility that a file of the declared name already exists and you wish to open
 * a new file regardless of that possibility, the flag #H5F_ACC_TRUNC will cause the operation to
 * overwrite the previous file. If the operation should fail in such a circumstance, use the flag
 * #H5F_ACC_EXCL instead.
 *
 * \subsubsection subsubsec_program_model_dset Creating and Initializing a Dataset
 * The essential objects within a dataset are datatype and dataspace. These are independent objects
 * and are created separately from any dataset to which they may be attached. Hence, creating a
 * dataset requires, at a minimum, the following steps:
 * <ol><li>Create and initialize a dataspace for the dataset</li>
 * <li>Define a datatype for the dataset</li>
 * <li>Create and initialize the dataset</li></ol>
 *
 * The code in the example below illustrates the execution of these steps.
 *
 * <em>Create a dataset</em>
 * \code
 *   hid_t dataset, datatype, dataspace; // declare identifiers
 *
 *   // Create a dataspace: Describe the size of the array and
 *   // create the dataspace for a fixed-size dataset.
 *   dimsf[0] = NX;
 *   dimsf[1] = NY;
 *   dataspace = H5Screate_simple(RANK, dimsf, NULL);
 *
 *   // Define a datatype for the data in the dataset.
 *   // We will store little endian integers.
 *   datatype = H5Tcopy(H5T_NATIVE_INT);
 *   status = H5Tset_order(datatype, H5T_ORDER_LE);
 *
 *   // Create a new dataset within the file using the defined
 *   // dataspace and datatype and default dataset creation
 *   // properties.
 *   // NOTE: H5T_NATIVE_INT can be used as the datatype if
 *   // conversion to little endian is not needed.
 *   dataset = H5Dcreate(file, DATASETNAME, datatype, dataspace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
 * \endcode
 *
 * \subsubsection subsubsec_program_model_close Closing an Object
 * An application should close an object such as a datatype, dataspace, or dataset once the object is
 * no longer needed. Since each is an independent object, each must be released (or closed)
 * separately. This action is frequently referred to as releasing the object's identifier. The code in
 * the example below closes the datatype, dataspace, and dataset that were created in the preceding
 * section.
 *
 * <em>Close an object</em>
 * \code
 *   H5Tclose(datatype);
 *   H5Dclose(dataset);
 *   H5Sclose(dataspace);
 * \endcode
 *
 * There is a long list of HDF5 library items that return a unique identifier when the item is created
 * or opened. Each time that one of these items is opened, a unique identifier is returned. Closing a
 * file does not mean that the groups, datasets, or other open items are also closed. Each opened
 * item must be closed separately.
 *
 * For more information,
 * @see <a href="http://www.hdfgroup.org/HDF5/doc/Advanced/UsingIdentifiers/index.html">Using Identifiers</a>
 * in the HDF5 Application Developer's Guide under General Topics in HDF5.
 *
 * <h4>How Closing a File Effects Other Open Structural Elements</h4>
 * Every structural element in an HDF5 file can be opened, and these elements can be opened more
 * than once. Elements range in size from the entire file down to attributes. When an element is
 * opened, the HDF5 library returns a unique identifier to the application. Every element that is
 * opened must be closed. If an element was opened more than once, each identifier that was
 * returned to the application must be closed. For example, if a dataset was opened twice, both
 * dataset identifiers must be released (closed) before the dataset can be considered closed. Suppose
 * an application has opened a file, a group in the file, and two datasets in the group. In order for
 * the file to be totally closed, the file, group, and datasets must each be closed. Closing the file
 * before the group or the datasets will not affect the state of the group or datasets: the group and
 * datasets will still be open.
 *
 * There are several exceptions to the above general rule. One is when the #H5close function is used.
 * #H5close causes a general shutdown of the library: all data is written to disk, all identifiers are
 * closed, and all memory used by the library is cleaned up. Another exception occurs on parallel
 * processing systems. Suppose on a parallel system an application has opened a file, a group in the
 * file, and two datasets in the group. If the application uses the #H5Fclose function to close the file,
 * the call will fail with an error. The open group and datasets must be closed before the file can be
 * closed. A third exception is when the file access property list includes the property
 * #H5F_CLOSE_STRONG. This property closes any open elements when the file is closed with
 * #H5Fclose. For more information, see the #H5Pset_fclose_degree function in the HDF5 Reference
 * Manual.
 *
 * \subsubsection subsubsec_program_model_data Writing or Reading a Dataset to or from a File
 * Having created the dataset, the actual data can be written with a call to #H5Dwrite. See the
 * example below.
 *
 * <em>Writing a dataset</em>
 * \code
 *   // Write the data to the dataset using default transfer properties.
 *   status = H5Dwrite(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, data);
 * \endcode
 *
 * Note that the third and fourth #H5Dwrite parameters in the above example describe the
 * dataspaces in memory and in the file, respectively. For now, these are both set to
 * #H5S_ALL which indicates that the entire dataset is to be written. The selection of partial datasets
 * and the use of differing dataspaces in memory and in storage will be discussed later in this
 * chapter and in more detail elsewhere in this guide.
 *
 * Reading the dataset from storage is similar to writing the dataset to storage. To read an entire
 * dataset, substitute #H5Dread for #H5Dwrite in the above example.
 *
 * \subsubsection subsubsec_program_model_partial Reading and Writing a Portion of a Dataset
 * The previous section described writing or reading an entire dataset. HDF5 also supports access to
 * portions of a dataset. These parts of datasets are known as selections.
 *
 * The simplest type of selection is a simple hyperslab. This is an n-dimensional rectangular sub-set
 * of a dataset where n is equal to the dataset's rank. Other available selections include a more
 * complex hyperslab with user-defined stride and block size, a list of independent points, or the
 * union of any of these.
 *
 * The figure below shows several sample selections.
 *
 * <table>
 *     <caption align=top>Dataset selections</caption>
 * <tr>
 * <td>
 * \image html Pmodel_fig5_a.gif
 * </td>
 * </tr>
 * <tr>
 * <td>
 * \image html Pmodel_fig5_b.gif
 * </td>
 * </tr>
 * <tr>
 * <td>
 * \image html Pmodel_fig5_c.gif
 * </td>
 * </tr>
 * <tr>
 * <td>
 * \image html Pmodel_fig5_d.gif<br />
 * \image html Pmodel_fig5_e.gif
 * </td>
 * </tr>
 * </table>
 *
 * Note: In the figure above, selections can take the form of a simple hyperslab, a hyperslab with
 * user-defined stride and block, a selection of points, or a union of any of these forms.
 *
 * Selections and hyperslabs are portions of a dataset. As described above, a simple hyperslab is a
 * rectangular array of data elements with the same rank as the dataset's dataspace. Thus, a simple
 * hyperslab is a logically contiguous collection of points within the dataset.
 *
 * The more general case of a hyperslab can also be a regular pattern of points or blocks within the
 * dataspace. Four parameters are required to describe a general hyperslab: the starting coordinates,
 * the block size, the stride or space between blocks, and the number of blocks. These parameters
 * are each expressed as a one-dimensional array with length equal to the rank of the dataspace and
 * are described in the table below.
 *
 * <table>
 * <caption></caption>
 * <tr>
 * <th>Parameter</th>
 * <th>Definition</th>
 * </tr>
 * <tr>
 * <td>start</td>
 * <td>The coordinates of the starting location of the hyperslab in the dataset's dataspace.</td>
 * </tr>
 * <tr>
 * <td>block</td>
 * <td>The size of each block to be selected from the dataspace. If the block parameter
 * is set to NULL, the block size defaults to a single element in each dimension, as
 * if the block array was set to all 1s (all ones). This will result in the selection of a
 * uniformly spaced set of count points starting at start and on the interval defined
 * by stride.</td>
 * </tr>
 * <tr>
 * <td>stride</td>
 * <td>The number of elements separating the starting point of each element or block to
 * be selected. If the stride parameter is set to NULL, the stride size defaults to 1
 * (one) in each dimension and no elements are skipped.</td>
 * </tr>
 * <tr>
 * <td>count</td>
 * <td>The number of elements or blocks to select along each dimension.</td>
 * </tr>
 * </table>
 *
 * <h4>Reading Data into a Differently Shaped Memory Block</h4>
 * For maximum flexibility in user applications, a selection in storage can be mapped into a
 * differently-shaped selection in memory. All that is required is that the two selections contain the
 * same number of data elements. In this example, we will first define the selection to be read from
 * the dataset in storage, and then we will define the selection as it will appear in application
 * memory.
 *
 * Suppose we want to read a 3 x 4 hyperslab from a two-dimensional dataset in a file beginning at
 * the dataset element <1,2>. The first task is to create the dataspace that describes the overall rank
 * and dimensions of the dataset in the file and to specify the position and size of the in-file
 * hyperslab that we are extracting from that dataset. See the code below.
 *
 * <em>Define the selection to be read from storage</em>
 * \code
 *   // Define dataset dataspace in file.
 *   dataspace = H5Dget_space(dataset); // dataspace identifier
 *   rank = H5Sget_simple_extent_ndims(dataspace);
 *
 *   status_n = H5Sget_simple_extent_dims(dataspace, dims_out, NULL);
 *
 *   // Define hyperslab in the dataset.
 *   offset[0] = 1;
 *   offset[1] = 2;
 *   count[0] = 3;
 *   count[1] = 4;
 *   status = H5Sselect_hyperslab(dataspace, H5S_SELECT_SET, offset, NULL, count, NULL);
 * \endcode
 *
 * The next task is to define a dataspace in memory. Suppose that we have in memory a
 * three-dimensional 7 x 7 x 3 array into which we wish to read the two-dimensional 3 x 4 hyperslab
 * described above and that we want the memory selection to begin at the element <3,0,0> and
 * reside in the plane of the first two dimensions of the array. Since the in-memory dataspace is
 * three-dimensional, we have to describe the in-memory selection as three-dimensional. Since we
 * are keeping the selection in the plane of the first two dimensions of the in-memory dataset, the
 * in-memory selection will be a 3 x 4 x 1 array defined as <3,4,1>.
 *
 * Notice that we must describe two things: the dimensions of the in-memory array, and the size
 * and position of the hyperslab that we wish to read in. The code below illustrates how this would
 * be done.
 *
 * <em>Define the memory dataspace and selection</em>
 * \code
 *   // Define memory dataspace.
 *   dimsm[0] = 7;
 *   dimsm[1] = 7;
 *   dimsm[2] = 3;
 *   memspace = H5Screate_simple(RANK_OUT,dimsm,NULL);
 *
 *   // Define memory hyperslab.
 *   offset_out[0] = 3;
 *   offset_out[1] = 0;
 *   offset_out[2] = 0;
 *   count_out[0] = 3;
 *   count_out[1] = 4;
 *   count_out[2] = 1;
 *   status = H5Sselect_hyperslab(memspace, H5S_SELECT_SET, offset_out, NULL, count_out, NULL);
 * \endcode
 *
 * The hyperslab defined in the code above has the following parameters: start=(3,0,0),
 * count=(3,4,1), stride and block size are NULL.
 *
 * <h4>Writing Data into a Differently Shaped Disk Storage Block</h4>
 * Now let's consider the opposite process of writing a selection from memory to a selection in a
 * dataset in a file. Suppose that the source dataspace in memory is a 50-element, one-dimensional
 * array called vector and that the source selection is a 48-element simple hyperslab that starts at the
 * second element of vector. See the figure below.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Pmodel_fig2.gif "A one-dimensional array"
 * </td>
 * </tr>
 * </table>
 *
 * Further suppose that we wish to write this data to the file as a series of 3 x 2-element blocks in a
 * two-dimensional dataset, skipping one row and one column between blocks. Since the source
 * selection contains 48 data elements and each block in the destination selection contains 6 data
 * elements, we must define the destination selection with 8 blocks. We will write 2 blocks in the
 * first dimension and 4 in the second. The code below shows how to achieve this objective.
 *
 * <em>The destination selection</em>
 * \code
 *   // Select the hyperslab for the dataset in the file, using
 *   // 3 x 2 blocks, a (4,3) stride, a (2,4) count, and starting
 *   // at the position (0,1).
 *   start[0] = 0; start[1] = 1;
 *   stride[0] = 4; stride[1] = 3;
 *   count[0] = 2; count[1] = 4;
 *   block[0] = 3; block[1] = 2;
 *   ret = H5Sselect_hyperslab(fid, H5S_SELECT_SET, start, stride, count, block);
 *
 *   // Create dataspace for the first dataset.
 *   mid1 = H5Screate_simple(MSPACE1_RANK, dim1, NULL);
 *
 *   // Select hyperslab.
 *   // We will use 48 elements of the vector buffer starting at the
 *   // second element. Selected elements are 1 2 3 . . . 48
 *   start[0] = 1;
 *   stride[0] = 1;
 *   count[0] = 48;
 *   block[0] = 1;
 *   ret = H5Sselect_hyperslab(mid1, H5S_SELECT_SET, start, stride, count, block);
 *
 *   // Write selection from the vector buffer to the dataset in the file.
 *   ret = H5Dwrite(dataset, H5T_NATIVE_INT, mid1, fid, H5P_DEFAULT, vector);
 * \endcode
 *
 * \subsubsection subsubsec_program_model_info Getting Information about a Dataset
 * Although reading is analogous to writing, it is often first necessary to query a file to obtain
 * information about the dataset to be read. For instance, we often need to determine the datatype
 * associated with a dataset, or its dataspace (in other words, rank and dimensions). As illustrated in
 * the code example below, there are several get routines for obtaining this information.
 *
 * <em>Routines to get dataset parameters</em>
 * \code
 *   // Get datatype and dataspace identifiers,
 *   // then query datatype class, order, and size, and
 *   // then query dataspace rank and dimensions.
 *   datatype = H5Dget_type (dataset); // datatype identifier
 *   class = H5Tget_class (datatype);
 *   if (class == H5T_INTEGER)
 *     printf("Dataset has INTEGER type \n");
 *
 *   order = H5Tget_order (datatype);
 *   if (order == H5T_ORDER_LE)
 *     printf("Little endian order \n");
 *
 *   size = H5Tget_size (datatype);
 *   printf ("Size is %d \n", size);
 *
 *   dataspace = H5Dget_space (dataset); // dataspace identifier
 *
 *   // Find rank and retrieve current and maximum dimension sizes.
 *   rank = H5Sget_simple_extent_dims (dataspace, dims, max_dims);
 * \endcode
 *
 * \subsubsection subsubsec_program_model_compound Creating and Defining Compound Datatypes
 * A compound datatype is a collection of one or more data elements. Each element might be an
 * atomic type, a small array, or another compound datatype.
 *
 * The provision for nested compound datatypes allows these structures to become quite complex.
 * An HDF5 compound datatype has some similarities to a C struct or a Fortran common block.
 * Though not originally designed with databases in mind, HDF5 compound datatypes are
 * sometimes used in a way that is similar to a database record. Compound datatypes can become
 * either a powerful tool or a complex and difficult-to-debug construct. Reasonable caution is
 * advised.
 *
 * To create and use a compound datatype, you need to create a datatype with class compound
 * (#H5T_COMPOUND) and specify the total size of the data element in bytes. A compound
 * datatype consists of zero or more uniquely named members. Members can be defined in any
 * order but must occupy non-overlapping regions within the datum. The table below lists the
 * properties of compound datatype members.
 *
 * <table>
 * <caption></caption>
 * <tr>
 * <th>Parameter</th>
 * <th>Definition</th>
 * </tr>
 * <tr>
 * <td>Index</td>
 * <td>An index number between zero and N-1, where N is the number of
 * members in the compound. The elements are indexed in the order of their
 * location in the array of bytes.</td>
 * </tr>
 * <tr>
 * <td>Name</td>
 * <td>A string that must be unique within the members of the same datatype.</td>
 * </tr>
 * <tr>
 * <td>Datatype</td>
 * <td>An HDF5 datatype.</td>
 * </tr>
 * <tr>
 * <td>Offset</td>
 * <td>A fixed byte offset which defines the location of the first byte of that
 * member in the compound datatype.</td>
 * </tr>
 * </table>
 *
 * Properties of the members of a compound datatype are defined when the member is added to the
 * compound type. These properties cannot be modified later.
 *
 * <h4>Defining Compound Datatypes</h4>
 * Compound datatypes must be built out of other datatypes. To do this, you first create an empty
 * compound datatype and specify its total size. Members are then added to the compound datatype
 * in any order.
 *
 * Each member must have a descriptive name. This is the key used to uniquely identify the
 * member within the compound datatype. A member name in an HDF5 datatype does not
 * necessarily have to be the same as the name of the corresponding member in the C struct in
 * memory although this is often the case. You also do not need to define all the members of the C
 * struct in the HDF5 compound datatype (or vice versa).
 *
 * Usually a C struct will be defined to hold a data point in memory, and the offsets of the members
 * in memory will be the offsets of the struct members from the beginning of an instance of the
 * struct. The library defines the macro that computes the offset of member m within a struct
 * variable s:
 * \code
 *   HOFFSET(s,m)
 * \endcode
 *
 * The code below shows an example in which a compound datatype is created to describe complex
 * numbers whose type is defined by the complex_t struct.
 *
 * <em>A compound datatype for complex numbers</em>
 * \code
 *   Typedef struct {
 *     double re; //real part
 *     double im; //imaginary part
 *   } complex_t;
 *
 *   complex_t tmp; //used only to compute offsets
 *   hid_t complex_id = H5Tcreate (H5T_COMPOUND, sizeof tmp);
 *   H5Tinsert (complex_id, "real", HOFFSET(tmp,re), H5T_NATIVE_DOUBLE);
 *   H5Tinsert (complex_id, "imaginary", HOFFSET(tmp,im), H5T_NATIVE_DOUBLE);
 * \endcode
 *
 * \subsubsection subsubsec_program_model_extend Creating and Writing Extendable Datasets
 * An extendable dataset is one whose dimensions can grow. One can define an HDF5 dataset to
 * have certain initial dimensions with the capacity to later increase the size of any of the initial
 * dimensions. For example, the figure below shows a 3 x 3 dataset (a) which is later extended to
 * be a 10 x 3 dataset by adding 7 rows (b), and further extended to be a 10 x 5 dataset by adding
 * two columns (c).
 *
 * <table>
 * <tr>
 * <td>
 * \image html Pmodel_fig3.gif "Extending a dataset"
 * </td>
 * </tr>
 * </table>
 *
 * HDF5 requires the use of chunking when defining extendable datasets. Chunking makes it
 * possible to extend datasets efficiently without having to reorganize contiguous storage
 * excessively.
 *
 * To summarize, an extendable dataset requires two conditions:
 * <ol><li>Define the dataspace of the dataset as unlimited in all dimensions that might eventually be
 * extended</li>
 * <li>Enable chunking in the dataset creation properties</li></ol>
 *
 * For example, suppose we wish to create a dataset similar to the one shown in the figure above.
 * We want to start with a 3 x 3 dataset, and then later we will extend it. To do this, go through the
 * steps below.
 *
 * First, declare the dataspace to have unlimited dimensions. See the code shown below. Note the
 * use of the predefined constant #H5S_UNLIMITED to specify that a dimension is unlimited.
 *
 * <em>Declaring a dataspace with unlimited dimensions</em>
 * \code
 *   // dataset dimensions at creation time
 *   hsize_t dims[2] = {3, 3};
 *   hsize_t maxdims[2] = {H5S_UNLIMITED, H5S_UNLIMITED};
 *
 *   // Create the data space with unlimited dimensions.
 *   dataspace = H5Screate_simple(RANK, dims, maxdims);
 * \endcode
 *
 * Next, set the dataset creation property list to enable chunking. See the code below.
 *
 * <em>Enable chunking</em>
 * \code
 *   hid_t cparms;
 *   hsize_t chunk_dims[2] ={2, 5};
 *
 *   // Modify dataset creation properties to enable chunking.
 *   cparms = H5Pcreate (H5P_DATASET_CREATE);
 *   status = H5Pset_chunk(cparms, RANK, chunk_dims);
 * \endcode
 *
 * The next step is to create the dataset. See the code below.
 *
 * <em>Create a dataset</em>
 * \code
 *   // Create a new dataset within the file using cparms creation properties.
 *   dataset = H5Dcreate(file, DATASETNAME, H5T_NATIVE_INT, dataspace, H5P_DEFAULT, cparms, H5P_DEFAULT);
 * \endcode
 *
 * Finally, when the time comes to extend the size of the dataset, invoke #H5Dextend. Extending the
 * dataset along the first dimension by seven rows leaves the dataset with new dimensions of
 * <10,3>. See the code below.
 *
 * <em>Extend the dataset by seven rows</em>
 * \code
 *   // Extend the dataset. Dataset becomes 10 x 3.
 *   dims[0] = dims[0] + 7;
 *   size[0] = dims[0];
 *   size[1] = dims[1];
 *
 *   status = H5Dextend (dataset, size);
 * \endcode
 *
 * \subsubsection subsubsec_program_model_group Creating and Working with Groups
 * Groups provide a mechanism for organizing meaningful and extendable sets of datasets within
 * an HDF5 file. The @ref H5G API provides several routines for working with groups.
 *
 * <h4>Creating a Group</h4>
 * With no datatype, dataspace, or storage layout to define, creating a group is considerably simpler
 * than creating a dataset. For example, the following code creates a group called Data in the root
 * group of file.
 *
 * <em> Create a group</em>
 * \code
 *   // Create a group in the file.
 *   grp = H5Gcreate(file, "/Data", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
 * \endcode
 *
 * A group may be created within another group by providing the absolute name of the group to the
 * #H5Gcreate function or by specifying its location. For example, to create the group Data_new in
 * the group Data, you might use the sequence of calls shown below.
 *
 * <em>Create a group within a group</em>
 * \code
 *   // Create group "Data_new" in the group "Data" by specifying
 *   // absolute name of the group.
 *   grp_new = H5Gcreate(file, "/Data/Data_new", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
 *
 *   // or
 *
 *   // Create group "Data_new" in the "Data" group.
 *   grp_new = H5Gcreate(grp, "Data_new", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
 * \endcode
 *
 * This first parameter of #H5Gcreate is a location identifier. file in the first example specifies only
 * the file. \em grp in the second example specifies a particular group in a particular file. Note that in
 * this instance, the group identifier \em grp is used as the first parameter in the #H5Gcreate call so that
 * the relative name of Data_new can be used.
 *
 * The third parameter of #H5Gcreate optionally specifies how much file space to reserve to store
 * the names of objects that will be created in this group. If a non-positive value is supplied, the
 * library provides a default size.
 *
 * Use #H5Gclose to close the group and release the group identifier.
 *
 * <h4>Creating a Dataset within a Group</h4>
 * As with groups, a dataset can be created in a particular group by specifying either its absolute
 * name in the file or its relative name with respect to that group. The next code excerpt uses the
 * absolute name.
 *
 * <em>Create a dataset within a group using a relative name</em>
 * \code
 *   // Create the dataset "Compressed_Data" in the group Data using
 *   // the absolute name. The dataset creation property list is
 *   // modified to use GZIP compression with the compression
 *   // effort set to 6. Note that compression can be used only when
 *   // the dataset is chunked.
 *   dims[0] = 1000;
 *   dims[1] = 20;
 *   cdims[0] = 20;
 *   cdims[1] = 20;
 *   dataspace = H5Screate_simple(RANK, dims, NULL);
 *
 *   plist = H5Pcreate(H5P_DATASET_CREATE);
 *   H5Pset_chunk(plist, 2, cdims);
 *   H5Pset_deflate(plist, 6);
 *
 *   dataset = H5Dcreate(file, "/Data/Compressed_Data", H5T_NATIVE_INT, dataspace, H5P_DEFAULT,
 *                       plist, H5P_DEFAULT);
 * \endcode
 *
 * Alternatively, you can first obtain an identifier for the group in which the dataset is to be
 * created, and then create the dataset with a relative name.
 *
 * <em>Create a dataset within a group using a relative name</em>
 * \code
 *   // Open the group.
 *  grp = H5Gopen(file, "Data", H5P_DEFAULT);
 *
 *  // Create the dataset "Compressed_Data" in the "Data" group
 *  // by providing a group identifier and a relative dataset
 *  // name as parameters to the H5Dcreate function.
 *  dataset = H5Dcreate(grp, "Compressed_Data", H5T_NATIVE_INT, dataspace, H5P_DEFAULT, plist, H5P_DEFAULT);
 * \endcode
 *
 * <h4>Accessing an Object in a Group</h4>
 * Any object in a group can be accessed by its absolute or relative name. The first code snippet
 * below illustrates the use of the absolute name to access the dataset <em>Compressed_Data</em> in the
 * group <em>Data</em> created in the examples above. The second code snippet illustrates the use of the
 * relative name.
 *
 * <em>Accessing a group using its relative name</em>
 * \code
 *   // Open the dataset "Compressed_Data" in the "Data" group.
 *   dataset = H5Dopen(file, "/Data/Compressed_Data", H5P_DEFAULT);
 *
 *   // Open the group "data" in the file.
 *   grp = H5Gopen(file, "Data", H5P_DEFAULT);
 *
 *   // Access the "Compressed_Data" dataset in the group.
 *   dataset = H5Dopen(grp, "Compressed_Data", H5P_DEFAULT);
 * \endcode
 *
 * \subsubsection subsubsec_program_model_attr Working with Attributes
 * An attribute is a small dataset that is attached to a normal dataset or group. Attributes share many
 * of the characteristics of datasets, so the programming model for working with attributes is
 * similar in many ways to the model for working with datasets. The primary differences are that an
 * attribute must be attached to a dataset or a group and sub-setting operations cannot be performed
 * on attributes.
 *
 * To create an attribute belonging to a particular dataset or group, first create a dataspace for the
 * attribute with the call to #H5Screate, and then create the attribute using #H5Acreate. For example,
 * the code shown below creates an attribute called “Integer attribute” that is a member of a dataset
 * whose identifier is dataset. The attribute identifier is attr2. #H5Awrite then sets the value of the
 * attribute of that of the integer variable point. #H5Aclose then releases the attribute identifier.
 *
 * <em>Create an attribute</em>
 * \code
 *   int point = 1; // Value of the scalar attribute
 *
 *   // Create scalar attribute.
 *   aid2 = H5Screate(H5S_SCALAR);
 *   attr2 = H5Acreate(dataset, "Integer attribute", H5T_NATIVE_INT, aid2, H5P_DEFAULT, H5P_DEFAULT);
 *
 *   // Write scalar attribute.
 *   ret = H5Awrite(attr2, H5T_NATIVE_INT, &point);
 *
 *   // Close attribute dataspace.
 *   ret = H5Sclose(aid2);
 *
 *   // Close attribute.
 *   ret = H5Aclose(attr2);
 * \endcode
 *
 * <em>Read a known attribute</em>
 * \code
 *   // Attach to the scalar attribute using attribute name, then
 *   // read and display its value.
 *   attr = H5Aopen_by_name(file_id, dataset_name, "Integer attribute", H5P_DEFAULT, H5P_DEFAULT);
 *   ret = H5Aread(attr, H5T_NATIVE_INT, &point_out);
 *   printf("The value of the attribute \"Integer attribute\" is %d \n", point_out);
 *   ret = H5Aclose(attr);
 * \endcode
 *
 * To read a scalar attribute whose name and datatype are known, first open the attribute using
 * #H5Aopen_by_name, and then use #H5Aread to get its value. For example, the code shown below
 * reads a scalar attribute called “Integer attribute” whose datatype is a native integer and whose
 * parent dataset has the identifier dataset.
 *
 * To read an attribute whose characteristics are not known, go through these steps. First, query the
 * file to obtain information about the attribute such as its name, datatype, rank, and dimensions,
 * and then read the attribute. The following code opens an attribute by its index value using
 * #H5Aopen_by_idx, and then it reads in information about the datatype with #H5Aread.
 *
 * <em>Read an unknown attribute</em>
 * \code
 *   // Attach to the string attribute using its index, then read and
 *   // display the value.
 *   attr = H5Aopen_by_idx(file_id, dataset_name, index_type, iter_order, 2, H5P_DEFAULT, H5P_DEFAULT);
 *
 *   atype = H5Tcopy(H5T_C_S1);
 *   H5Tset_size(atype, 4);
 *
 *   ret = H5Aread(attr, atype, string_out);
 *   printf("The value of the attribute with the index 2 is %s \n", string_out);
 * \endcode
 *
 * In practice, if the characteristics of attributes are not known, the code involved in accessing and
 * processing the attribute can be quite complex. For this reason, HDF5 includes a function called
 * #H5Aiterate. This function applies a user-supplied function to each of a set of attributes. The
 * user-supplied function can contain the code that interprets, accesses, and processes each attribute.
 *
 * \subsection subsec_program_transfer_pipeline The Data Transfer Pipeline
 * The HDF5 library implements data transfers between different storage locations. At the lowest
 * levels, the HDF5 Library reads and writes blocks of bytes to and from storage using calls to the
 * virtual file layer (VFL) drivers. In addition to this, the HDF5 library manages caches of metadata
 * and a data I/O pipeline. The data I/O pipeline applies compression to data blocks, transforms
 * data elements, and implements selections.
 *
 * A substantial portion of the HDF5 library's work is in transferring data from one environment or
 * media to another. This most often involves a transfer between system memory and a storage
 * medium. Data transfers are affected by compression, encryption, machine-dependent differences
 * in numerical representation, and other features. So, the bit-by-bit arrangement of a given dataset
 * is often substantially different in the two environments.
 *
 * Consider the representation on disk of a compressed and encrypted little-endian array as
 * compared to the same array after it has been read from disk, decrypted, decompressed, and
 * loaded into memory on a big-endian system. HDF5 performs all of the operations necessary to
 * make that transition during the I/O process with many of the operations being handled by the
 * VFL and the data transfer pipeline.
 *
 * The figure below provides a simplified view of a sample data transfer with four stages. Note that
 * the modules are used only when needed. For example, if the data is not compressed, the
 * compression stage is omitted.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Pmodel_fig6.gif "A data transfer from storage to memory"
 * </td>
 * </tr>
 * </table>
 *
 * For a given I/O request, different combinations of actions may be performed by the pipeline. The
 * library automatically sets up the pipeline and passes data through the processing steps. For
 * example, for a read request (from disk to memory), the library must determine which logical
 * blocks contain the requested data elements and fetch each block into the library's cache. If the
 * data needs to be decompressed, then the compression algorithm is applied to the block after it is
 * read from disk. If the data is a selection, the selected elements are extracted from the data block
 * after it is decompressed. If the data needs to be transformed (for example, byte swapped), then
 * the data elements are transformed after decompression and selection.
 *
 * While an application must sometimes set up some elements of the pipeline, use of the pipeline is
 * normally transparent to the user program. The library determines what must be done based on the
 * metadata for the file, the object, and the specific request. An example of when an application
 * might be required to set up some elements in the pipeline is if the application used a custom
 * error-checking algorithm.
 *
 * In some cases, it is necessary to pass parameters to and from modules in the pipeline or among
 * other parts of the library that are not directly called through the programming API. This is
 * accomplished through the use of dataset transfer and data access property lists.
 *
 * The VFL provides an interface whereby user applications can add custom modules to the data
 * transfer pipeline. For example, a custom compression algorithm can be used with the HDF5
 * Library by linking an appropriate module into the pipeline through the VFL. This requires
 * creating an appropriate wrapper for the compression module and registering it with the library
 * with #H5Zregister. The algorithm can then be applied to a dataset with an #H5Pset_filter call which
 * will add the algorithm to the selected dataset's transfer property list.
 *
 * Previous Chapter \ref sec_data_model - Next Chapter \ref sec_file
 *
 */

/**
 * \defgroup H5 Library General (H5)
 *
 * Use the functions in this module to manage the life cycle of HDF5 library
 * instances.
 *
 * <table>
 * <tr><th>Create</th><th>Read</th></tr>
 * <tr valign="top">
 *   <td>
 *   \snippet{lineno} H5_examples.c create
 *   </td>
 *   <td>
 *   \snippet{lineno} H5_examples.c read
 *   </td>
 * <tr><th>Update</th><th>Delete</th></tr>
 * <tr valign="top">
 *   <td>
 *   \snippet{lineno} H5_examples.c update
 *   </td>
 *   <td>
 *   \snippet{lineno} H5_examples.c closing_shop
 *   \snippet{lineno} H5_examples.c delete
 *   </td>
 * </tr>
 * </table>
 *
 */

#endif /* H5module_H */
