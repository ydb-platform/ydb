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
 *          H5G package.  Including this header means that the source file
 *          is part of the H5G package.
 */
#ifndef H5Gmodule_H
#define H5Gmodule_H

/* Define the proper control macros for the generic FUNC_ENTER/LEAVE and error
 *      reporting macros.
 */
#define H5G_MODULE
#define H5_MY_PKG     H5G
#define H5_MY_PKG_ERR H5E_SYM

/**  \page H5G_UG HDF5 Groups
 *
 * \section sec_group HDF5 Groups
 * \subsection subsec_group_intro Introduction
 * As suggested by the name Hierarchical Data Format, an HDF5 file is hierarchically structured.
 * The HDF5 group and link objects implement this hierarchy.
 *
 * In the simple and most common case, the file structure is a tree structure; in the general case, the
 * file structure may be a directed graph with a designated entry point. The tree structure is very
 * similar to the file system structures employed on UNIX systems, directories and files, and on
 * Apple and Microsoft Windows systems, folders and files. HDF5 groups are analogous
 * to the directories and folders; HDF5 datasets are analogous to the files.
 *
 * The one very important difference between the HDF5 file structure and the above-mentioned file
 * system analogs is that HDF5 groups are linked as a directed graph, allowing circular references;
 * the file systems are strictly hierarchical, allowing no circular references. The figures below
 * illustrate the range of possibilities.
 *
 * In the first figure below, the group structure is strictly hierarchical, identical to the file system
 * analogs.
 *
 * In the next two figures below, the structure takes advantage of the directed graph's allowance of
 * circular references. In the second figure, GroupA is not only a member of the root group, /, but a
 * member of GroupC. Since Group C is a member of Group B and Group B is a member of Group
 * A, Dataset1 can be accessed by means of the circular reference /Group A/Group B/Group
 * C/Group A/Dataset1. The third figure below illustrates an extreme case in which GroupB is a
 * member of itself, enabling a reference to a member dataset such as /Group A/Group B/Group
 * B/Group B/Dataset2.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Groups_fig1.gif "A file with a strictly hierarchical group structure"
 * </td>
 * </tr>
 * </table>
 *
 * <table>
 * <tr>
 * <td>
 * \image html Groups_fig2.gif "A file with a circular reference"
 * </td>
 * </tr>
 * </table>
 *
 * <table>
 * <tr>
 * <td>
 * \image html Groups_fig3.gif "A file with one group as a member of itself"
 * </td>
 * </tr>
 * </table>
 *
 * As becomes apparent upon reflection, directed graph structures can become quite complex;
 * caution is advised!
 *
 * The balance of this chapter discusses the following topics:
 * \li The HDF5 group object (or a group) and its structure in more detail
 * \li HDF5 link objects (or links)
 * \li The programming model for working with groups and links
 * \li HDF5 functions provided for working with groups, group members, and links
 * \li Retrieving information about objects in a group
 * \li Discovery of the structure of an HDF5 file and the contained objects
 * \li Examples of file structures
 *
 * \subsection subsec_group_descr Description of the Group Object
 * \subsubsection subsubsec_group_descr_object The Group Object
 * Abstractly, an HDF5 group contains zero or more objects and every object must be a member of
 * at least one group. The root group, the sole exception, may not belong to any group.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Groups_fig4.gif "Abstract model of the HDF5 group object"
 * </td>
 * </tr>
 * </table>
 *
 * Group membership is actually implemented via link objects. See the figure above. A link object
 * is owned by a group and points to a named object. Each link has a name, and each link points to
 * exactly one object. Each named object has at least one and possibly many links to it.
 *
 * There are three classes of named objects: group, dataset, and committed datatype (formerly
 * called named datatype). See the figure below. Each of these objects is the member of at least one
 * group, which means there is at least one link to it.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Groups_fig5.gif "Classes of named objects"
 * </td>
 * </tr>
 * </table>
 *
 * The primary operations on a group are to add and remove members and to discover member
 * objects. These abstract operations, as listed in the figure below, are implemented in the \ref H5G
 * APIs. For more information, @see @ref subsec_group_function.
 *
 * To add and delete members of a group, links from the group to existing objects in the file are
 * created and deleted with the link and unlink operations. When a new named object is created, the
 * HDF5 Library executes the link operation in the background immediately after creating the
 * object (in other words, a new object is added as a member of the group in which it is created
 * without further user intervention).
 *
 * Given the name of an object, the get_object_info method retrieves a description of the object,
 * including the number of references to it. The iterate method iterates through the members of the
 * group, returning the name and type of each object.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Groups_fig6.gif "The group object"
 * </td>
 * </tr>
 * </table>
 *
 * Every HDF5 file has a single root group, with the name /. The root group is identical to any other
 * HDF5 group, except:
 * \li The root group is automatically created when the HDF5 file is created (#H5Fcreate).
 * \li The root group has no parent, but by convention has a reference count of 1.
 * \li The root group cannot be deleted (in other words, unlinked)!
 *
 * \subsubsection subsubsec_group_descr_model The Hierarchy of Data Objects
 * An HDF5 file is organized as a rooted, directed graph using HDF5 group objects. The named
 * data objects are the nodes of the graph, and the links are the directed arcs. Each arc of the graph
 * has a name, with the special name / reserved for the root group. New objects are created and then
 * inserted into the graph with a link operation that is automatically executed by the library;
 * existing objects are inserted into the graph with a link operation explicitly called by the user,
 * which creates a named link from a group to the object.
 *
 * An object can be the target of more than one link.
 *
 * The names on the links must be unique within each group, but there may be many links with the
 * same name in different groups. These are unambiguous, because some ancestor must have a
 * different name, or else they are the same object. The graph is navigated with path names,
 * analogous to Unix file systems. For more information, @see @ref subsubsec_group_descr_path.
 *
 * An object can be opened with a full path starting at the root group, or with a relative path and a
 * starting point. That starting point is always a group, though it may be the current working group,
 * another specified group, or the root group of the file. Note that all paths are relative to a single
 * HDF5 file. In this sense, an HDF5 file is analogous to a single UNIX file system.
 *
 * It is important to note that, just like the UNIX file system, HDF5 objects do not have names, the
 * names are associated with paths. An object has an object identifier that is unique within the file,
 * but a single object may have many names because there may be many paths to the same object.
 * An object can be renamed, or moved to another group, by adding and deleting links. In this case,
 * the object itself never moves. For that matter, membership in a group has no implication for the
 * physical location of the stored object.
 *
 * Deleting a link to an object does not necessarily delete the object. The object remains available
 * as long as there is at least one link to it. After all links to an object are deleted, it can no longer
 * be opened, and the storage may be reclaimed.
 *
 * It is also important to realize that the linking mechanism can be used to construct very complex
 * graphs of objects. For example, it is possible for an object to be shared between several groups
 * and even to have more than one name in the same group. It is also possible for a group to be a
 * member of itself, or to create other cycles in the graph, such as in the case where a child group is
 * linked to one of its ancestors.
 *
 * HDF5 also has soft links similar to UNIX soft links. A soft link is an object that has a name and
 * a path name for the target object. The soft link can be followed to open the target of the link just
 * like a regular or hard link. The differences are that the hard link cannot be created if the target
 * object does not exist and it always points to the same object. A soft link can be created with any
 * path name, whether or not the object exists; it may or may not, therefore, be possible to follow a
 * soft link. Furthermore, a soft link's target object may be changed.
 *
 * \subsubsection subsubsec_group_descr_path HDF5 Path Names
 * The structure of the HDF5 file constitutes the name space for the objects in the file. A path name
 * is a string of components separated by slashes (/). Each component is the name of a hard or soft
 * link which points to an object in the file. The slash not only separates the components, but
 * indicates their hierarchical relationship; the component indicated by the link name following a
 * slash is a always a member of the component indicated by the link name preceding that slash.
 *
 * The first component in the path name may be any of the following:
 * \li The special character dot (., a period), indicating the current group
 * \li The special character slash (/), indicating the root group
 * \li Any member of the current group
 *
 * Component link names may be any string of ASCII characters not containing a slash or a dot
 * (/ and ., which are reserved as noted above). However, users are advised to avoid the use of
 * punctuation and non-printing characters, as they may create problems for other software. The
 * figure below provides a BNF grammar for HDF5 path names.
 *
 * <em>A BNF grammar for HDF5 path names</em>
 * \code
 *     PathName ::= AbsolutePathName | RelativePathName
 *     Separator ::= "/" ["/"]*
 *     AbsolutePathName ::= Separator [ RelativePathName ]
 *     RelativePathName ::= Component [ Separator RelativePathName ]*
 *     Component ::= "." | Characters
 *     Characters ::= Character+ - { "." }
 *     Character ::= {c: c Î { { legal ASCII characters } - {'/'} }
 * \endcode
 *
 * An object can always be addressed by either a full or an absolute path name, starting at the root
 * group, or by a relative path name, starting in a known location such as the current working
 * group. As noted elsewhere, a given object may have multiple full and relative path names.
 *
 * Consider, for example, the file illustrated in the figure below. Dataset1 can be identified by either
 * of these absolute path names:
 * <em>/GroupA/Dataset1</em>
 *
 * <em>/GroupA/GroupB/GroupC/Dataset1</em>
 *
 * Since an HDF5 file is a directed graph structure, and is therefore not limited to a strict tree
 * structure, and since this illustrated file includes the sort of circular reference that a directed graph
 * enables, Dataset1 can also be identified by this absolute path name:
 * <em>/GroupA/GroupB/GroupC/GroupA/Dataset1</em>
 *
 * Alternatively, if the current working location is GroupB, Dataset1 can be identified by either of
 * these relative path names:
 * <em>GroupC/Dataset1</em>
 *
 * <em>GroupC/GroupA/Dataset1</em>
 *
 * Note that relative path names in HDF5 do not employ the ../ notation, the UNIX notation
 * indicating a parent directory, to indicate a parent group.
 *
 * <table>
 * <tr>
 * <td>
 * \image html Groups_fig2.gif "A file with a circular reference"
 * </td>
 * </tr>
 * </table>
 *
 * \subsubsection subsubsec_group_descr_impl Group Implementations in HDF5
 * The original HDF5 group implementation provided a single indexed structure for link storage. A
 * new group implementation, as of HDF5 Release 1.8.0, enables more efficient compact storage
 * for very small groups, improved link indexing for large groups, and other advanced features.
 * <ul>
 * <li>The original indexed format remains the default. Links are stored in a B-tree in the
 * group's local heap.</li>
 * <li>Groups created in the new compact-or-indexed format, the implementation introduced
 * with Release 1.8.0, can be tuned for performance, switching between the compact and
 * indexed formats at thresholds set in the user application.
 * <ul>
 * <li>The compact format will conserve file space and processing overhead when
 * working with small groups and is particularly valuable when a group contains
 * no links. Links are stored as a list of messages in the group's header.</li>
 * <li>The indexed format will yield improved performance when working with large
 * groups. A large group may contain thousands to millions of members. Links
 * are stored in a fractal heap and indexed with an improved B-tree.</li>
 * </ul></li>
 * <li>The new implementation also enables the use of link names consisting of non-ASCII
 * character sets (see #H5Pset_char_encoding) and is required for all link types other than
 * hard or soft links; the link types other than hard or soft links are external links and
 * user-defined links @see @ref H5L APIs.</li>
 * </ul>
 *
 * The original group structure and the newer structures are not directly interoperable. By default, a
 * group will be created in the original indexed format. An existing group can be changed to a
 * compact-or-indexed format if the need arises; there is no capability to change back. As stated
 * above, once in the compact-or-indexed format, a group can switch between compact and indexed
 * as needed.
 *
 * Groups will be initially created in the compact-or-indexed format only when one or more of the
 * following conditions is met:
 * <ul>
 * <li>The low version bound value of the library version bounds property has been set to
 * Release 1.8.0 or later in the file access property list (see #H5Pset_libver_bounds).
 * Currently, that would require an #H5Pset_libver_bounds call with the low parameter
 * set to #H5F_LIBVER_LATEST.
 *
 * When this property is set for an HDF5 file, all objects in the file will be created using
 * the latest available format; no effort will be made to create a file that can be read by
 * older libraries.</li>
 * <li>The creation order tracking property, #H5P_CRT_ORDER_TRACKED, has been set
 * in the group creation property list (see #H5Pset_link_creation_order).</li>
 * </ul>
 *
 * An existing group, currently in the original indexed format, will be converted to the compact-or-
 * indexed format upon the occurrence of any of the following events:
 * <ul>
 * <li>An external or user-defined link is inserted into the group.
 * <li>A link named with a string composed of non-ASCII characters is inserted into the
 * group.
 * </ul>
 *
 * The compact-or-indexed format offers performance improvements that will be most notable at
 * the extremes (for example, in groups with zero members and in groups with tens of thousands of
 * members). But measurable differences may sometimes appear at a threshold as low as eight
 * group members. Since these performance thresholds and criteria differ from application to
 * application, tunable settings are provided to govern the switch between the compact and indexed
 * formats (see #H5Pset_link_phase_change). Optimal thresholds will depend on the application and
 * the operating environment.
 *
 * Future versions of HDF5 will retain the ability to create, read, write, and manipulate all groups
 * stored in either the original indexed format or the compact-or-indexed format.
 *
 * \subsection subsec_group_h5dump Using h5dump
 * You can use h5dump, the command-line utility distributed with HDF5, to examine a file for
 * purposes either of determining where to create an object within an HDF5 file or to verify that
 * you have created an object in the intended place.
 *
 * In the case of the new group created later in this chapter, the following h5dump command will
 * display the contents of FileA.h5:
 * \code
 * h5dump FileA.h5
 * \endcode
 *
 * For more information, @see @ref subsubsec_group_program_create.
 *
 * Assuming that the discussed objects, GroupA and GroupB are the only objects that exist in
 * FileA.h5, the output will look something like the following:
 * \code
 * HDF5 "FileA.h5" {
 * GROUP "/" {
 * GROUP GroupA {
 * GROUP GroupB {
 * }
 * }
 * }
 * }
 * \endcode
 *
 * h5dump is described on the “HDF5 Tools” page of the \ref RM.
 *
 * The HDF5 DDL grammar is described in the @ref DDLBNF110.
 *
 * \subsection subsec_group_function Group Function Summaries
 * Functions that can be used with groups (\ref H5G functions) and property list functions that can used
 * with groups (\ref H5P functions) are listed below. A number of group functions have been
 * deprecated. Most of these have become link (\ref H5L) or object (\ref H5O) functions. These replacement
 * functions are also listed below.
 *
 * <table>
 * <caption>Group functions</caption>
 * <tr>
 * <th>Function</th>
 * <th>Purpose</th>
 * </tr>
 * <tr>
 * <td>#H5Gcreate</td>
 * <td>Creates a new empty group and gives it a name. The
 * C function is a macro: \see \ref api-compat-macros.</td>
 * </tr>
 * <tr>
 * <td>#H5Gcreate_anon</td>
 * <td>Creates a new empty group without linking it into the file structure.</td>
 * </tr>
 * <tr>
 * <td>#H5Gopen</td>
 * <td>Opens an existing group for modification and returns a group identifier for that group.
 * The C function is a macro: \see \ref api-compat-macros.</td>
 * </tr>
 * <tr>
 * <td>#H5Gclose</td>
 * <td>Closes the specified group.</td>
 * </tr>
 * <tr>
 * <td>#H5Gget_create_plist</td>
 * <td>Gets a group creation property list identifier.</td>
 * </tr>
 * <tr>
 * <td>#H5Gget_info</td>
 * <td>Retrieves information about a group. Use instead of H5Gget_num_objs.</td>
 * </tr>
 * <tr>
 * <td>#H5Gget_info_by_idx</td>
 * <td>Retrieves information about a group according to the group's position within an index.</td>
 * </tr>
 * <tr>
 * <td>#H5Gget_info_by_name</td>
 * <td>Retrieves information about a group.</td>
 * </tr>
 * </table>
 *
 * <table>
 * <caption>Link and object functions</caption>
 * <tr>
 * <th>Function</th>
 * <th>Purpose</th>
 * </tr>
 * <tr>
 * <td>#H5Lcreate_hard</td>
 * <td>Creates a hard link to an object. Replaces H5Glink and H5Glink2.</td>
 * </tr>
 * <tr>
 * <td>#H5Lcreate_soft</td>
 * <td>Creates a soft link to an object. Replaces H5Glink and H5Glink2.</td>
 * </tr>
 * <tr>
 * <td>#H5Lcreate_external</td>
 * <td>Creates a soft link to an object in a different file. Replaces H5Glink and H5Glink2.</td>
 * </tr>
 * <tr>
 * <td>#H5Lcreate_ud</td>
 * <td>Creates a link of a user-defined type.</td>
 * </tr>
 * </tr>
 * <tr>
 * <td>#H5Lget_val</td>
 * <td>Returns the value of a symbolic link. Replaces H5Gget_linkval.</td>
 * </tr>
 * <tr>
 * <td>#H5Literate</td>
 * <td>Iterates through links in a group. Replaces H5Giterate.
 * See also #H5Ovisit and #H5Lvisit.</td>
 * </tr>
 * <tr>
 * <td>#H5Literate_by_name</td>
 * <td>Iterates through links in a group.</td>
 * </tr>
 * <tr>
 * <td>#H5Lvisit</td>
 * <td>Recursively visits all links starting from a specified group.</td>
 * </tr>
 * <tr>
 * <td>#H5Ovisit</td>
 * <td>Recursively visits all objects accessible from a specified object.</td>
 * </tr>
 * <tr>
 * <td>#H5Lget_info</td>
 * <td>Returns information about a link. Replaces H5Gget_objinfo.</td>
 * </tr>
 * <tr>
 * <td>#H5Oget_info</td>
 * <td>Retrieves the metadata for an object specified by an identifier. Replaces H5Gget_objinfo.</td>
 * </tr>
 * <tr>
 * <td>#H5Lget_name_by_idx</td>
 * <td>Retrieves name of the nth link in a group, according to the order within a specified field
 * or index. Replaces H5Gget_objname_by_idx.</td>
 * </tr>
 * <tr>
 * <td>#H5Oget_info_by_idx</td>
 * <td>Retrieves the metadata for an object, identifying the object by an index position. Replaces
 * H5Gget_objtype_by_idx.</td>
 * </tr>
 * <tr>
 * <td>#H5Oget_info_by_name</td>
 * <td>Retrieves the metadata for an object, identifying the object by location and relative name.</td>
 * </tr>
 * <tr>
 * <td>#H5Oset_comment</td>
 * <td>Sets the comment for specified object. Replaces H5Gset_comment.</td>
 * </tr>
 * <tr>
 * <td>#H5Oget_comment</td>
 * <td>Gets the comment for specified object. Replaces H5Gget_comment.</td>
 * </tr>
 * <tr>
 * <td>#H5Ldelete</td>
 * <td>Removes a link from a group. Replaces H5Gunlink.</td>
 * </tr>
 * <tr>
 * <td>#H5Lmove</td>
 * <td>Renames a link within an HDF5 file. Replaces H5Gmove and H5Gmove2.</td>
 * </tr>
 * </table>
 *
 * \snippet{doc} tables/propertyLists.dox gcpl_table
 *
 * <table>
 * <caption>Other external link functions</caption>
 * <tr>
 * <th>Function</th>
 * <th>Purpose</th>
 * </tr>
 * <tr>
 * <td>#H5Pset_elink_file_cache_size</td>
 * <td>Sets the size of the external link open file cache from the specified
 * file access property list.</td>
 * </tr>
 * <tr>
 * <td>#H5Pget_elink_file_cache_size</td>
 * <td>Retrieves the size of the external link open file cache from the specified
 * file access property list.</td>
 * </tr>
 * <tr>
 * <td>#H5Fclear_elink_file_cache</td>
 * <td>Clears the external link open file cache for a file.</td>
 * </tr>
 * </table>
 *
 * \subsection subsec_group_program Programming Model for Groups
 * The programming model for working with groups is as follows:
 * <ol><li>Create a new group or open an existing one.</li>
 * <li>Perform the desired operations on the group.
 * <ul><li>Create new objects in the group.</li>
 * <li>Insert existing objects as group members.</li>
 * <li>Delete existing members.</li>
 * <li>Open and close member objects.</li>
 * <li>Access information regarding member objects.</li>
 * <li>Iterate across group members.</li>
 * <li>Manipulate links.</li></ul>
 * <li>Terminate access to the group (Close the group).</li></ol>
 *
 * \subsubsection subsubsec_group_program_create Creating a Group
 * To create a group, use #H5Gcreate, specifying the location and the path of the new group. The
 * location is the identifier of the file or the group in a file with respect to which the new group is to
 * be identified. The path is a string that provides either an absolute path or a relative path to the
 * new group. For more information, @see @ref subsubsec_group_descr_path.
 *
 * A path that begins with a slash (/) is
 * an absolute path indicating that it locates the new group from the root group of the HDF5 file. A
 * path that begins with any other character is a relative path. When the location is a file, a relative
 * path is a path from that file's root group; when the location is a group, a relative path is a path
 * from that group.
 *
 * The sample code in the example below creates three groups. The group Data is created in the
 * root directory; two groups are then created in /Data, one with absolute path, the other with a
 * relative path.
 *
 * <em>Creating three new groups</em>
 * \code
 *   hid_t file;
 *   file = H5Fopen(....);
 *
 *   group = H5Gcreate(file, "/Data", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
 *   group_new1 = H5Gcreate(file, "/Data/Data_new1", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
 *   group_new2 = H5Gcreate(group, "Data_new2", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
 * \endcode
 * The third #H5Gcreate parameter optionally specifies how much file space to reserve to store the
 * names that will appear in this group. If a non-positive value is supplied, a default size is chosen.
 *
 * \subsubsection subsubsec_group_program_open Opening a Group and Accessing an Object in that Group
 * Though it is not always necessary, it is often useful to explicitly open a group when working
 * with objects in that group. Using the file created in the example above, the example below
 * illustrates the use of a previously-acquired file identifier and a path relative to that file to open
 * the group Data.
 *
 * Any object in a group can be also accessed by its absolute or relative path. To open an object
 * using a relative path, an application must first open the group or file on which that relative path
 * is based. To open an object using an absolute path, the application can use any location identifier
 * in the same file as the target object; the file identifier is commonly used, but object identifier for
 * any object in that file will work. Both of these approaches are illustrated in the example below.
 *
 * Using the file created in the examples above, the example below provides sample code
 * illustrating the use of both relative and absolute paths to access an HDF5 data object. The first
 * sequence (two function calls) uses a previously-acquired file identifier to open the group Data,
 * and then uses the returned group identifier and a relative path to open the dataset CData. The
 * second approach (one function call) uses the same previously-acquired file identifier and an
 * absolute path to open the same dataset.
 *
 * <em>Open a dataset with relative and absolute paths</em>
 * \code
 *   group = H5Gopen(file, "Data", H5P_DEFAULT);
 *
 *   dataset1 = H5Dopen(group, "CData", H5P_DEFAULT);
 *   dataset2 = H5Dopen(file, "/Data/CData", H5P_DEFAULT);
 * \endcode
 *
 * \subsubsection subsubsec_group_program_dataset Creating a Dataset in a Specific Group
 * Any dataset must be created in a particular group. As with groups, a dataset may be created in a
 * particular group by specifying its absolute path or a relative path. The example below illustrates
 * both approaches to creating a dataset in the group /Data.
 *
 * <em> Create a dataset with absolute and relative paths</em>
 * \code
 *   dataspace = H5Screate_simple(RANK, dims, NULL);
 *   dataset1 = H5Dcreate(file, "/Data/CData", H5T_NATIVE_INT, dataspace,
 *                        H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
 *   group = H5Gopen(file, "Data", H5P_DEFAULT);
 *   dataset2 = H5Dcreate(group, "Cdata2", H5T_NATIVE_INT, dataspace,
 *                        H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
 * \endcode
 *
 * \subsubsection subsubsec_group_program_close Closing a Group
 * To ensure the integrity of HDF5 objects and to release system resources, an application should
 * always call the appropriate close function when it is through working with an HDF5 object. In
 * the case of groups, H5Gclose ends access to the group and releases any resources the HDF5
 * library has maintained in support of that access, including the group identifier.
 *
 * As illustrated in the example below, all that is required for an H5Gclose call is the group
 * identifier acquired when the group was opened; there are no relative versus absolute path
 * considerations.
 *
 * <em>Close a group</em>
 * \code
 *   herr_t status;
 *
 *   status = H5Gclose(group);
 * \endcode
 *
 * A non-negative return value indicates that the group was successfully closed and the resources
 * released; a negative return value indicates that the attempt to close the group or release resources
 * failed.
 *
 * \subsubsection subsubsec_group_program_links Creating Links
 * As previously mentioned, every object is created in a specific group. Once created, an object can
 * be made a member of additional groups by means of links created with one of the H5Lcreate_*
 * functions.
 *
 * A link is, in effect, a path by which the target object can be accessed; it therefore has a name
 * which functions as a single path component. A link can be removed with an #H5Ldelete call,
 * effectively removing the target object from the group that contained the link (assuming, of
 * course, that the removed link was the only link to the target object in the group).
 *
 * <h4>Hard Links</h4>
 * There are two kinds of links, hard links and symbolic links. Hard links are reference counted;
 * symbolic links are not. When an object is created, a hard link is automatically created. An object
 * can be deleted from the file by removing all the hard links to it.
 *
 * Working with the file from the previous examples, the code in the example below illustrates the
 * creation of a hard link, named Data_link, in the root group, /, to the group Data. Once that link is
 * created, the dataset Cdata can be accessed via either of two absolute paths, /Data/Cdata or
 * /Data_Link/Cdata.
 *
 * <em>Create a hard link</em>
 * \code
 *   status = H5Lcreate_hard(Data_loc_id, "Data", DataLink_loc_id, "Data_link", H5P_DEFAULT, H5P_DEFAULT);
 *
 *   dataset1 = H5Dopen(file, "/Data_link/CData", H5P_DEFAULT);
 *   dataset2 = H5Dopen(file, "/Data/CData", H5P_DEFAULT);
 * \endcode
 *
 * The example below shows example code to delete a link, deleting the hard link Data from the
 * root group. The group /Data and its members are still in the file, but they can no longer be
 * accessed via a path using the component /Data.
 *
 * <em>Delete a link</em>
 * \code
 *   status = H5Ldelete(Data_loc_id, "Data", H5P_DEFAULT);
 *
 *   dataset1 = H5Dopen(file, "/Data_link/CData", H5P_DEFAULT);
 *   // This call should succeed; all path components still exist
 *   dataset2 = H5Dopen(file, "/Data/CData", H5P_DEFAULT);
 *   // This call will fail; the path component '/Data' has been deleted.
 * \endcode
 *
 * When the last hard link to an object is deleted, the object is no longer accessible. #H5Ldelete will
 * not prevent you from deleting the last link to an object. To see if an object has only one link, use
 * the #H5Oget_info function. If the value of the rc (reference count) field in the is greater than 1,
 * then the link can be deleted without making the object inaccessible.
 *
 * The example below shows #H5Oget_info to the group originally called Data.
 *
 * <em>Finding the number of links to an object</em>
 * \code
 *   status = H5Oget_info(Data_loc_id, object_info);
 * \endcode
 *
 * It is possible to delete the last hard link to an object and not make the object inaccessible.
 * Suppose your application opens a dataset, and then deletes the last hard link to the dataset. While
 * the dataset is open, your application still has a connection to the dataset. If your application
 * creates a hard link to the dataset before it closes the dataset, then the dataset will still be
 * accessible.
 *
 * <h4>Symbolic Links</h4>
 * Symbolic links are objects that assign a name in a group to a path. Notably, the target object is
 * determined only when the symbolic link is accessed, and may, in fact, not exist. Symbolic links
 * are not reference counted, so there may be zero, one, or more symbolic links to an object.
 *
 * The major types of symbolic links are soft links and external links. Soft links are symbolic links
 * within an HDF5 file and are created with the #H5Lcreate_soft function. Symbolic links to objects
 * located in external files, in other words external links, can be created with the
 * #H5Lcreate_external function. Symbolic links are removed with the #H5Ldelete function.
 *
 * The example below shows the creating two soft links to the group /Data.
 *
 * <em>Create a soft link</em>
 * \code
 *   status = H5Lcreate_soft(path_to_target, link_loc_id, "Soft2", H5P_DEFAULT, H5P_DEFAULT);
 *   status = H5Lcreate_soft(path_to_target, link_loc_id, "Soft3", H5P_DEFAULT, H5P_DEFAULT);
 *   dataset = H5Dopen(file, "/Soft2/CData", H5P_DEFAULT);
 * \endcode
 *
 * With the soft links defined in the example above, the dataset CData in the group /Data can now
 * be opened with any of the names /Data/CData, /Soft2/CData, or /Soft3/CData.
 *
 * In release 1.8.7, a cache was added to hold the names of files accessed via external links. The
 * size of this cache can be changed to help improve performance. For more information, see the
 * entry in the \ref RM for the #H5Pset_elink_file_cache_size function call.
 *
 * <h4>Note Regarding Hard Links and Soft Links</h4>
 * Note that an object's existence in a file is governed by the presence of at least one hard link to
 * that object. If the last hard link to an object is removed, the object is removed from the file and
 * any remaining soft link becomes a dangling link, a link whose target object does not exist.
 *
 * <h4>Moving or Renaming Objects, and a Warning</h4>
 * An object can be renamed by changing the name of a link to it with #H5Lmove. This has the same
 * effect as creating a new link with the new name and deleting the link with the old name.
 *
 * Exercise caution in the use of #H5Lmove and #H5Ldelete as these functions each include a step
 * that unlinks a pointer to an HDF5 object. If the link that is removed is on the only path leading to
 * an HDF5 object, that object will become permanently inaccessible in the file.
 *
 * <h5>Scenario 1: Removing the Last Link</h5>
 * To avoid removing the last link to an object or otherwise making an object inaccessible, use the
 * #H5Oget_info function. Make sure that the value of the reference count field (rc) is greater than 1.
 *
 * <h5>Scenario 2: Moving a Link that Isolates an Object</h5>
 * Consider the following example: assume that the group group2 can only be accessed via the
 * following path, where top_group is a member of the file's root group:
 * <em>/top_group/group1/group2/</em>
 *
 * Using #H5Lmove, top_group is renamed to be a member ofgroup2. At this point, since
 * top_group was the only route from the root group to group1, there is no longer a path by which
 * one can access group1, group2, or any member datasets. And since top_group is now a member
 * of group2, top_group itself and any member datasets have thereby also become inaccessible.
 *
 * <h4>Mounting a File</h4>
 * An external link is a permanent connection between two files. A temporary connection can be set
 * up with the #H5Fmount function. For more information, @see sec_file.
 * For more information, see the #H5Fmount function in the \ref RM.
 *
 * \subsubsection subsubsec_group_program_info Discovering Information about Objects
 * There is often a need to retrieve information about a particular object. The #H5Lget_info and
 * #H5Oget_info functions fill this niche by returning a description of the object or link in an
 * #H5L_info_t or #H5O_info_t structure.
 *
 * \subsubsection subsubsec_group_program_objs Discovering Objects in a Group
 * To examine all the objects or links in a group, use the #H5Literate or #H5Ovisit functions to
 * examine the objects, and use the #H5Lvisit function to examine the links. #H5Literate is useful
 * both with a single group and in an iterative process that examines an entire file or section of a
 * file (such as the contents of a group or the contents of all the groups that are members of that
 * group) and acts on objects as they are encountered. #H5Ovisit recursively visits all objects
 * accessible from a specified object. #H5Lvisit recursively visits all the links starting from a
 * specified group.
 *
 * \subsubsection subsubsec_group_program_all Discovering All of the Objects in the File
 * The structure of an HDF5 file is self-describing, meaning that an application can navigate an
 * HDF5 file to discover and understand all the objects it contains. This is an iterative process
 * wherein the structure is traversed as a graph, starting at one node and recursively visiting linked
 * nodes. To explore the entire file, the traversal should start at the root group.
 *
 * \subsection subsec_group_examples Examples of File Structures
 * This section presents several samples of HDF5 file structures.
 *
 * Figure 9 shows examples of the structure of a file with three groups and one dataset. The file in
 * part a contains three groups: the root group and two member groups. In part b, the dataset
 * dset1 has been created in /group1. In part c, a link named dset2 from /group2 to the dataset has
 * been added. Note that there is only one copy of the dataset; there are two links to it and it can be
 * accessed either as /group1/dset1 or as /group2/dset2.
 *
 * Part d illustrates that one of the two links to the dataset can be deleted. In this case, the link from
 * <em>/group1</em>
 * has been removed. The dataset itself has not been deleted; it is still in the file but can only be
 * accessed as
 * <em>/group2/dset2</em>
 *
 * <table>
 * <caption>Figure 9 - Some file structures</caption>
 * <tr>
 * <td>
 * \image html Groups_fig9_a.gif "a) The file contains three groups: the root group, /group1, and  /group2."
 * </td>
 * <td>
 * \image html Groups_fig9_b.gif "b) The dataset dset1 (or /group1/dset1) is created in /group1."
 * </td>
 * </tr>
 * <tr>
 * <td>
 * \image html Groups_fig9_aa.gif "c) A link named dset2 to the same dataset is created in /group2."
 * </td>
 * <td>
 * \image html Groups_fig9_bb.gif "d) The link from /group1 to dset1 is removed. The dataset is
 * still in the file, but can be accessed only as /group2/dset2."
 * </td>
 * </tr>
 * </table>
 *
 * Figure 10 illustrates loops in an HDF5 file structure. The file in part a contains three groups
 * and a dataset; group2 is a member of the root group and of the root group's other member group,
 * group1. group2 thus can be accessed by either of two paths: /group2 or /group1/GXX. Similarly,
 * the dataset can be accessed either as /group2/dset1 or as /group1/GXX/dset1.
 *
 * Part b illustrates a different case: the dataset is a member of a single group but with two links, or
 * names, in that group. In this case, the dataset again has two names, /group1/dset1 and
 * /group1/dset2.
 *
 * In part c, the dataset dset1 is a member of two groups, one of which can be accessed by either of
 * two names. The dataset thus has three path names: /group1/dset1, /group2/dset2, and
 * /group1/GXX/dset2.
 *
 * And in part d, two of the groups are members of each other and the dataset is a member of both
 * groups. In this case, there are an infinite number of paths to the dataset because GXX and
 * GYY can be traversed any number of times on the way from the root group, /, to the dataset. This
 * can yield a path name such as /group1/GXX/GYY/GXX/GYY/GXX/dset2.
 *
 * <table>
 * <caption>Figure 10 - More sample file structures</caption>
 * <tr>
 * <td>
 * \image html Groups_fig10_a.gif "a) dset1 has two names: /group2/dset1 and /group1/GXX/dset1."
 * </td>
 * <td>
 * \image html Groups_fig10_b.gif "b) dset1 again has two names: /group1/dset1 and /group1/dset2."
 * </td>
 * </tr>
 * <tr>
 * <td>
 * \image html Groups_fig10_c.gif "c) dset1 has three names: /group1/dset1, /group2/dset2, and
 * /group1/GXX/dset2."
 * </td>
 * <td>
 * \image html Groups_fig10_d.gif "d) dset1 has an infinite number of available path names."
 * </td>
 * </tr>
 * </table>
 *
 * Figure 11 takes us into the realm of soft links. The original file, in part a, contains only three
 * hard links. In part b, a soft link named dset2 from group2 to /group1/dset1 has been created,
 * making this dataset accessible as /group2/dset2.
 *
 * In part c, another soft link has been created in group2. But this time the soft link, dset3, points
 * to a target object that does not yet exist. That target object, dset, has been added in part d and is
 * now accessible as either /group2/dset or /group2/dset3.
 *
 * It could be said that HDF5 extends the organizing concepts of a file system to the internal
 * structure of a single file.
 *
 * <table>
 * <caption>Figure 11 - Hard and soft links</caption>
 * <tr>
 * <td>
 * \image html Groups_fig11_a.gif "a) The file contains only hard links."
 * </td>
 * <td>
 * \image html Groups_fig11_b.gif "b) A soft link is added from group2 to /group1/dset1."
 * </td>
 * </tr>
 * <tr>
 * <td>
 * \image html Groups_fig11_c.gif "c) A soft link named dset3 is added with a target that does not yet exist."
 * </td>
 * <td>
 * \image html Groups_fig11_d.gif "d) The target of the soft link is created or linked."
 * </td>
 * </tr>
 * </table>
 *
 * Previous Chapter \ref sec_file - Next Chapter \ref sec_dataset
 *
 */

/**
 * \defgroup H5G Groups (H5G)
 *
 * Use the functions in this module to manage HDF5 groups.
 *
 * <table>
 * <tr><th>Create</th><th>Read</th></tr>
 * <tr valign="top">
 *   <td>
 *   \snippet{lineno} H5G_examples.c create
 *   </td>
 *   <td>
 *   \snippet{lineno} H5G_examples.c read
 *   </td>
 * <tr><th>Update</th><th>Delete</th></tr>
 * <tr valign="top">
 *   <td>
 *   \snippet{lineno} H5G_examples.c update
 *   </td>
 *   <td>
 *   \snippet{lineno} H5G_examples.c delete
 *   </td>
 * </tr>
 * </table>
 *
 * \details \Bold{Groups in HDF5:} A group associates names with objects and
 *          provides a mechanism for mapping a name to an object. Since all
 *          objects appear in at least one group (with the possible exception of
 *          the root object) and since objects can have names in more than one
 *          group, the set of all objects in an HDF5 file is a directed
 *          graph. The internal nodes (nodes with an out-degree greater than zero)
 *          must be groups, while the leaf nodes (nodes with an out-degree zero) are
 *          either empty groups or objects of some other type. Exactly one
 *          object in every non-empty file is the root object. The root object
 *          always has a positive in-degree because it is pointed to by the file
 *          superblock.
 *
 *          \Bold{Locating objects in the HDF5 file hierarchy:} An object name
 *          consists of one or more components separated from one another by
 *          slashes. An absolute name begins with a slash, and the object is
 *          located by looking for the first component in the root object, then
 *          looking for the second component in the first object, etc., until
 *          the entire name is traversed. A relative name does not begin with a
 *          slash, and the traversal begins at the location specified by the
 *          create or access function.
 *
 *          \Bold{Group implementations in HDF5:} The original HDF5 group
 *          implementation provided a single-indexed structure for link
 *          storage. A new group implementation, in HDF5 Release 1.8.0, enables
 *          more efficient compact storage for very small groups, improved link
 *          indexing for large groups, and other advanced features.
 *
 *          \li The \Emph{original indexed} format remains the default. Links
 *              are stored in a B-tree in the group's local heap.
 *          \li Groups created in the new \Emph{compact-or-indexed} format, the
 *              implementation introduced with Release 1.8.0, can be tuned for
 *              performance, switching between the compact and indexed formats
 *              at thresholds set in the user application.
 *              - The \Emph{compact} format will conserve file space and processing
 *                overhead when working with small groups and is particularly
 *                valuable when a group contains no links. Links are stored
 *                as a list of messages in the group's header.
 *              - The \Emph{indexed} format will yield improved
 *                performance when working with large groups, e.g., groups
 *                containing thousands to millions of members. Links are stored in
 *                a fractal heap and indexed with an improved B-tree.
 *          \li The new implementation also enables the use of link names consisting of
 *              non-ASCII character sets (see #H5Pset_char_encoding) and is
 *              required for all link types other than hard or soft links, e.g.,
 *              external and user-defined links (see the \ref H5L APIs).
 *
 *          The original group structure and the newer structures are not
 *          directly interoperable. By default, a group will be created in the
 *          original indexed format. An existing group can be changed to a
 *          compact-or-indexed format if the need arises; there is no capability
 *          to change back. As stated above, once in the compact-or-indexed
 *          format, a group can switch between compact and indexed as needed.
 *
 *          Groups will be initially created in the compact-or-indexed format
 *          only when one or more of the following conditions is met:
 *          \li The low version bound value of the library version bounds property
 *              has been set to Release 1.8.0 or later in the file access property
 *              list (see H5Pset_libver_bounds()). Currently, that would require an
 *              H5Pset_libver_bounds() call with the low parameter set to
 *              #H5F_LIBVER_LATEST.\n When this property is set for an HDF5 file,
 *              all objects in the file will be created using the latest available
 *              format; no effort will be made to create a file that can be read by
 *              older libraries.
 *          \li The creation order tracking property, #H5P_CRT_ORDER_TRACKED, has been
 *              set in the group creation property list (see H5Pset_link_creation_order()).
 *
 *          An existing group, currently in the original indexed format, will be
 *          converted to the compact-or-indexed format upon the occurrence of
 *          any of the following events:
 *          \li An external or user-defined link is inserted into the group.
 *          \li A link named with a string composed of non-ASCII characters is
 *              inserted into the group.
 *
 *          The compact-or-indexed format offers performance improvements that
 *          will be most notable at the extremes, i.e., in groups with zero
 *          members and in groups with tens of thousands of members. But
 *          measurable differences may sometimes appear at a threshold as low as
 *          eight group members. Since these performance thresholds and criteria
 *          differ from application to application, tunable settings are
 *          provided to govern the switch between the compact and indexed
 *          formats (see H5Pset_link_phase_change()). Optimal thresholds will
 *          depend on the application and the operating environment.
 *
 *          Future versions of HDF5 will retain the ability to create, read,
 *          write, and manipulate all groups stored in either the original
 *          indexed format or the compact-or-indexed format.
 *
 */

#endif /* H5Gmodule_H */
