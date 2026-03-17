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
 *          H5E package.  Including this header means that the source file
 *          is part of the H5E package.
 */
#ifndef H5Emodule_H
#define H5Emodule_H

/* Define the proper control macros for the generic FUNC_ENTER/LEAVE and error
 *      reporting macros.
 */
#define H5E_MODULE
#define H5_MY_PKG     H5E
#define H5_MY_PKG_ERR H5E_ERROR

/** \page H5E_UG HDF5 Error Handling
 *
 * \section sec_error HDF5 Error Handling
 *
 * The HDF5 library provides an error reporting mechanism for both the library itself and for user
 * application programs. It can trace errors through function stack and error information like file
 * name, function name, line number, and error description.
 *
 * \subsection subsec_error_intro Introduction
 * The HDF5 Library provides an error reporting mechanism for both the library itself and for user application
 * programs. It can trace errors through function stack and error information like file name, function name,
 * line number, and error description.
 *
 * \ref subsec_error_ops discusses the basic error concepts such as error stack, error record, and error
 * message and describes the related API functions. These concepts and functions are sufficient for
 * application programs to trace errors inside the HDF5 Library.
 *
 * \ref subsec_error_adv talks about the advanced concepts of error
 * class and error stack handle and talks about the related functions. With these concepts and functions, an
 * application library or program using the HDF5 Library can have its own error report blended with HDF5's
 * error report.
 *
 * Starting with Release 1.8, we have a new set of Error Handling API functions. For the purpose of backward
 * compatibility with version 1.6 and before, we still keep the old API functions, \ref H5Epush1,
 * \ref H5Eprint1, \ref H5Ewalk1, \ref H5Eclear1, \ref H5Eget_auto1, \ref H5Eset_auto1. These functions do
 * not have the error stack as a parameter. The library allows them to operate on the default error stack.
 * (The H5E compatibility macros will choose the correct function based on the parameters)
 *
 * The old API is similar to functionality discussed in \ref subsec_error_ops. The functionality discussed in
 * \ref subsec_error_adv,the ability of allowing applications to add their own error records, is the new
 * design for the Error Handling API.
 *
 * \subsection subsec_error_H5E Error Handling Function Summaries
 * @see H5E reference manual
 *
 * \subsection subsec_error_program Programming Model for Error Handling
 * This section is under construction.
 *
 * \subsection subsec_error_ops Basic Error Handling Operations
 * Let us first try to understand the error stack. An error stack is a collection of error records. Error
 * records can be pushed onto or popped off the error stack. By default, when an error occurs deep within
 * the HDF5 Library, an error record is pushed onto an error stack and that function returns a failure
 * indication.
 * Its caller detects the failure, pushes another record onto the stack, and returns a failure indication.
 * This continues until the API function called by the application returns a failure indication. The next
 * API function being called will reset the error stack. All HDF5 Library error records belong to the same
 * error class. For more information, see \ref subsec_error_adv.
 *
 * \subsubsection subsubsec_error_ops_stack Error Stack and Error Message
 * In normal circumstances, an error causes the stack to be printed on the standard error stream
 * automatically.
 * This automatic error stack is the library's default stack. For all the functions in this section, whenever
 * an error stack ID is needed as a parameter, \ref H5E_DEFAULT can be used to indicate the library's default
 * stack. The first error record of the error stack, number #000, is produced by the API function itself and
 * is usually sufficient to indicate to the application what went wrong.
 *  <table>
 *     <caption align=top>Example: An Error Message</caption>
 *     <tr>
 *       <td>
 *         <p>If an application calls \ref H5Tclose  on a
 *       predefined datatype then the following message is
 *       printed on the standard error stream.  This is a
 *       simple error that has only one component, the API
 *       function; other errors may have many components.
 *         <p><code><pre>
 * HDF5-DIAG: Error detected in HDF5 (1.10.9) thread 0.
 *    #000: H5T.c line ### in H5Tclose(): predefined datatype
 *       major: Function argument
 *       minor: Bad value
 *         </pre></code>
 *       </td>
 *     </tr>
 *   </table>
 * In the example above, we can see that an error record has a major message and a minor message. A major
 * message generally indicates where the error happens. The location can be a dataset or a dataspace, for
 * example. A minor message explains further details of the error. An example is “unable to open file”.
 * Another specific detail about the error can be found at the end of the first line of each error record.
 * This error description is usually added by the library designer to tell what exactly goes wrong. In the
 * example above, the “predefined datatype” is an error description.
 *
 * \subsubsection subsubsec_error_ops_print  Print and Clear an Error Stack
 * Besides the automatic error report, the error stack can also be printed and cleared by the functions
 * \ref H5Eprint2 and \ref H5Eclear2. If an application wishes to make explicit
 * calls to \ref H5Eprint2 to print the error stack, the automatic printing should be turned off
 * to prevent error messages from being displayed twice (see \ref H5Eset_auto2).
 *
 * <em>To print an error stack:</em>
 * \code
 *      herr_t H5Eprint2(hid_t error_stack, FILE * stream)
 * \endcode
 * This function prints the error stack specified by error_stack on the specified stream, stream. If the
 * error stack is empty, a one‐line message will be printed. The following is an example of such a message.
 * This message would be generated if the error was in the HDF5 Library.
 * \code
 *      HDF5-DIAG: Error detected in HDF5 Library version: 1.10.9 thread 0.
 * \endcode
 *
 * <em>To clear an error stack:</em>
 * \code
 *      herr_t H5Eclear2(hid_t error_stack)
 * \endcode
 * The \ref H5Eclear2 function shown above clears the error stack specified by error_stack.
 * \ref H5E_DEFAULT can be passed in to clear the current error stack. The current stack is also cleared
 * whenever an API function is called; there are certain exceptions to this rule such as \ref H5Eprint2.
 *
 * \subsubsection subsubsec_error_ops_mute Mute Error Stack
 * Sometimes an application calls a function for the sake of its return value, fully expecting the function
 * to fail; sometimes the application wants to call \ref H5Eprint2 explicitly. In these situations,
 * it would be misleading if an error message were still automatically printed. Using the
 * \ref H5Eset_auto2 function can control the automatic printing of error messages.
 *
 * <em>To enable or disable automatic printing of errors:</em>
 * \code
 *      herr_t H5Eset_auto2(hid_t error_stack, H5E_auto_t func, void *client_data)
 * \endcode
 * The \ref H5Eset_auto2 function can be used to turn on or off the automatic printing of errors
 * for the error stack specified by error_stack. When turned on (non‐null func pointer), any API function
 * which returns an error indication will first call func, passing it client_data as an argument. When the
 * library is first initialized the auto printing function is set to \ref H5Eprint2 and client_data
 * is the standard error stream pointer, stderr.
 *
 * <em>To see the current settings:</em>
 * \code
 *      herr_t H5Eget_auto(hid_t error_stack, H5E_auto_t * func, void **client_data)
 * \endcode
 * The function above returns the current settings for the automatic error stack traversal function, func, and
 * its data, client_data. If either or both of the arguments are null, then the value is not returned.
 *
 * An application can temporarily turn off error messages while “probing” a function. See the
 * example below.
 *
 * <em>Example: Turn off error messages while probing a function</em>
 * \code
 *     ***  Save old error handler  ***
 *      H5E_auto2_t oldfunc;
 *      void *old_client_data;
 *      H5Eget_auto2(error_stack, &old_func, &old_client_data);
 *      ***  Turn off error handling  ***
 *      H5Eset_auto2(error_stack, NULL, NULL);
 *      ***  Probe. Likely to fail, but that's okay  ***
 *      status = H5Fopen (......);
 *      ***  Restore previous error handler  ***
 *      H5Eset_auto2(error_stack, old_func, old_client_data);
 * \endcode
 *
 * Or automatic printing can be disabled altogether and error messages can be explicitly printed.
 *
 * <em>Example: Disable automatic printing and explicitly print error messages</em>
 * \code
 *      ***  Turn off error handling permanently  ***
 *      H5Eset_auto2(error_stack, NULL, NULL);
 *      ***  If failure, print error message  ***
 *      if (H5Fopen (....)<0) {
 *          H5Eprint2(H5E_DEFAULT, stderr);
 *          exit (1);
 *      }
 * \endcode
 *
 * \subsubsection subsubsec_error_ops_custom_print Customized Printing of an Error Stack
 * Applications are allowed to define an automatic error traversal function other than the default
 * \ref H5Eprint(). For instance, one can define a function that prints a simple, one‐line error message to
 * the standard error stream and then exits. The first example below defines a such a function. The second
 * example below installs the function as the error handler.
 *
 * <em>Example: Defining a function to print a simple error message</em>
 * \code
 *      herr_t
 *      my_hdf5_error_handler(void *unused)
 *      {
 *          fprintf (stderr, “An HDF5 error was detected. Bye.\\n”);
 *          exit (1);
 *      }
 * \endcode
 *
 * <em>Example: The user‐defined error handler</em>
 * \code
 *      H5Eset_auto2(H5E_DEFAULT, my_hdf5_error_handler, NULL);
 * \endcode
 *
 * \subsubsection subsubsec_error_ops_walk Walk through the Error Stack
 * The \ref H5Eprint2 function is actually just a wrapper around the more complex \ref H5Ewalk function
 * which traverses an error stack and calls a user‐defined function for each member of the stack. The example
 * below shows how \ref H5Ewalk is used.
 * \code
 *      herr_t H5Ewalk(hid_t err_stack, H5E_direction_t direction,
 *      H5E_walk_t func, void *client_data)
 * \endcode
 * The error stack err_stack is traversed and func is called for each member of the stack. Its arguments
 * are an integer sequence number beginning at zero (regardless of direction) and the client_data
 * pointer. If direction is \ref H5E_WALK_UPWARD, then traversal begins at the inner‐most function that
 * detected the error and concludes with the API function. Use \ref H5E_WALK_DOWNWARD for the opposite
 * order.
 *
 * \subsubsection subsubsec_error_ops_travers Traverse an Error Stack with a Callback Function
 * An error stack traversal callback function takes three arguments: n is a sequence number beginning at
 * zero for each traversal, eptr is a pointer to an error stack member, and client_data is the same pointer
 * used in the example above passed to \ref H5Ewalk. See the example below.
 * \code
 *      typedef herr_t (*H5E_walk_t)(unsigned n, H5E_error2_t *eptr, void *client_data)
 * \endcode
 * The H5E_error2_t structure is shown below.
 * \code
 *      typedef struct {
 *          hid_t cls_id;
 *          hid_t maj_num;
 *          hid_t min_num;
 *          unsigned line;
 *          const char *func_name;
 *          const char *file_name;
 *          const char *desc;
 *      } H5E_error2_t;
 * \endcode
 * The maj_num and min_num are major and minor error IDs, func_name is the name of the function where
 * the error was detected, file_name and line locate the error within the HDF5 Library source code, and
 * desc points to a description of the error.
 *
 * The following example shows a user‐defined callback function.
 *
 * <em>Example: A user‐defined callback function</em>
 * \code
 *      \#define MSG_SIZE 64
 *      herr_t
 *      custom_print_cb(unsigned n, const H5E_error2_t *err_desc, void *client_data)
 *      {
 *          FILE *stream = (FILE *)client_data;
 *          char maj[MSG_SIZE];
 *          char min[MSG_SIZE];
 *          char cls[MSG_SIZE];
 *          const int indent = 4;
 *
 *          ***  Get descriptions for the major and minor error numbers  ***
 *          if(H5Eget_class_name(err_desc->cls_id, cls, MSG_SIZE) < 0)
 *              TEST_ERROR;
 *          if(H5Eget_msg(err_desc->maj_num, NULL, maj, MSG_SIZE) < 0)
 *              TEST_ERROR;
 *          if(H5Eget_msg(err_desc->min_num, NULL, min, MSG_SIZE) < 0)
 *              TEST_ERROR;
 *          fprintf (stream, “%*serror #%03d: %s in %s():
 *                  line %u\\n”,
 *                  indent, “”, n, err_desc->file_name,
 *                  err_desc->func_name, err_desc->line);
 *          fprintf (stream, “%*sclass: %s\\n”, indent*2, “”, cls);
 *          fprintf (stream, “%*smajor: %s\\n”, indent*2, “”, maj);
 *          fprintf (stream, “%*sminor: %s\\n”, indent*2, “”, min);
 *          return 0;
 *      error:
 *          return -1;
 *      }
 * \endcode
 *
 * <h4>Programming Note for C++ Developers Using C Functions</h4>
 * If a C routine that takes a function pointer as an argument is called from within C++ code, the C routine
 * should be returned from normally.
 *
 * Examples of this kind of routine include callbacks such as \ref H5Pset_elink_cb and
 * \ref H5Pset_type_conv_cb and
 * functions such as \ref H5Tconvert and \ref H5Ewalk2.
 *
 * Exiting the routine in its normal fashion allows the HDF5 C Library to clean up its work properly. In other
 * words, if the C++ application jumps out of the routine back to the C++ “catch” statement, the library is
 * not given the opportunity to close any temporary data structures that were set up when the routine was
 * called. The C++ application should save some state as the routine is started so that any problem that
 * occurs might be diagnosed.
 *
 * \subsection subsec_error_adv Advanced Error Handling Operations
 * The section above, see \ref subsec_error_ops, discusses the basic error
 * handling operations of the library. In that section, all the error records on the error stack are from the
 * library itself. In this section, we are going to introduce the operations that allow an application program
 * to push its own error records onto the error stack once it declares an error class of its own through the
 * HDF5 Error API.
 *
 * <table>
 *   <caption align=top>Example:  An Error Report</caption>
 *   <tr>
 *     <td>
 *       <p>An error report shows both the library's error record and the application's error records.
 *          See the example below.
 *       <p><code><pre>
 * Error Test-DIAG: Error detected in Error Program (1.0)
 *         thread 8192:
 *     #000: ../../hdf5/test/error_test.c line ### in main():
 *         Error test failed
 *       major: Error in test
 *       minor: Error in subroutine
 *     #001: ../../hdf5/test/error_test.c line ### in
 *         test_error(): H5Dwrite failed as supposed to
 *       major: Error in IO
 *       minor: Error in H5Dwrite
 *   HDF5-DIAG: Error detected in HDF5 (1.10.9) thread #####:
 *     #002: ../../hdf5/src/H5Dio.c line ### in H5Dwrite():
 *         not a dataset
 *       major: Invalid arguments to routine
 *       minor: Inappropriate type
 *       </pre></code>
 *     </td>
 *   </tr>
 * </table>
 * In the line above error record #002 in the example above, the starting phrase is HDF5. This is the error
 * class name of the HDF5 Library. All of the library's error messages (major and minor) are in this default
 * error class. The Error Test in the beginning of the line above error record #000 is the name of the
 * application's error class. The first two error records, #000 and #001, are from application's error class.
 * By definition, an error class is a group of major and minor error messages for a library (the HDF5 Library
 * or an application library built on top of the HDF5 Library) or an application program. The error class can
 * be registered for a library or program through the HDF5 Error API. Major and minor messages can be defined
 * in an error class. An application will have object handles for the error class and for major and minor
 * messages for further operation. See the example below.
 *
 * <em>Example: The user‐defined error handler</em>
 * \code
 *      \#define MSG_SIZE 64
 *      herr_t
 *      custom_print_cb(unsigned n, const H5E_error2_t *err_desc,
 *      void* client_data)
 *      {
 *          FILE *stream = (FILE *)client_data;
 *          char maj[MSG_SIZE];
 *          char min[MSG_SIZE];
 *          char cls[MSG_SIZE];
 *          const int indent = 4;
 *
 *          ***  Get descriptions for the major and minor error numbers  ***
 *          if(H5Eget_class_name(err_desc->cls_id, cls, MSG_SIZE) < 0)
 *              TEST_ERROR;
 *          if(H5Eget_msg(err_desc->maj_num, NULL, maj, MSG_SIZE) < 0)
 *              TEST_ERROR;
 *          if(H5Eget_msg(err_desc->min_num, NULL, min, MSG_SIZE) < 0)
 *              TEST_ERROR;
 *          fprintf (stream, “%*serror #%03d: %s in %s():
 *                  line %u\\n”,
 *                  indent, “”, n, err_desc->file_name,
 *                  err_desc->func_name, err_desc->line);
 *          fprintf (stream, “%*sclass: %s\\n”, indent*2, “”, cls);
 *          fprintf (stream, “%*smajor: %s\\n”, indent*2, “”, maj);
 *          fprintf (stream, “%*sminor: %s\\n”, indent*2, “”, min);
 *          return 0;
 *      error:
 *          return -1;
 *      }
 * \endcode
 *
 * \subsubsection subsubsec_error_adv_more More Error API Functions
 * The Error API has functions that can be used to register or unregister an error class, to create or close
 * error messages, and to query an error class or error message. These functions are illustrated below.
 *
 * <em>To register an error class:</em>
 * \code
 *      hid_t H5Eregister_class(const char* cls_name, const char* lib_name, const char* version)
 * \endcode
 * This function registers an error class with the HDF5 Library so that the application library or program
 * can report errors together with the HDF5 Library.
 *
 * <em>To add an error message to an error class:</em>
 * \code
 *      hid_t H5Ecreate_msg(hid_t class, H5E_type_t msg_type, const char* mesg)
 * \endcode
 * This function adds an error message to an error class defined by an application library or program. The
 * error message can be either major or minor which is indicated by parameter msg_type.
 *
 * <em>To get the name of an error class:</em>
 * \code
 *      ssize_t H5Eget_class_name(hid_t class_id, char* name, size_t size)
 * \endcode
 * This function retrieves the name of the error class specified by the class ID.
 *
 * <em>To retrieve an error message:</em>
 * \code
 *      ssize_t H5Eget_msg(hid_t mesg_id, H5E_type_t* mesg_type, char* mesg, size_t size)
 * \endcode
 * This function retrieves the error message including its length and type.
 *
 * <em>To close an error message:</em>
 * \code
 *      herr_t H5Eclose_msg(hid_t mesg_id)
 * \endcode
 * This function closes an error message.
 *
 * <em>To remove an error class:</em>
 * \code
 *      herr_t H5Eunregister_class(hid_t class_id)
 * \endcode
 * This function removes an error class from the Error API.
 *
 * The example below shows how an application creates an error class and error messages.
 *
 * <em>Example: Create an error class and error messages</em>
 * \code
 *      ***  Create an error class  ***
 *      class_id = H5Eregister_class(ERR_CLS_NAME, PROG_NAME, PROG_VERS);
 *      ***  Retrieve class name  ***
 *      H5Eget_class_name(class_id, cls_name, cls_size);
 *      ***  Create a major error message in the class  ***
 *      maj_id = H5Ecreate_msg(class_id, H5E_MAJOR, “... ...”);
 *      ***  Create a minor error message in the class  ***
 *      min_id = H5Ecreate_msg(class_id, H5E_MINOR, “... ...”);
 * \endcode
 *
 * The example below shows how an application closes error messages and unregisters the error class.
 *
 * <em>Example: Closing error messages and unregistering the error class</em>
 * \code
 *      H5Eclose_msg(maj_id);
 *      H5Eclose_msg(min_id);
 *      H5Eunregister_class(class_id);
 * \endcode
 *
 * \subsubsection subsubsec_error_adv_app Pushing an Application Error Message onto Error Stack
 * An application can push error records onto or pop error records off of the error stack just as the library
 * does internally. An error stack can be registered, and an object handle can be returned to the application
 * so that the application can manipulate a registered error stack.
 *
 * <em>To register the current stack:</em>
 * \code
 *      hid_t H5Eget_current_stack(void)
 * \endcode
 * This function registers the current error stack, returns an object handle, and clears the current error
 * stack.
 * An empty error stack will also be assigned an ID.
 *
 * <em>To replace the current error stack with another:</em>
 * \code
 *      herr_t H5Eset_current_stack(hid_t error_stack)
 * \endcode
 * This function replaces the current error stack with another error stack specified by error_stack and
 * clears the current error stack. The object handle error_stack is closed after this function call.
 *
 * <em>To push a new error record to the error stack:</em>
 * \code
 *      herr_t H5Epush(hid_t error_stack, const char* file, const char* func,
 *                     unsigned line, hid_t cls_id, hid_t major_id, hid_t minor_id,
 *                     const char* desc, ... )
 * \endcode
 * This function pushes a new error record onto the error stack for the current thread.
 *
 * <em>To delete some error messages:</em>
 * \code
 *      herr_t H5Epop(hid_t error_stack, size_t count)
 * \endcode
 * This function deletes some error messages from the error stack.
 *
 * <em>To retrieve the number of error records:</em>
 * \code
 *      int H5Eget_num(hid_t error_stack)
 * \endcode
 * This function retrieves the number of error records from an error stack.
 *
 * <em>To clear the error stack:</em>
 * \code
 *      herr_t H5Eclear_stack(hid_t error_stack)
 * \endcode
 * This function clears the error stack.
 *
 * <em>To close the object handle for an error stack:</em>
 * \code
 *      herr_t H5Eclose_stack(hid_t error_stack)
 * \endcode
 * This function closes the object handle for an error stack and releases its resources.
 *
 * The example below shows how an application pushes an error record onto the default error stack.
 *
 * <em>Example: Pushing an error message to an error stack</em>
 * \code
 *      ***  Make call to HDF5 I/O routine  ***
 *      if((dset_id=H5Dopen(file_id, dset_name, access_plist)) < 0)
 *      {
 *          ***  Push client error onto error stack  ***
 *          H5Epush(H5E_DEFAULT,__FILE__,FUNC,__LINE__,cls_id,
 *                  CLIENT_ERR_MAJ_IO,CLIENT_ERR_MINOR_OPEN, “H5Dopen failed”);
 *      }
 *      ***  Indicate error occurred in function  ***
 *      return 0;
 * \endcode
 *
 * The example below shows how an application registers the current error stack and
 * creates an object handle to avoid another HDF5 function from clearing the error stack.
 *
 * <em>Example: Registering the error stack</em>
 * \code
 *      if (H5Dwrite(dset_id, mem_type_id, mem_space_id, file_space_id, dset_xfer_plist_id, buf) < 0)
 *      {
 *          ***  Push client error onto error stack  ***
 *          H5Epush2(H5E_DEFAULT,__FILE__,FUNC,__LINE__,cls_id,
 *                  CLIENT_ERR_MAJ_IO,CLIENT_ERR_MINOR_HDF5,
 *                  “H5Dwrite failed”);
 *          ***  Preserve the error stack by assigning an object handle to it  ***
 *          error_stack = H5Eget_current_stack();
 *          ***  Close dataset  ***
 *          H5Dclose(dset_id);
 *          ***  Replace the current error stack with the preserved one  ***
 *          H5Eset_current_stack(error_stack);
 *      }
 *      return 0;
 * \endcode
 *
 * Previous Chapter \ref sec_attribute - Next Chapter \ref sec_plist
 *
 * \defgroup H5E Error Handling (H5E)
 *
 * \internal The \c FUNC_ENTER macro clears the error stack whenever an
 *           interface function is entered. When an error is detected, an entry
 *           is pushed onto the stack. As the functions unwind, additional
 *           entries are pushed onto the stack. The API function will return
 *           some indication that an error occurred and the application can
 *           print the error stack.
 *
 * \internal Certain API functions in the \ref H5E package, such as H5Eprint(),
 *           do not clear the error stack. Otherwise, any function which does
 *           not have an underscore immediately after the package name will
 *           clear the error stack. For instance, H5Fopen() clears the error
 *           stack while \Code{H5F_open} does not.
 *
 * \internal An error stack has a fixed maximum size. If this size is exceeded
 *           then the stack will be truncated and only the inner-most functions
 *           will have entries on the stack. This is expected to be a rare
 *           condition.
 *
 * \internal Each thread has its own error stack, but since multi-threading has
 *           not been added to the library yet, this package maintains a single
 *           error stack. The error stack is statically allocated to reduce the
 *           complexity of handling errors within the \ref H5E package.
 *
 * @see sec_error
 *
 */

#endif /* H5Emodule_H */
