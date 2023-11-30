/** Compiler deficiency workarounds for compiling libpqxx headers.
 *
 * To be called at the start of each libpqxx header, in order to push the
 * client program's settings and apply libpqxx's settings.
 *
 * Must be balanced by an include of -header-post.hxx at the end
 * of the header.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
// NO GUARDS HERE! This code should be executed every time!

#ifdef _WIN32
#ifdef _MSC_VER

// Save client program warning state, and set warning level 4.
// Setting the warning level explicitly ensures that libpqxx
// headers will work with this warning level as well.
#pragma warning (push,4)

#pragma warning (disable: 4251)
#pragma warning (disable: 4273)
#pragma warning (disable: 4275)
#pragma warning (disable: 4355)
#pragma warning (disable: 4511) // Copy constructor could not be generated.
#pragma warning (disable: 4512) // Assignment operator could not be generated.
#pragma warning (disable: 4996) // Deprecation warning, e.g. about strncpy().

#endif // _MSC_VER
#endif // _WIN32

