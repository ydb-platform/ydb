/**************************************************
 * dltest
 *
 **************************************************
 * This code was created by Peter Harvey @ CodeByDesign.
 * Released under GPL 31.JAN.99
 *
 * Contributions from...
 * -----------------------------------------------
 * Peter Harvey		- pharvey@codebydesign.com
 **************************************************/
#include <config.h>
#include <stdio.h>
#include <stdlib.h>
#include <ltdl.h>

char *szSyntax =
"\n" \
"**********************************************\n" \
"* unixODBC - dltest                          *\n" \
"**********************************************\n" \
"* Syntax                                     *\n" \
"*                                            *\n" \
"*      dltest libName Symbol                 *\n" \
"*                                            *\n" \
"* libName                                    *\n" \
"*                                            *\n" \
"*      Full path + file name of share to test*\n" \
"*                                            *\n" \
"* Symbol                                     *\n" \
"*                                            *\n" \
"*      ie a function name in the share       *\n" \
"*                                            *\n" \
"* Notes                                      *\n" \
"*                                            *\n" \
"*      This can be placed into a makefile    *\n" \
"*      to throw an error if test fails.      *\n" \
"*                                            *\n" \
"*      If this segfaults you probably have an*\n" \
"*      unresolved symbol in the lib. This is *\n" \
"*      not caught since dltest started using *\n" \
"*      libtool. Linux users can refer to the *\n" \
"*      man page for dlopen to create a       *\n" \
"*      better test.                          *\n" \
"*                                            *\n" \
"*                                            *\n" \
"* Examples                                   *\n" \
"*                                            *\n" \
"*      dltest /usr/lib/libMy.so MyFunc       *\n" \
"*                                            *\n" \
"* Please visit;                              *\n" \
"*                                            *\n" \
"*      http://www.unixodbc.org/              *\n" \
"*      Peter Harvey                          *\n" \
"**********************************************\n\n";

int main( int argc, char *argv[] )
{
	void	*hDLL;
	void	(*pFunc)();
	const char	*pError;

	if ( argc < 2  )
	{
		puts( szSyntax );
		exit( 1 );
	}

    /*
     * initialize libtool
     */

    if ( lt_dlinit() )
	{
		printf( "ERROR: Failed to lt_dlinit()\n" );
		exit( 1 );
	}

    hDLL = lt_dlopen( argv[1] );
	if ( !hDLL )
	{
		printf( "[dltest] ERROR dlopen: %s\n", lt_dlerror() );
		exit( 1 );
	}
	printf( "SUCCESS: Loaded %s\n", argv[1] );
	if ( argc > 2 )
	{
		pFunc = (void (*)()) lt_dlsym( hDLL, argv[2] );
/* PAH - lt_dlerror() is not a good indicator of success    */
/*		if ( (pError = lt_dlerror()) != NULL )              */
		if ( !pFunc )
		{
            if ( (pError = lt_dlerror()) != NULL )
			    printf( "ERROR: %s\n Could not find %s\n", pError, argv[2] );
            else
			    printf( "ERROR: Could not find %s\n", argv[2] );
			exit( 1 );
		}
		printf( "SUCCESS: Found %s\n", argv[2] );
	}
	lt_dlclose( hDLL );

	return ( 0 );
}

