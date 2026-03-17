#include <config.h>
#include <stdlib.h>
#include <ctype.h>

#ifndef HAVE_STRCASECMP

int strcasecmp( const char *s1, const char * s2 )
{
    const unsigned char *p1 = (const unsigned char *) s1;
    const unsigned char *p2 = (const unsigned char *) s2;
    unsigned char c1, c2;

    if (p1 == p2)
        return 0;

    do
    {
        c1 = tolower(*p1++);
        c2 = tolower(*p2++);
        if (c1 == '\0')
            break;
    }
    while (c1 == c2);

    return c1 - c2;
}

#endif
#ifndef HAVE_STRNCASECMP

int strncasecmp (const char *s1, const char *s2, int n )
{
    const unsigned char *p1 = (const unsigned char *) s1;
    const unsigned char *p2 = (const unsigned char *) s2;
    unsigned char c1, c2;

    if (p1 == p2)
        return 0;

    do
    {
        c1 = tolower(*p1++);
        c2 = tolower(*p2++);
        if (c1 == '\0')
            break;

        n --;
    }
    while (c1 == c2 && n > 0 );

    if ( n == 0 )
        return 0;

    return c1 - c2;
}

#endif

void ___extra_func_to_mollify_linker( void )
{
}
