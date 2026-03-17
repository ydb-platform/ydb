/**************************************************
 * SQLGetConfigMode
 *
 **************************************************
 * This code was created by Peter Harvey @ CodeByDesign.
 * Released under LGPL 28.JAN.99
 *
 * Contributions from...
 * -----------------------------------------------
 * Peter Harvey		- pharvey@codebydesign.com
 * Nick Gorham      - nick@lurcher.org
 **************************************************/
#include <config.h>
#include <stdlib.h>
#include <odbcinstext.h>

BOOL SQLGetConfigMode( UWORD *pnConfigMode )
{
    inst_logClear();

    __lock_config_mode();
    *pnConfigMode = __get_config_mode();
    __unlock_config_mode();

    return TRUE;
}


