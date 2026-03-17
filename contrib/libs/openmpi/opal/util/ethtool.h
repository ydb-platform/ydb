/*
 * Copyright (c) 2016 Karol Mroz.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_ETHTOOL_H
#define OPAL_ETHTOOL_H

/*
 * Obtain an appropriate bandwidth for the interface if_name. On Linux, we
 * get this via an ioctl(). Elsewhere or in the error case, we return the
 * speed as 0.
 */
unsigned int opal_ethtool_get_speed(const char *if_name);

#endif
