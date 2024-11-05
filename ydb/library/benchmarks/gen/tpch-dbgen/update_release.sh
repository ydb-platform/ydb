#!/bin/bash
PATH=.:$PATH;export PATH
if [ -f UPDATE_RELEASE_NUMBER ]
then
cvs -q update
if [ ! -f release.h ]
then
BUILD=1
else
BUILD=`grep BUILD release.h |cut -f3 -d' '`
BUILD=`expr $BUILD + 1`
fi
cat > release.h << __EOF__
/*
 * $Id: update_release.sh,v 1.4 2008/03/21 17:38:39 jms Exp $
 */
#define VERSION $1
#define RELEASE $2
#define PATCH $3
#define BUILD $BUILD
__EOF__
cvs commit -m "update release number" release.h
fi
