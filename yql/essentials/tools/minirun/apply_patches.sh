#!/usr/bin/env bash
set -eu

errexit() {
	    echo $1
	        exit 1
	}

echo patching
cd ../../../../
patch -p0 < yql/essentials/tools/minirun/patches/01_no_icu.patch || errexit "Source patching failed"
echo done


