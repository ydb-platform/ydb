#! /bin/sh

while [ ${#} -gt 0 ] ; do
	if [ \( ${#} -gt 0 \) -a \( "x${1}" = "xpurge" \) ] ; then
		${sudo} /bin/rm -rf build
		shift
		continue
	fi

	if [ \( ${#} -gt 0 \) -a \( "x${1}" = "xsudo" \) ] ; then
		sudo="sudo"
		shift
		continue
	fi

	if [ \( ${#} -gt 0 \) -a \( "x${1}" = "xmsvcxx" \) ] ; then
		msvcxx=1
		shift
		continue
	fi

	if [ \( ${#} -gt 0 \) -a \( "x${1}" = "xx86" \) ] ; then
		arch="-A Win32"
		shift
		continue
	fi

	if [ \( ${#} -gt 0 \) -a \( "x${1}" = "xstatic-only" \) ] ; then
		skip="shared"
		shift
		continue
	fi

	if [ \( ${#} -gt 0 \) -a \( "x${1}" = "xshared-only" \) ] ; then
		skip="static"
		shift
		continue
	fi

	if [ ${#} -gt 0 ] ; then
		installPrefix="-DCMAKE_INSTALL_PREFIX=${1}"
		shift
		continue
	fi
done

umask 0022

build_target() {
	target="${1}"
	linkMode="${2}"
	if [ "x${linkMode}" = "xshared" ] ; then
		shared="-DBUILD_SHARED_LIBS=ON"
		examples="-DREPLXX_BUILD_EXAMPLES=OFF"
	else
		shared="-DBUILD_SHARED_LIBS=OFF"
		examples="-DREPLXX_BUILD_EXAMPLES=ON"
	fi
	mkdir -p "build/${target}"
	cd "build/${target}"
	if [ "x${msvcxx}" = "x1" ] ; then
		vswhere="/cygdrive/c/Program Files (x86)/Microsoft Visual Studio/Installer/vswhere.exe"
		if [ -f "${vswhere}" ] ; then
			name="$("${vswhere}" -latest -property catalog_productName | tr -d '\r')"
			ver=$("${vswhere}" -latest -property installationVersion | awk -F '.' '{print $1}')
		else
			name="Visual Studio"
			ver="14"
		fi
		cmake="/cygdrive/c/Program Files/CMake/bin/cmake.exe"
		"${cmake}" ${STATIC} ${shared} ${examples} -G "${name} ${ver}" ${arch} ${installPrefix} ../../
		"${cmake}" --build . --config Debug
		"${cmake}" --build . --config Release
		"${cmake}" --build . --config Debug --target Install
		"${cmake}" --build . --config Release --target Install
	else
		cmake -DCMAKE_BUILD_TYPE=${target} ${shared} ${examples} ${installPrefix} ../../
		make
		echo "### `hostname` ###"
		${sudo} make install
	fi
	cd ../..
}

for target in debug release ; do
	for linkMode in shared static ; do
		if [ "x${linkMode}" = "x${skip}" ] ; then
			continue
		fi
		build_target "${target}" "${linkMode}"
	done
done

