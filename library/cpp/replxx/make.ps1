param (
	[Parameter(Mandatory=$True)] [string]$target,
	[Parameter(Mandatory=$False)] [string]$prefix = "",
	[Parameter(Mandatory=$False)] [string]$generator = "",
	[Parameter(Mandatory=$False)] [switch]$with_shared = $false,
	[Parameter(Mandatory=$False)] [switch]$with_examples = $false
)

function make_absolute( [string]$path ) {
	if ( -Not( [System.IO.Path]::IsPathRooted( $path ) ) ) {
		$path = [IO.Path]::GetFullPath( [IO.Path]::Combine( ( ($pwd).Path ), ( $path ) ) )
	}
	return $path.Replace( "\", "/" )
}

function purge {
	Write-Host -NoNewline "Purging... "
	Remove-Item "build" -Recurse -ErrorAction Ignore
	Write-Host "done."
}

function build( [string]$config, [boolean]$install, [boolean]$build_shared ) {
	New-Item -ItemType Directory -Force -Path "build/$config" > $null
	if ( $prefix -eq "" ) {
		throw "The ``prefix`` paremeter was not specified."
	}
	$prefix = make_absolute( $prefix )
	Push-Location "build/$config"
	$shared="-DBUILD_SHARED_LIBS=$(if ( $build_shared ) { "ON" } else { "OFF" } )"
	$examples="-DREPLXX_BUILD_EXAMPLES=$( if ( $with_examples ) { "ON" } else { "OFF" } )"
	if ( $generator -ne "" ) {
		$genOpt = "-G"
	}
	cmake $shared $examples $genOpt $generator "-DCMAKE_INSTALL_PREFIX=$prefix" ../../
	cmake --build . --config $config
	if ( $install ) {
		cmake --build . --target install --config $config
	}
	Pop-Location
}

function debug( [boolean]$install = $false ) {
	build "debug" $install $false
	if ( $with_shared ) {
		build "debug" $install $true	
	}
}

function release( [boolean]$install = $false ) {
	build "release" $install $false
	if ( $with_shared ) {
		build "release" $install $true	
	}
}

function install-debug {
	debug $true
}

function install-release {
	release $true
}

if (
	( $target -ne "debug" ) -and
	( $target -ne "release" ) -and
	( $target -ne "install-debug" ) -and
	( $target -ne "install-release" ) -and
	( $target -ne "purge" )
) {
	Write-Error "Unknown target: ``$target``"
	exit 1
}

try {
	&$target
} catch {
	Pop-Location
	Write-Error "$_"
}
