pkgs: attrs: with pkgs; with attrs; rec {
  version = "5.3.1";

  src = fetchurl {
    url = "https://github.com/jemalloc/jemalloc/releases/download/${version}/${pname}-${version}.tar.bz2";
    sha256 = "sha256-OCa8gCMvIu1cRmLzA095nKMW6BkQO9x7uZAYpCFwb5I=";
	};	
  
	# FIXME:
  #   Study how to switch this import to fetchFromGitHub.
	#   Running autogen.sh fails.  
	# src = fetchFromGitHub {
  #  owner = "jemalloc";
  #  repo = "jemalloc";
  #  rev = "${version}";
  #  sha256 = "sha256-bb0OhZVXyvN+hf9BpPSykn5cGm87a0C+Y/iXKt9wTSs=";
  # };

  patches = [
		# Disable gcc -pipe attribute which breaks fptrace tracing
    ./disable-gcc-pipe.patch
  ];

  # preConfigure = ''
  #   ./autogen.sh
  # '';

  buildInputs = [ libunwind ];
}
