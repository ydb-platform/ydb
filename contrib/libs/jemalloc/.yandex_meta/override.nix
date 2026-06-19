pkgs: attrs: with pkgs; with attrs; rec {
  version = "5.3.0";

  src = fetchurl {
    url = "https://github.com/jemalloc/jemalloc/releases/download/${version}/${pname}-${version}.tar.bz2";
    sha256 = "1apyxjd1ixy4g8xkr61p0ny8jiz8vyv1j0k4nxqkxpqrf4g2vf1d";
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
