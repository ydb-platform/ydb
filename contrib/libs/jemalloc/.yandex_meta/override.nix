pkgs: attrs: with pkgs; with attrs; rec {
  version = "5.3.0";

  # FIXME: switch to fetchFromGitHub
  src = fetchurl {
    url = "https://github.com/jemalloc/jemalloc/releases/download/${version}/${pname}-${version}.tar.bz2";
    sha256 = "1apyxjd1ixy4g8xkr61p0ny8jiz8vyv1j0k4nxqkxpqrf4g2vf1d";
  };

  buildInputs = [ libunwind ];
}
