pkgs: attrs: with pkgs; rec {
  version = "1.52.0";
  pname = "libuv";

  src = fetchFromGitHub {
    owner = pname;
    repo = pname;
    rev = "v${version}";
    hash = "sha256-WyIBJjxsGo1sSjmbM1zRBF2cR97n6iSBK12FGbg73n0=";
  };

  # Setup split build.
  preConfigure = ''
    mkdir -p build
    cd build
    LIBTOOLIZE=libtoolize ../autogen.sh
  '';

  configureScript = "../configure";
}
