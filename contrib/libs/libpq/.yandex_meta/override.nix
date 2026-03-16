pkgs: attrs: with pkgs; with attrs; rec {
  version = "18.3";
  version_with_underscores = "${lib.replaceStrings ["."] ["_"] version}";

  src = fetchFromGitHub {
    owner = "postgres";
    repo = "postgres";
    rev = "REL_${version_with_underscores}";
    hash = "sha256-3cu3oyPJ5q6ewv7RFY7Nys5l+10dsQv5f1HNIoYtrO8=";
  };

  patches = [];

  nativeBuildInputs = [
    bison
    flex
    perl
    pkg-config
  ];

  buildPhase = ''
    make -j$NIX_BUILD_CORES submake-generated-headers
    make -j$NIX_BUILD_CORES -C src/common
    make -j$NIX_BUILD_CORES -C src/port
    make -j$NIX_BUILD_CORES -C src/interfaces/libpq all-shared-lib
  '';

  configureFlags = attrs.configureFlags ++ [
    "--without-gssapi"
    "--build=x86_64-unknown-linux-gnu"
  ];
}
