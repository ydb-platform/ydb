pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.5";

  src = fetchurl {
    url = "https://github.com/google/cctz/archive/refs/tags/v${version}.tar.gz";
    hash = "sha256-R9LWjny1rzKW3H5psPSnZVifGy9K9LnELnckFMQotCE=";
  };

  nativeBuildInputs = [ cmake ];

  buildInputs = [
    gtest
    gbenchmark
  ];

  cmakeFlags = [
    "-DBUILD_TESTING=ON"
    "-DBUILD_TOOLS=OFF"
    "-DBUILD_EXAMPLES=OFF"
  ];
}
