pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.7.4";

  src = fetchFromGitHub {
    owner = "OSGeo";
    repo = "libgeotiff";
    rev = "${version}";
    hash = "sha256-oiuooLejCRI1DFTjhgYoePtKS+OAGnW6OBzgITcY500=";
  };

  cmakeFlags = [
    "-DWITH_UTILITIES=FALSE"
  ];

  nativeBuildInputs = [
    cmake
    pkg-config
  ];

  buildInputs = [
    libtiff
    proj
  ];
}
