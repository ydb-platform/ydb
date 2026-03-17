pkgs: attrs: with pkgs; with attrs; rec {
  version = "4.10.0";

  src = fetchFromGitHub {
    owner = "Unidata";
    repo = "netcdf-c";
    rev = "v${version}";
    hash = "sha256-Q+T3NhdoJ1gdG7DK9mJfLGqKE8JpigTxWkXto9hHf/Q=";
  };

  buildInputs = [ m4 zlib hdf5 curl ];
  nativeBuildInputs = [ cmake ];

  patches = [];

  cmakeFlags=[
      "-DENABLE_HDF5=ON"
  ];
}
