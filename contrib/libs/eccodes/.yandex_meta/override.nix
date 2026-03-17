pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.40.0";

  src = fetchurl {
    url = "https://confluence.ecmwf.int/download/attachments/45757960/eccodes-${version}-Source.tar.gz";
    hash = "sha256-9Y1dc5D86Gxism12ubw8TX2abPLl+BRdHVmAiRleUf8=";
  };

  buildInputs = [
    openjpeg
    libaec
    libpng

    python3
  ];

  cmakeFlags = [
    "-DCMAKE_BUILD_TYPE=Release"
    "-DENABLE_FORTRAN=OFF"
    "-DENABLE_MEMFS=ON"
    "-DENABLE_NETCDF=OFF"
    "-DENABLE_PNG=ON"
  ];
}
