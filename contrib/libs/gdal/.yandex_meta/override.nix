pkgs: attrs: with pkgs; rec {
  version = "3.11.4";

  patches = [ ];

  src = fetchFromGitHub {
      owner = "OSGeo";
      repo = "gdal";
      rev = "v${version}";
      hash = "sha256-CFQF3vDhhXsAnIfUcn6oTQ4Xm+GH/36dqSGc0HvyEJ0=";
  };

  sourceRoot = "source";

  nativeBuildInputs = [ cmake ];

  buildInputs = [
    curl
    expat
    geos
    giflib
    hdf5
    hdf5-cpp
    libaec
    libarchive
    libgeotiff
    libjpeg
    libpng
    libtiff
    libwebp
    libxml2
    lzma
    netcdf
    openjpeg
    openssl
    postgresql
    proj
    qhull
    sqlite
    unzip
    zstd
  ];

  enableParallelBuilding = true;

  cmakeFlags = [
    "-DGDAL_ENABLE_DRIVER_JP2OPENJPEG=ON"
    "-DGDAL_ENABLE_DRIVER_LIBERTIFF=OFF"
    "-DGDAL_ENABLE_DRIVER_PCRASTER=OFF"

    "-DGDAL_USE_DEFLATE=OFF"
    "-DGDAL_USE_JPEG12_INTERNAL=OFF"
  ];
}
