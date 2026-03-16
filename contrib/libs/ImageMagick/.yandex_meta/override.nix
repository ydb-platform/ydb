pkgs: attrs: with pkgs; rec {
  version = "6.9.13-40";

  src = fetchFromGitHub {
    owner = "ImageMagick";
    repo = "ImageMagick6";
    rev = version;
    sha256 = "sha256-53d369evC38A0WmIGqhqERipEmTyysuNf+wi7pBzBr4=";
  };

  nativeBuildInputs = [
    pkg-config
    # do not add libtool here, as we are building --without-modules
  ];

  buildInputs = [
    glib
    lcms
    libheif
    libpng
    libraw
    librsvg
    libxml2
    libwebp
    openjpeg
    potrace
    zlib
    zstd
  ];

  configureFlags = attrs.configureFlags ++ [
    "--build=x86_64-pc-linux-gnu"
    "--disable-dependency-tracking"
    "--disable-openmp"
    "--with-heic"
    "--with-lcms"
    "--with-quantum-depth=8"
    "--with-rsvg"
    "--without-fftw"
    "--without-fontconfig"
    "--without-modules"
    "--without-opencl"
    "--without-x"
  ];
}
