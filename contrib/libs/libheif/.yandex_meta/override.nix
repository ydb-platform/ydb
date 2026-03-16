pkgs: attrs: with pkgs; rec {
  version = "1.21.2";

  src = fetchFromGitHub {
    owner = "strukturag";
    repo = "libheif";
    rev = "v${version}";
    hash = "sha256-odkJ0wZSGoZ7mX9fkaNREDpMvQuQA9HKaf3so1dYrbc=";
  };

  nativeBuildInputs = [ cmake ];

  cmakeFlags = [
    "-DENABLE_PLUGIN_LOADING=0"
    "-DENABLE_MULTITHREADING_SUPPORT=0"
    "-DENABLE_PARALLEL_TILE_DECODING=0"
  ];

  buildInputs = [
    libaom
    libde265
    x265
    libpng
    libjpeg
  ];
}

