pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.6.1";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-wks7kkF0va7RUJXY74ku/yWTSsHQKlFczfhAHyuNudM=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}
