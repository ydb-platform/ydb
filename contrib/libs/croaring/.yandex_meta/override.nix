pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.3.11";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-6vUZbNw6MHnm65Tb3rFNowdxjjMl+hx/wQOopvXcGjc=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}
