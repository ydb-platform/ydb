pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "5.0.0";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-DLVIEFXQCmfSkFIRd6s9VbpdsypuyYgaI+ZwmV55YVs=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}
