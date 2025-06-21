pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.3.5";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-MNBlKAosKZ4wV3vbas77rfGOjTlWxTyluQYaFhES3Ro=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}
