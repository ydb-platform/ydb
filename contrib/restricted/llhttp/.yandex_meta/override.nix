pkgs: attrs: with pkgs; with attrs; rec {
  version = "9.3.0";

  src = fetchFromGitHub {
    owner = "nodejs";
    repo = "llhttp";
    rev = "release/v${version}";
    hash = "sha256-VL58h8sdJIpzMiWNqTvfp8oITjb0b3X/F8ygaE9cH94=";
  };

  patches = [];

  cmakeFlags = [
    "-DBUILD_STATIC_LIBS=OFF"
  ];
}
