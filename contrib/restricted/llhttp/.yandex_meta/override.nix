pkgs: attrs: with pkgs; with attrs; rec {
  version = "9.4.2";

  src = fetchFromGitHub {
    owner = "nodejs";
    repo = "llhttp";
    rev = "release/v${version}";
    hash = "sha256-LS8HS8CnXJ3X8WlIvtxBLc0h1wLL/HmTqZWHlvBjTEo=";
  };

  patches = [];

  cmakeFlags = [
    "-DBUILD_STATIC_LIBS=OFF"
  ];
}
