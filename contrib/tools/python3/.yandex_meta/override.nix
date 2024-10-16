pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.12.6";

  src = fetchFromGitHub {
    owner = "python";
    repo = "cpython";
    rev = "v${version}";
    hash = "sha256-RxhBi+QDVZeKwN5uYJDkci2Gay/JELTK3K6xM8U8yo8=";
  };

  patches = [];
  postPatch = "";
}
