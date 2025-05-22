pkgs: attrs: with pkgs; with attrs; rec {
  pname = "flatbuffers";
  version = "24.3.25";

  src = fetchFromGitHub {
    owner = "google";
    repo = "flatbuffers";
    rev = "v${version}";
    hash = "sha256-uE9CQnhzVgOweYLhWPn2hvzXHyBbFiFVESJ1AEM3BmA=";
  };

  cmakeFlags = [
    "-DFLATBUFFERS_BUILD_FLATHASH=OFF"
    "-DFLATBUFFERS_BUILD_TESTS=OFF"
  ];

  patches = [];
}
