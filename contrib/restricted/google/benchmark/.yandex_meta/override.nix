pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.9.5";

  src = fetchFromGitHub {
    owner = "google";
    repo = "benchmark";
    rev = "v${version}";
    hash = "sha256-Mm4pG7zMB00iof32CxreoNBFnduPZTMp3reHMCIAFPQ=";
  };

  buildInputs = [ gtest ];

  patches = [];

  # Do not copy gtest sources into googletest.
  postPatch = "";
}
