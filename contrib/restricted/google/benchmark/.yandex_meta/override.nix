pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.9.3";

  src = fetchFromGitHub {
    owner = "google";
    repo = "benchmark";
    rev = "v${version}";
    hash = "sha256-iPK3qLrZL2L08XW1a7SGl7GAt5InQ5nY+Dn8hBuxSOg=";
  };

  buildInputs = [ gtest ];

  patches = [];

  # Do not copy gtest sources into googletest.
  postPatch = "";
}
