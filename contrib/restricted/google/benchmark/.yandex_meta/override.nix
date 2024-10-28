pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.9.0";

  src = fetchFromGitHub {
    owner = "google";
    repo = "benchmark";
    rev = "v${version}";
    hash = "sha256-5cl1PIjhXaL58kSyWZXRWLq6BITS2BwEovPhwvk2e18=";
  };

  buildInputs = [ gtest ];

  patches = [];

  # Do not copy gtest sources into googletest.
  postPatch = "";
}
