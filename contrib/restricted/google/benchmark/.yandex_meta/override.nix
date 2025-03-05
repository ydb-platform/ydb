pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.9.1";

  src = fetchFromGitHub {
    owner = "google";
    repo = "benchmark";
    rev = "v${version}";
    hash = "sha256-5xDg1duixLoWIuy59WT0r5ZBAvTR6RPP7YrhBYkMxc8=";
  };

  buildInputs = [ gtest ];

  patches = [];

  # Do not copy gtest sources into googletest.
  postPatch = "";
}
