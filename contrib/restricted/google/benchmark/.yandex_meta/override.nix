pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.10.0";

  src = fetchFromGitHub {
    owner = "google";
    repo = "benchmark";
    rev = "v${version}";
    hash = "sha256-yJzrkJxJUtCXmBJK474mu4MHJTT6ZnNnBRNwuCkRHEo=";
  };

  buildInputs = [ gtest ];

  patches = [];

  # Do not copy gtest sources into googletest.
  postPatch = "";
}
