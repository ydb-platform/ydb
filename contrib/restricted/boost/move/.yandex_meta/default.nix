self: super: with self; {
  boost_move = stdenv.mkDerivation rec {
    pname = "boost_move";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "move";
      rev = "boost-${version}";
      hash = "sha256-W9RbaJjXHm0qQEVvhH78nzvh+ybpvSigPeTE0xBajWY=";
    };
  };
}
