self: super: with self; {
  boost_iostreams = stdenv.mkDerivation rec {
    pname = "boost_iostreams";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "iostreams";
      rev = "boost-${version}";
      hash = "sha256-06nyC2gTshoIj4DFy0SPdND0FyKmhdYxjlRAdFJ15Ug=";
    };
  };
}
