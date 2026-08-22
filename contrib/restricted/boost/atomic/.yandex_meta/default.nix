self: super: with self; {
  boost_atomic = stdenv.mkDerivation rec {
    pname = "boost_atomic";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "atomic";
      rev = "boost-${version}";
      hash = "sha256-MkmP/I6kaCzrcelmPZytBbEeEz2SEleNVQ0aJ+weBno=";
    };
  };
}
