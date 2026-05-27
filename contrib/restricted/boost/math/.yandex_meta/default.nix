self: super: with self; {
  boost_math = stdenv.mkDerivation rec {
    pname = "boost_math";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "math";
      rev = "boost-${version}";
      hash = "sha256-SGBw2KLfH1QqSSDkm368XD5w57pmy2lrTl/wKQD7maw=";
    };
  };
}
