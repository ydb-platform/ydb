self: super: with self; {
  boost_random = stdenv.mkDerivation rec {
    pname = "boost_random";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "random";
      rev = "boost-${version}";
      hash = "sha256-pdUF6U8hI1+FTmIev9rsLYkQXfTBMfM3g7oKik9Uxxc=";
    };
  };
}
