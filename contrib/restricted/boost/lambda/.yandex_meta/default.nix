self: super: with self; {
  boost_lambda = stdenv.mkDerivation rec {
    pname = "boost_lambda";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "lambda";
      rev = "boost-${version}";
      hash = "sha256-1mYmY2a+vFCmP9lpWtofgibcXd8So1/2VFrAkq4FYT4=";
    };
  };
}
