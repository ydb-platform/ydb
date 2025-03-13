self: super: with self; {
  boost_rational = stdenv.mkDerivation rec {
    pname = "boost_rational";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "rational";
      rev = "boost-${version}";
      hash = "sha256-efGJRjsugdIR83K+1Qk6dUBnU1/p/TTF9qgacvM18qk=";
    };
  };
}
