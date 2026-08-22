self: super: with self; {
  boost_rational = stdenv.mkDerivation rec {
    pname = "boost_rational";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "rational";
      rev = "boost-${version}";
      hash = "sha256-oThOq5HStCze/P+aqCc9hcFdL0Rca7h2yhjsZB3vVok=";
    };
  };
}
