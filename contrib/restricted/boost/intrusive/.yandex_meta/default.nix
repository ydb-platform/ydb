self: super: with self; {
  boost_intrusive = stdenv.mkDerivation rec {
    pname = "boost_intrusive";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "intrusive";
      rev = "boost-${version}";
      hash = "sha256-4tXrz1mXBjOaEHJMM1T0T7A3FpGuBV7eSCxVJqSKsTM=";
    };
  };
}
