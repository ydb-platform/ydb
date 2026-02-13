self: super: with self; {
  boost_math = stdenv.mkDerivation rec {
    pname = "boost_math";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "math";
      rev = "boost-${version}";
      hash = "sha256-gQzZvnZ4qYwniz/2a7g6e8wGJHld2ptf0YHdRQGyO/M=";
    };
  };
}
