self: super: with self; {
  boost_lockfree = stdenv.mkDerivation rec {
    pname = "boost_lockfree";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "lockfree";
      rev = "boost-${version}";
      hash = "sha256-0CB1iJcZVpZYni0POMO14kZeODf7sFXp74LMb/5RH2c=";
    };
  };
}
