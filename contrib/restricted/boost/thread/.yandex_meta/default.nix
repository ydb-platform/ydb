self: super: with self; {
  boost_thread = stdenv.mkDerivation rec {
    pname = "boost_thread";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "thread";
      rev = "boost-${version}";
      hash = "sha256-H9iOI5SHWv893L/opKS9eDQNpzSkwnx4CW51b+ilOGM=";
    };
  };
}
