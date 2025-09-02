self: super: with self; {
  boost_intrusive = stdenv.mkDerivation rec {
    pname = "boost_intrusive";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "intrusive";
      rev = "boost-${version}";
      hash = "sha256-PmMRJH3nDe3umDPH8gHe3se1xryk8buMqnIO+1VYuzg=";
    };
  };
}
