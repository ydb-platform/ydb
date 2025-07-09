self: super: with self; {
  boost_chrono = stdenv.mkDerivation rec {
    pname = "boost_chrono";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "chrono";
      rev = "boost-${version}";
      hash = "sha256-AK5kbnidHvzGpUd/VU4RPrsJsbQ2rB8obyJOa4ZrWMc=";
    };
  };
}
