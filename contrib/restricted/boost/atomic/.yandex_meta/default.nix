self: super: with self; {
  boost_atomic = stdenv.mkDerivation rec {
    pname = "boost_atomic";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "atomic";
      rev = "boost-${version}";
      hash = "sha256-YD1f1dzQ05+av0PG1btTBOGhnH520WpE8pongfYo1ow=";
    };
  };
}
