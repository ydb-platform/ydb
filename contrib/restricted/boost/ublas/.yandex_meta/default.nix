self: super: with self; {
  boost_ublas = stdenv.mkDerivation rec {
    pname = "boost_ublas";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "ublas";
      rev = "boost-${version}";
      hash = "sha256-6klr9JqynTk/OMoVWZQM0x2EAgXh3+55VjE1Gl1MYI4=";
    };
  };
}
