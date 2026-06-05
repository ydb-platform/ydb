self: super: with self; {
  boost_range = stdenv.mkDerivation rec {
    pname = "boost_range";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "range";
      rev = "boost-${version}";
      hash = "sha256-VF24eBNY4F0emkICSzWCeTtcLAcMdLIJM6U3Yz8h1X4=";
    };
  };
}
