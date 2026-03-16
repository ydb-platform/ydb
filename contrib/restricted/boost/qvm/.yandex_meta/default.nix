self: super: with self; {
  boost_qvm = stdenv.mkDerivation rec {
    pname = "boost_qvm";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "qvm";
      rev = "boost-${version}";
      hash = "sha256-zMIH1Fv8tNM7tnEX9eZjCyzCHdwCcLsdG/+6hatPOP4=";
    };
  };
}
