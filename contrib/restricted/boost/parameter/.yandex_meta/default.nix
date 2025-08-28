self: super: with self; {
  boost_parameter = stdenv.mkDerivation rec {
    pname = "boost_parameter";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "parameter";
      rev = "boost-${version}";
      hash = "sha256-WSExKYojn5swQ1Zxuwggav+LfwC10kwpMdpkeDEpD18=";
    };
  };
}
