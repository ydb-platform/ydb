self: super: with self; {
  boost_describe = stdenv.mkDerivation rec {
    pname = "boost_describe";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "describe";
      rev = "boost-${version}";
      hash = "sha256-vUaqUSBPSniCQ1Z7T40EeJQbn1aTgAxvH7fLnr0pkkk=";
    };
  };
}
