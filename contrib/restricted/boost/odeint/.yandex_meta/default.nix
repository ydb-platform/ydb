self: super: with self; {
  boost_odeint = stdenv.mkDerivation rec {
    pname = "boost_odeint";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "odeint";
      rev = "boost-${version}";
      hash = "sha256-bidGeOEtd2+AVZ4Si7Zje3LiLvSLQLq29bYbW/NolOk=";
    };
  };
}
