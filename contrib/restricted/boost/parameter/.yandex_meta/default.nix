self: super: with self; {
  boost_parameter = stdenv.mkDerivation rec {
    pname = "boost_parameter";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "parameter";
      rev = "boost-${version}";
      hash = "sha256-ez3LuR24eWReX5Xvs3ysnPWVjvQTM24bbOtEdVN6lYo=";
    };
  };
}
