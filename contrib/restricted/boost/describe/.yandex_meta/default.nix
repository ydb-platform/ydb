self: super: with self; {
  boost_describe = stdenv.mkDerivation rec {
    pname = "boost_describe";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "describe";
      rev = "boost-${version}";
      hash = "sha256-20NRrM3S0JU8NFd4923iFvlOR/lC2nrWNMZqTEvyP+0=";
    };
  };
}
