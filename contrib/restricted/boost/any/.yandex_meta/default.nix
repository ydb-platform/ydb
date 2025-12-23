self: super: with self; {
  boost_any = stdenv.mkDerivation rec {
    pname = "boost_any";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "any";
      rev = "boost-${version}";
      hash = "sha256-vXWlfUWiGwLnPIiSFWNycJDvNnYgQ1JGP8Z3C0URXUQ=";
    };
  };
}
