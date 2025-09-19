self: super: with self; {
  boost_variant2 = stdenv.mkDerivation rec {
    pname = "boost_variant2";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "variant2";
      rev = "boost-${version}";
      hash = "sha256-CdsNTYYqhZEEoZ/LAPlOxDr9YPG3VQy9Iz4VJ71STl4=";
    };
  };
}
