self: super: with self; {
  boost_mp11 = stdenv.mkDerivation rec {
    pname = "boost_mp11";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "mp11";
      rev = "boost-${version}";
      hash = "sha256-shcVHaz0Q7oS8nTnlJiVyAeIbse3nAwtQCUTXR6/gww=";
    };
  };
}
