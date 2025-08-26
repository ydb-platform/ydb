self: super: with self; {
  boost_variant = stdenv.mkDerivation rec {
    pname = "boost_variant";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "variant";
      rev = "boost-${version}";
      hash = "sha256-ttj+UUHWtRaDucaR7XxBbelg4fLzOUpL5qyQdIPBH7E=";
    };
  };
}
