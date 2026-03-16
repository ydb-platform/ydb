self: super: with self; {
  boost_uuid = stdenv.mkDerivation rec {
    pname = "boost_uuid";
    version = "1.85.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "uuid";
      rev = "boost-${version}";
      hash = "sha256-Ki3QnkvnzEqVEYRKWOfppMU48mqoHI5MaQQqD14TKd0=";
    };
  };
}
