self: super: with self; {
  boost_program_options = stdenv.mkDerivation rec {
    pname = "boost_program_options";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "program_options";
      rev = "boost-${version}";
      hash = "sha256-wMCxqqR0y36KQAWyUa0LBEtWGGEkXt7o683a3ZPbnH0=";
    };
  };
}
