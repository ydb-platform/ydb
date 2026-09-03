self: super: with self; {
  boost_program_options = stdenv.mkDerivation rec {
    pname = "boost_program_options";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "program_options";
      rev = "boost-${version}";
      hash = "sha256-Ef48iBpHll+dlUj1cqbMrFg9+v2NS1tSBeinB36o97Q=";
    };
  };
}
