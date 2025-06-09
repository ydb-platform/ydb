self: super: with self; {
  boost_type_index = stdenv.mkDerivation rec {
    pname = "boost_type_index";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "type_index";
      rev = "boost-${version}";
      hash = "sha256-kn9AVs2/Mled435VB2TaHjgGPIFON9DR2E0kGv1CXfY=";
    };
  };
}
