self: super: with self; {
  boost_type_index = stdenv.mkDerivation rec {
    pname = "boost_type_index";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "type_index";
      rev = "boost-${version}";
      hash = "sha256-3njyTPlLV79AAeFoHXSBVFy98k3DFTZpfnIfnWOI4to=";
    };
  };
}
