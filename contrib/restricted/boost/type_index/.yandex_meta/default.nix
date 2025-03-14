self: super: with self; {
  boost_type_index = stdenv.mkDerivation rec {
    pname = "boost_type_index";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "type_index";
      rev = "boost-${version}";
      hash = "sha256-+WBnXkT1cs+Do4L8IfWfqGbdhP/ubxotXUIHH9tL/jU=";
    };
  };
}
