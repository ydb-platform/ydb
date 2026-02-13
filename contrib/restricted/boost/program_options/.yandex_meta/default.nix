self: super: with self; {
  boost_program_options = stdenv.mkDerivation rec {
    pname = "boost_program_options";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "program_options";
      rev = "boost-${version}";
      hash = "sha256-a9b+7CJpuFHtvS3CiPHwGyT4DyX1nJiqamltxnjTYoU=";
    };
  };
}
