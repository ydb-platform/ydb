self: super: with self; {
  yamaker-magic-enum = stdenv.mkDerivation rec {
    name = "magic_enum";
    version = "0.9.7";

    src = fetchFromGitHub {
      owner = "Neargye";
      repo = "magic_enum";
      rev = "v${version}";

      hash = "sha256-P6fl/dcGOSE1lTJwZlimbvsTPelHwdQdZr18H4Zji20=";
    };
  };
}
