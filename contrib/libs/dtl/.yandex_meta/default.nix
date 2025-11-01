self: super: with self; {
  dtl = stdenv.mkDerivation rec {
    pname = "dtl";
    version = "1.21";

    src = fetchFromGitHub {
      owner = "cubicdaiya";
      repo = "dtl";
      rev = "v${version}";
      sha256 = "sha256-s+syRiJhcxvmE0FBcbCi6DrL1hwu+0IJNMgg5Tldsv4=";
    };

  };
}

