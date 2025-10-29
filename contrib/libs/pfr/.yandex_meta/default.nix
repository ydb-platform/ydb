self: super: with self; {
  pfr = stdenv.mkDerivation rec {
    pname = "pfr";
    version = "2.3.2";

    src = fetchFromGitHub {
      owner = "apolukhin";
      repo = "pfr_non_boost";
      rev = "${version}";
      hash = "sha256-Evp15skYDclMZ3IIV7VLwYGwu9BsZMCDKIP0k9cb+6E=";
    };

  };
}
