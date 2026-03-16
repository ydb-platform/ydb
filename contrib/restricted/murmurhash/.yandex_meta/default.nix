self: super: with self; {
  murmurhash = stdenv.mkDerivation rec {
    pname = "smhasher";
    version = "2024-11-14";

    src = fetchFromGitHub {
      owner = "aappleby";
      repo = "smhasher";
      rev = "0ff96f7835817a27d0487325b6c16033e2992eb5";
      sha256 = "sha256-OgZQwkQcVgRMf62ROGuY+3zQhBoWuUSP4naTmSKdq8s=";
    };

    doCheck = true;
  };
}
