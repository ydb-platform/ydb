self: super: with self; with pkgs; {
  miniselect = stdenv.mkDerivation rec {
    pname = "miniselect";
    version = "0.4.0";

    src = fetchFromGitHub {
      owner = "danlark1";
      repo = "miniselect";
      rev = "${version}";
      hash = "sha256-dcV70u7ey07/ttfLbaOIOJb4/uSP2rYXDa4RmRC+u6M=";
    };

  };
}
