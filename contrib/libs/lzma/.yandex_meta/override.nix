pkgs: attrs: with pkgs; with attrs; rec {
  version = "5.8.3";

  src = fetchFromGitHub {
    owner = "tukaani-project";
    repo = "xz";
    rev = "v${version}";
    hash = "sha256-tdgRR5QCIrR5um0NGIXHY79c8ppCvsL+AA9xlvnj/jE=";
  };

  nativeBuildInputs = [ autoreconfHook ];

  configureFlags = [
    "--build=x86_64-pc-linux-gnu"
  ];
}
