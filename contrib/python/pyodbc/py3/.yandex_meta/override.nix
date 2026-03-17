pkgs: attrs: with pkgs.python310.pkgs; with pkgs; with attrs; rec {
  pname = "pyodbc";
  version = "5.3.0";

  src = fetchPypi {
    inherit pname version;
    hash = "sha256-L+DgY9j7Zu/QrG3DkjbE3hpF8Xwz6t7Q1VPSHBmfTQU=";
  };
}
